from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sqlite3

import pytest

from gateway import pnc_rca_control_store as control_store_module
from gateway.pnc_rca_admission import build_rca_admission
from gateway.pnc_rca_control_store import (
    ACTIVATION_RELEASE_SLOT_KINDS,
    ActivationEpochError,
    ActivationIngressDeferredError,
    CapacityTransitionStateError,
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
    ShadowPromotionError,
    StaleOutboxLeaseError,
    build_silent_terminal_rerun_authority,
    build_batch_terminal_rerun_authority,
)
from gateway.pnc_rca_kafka_contract import WorkflowEventPolicy, WorkflowTransition
from gateway.pnc_rca_delivery_store import RcaDeliveryStore
from gateway.pnc_rca_runtime_identity import canonical_json_sha256


TOPIC = "feishu-project-workflow-event"
PREAUTHORIZATION_FINGERPRINT = "1" * 64
PREAUTHORIZATION_RECEIPT_SHA256 = "a" * 64
PREAUTHORIZATION_CAPSULE_SHA256 = "b" * 64
PREPRODUCTION_FINGERPRINT = "c" * 64
PREPRODUCTION_RECEIPT_SHA256 = "d" * 64
PREPRODUCTION_CAPSULE_SHA256 = "e" * 64


CAPACITY_BOOTSTRAP_NOW = datetime(2026, 7, 20, 0, 0, tzinfo=timezone.utc)


def _capacity_steady_kwargs(**overrides):
    now = datetime(2026, 7, 20, 1, 0, tzinfo=timezone.utc)
    values = {
        "expected_generation": 1,
        "release_id": "release-20260713",
        "bootstrap_epoch_id": "rca-bootstrap-release-20260713",
        "final_ledger_sha256": "1" * 64,
        "transition_authorization_sha256": "2" * 64,
        "transition_authorization_fingerprint": "3" * 64,
        "transition_receipt_sha256": "4" * 64,
        "transition_receipt_fingerprint": "5" * 64,
        "commit_marker_sha256": "6" * 64,
        "commit_marker_fingerprint": "7" * 64,
        "evidence_bundle_sha256": "8" * 64,
        "evidence_bundle_fingerprint": "9" * 64,
        "authorization_issued_at": (now - timedelta(minutes=3)).isoformat(),
        "authorization_expires_at": (now + timedelta(minutes=10)).isoformat(),
        "receipt_created_at": (now - timedelta(minutes=2)).isoformat(),
        "marker_committed_at": (now - timedelta(minutes=1)).isoformat(),
        "now": now,
    }
    values.update(overrides)
    return values


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
    from tests.gateway.test_pnc_rca_delivery_store import _bind_activation_execution
    from types import SimpleNamespace

    clock = [delivery_now]
    collector = _real_terminal_collector(
        tmp_path,
        clock=clock,
        status_reader=lambda task_id: {
            "success": True,
            "task_id": task_id,
            "state": "running",
        },
    )
    control = RcaControlStore(collector.store.db_path)
    [trigger] = control.list_rows("business_triggers")
    _bind_activation_execution(
        control,
        SimpleNamespace(
            business_key=trigger["business_key"],
            generation=trigger["generation"],
            submission_key=trigger["submission_key"],
        ),
    )
    collector.config = replace(collector.config, activation_required=True)
    assert collector.collect_one().status == "running"
    clock[0] = delivery_now + timedelta(seconds=1800)
    terminal = collector.collect_one()
    assert terminal.status == "terminal_failed"
    assert terminal.error_code == "rca_work_deadline_exceeded"
    return RcaControlStore(collector.store.db_path), clock[0]


def _rewrite_silent_terminal_error_code(
    store: RcaControlStore, error_code: str
) -> None:
    delivery = RcaDeliveryStore(store.db_path)
    [watch] = delivery.list_rows("rca_execution_watch")
    [route] = delivery.list_rows("rca_failure_routes")
    status = json.loads(watch["last_status_json"])
    taxonomy = status["failure_taxonomy"]
    taxonomy["raw_code"] = error_code
    taxonomy["terminal_error_code"] = error_code
    route_payload = json.loads(route["route_payload_json"])
    route_payload["blocker"]["kind"] = error_code
    route_payload["decision"]["raw_code"] = error_code
    route_payload["decision"]["terminal_error_code"] = error_code
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
    ],
)
def test_operator_silent_terminal_rerun_creates_new_generation_without_old_mutation(
    tmp_path, error_code,
):
    store, terminal_at = _silent_deadline_terminal_store(tmp_path)
    if error_code != "rca_work_deadline_exceeded":
        _rewrite_silent_terminal_error_code(store, error_code)
    request = _silent_batch_request()
    authority = _silent_batch_authority(store, request)
    delivery = RcaDeliveryStore(store.db_path)
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
    delivery = RcaDeliveryStore(store.db_path)
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


def test_operator_silent_terminal_rerun_rejects_materialized_old_effect(tmp_path):
    store, terminal_at = _silent_deadline_terminal_store(tmp_path)
    request = _silent_batch_request()
    authority = _silent_batch_authority(store, request)
    [watch] = RcaDeliveryStore(store.db_path).list_rows("rca_execution_watch")
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


def test_batch_terminal_authority_creates_refresh_generation_for_settled_delivery(
    tmp_path, monkeypatch
):
    from types import SimpleNamespace

    from tests.gateway.test_pnc_rca_delivery_store import _bind_activation_execution

    store = RcaControlStore(tmp_path / "control.sqlite3")
    stock_issue_id = "7055722720"
    original_utc_datetime = control_store_module._utc_datetime
    pre_cutoff = datetime(2026, 7, 24, tzinfo=timezone.utc)
    monkeypatch.setattr(
        control_store_module,
        "_utc_datetime",
        lambda value=None: (
            pre_cutoff if value is None else original_utc_datetime(value)
        ),
    )
    first = store.ingest_record(
        _record(value=_value(work_item_id=int(stock_issue_id))),
        policy=_policy(),
        submit_enabled=True,
    )
    monkeypatch.setattr(
        control_store_module, "_utc_datetime", original_utc_datetime
    )
    RcaDeliveryStore(store.db_path)
    stock_target = f"feishu_project:project-key:problem-type:{stock_issue_id}"
    with sqlite3.connect(store.db_path) as conn:
        conn.execute("PRAGMA foreign_keys=OFF")
        conn.execute(
            """
            INSERT INTO rca_delivery_jobs(
                delivery_id, submission_key, business_key, generation,
                artifact_set_id, project_key, work_item_type_key, work_item_id,
                target_key, issue_url, report_url, status, manifest_json,
                contract_json, artifacts_json, created_at, updated_at
            ) VALUES('stock-seed-delivery', 'stock-seed-submission',
                     'stock-seed-business', 1, 'stock-seed-artifact',
                     'project-key', 'problem-type', ?, ?, ?, '', 'delivered',
                     '{}', '{}', '[]', ?, ?)
            """,
            (
                stock_issue_id,
                stock_target,
                f"https://project.feishu.cn/g1q3/issue/detail/{stock_issue_id}",
                "2026-07-24T00:00:00+00:00",
                "2026-07-24T00:00:00+00:00",
            ),
        )
        conn.execute(
            """
            INSERT INTO rca_delivery_effects(
                effect_key, delivery_id, effect_kind, required, target_key,
                payload_json, payload_sha256, status, created_at, updated_at,
                completed_at
            ) VALUES('stock-seed-effect', 'stock-seed-delivery',
                     'feishu_issue_comment', 1, ?,
                     '{"schema_version":"pnc_rca_delivery_effect_v1"}', ?,
                     'succeeded', ?, ?, ?)
            """,
            (
                stock_target,
                "a" * 64,
                "2026-07-24T00:00:00+00:00",
                "2026-07-24T00:00:00+00:00",
                "2026-07-24T00:00:00+00:00",
            ),
        )
    stock_now = datetime(2026, 7, 26, tzinfo=timezone.utc)
    cohort = store.seal_learning_lane_cohort(now=stock_now)
    assert cohort["stock_count"] == 1
    assert store.list_rows("rca_learning_lane_stock_items")[0][
        "work_item_id"
    ] == stock_issue_id
    _terminalize_permanent(store, first.submission_key)
    _bind_activation_execution(
        store,
        SimpleNamespace(
            business_key=first.business_key,
            generation=first.generation,
            submission_key=first.submission_key,
        ),
        start_offset=11,
    )
    with sqlite3.connect(store.db_path) as conn:
        delivery_id = conn.execute(
            "SELECT delivery_id FROM rca_execution_watch WHERE submission_key=?",
            (first.submission_key,),
        ).fetchone()[0]
        conn.execute(
            "UPDATE rca_delivery_jobs "
            "SET status='delivered', outcome='success', terminal_error_code='' "
            "WHERE delivery_id=?",
            (str(delivery_id),),
        )
    batch_id = "batch-delivery"
    request = replace(
        _operator_request(
            "batch-delivery-try-1",
            issue_url=(
                f"https://project.feishu.cn/g1q3/issue/detail/{stock_issue_id}"
            ),
        ),
        reason=f"production_gray_batch:{batch_id}",
    )
    authority = build_batch_terminal_rerun_authority(
        batch_id=batch_id,
        queue_sha256="1" * 64,
        issue_id=stock_issue_id,
        prior_submission_key=first.submission_key,
        prior_generation=1,
        prior_delivery_id=str(delivery_id),
        owner_receipt_path=str(tmp_path / "owner.json"),
        owner_receipt_sha256="2" * 64,
        requester_id=request.requester_id,
        reason=request.reason,
    )

    def unexpected_learning_lane_admission(cls, conn, *, admission, current):
        raise AssertionError("terminal correction must stay on the delivery lane")

    monkeypatch.setattr(
        RcaControlStore,
        "_ensure_learning_lane_admission_tx",
        classmethod(unexpected_learning_lane_admission),
    )
    rerun = store.admit_manual_trigger(
        request,
        allowed_chat_ids=set(),
        submit_enabled=True,
        operator_authorized=True,
        batch_terminal_rerun_authority=authority,
        activation_required=True,
    )
    assert rerun.outcome == "created"
    assert rerun.generation == 2
    assert len(store.list_rows("business_triggers")) == 2
    assert [
        row["effect_kind"]
        for row in store.list_rows("rca_delivery_subscriptions")
        if row["generation"] == 2
    ] == ["feishu_issue_comment"]
    assert [
        row
        for row in store.list_rows("rca_learning_lane_admissions")
        if row["generation"] == 2
    ] == []
    [persisted_authority] = store.list_rows(
        "rca_terminal_rerun_delivery_authorities"
    )
    assert persisted_authority["authority_sha256"] == authority["selection_sha256"]
    assert persisted_authority["authority_kind"] == "batch_terminal"
    assert persisted_authority["submission_key"] == rerun.submission_key
    assert persisted_authority["outbox_id"] > 0
    assert persisted_authority["activation_epoch_id"]
    assert persisted_authority["activation_ledger_id"] > 0
    authority_conn = store._connect()
    try:
        with pytest.raises(
            sqlite3.IntegrityError,
            match="terminal_rerun_delivery_authority_update_forbidden",
        ):
            authority_conn.execute(
                "UPDATE rca_terminal_rerun_delivery_authorities "
                "SET created_at=created_at"
            )
        with pytest.raises(
            sqlite3.IntegrityError,
            match="terminal_rerun_delivery_authority_delete_forbidden",
        ):
            authority_conn.execute(
                "DELETE FROM rca_terminal_rerun_delivery_authorities"
            )
        with pytest.raises(
            sqlite3.IntegrityError,
            match="terminal_rerun_delivery_authority_replace_forbidden",
        ):
            authority_conn.execute(
                "INSERT OR REPLACE INTO rca_terminal_rerun_delivery_authorities "
                "SELECT * FROM rca_terminal_rerun_delivery_authorities"
            )
    finally:
        authority_conn.close()
    generation = next(
        row
        for row in store.list_rows("business_triggers")
        if row["generation"] == 2
    )
    correction_target = (
        f"feishu_project:{generation['project_key']}:"
        f"{generation['work_item_type_key']}:{generation['work_item_id']}"
    )
    correction_delivery_id = "terminal-rerun-delivery"
    terminal_payload = {
        "schema_version": "pnc_rca_terminal_delivery_effect_v3",
        "delivery_id": correction_delivery_id,
        "effect_kind": "feishu_issue_comment",
        "target_key": correction_target,
        "project_key": generation["project_key"],
        "work_item_type_key": generation["work_item_type_key"],
        "work_item_id": generation["work_item_id"],
        "outcome": "terminal_failed",
        "terminal_state": "terminal_failed",
        "error_code": "analysis_failed",
        "submission_key": rerun.submission_key,
        "generation": 2,
    }
    delivery = RcaDeliveryStore(store.db_path)
    conn = delivery._connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            """
            INSERT INTO rca_execution_watch(
                submission_key, submission_outbox_id, business_key, generation,
                project_key, work_item_type_key, work_item_id, state,
                next_poll_at, delivery_id, created_at, updated_at
            ) VALUES(?, ?, ?, 2, ?, ?, ?, 'delivery_created', ?, ?, ?, ?)
            """,
            (
                rerun.submission_key,
                persisted_authority["outbox_id"],
                rerun.business_key,
                generation["project_key"],
                generation["work_item_type_key"],
                generation["work_item_id"],
                stock_now.isoformat(),
                correction_delivery_id,
                stock_now.isoformat(),
                stock_now.isoformat(),
            ),
        )
        conn.execute(
            """
            INSERT INTO rca_delivery_jobs(
                delivery_id, submission_key, business_key, generation,
                artifact_set_id, project_key, work_item_type_key, work_item_id,
                target_key, issue_url, report_url, status, manifest_json,
                contract_json, artifacts_json, created_at, updated_at
            ) VALUES (?, ?, ?, 2, 'terminal-rerun-artifact', ?, ?, ?, ?, ?, '',
                      'ready', '{}', '{}', '[]', ?, ?)
            """,
            (
                correction_delivery_id,
                rerun.submission_key,
                rerun.business_key,
                generation["project_key"],
                generation["work_item_type_key"],
                generation["work_item_id"],
                correction_target,
                request.issue_url,
                stock_now.isoformat(),
                stock_now.isoformat(),
            ),
        )
        forged_payload = {**terminal_payload, "target_key": "forged-target"}
        with pytest.raises(sqlite3.IntegrityError, match="learning_lane_admission_missing"):
            conn.execute(
                """
                INSERT INTO rca_delivery_effects(
                    effect_key, delivery_id, effect_kind, required, target_key,
                    payload_json, payload_sha256, status, created_at, updated_at
                ) VALUES('forged-terminal-rerun-effect', ?,
                         'feishu_issue_comment', 1, 'forged-target', ?, ?,
                         'pending', ?, ?)
                """,
                (
                    correction_delivery_id,
                    json.dumps(forged_payload, sort_keys=True, separators=(",", ":")),
                    canonical_json_sha256(forged_payload),
                    stock_now.isoformat(),
                    stock_now.isoformat(),
                ),
            )
        slot = delivery.enforce_issue_comment_budget_tx(
            conn,
            delivery_id=correction_delivery_id,
            business_key=rerun.business_key,
            generation=2,
            target_key=correction_target,
            payload=terminal_payload,
        )
        conn.execute(
            """
            INSERT INTO rca_delivery_effects(
                effect_key, delivery_id, effect_kind, required, target_key,
                payload_json, payload_sha256, status,
                comment_slot_schema_version, comment_slot_key,
                comment_slot_kind, comment_slot_generation,
                comment_slot_revision, comment_slot_budget_exempt,
                created_at, updated_at
            ) VALUES('terminal-rerun-effect', ?, 'feishu_issue_comment', 1,
                     ?, ?, ?, 'pending', ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                correction_delivery_id,
                correction_target,
                json.dumps(terminal_payload, sort_keys=True, separators=(",", ":")),
                canonical_json_sha256(terminal_payload),
                slot["comment_slot_schema_version"],
                slot["comment_slot_key"],
                slot["comment_slot_kind"],
                slot["comment_slot_generation"],
                slot["comment_slot_revision"],
                slot["comment_slot_budget_exempt"],
                stock_now.isoformat(),
                stock_now.isoformat(),
            ),
        )
        with pytest.raises(
            sqlite3.IntegrityError, match="learning_lane_admission_missing"
        ):
            conn.execute(
                """
                INSERT INTO rca_delivery_subscriptions(
                    subscription_key, business_key, generation, source_id,
                    effect_kind, target_key, target_json, required, status,
                    reason, created_at, updated_at
                ) VALUES('terminal-rerun-thread', ?, 2, NULL,
                         'feishu_thread_reply', 'feishu_thread:forged', '{}', 1,
                         'pending', 'awaiting_delivery_materialization', ?, ?)
                """,
                (
                    rerun.business_key,
                    stock_now.isoformat(),
                    stock_now.isoformat(),
                ),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    delivery.validate_learning_lane_external_operation(
        business_key=rerun.business_key,
        generation=2,
        operation="feishu_issue_comment",
    )
    with pytest.raises(RuntimeError, match="learning_lane_external_effect_forbidden"):
        delivery.validate_learning_lane_external_operation(
            business_key=rerun.business_key,
            generation=2,
            operation="feishu_issue_field_update",
        )
    claimed = delivery.claim_due_effect(
        lease_owner="terminal-rerun-provider-test",
        lease_seconds=300,
        max_age_seconds=10**8,
        activation_required=True,
    )
    assert claimed is not None
    assert claimed.effect_key == "terminal-rerun-effect"
    assert delivery.mark_effect_write_started(
        claim=claimed,
        activation_required=True,
    ) is None
    live_provider_binding = delivery.validate_terminal_rerun_external_write_binding(
        effect_key=claimed.effect_key,
        delivery_id=claimed.delivery_id,
        lease_token=claimed.lease_token,
        lease_fence=claimed.fence,
        operation="feishu_issue_comment",
        issue_url=claimed.issue_url,
        target_key=claimed.target_key,
        business_key=claimed.business_key,
        submission_key=claimed.submission_key,
        generation=claimed.generation,
        require_write_started=True,
    )
    field_update_binding = delivery.validate_terminal_rerun_external_write_binding(
        effect_key=claimed.effect_key,
        delivery_id=claimed.delivery_id,
        lease_token=claimed.lease_token,
        lease_fence=claimed.fence,
        operation="feishu_issue_field_update",
        issue_url=claimed.issue_url,
        target_key=claimed.target_key,
        business_key=claimed.business_key,
        submission_key=claimed.submission_key,
        generation=claimed.generation,
        require_write_started=True,
    )
    assert field_update_binding["operation"] == "feishu_issue_comment"
    from gateway import pnc_rca_provider_fence as provider_fence

    provider_claim = provider_fence.build_terminal_rerun_provider_claim(
        authority_sha256=live_provider_binding["authority_sha256"],
        outbox_id=live_provider_binding["outbox_id"],
        epoch_id=live_provider_binding["epoch_id"],
        activation_ledger_id=live_provider_binding["activation_ledger_id"],
        effect_key=live_provider_binding["effect_key"],
        delivery_id=live_provider_binding["delivery_id"],
        lease_token=live_provider_binding["lease_token"],
        lease_fence=live_provider_binding["lease_fence"],
        issue_target=live_provider_binding["issue_url"],
        target_key=live_provider_binding["target_key"],
        business_key=live_provider_binding["business_key"],
        submission_key=live_provider_binding["submission_key"],
        generation=live_provider_binding["generation"],
        project_key=live_provider_binding["project_key"],
        project_simple_name=live_provider_binding["project_simple_name"],
        work_item_type_key=live_provider_binding["work_item_type_key"],
        work_item_id=live_provider_binding["work_item_id"],
    )
    monkeypatch.setattr(provider_fence, "_canonical_store", lambda: store)
    assert provider_fence.revalidate_provider_write_claim(
        provider_claim,
        operation="feishu_issue_comment",
        issue_project_key=generation["project_key"],
        issue_work_item_id=stock_issue_id,
    )["authority_kind"] == "terminal_rerun"
    assert provider_fence.revalidate_provider_write_claim(
        provider_claim,
        operation="feishu_issue_field_update",
        issue_project_key=generation["project_key"],
        issue_work_item_id=stock_issue_id,
    )["authority_kind"] == "terminal_rerun"
    assert provider_claim.payload()["authority"]["operation"] == (
        "feishu_issue_comment"
    )
    for denied_operation in ("feishu_thread_reply", "feishu_card_patch"):
        with pytest.raises(
            provider_fence.ExternalWriteFenceError,
            match="external_write_fence_operation_denied",
        ):
            provider_fence.revalidate_provider_write_claim(
                provider_claim,
                operation=denied_operation,
                issue_project_key=generation["project_key"],
                issue_work_item_id=stock_issue_id,
            )
    no_authority_store = RcaControlStore(tmp_path / "no-authority.sqlite3")
    RcaDeliveryStore(no_authority_store.db_path)
    monkeypatch.setattr(
        provider_fence,
        "_canonical_store",
        lambda: no_authority_store,
    )
    with pytest.raises(
        provider_fence.ExternalWriteFenceError,
        match="external_write_fence_identity_mismatch",
    ):
        provider_fence.revalidate_provider_write_claim(
            provider_claim,
            operation="feishu_issue_field_update",
            issue_project_key=generation["project_key"],
            issue_work_item_id=stock_issue_id,
        )
    monkeypatch.setattr(provider_fence, "_canonical_store", lambda: store)
    with pytest.raises(
        provider_fence.ExternalWriteFenceError,
        match="external_write_fence_target_mismatch",
    ):
        provider_fence.revalidate_provider_write_claim(
            provider_claim,
            operation="feishu_issue_comment",
            issue_project_key="g1q3",
            issue_work_item_id=stock_issue_id,
        )
    assert [
        row
        for row in store.list_rows("rca_trigger_delivery_bindings")
        if row["source_id"] == rerun.source_id
    ]
    [audit] = [
        row
        for row in store.list_rows("rca_shadow_promotion_audit")
        if row["outcome"] == "batch_terminal_rerun_new_generation_created"
    ]
    assert json.loads(audit["detail"]) == authority

    with sqlite3.connect(store.db_path) as conn:
        conn.execute(
            "UPDATE rca_trigger_sources SET outcome='joined' WHERE source_id=?",
            (rerun.source_id,),
        )
    with pytest.raises(RuntimeError, match="learning_lane_admission_missing"):
        delivery.validate_learning_lane_external_operation(
            business_key=rerun.business_key,
            generation=2,
            operation="feishu_issue_comment",
        )
    with sqlite3.connect(store.db_path) as conn:
        conn.execute(
            "UPDATE rca_trigger_sources SET outcome='created' WHERE source_id=?",
            (rerun.source_id,),
        )
        conn.execute(
            "UPDATE rca_outbox SET action='tampered_action' WHERE outbox_id=?",
            (persisted_authority["outbox_id"],),
        )
    with pytest.raises(RuntimeError, match="learning_lane_admission_missing"):
        delivery.validate_learning_lane_external_operation(
            business_key=rerun.business_key,
            generation=2,
            operation="feishu_issue_comment",
        )
    with sqlite3.connect(store.db_path) as conn:
        conn.execute(
            "UPDATE rca_outbox SET action='submit_rca_issue_intake' "
            "WHERE outbox_id=?",
            (persisted_authority["outbox_id"],),
        )
    active_epoch = store.activation_epoch()
    assert active_epoch is not None
    with sqlite3.connect(store.db_path) as conn:
        conn.execute(
            "UPDATE rca_activation_epochs SET is_current=0 WHERE epoch_id=?",
            (active_epoch["epoch_id"],),
        )
    with pytest.raises(RuntimeError, match="learning_lane_admission_missing"):
        delivery.validate_learning_lane_external_operation(
            business_key=rerun.business_key,
            generation=2,
            operation="feishu_issue_comment",
        )


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
    store = RcaControlStore(tmp_path / "control.sqlite3")
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
    store = RcaControlStore(tmp_path / "control.sqlite3")
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
    store = RcaControlStore(tmp_path / "control.sqlite3")
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
    store = RcaControlStore(tmp_path / "control.sqlite3")
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
    store = RcaControlStore(tmp_path / "control.sqlite3")

    with pytest.raises(
        ManualRcaAdmissionError,
        match="manual_feishu_requester_identity_invalid",
    ):
        store.admit_manual_trigger(
            _manual_request(
                "om_machine_user_rerun",
                mode="rerun",
                requester_id="automation:gray-sample",
            ),
            allowed_chat_ids={"oc_allowed"},
            submit_enabled=True,
            user_rerun_authority=_group_user_rerun_authority(),
        )

    assert store.list_rows("rca_trigger_sources") == []


def _create_activation_epoch(
    store: RcaControlStore,
    *,
    epoch_id: str = "rca-release-20260712",
    start_offset: int = 20,
    preauthorize: bool = True,
):
    created = store.create_activation_epoch(
        epoch_id=epoch_id,
        preauthorization_fingerprint=PREAUTHORIZATION_FINGERPRINT,
        preauthorization_gate_receipt_sha256=PREAUTHORIZATION_RECEIPT_SHA256,
        preauthorization_capsule_sha256=PREAUTHORIZATION_CAPSULE_SHA256,
        config_sha256="2" * 64,
        db_logical_identity={
            "device": 7,
            "inode": 11,
            "logical_store_id": "rca-control-primary",
        },
        partition_start_fence={TOPIC: {"2": start_offset}},
        operator="release-test",
        reason="focused activation epoch test",
    )
    if not preauthorize:
        return created
    return store.preauthorize_activation_epoch(
        epoch_id=epoch_id,
        preproduction_fingerprint=PREPRODUCTION_FINGERPRINT,
        preproduction_gate_receipt_sha256=PREPRODUCTION_RECEIPT_SHA256,
        preproduction_capsule_sha256=PREPRODUCTION_CAPSULE_SHA256,
        expected_preauthorization_fingerprint=PREAUTHORIZATION_FINGERPRINT,
        expected_preauthorization_gate_receipt_sha256=(
            PREAUTHORIZATION_RECEIPT_SHA256
        ),
        expected_preauthorization_capsule_sha256=PREAUTHORIZATION_CAPSULE_SHA256,
        expected_config_sha256=created["config_sha256"],
        expected_db_logical_identity_sha256=created[
            "db_logical_identity_sha256"
        ],
        expected_partition_start_fence_sha256=created[
            "partition_start_fence_sha256"
        ],
        operator="release-test",
        reason="bind exact preproduction capsule for focused tests",
    )


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
        release_fingerprint=release_fingerprint,
        release_binding_sha256=receipt_sha256,
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


def _confirmation_preconditions(
    store: RcaControlStore,
    partition_end_fence,
    *,
    release_binding_sha256: str | None = None,
):
    epoch = store.activation_epoch()
    assert epoch is not None
    release_binding = release_binding_sha256 or (
        store.activation_release_binding_sha256(
            epoch_id=epoch["epoch_id"],
            partition_end_fence=partition_end_fence,
        )
    )
    return {
        "expected_config_sha256": epoch["config_sha256"],
        "expected_db_logical_identity_sha256": epoch[
            "db_logical_identity_sha256"
        ],
        "expected_partition_start_fence_sha256": epoch[
            "partition_start_fence_sha256"
        ],
        "expected_release_binding_sha256": release_binding,
    }


def _authorize_activation_slots(
    store: RcaControlStore,
    *,
    epoch_id: str = "rca-release-20260712",
    kafka_offset: int = 20,
):
    identities = {
        "kafka_success": (
            "kafka",
            {"event_uid": f"{TOPIC}:2:{kafka_offset}"},
        ),
        "manual_success": (
            "manual",
            _manual_activation_identity(
                "om_manual_success",
                issue_url="https://project.feishu.cn/g1q3/issue/detail/7041712814",
            ),
        ),
        "manual_terminal_failure": (
            "manual",
            _manual_activation_identity(
                "om_manual_terminal",
                mode="debug",
                issue_url="https://project.feishu.cn/g1q3/issue/detail/7041712815",
            ),
        ),
    }
    for slot_kind in ACTIVATION_RELEASE_SLOT_KINDS:
        source_kind, source_identity = identities[slot_kind]
        store.authorize_activation_slot(
            epoch_id=epoch_id,
            slot_kind=slot_kind,
            source_kind=source_kind,
            source_identity=source_identity,
            operator="release-test",
            reason=f"authorize exact {slot_kind} test canary",
        )
    return identities


def _begin_bounded_activation(
    store: RcaControlStore,
    *,
    epoch_id: str = "rca-release-20260712",
    kafka_offset: int = 20,
):
    _create_activation_epoch(
        store,
        epoch_id=epoch_id,
        start_offset=kafka_offset,
    )
    identities = _authorize_activation_slots(
        store,
        epoch_id=epoch_id,
        kafka_offset=kafka_offset,
    )
    store.transition_activation_epoch(
        epoch_id=epoch_id,
        expected_state="preauthorized",
        target_state="bounded_active",
        operator="release-test",
        reason="begin exact two-canary bounded activation",
    )
    return identities


def _adjudicate_activation(
    store: RcaControlStore,
    *,
    entrypoint: str,
    source_kind: str,
    source_identity,
    business_key: str,
    submission_key: str,
    generation: int,
    new_execution: bool,
    requested_slot_kind: str = "",
    activation_required: bool = True,
):
    conn = store._connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        result = store.adjudicate_activation_tx(
            conn,
            entrypoint=entrypoint,
            source_kind=source_kind,
            source_identity=source_identity,
            business_key=business_key,
            submission_key=submission_key,
            generation=generation,
            new_execution=new_execution,
            requested_slot_kind=requested_slot_kind,
            activation_required=activation_required,
        )
        conn.commit()
        return result
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def test_kafka_runtime_transition_is_atomic_and_duplicate_preserves_first_identity(
    tmp_path,
):
    store = RcaControlStore(tmp_path / "control.sqlite3")
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
    store = RcaControlStore(tmp_path / "control.sqlite3")
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
    conn = store._connect()
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
    conn = store._connect()
    try:
        conn.execute("DROP TABLE rca_activation_transition_audit")
        conn.execute("DROP TABLE rca_activation_admission_ledger")
        conn.execute("DROP TABLE rca_activation_budget_slots")
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
        "activation_slot_kind",
        "activation_source_identity_sha256",
    }.issubset(inbox_columns)
    assert {"activation_epoch_id", "activation_ledger_id"}.issubset(
        outbox_columns
    )


def test_activation_creation_is_safe_off_and_binds_exact_preauthorization(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")

    first = _create_activation_epoch(store, preauthorize=False)
    second = _create_activation_epoch(store, preauthorize=False)

    assert first == second
    assert first["state"] == "safe_off"
    assert first["preauthorization_fingerprint"] == PREAUTHORIZATION_FINGERPRINT
    assert (
        first["preauthorization_gate_receipt_sha256"]
        == PREAUTHORIZATION_RECEIPT_SHA256
    )
    assert (
        first["preauthorization_capsule_sha256"]
        == PREAUTHORIZATION_CAPSULE_SHA256
    )
    assert first["preproduction_fingerprint"] == ""
    assert first["preproduction_gate_receipt_sha256"] == ""
    assert first["preproduction_capsule_sha256"] == ""


def test_safe_off_rejects_slot_authorization_and_generic_preauthorization(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    _create_activation_epoch(store, preauthorize=False)

    with pytest.raises(
        ActivationEpochError, match="activation_slot_authorization_closed"
    ):
        store.authorize_activation_slot(
            epoch_id="rca-release-20260712",
            slot_kind="kafka_success",
            source_kind="kafka",
            source_identity={"event_uid": f"{TOPIC}:2:20"},
            operator="release-test",
            reason="safe-off must not authorize a canary",
        )
    with pytest.raises(
        ActivationEpochError, match="activation_preproduction_capsule_required"
    ):
        store.transition_activation_epoch(
            epoch_id="rca-release-20260712",
            target_state="preauthorized",
            operator="release-test",
            reason="generic transition must not bypass evidence",
        )


def test_preproduction_capsule_transition_is_atomic_idempotent_and_exact(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    safe_off = _create_activation_epoch(store, preauthorize=False)
    kwargs = {
        "epoch_id": "rca-release-20260712",
        "preproduction_fingerprint": PREPRODUCTION_FINGERPRINT,
        "preproduction_gate_receipt_sha256": PREPRODUCTION_RECEIPT_SHA256,
        "preproduction_capsule_sha256": PREPRODUCTION_CAPSULE_SHA256,
        "expected_preauthorization_fingerprint": PREAUTHORIZATION_FINGERPRINT,
        "expected_preauthorization_gate_receipt_sha256": (
            PREAUTHORIZATION_RECEIPT_SHA256
        ),
        "expected_preauthorization_capsule_sha256": (
            PREAUTHORIZATION_CAPSULE_SHA256
        ),
        "expected_config_sha256": safe_off["config_sha256"],
        "expected_db_logical_identity_sha256": safe_off[
            "db_logical_identity_sha256"
        ],
        "expected_partition_start_fence_sha256": safe_off[
            "partition_start_fence_sha256"
        ],
        "operator": "release-test",
        "reason": "consume exact preproduction capsule",
    }

    first = store.preauthorize_activation_epoch(**kwargs)
    second = store.preauthorize_activation_epoch(**kwargs)

    assert first == second
    assert first["state"] == "preauthorized"
    assert first["preproduction_fingerprint"] == PREPRODUCTION_FINGERPRINT
    assert (
        first["preproduction_gate_receipt_sha256"]
        == PREPRODUCTION_RECEIPT_SHA256
    )
    assert first["preproduction_capsule_sha256"] == PREPRODUCTION_CAPSULE_SHA256
    empty_hold = store.activation_historical_outbox_hold_evidence(
        epoch_id=first["epoch_id"]
    )
    assert empty_hold["sealed_count"] == empty_hold["current_count"] == 0
    assert empty_hold["sealed_sha256"] == empty_hold["current_sha256"]
    assert empty_hold["matches"] is True
    restarted = RcaControlStore(store.db_path, require_current=True)
    assert (
        restarted.activation_historical_outbox_hold_evidence(epoch_id=first["epoch_id"])
        == empty_hold
    )
    with pytest.raises(
        ActivationEpochError, match="activation_preproduction_binding_conflict"
    ):
        store.preauthorize_activation_epoch(
            **{**kwargs, "preproduction_capsule_sha256": "f" * 64}
        )
    with pytest.raises(
        ActivationEpochError,
        match="activation_preproduction_epoch_binding_changed",
    ):
        store.preauthorize_activation_epoch(
            **{**kwargs, "expected_config_sha256": "9" * 64}
        )


def test_activation_state_machine_requires_exact_receipts_and_is_audited(
    tmp_path, monkeypatch
):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    store.ingest_record(
        _record(offset=10, value=_value(work_item_id=7041712800)),
        policy=_policy(),
        submit_enabled=True,
    )
    _create_activation_epoch(store)

    with pytest.raises(
        ActivationEpochError, match="activation_slots_not_preauthorized"
    ):
        store.transition_activation_epoch(
            epoch_id="rca-release-20260712",
            target_state="bounded_active",
            operator="release-test",
            reason="must fail before exact identities exist",
        )

    identities = _authorize_activation_slots(store)
    store.transition_activation_epoch(
        epoch_id="rca-release-20260712",
        expected_state="preauthorized",
        target_state="bounded_active",
        operator="release-test",
        reason="open bounded two-slot activation",
    )
    manual_success = identities["manual_success"][1]
    success = store.admit_manual_trigger(
        _manual_request(
            manual_success["message_id"],
            issue_url=manual_success["issue_url"],
            thread_id=manual_success["thread_id"],
        ),
        allowed_chat_ids={"oc_allowed"},
        submit_enabled=True,
        active_policy=_policy(),
        activation_required=True,
    )
    manual_terminal = identities["manual_terminal_failure"][1]
    terminal = store.admit_manual_trigger(
        _manual_request(
            manual_terminal["message_id"],
            mode=manual_terminal["mode"],
            issue_url=manual_terminal["issue_url"],
            thread_id=manual_terminal["thread_id"],
        ),
        allowed_chat_ids={"oc_allowed"},
        submit_enabled=True,
        operator_authorized=True,
        active_policy=_policy(),
        activation_required=True,
    )
    assert {success.submission_key, terminal.submission_key} == {
        row["submission_key"]
        for row in store.list_rows("rca_outbox")
        if row["activation_epoch_id"] == "rca-release-20260712"
    }

    claim = store.claim_outbox(
        lease_owner="activation-success-canary",
        activation_required=True,
    )
    assert claim is not None and claim.submission_key == success.submission_key
    completed = store.complete_outbox(
        outbox_id=claim.outbox_id,
        lease_token=claim.lease_token,
        result={"outcome": "canary_evidence_recorded"},
    )
    assert completed.status == "completed"
    _terminalize_permanent(store, terminal.submission_key)
    assert store.claim_outbox(
        lease_owner="activation-canary-empty", activation_required=True
    ) is None
    freeze = store.health()["activation"]["ingress_freeze_readiness"]
    assert store.activation_ingress_freeze_readiness() == freeze
    assert freeze == {
        "epoch_id": "rca-release-20260712",
        "state": "bounded_active",
        "ready": True,
        "reason": "ready",
        "required_slot_count": 2,
        "consumed_slot_count": 2,
        "completed_bound_slot_count": 2,
        "pending_inbox": 0,
        "unbound_ledger": 0,
        "inflight_writes": 0,
    }

    with pytest.raises(
        ActivationEpochError,
        match="activation_confirmation_preconditions_required",
    ):
        store.transition_activation_epoch(
            epoch_id="rca-release-20260712",
            target_state="confirmed",
            partition_end_fence={TOPIC: {"2": 21}},
            operator="release-test",
            reason="missing production gate bindings must fail",
        )

    bounded = store.activation_epoch()
    assert bounded is not None
    release_bindings = []
    canonical_sha256 = control_store_module._canonical_sha256

    def capture_release_binding(value):
        if isinstance(value, dict) and "historical_hold_sha256" in value:
            release_bindings.append(value)
        return canonical_sha256(value)

    monkeypatch.setattr(
        control_store_module, "_canonical_sha256", capture_release_binding
    )
    confirmation_preconditions = _confirmation_preconditions(
        store, {TOPIC: {"2": 21}}
    )
    historical_hold = store.activation_historical_outbox_hold_evidence(
        epoch_id="rca-release-20260712"
    )
    assert release_bindings[-1]["historical_hold_count"] == 1
    assert (
        release_bindings[-1]["historical_hold_sha256"]
        == historical_hold["sealed_sha256"]
    )
    assert release_bindings[-1]["historical_hold_row_schema_version"] == (
        "pnc_rca_activation_historical_outbox_row_v1"
    )
    with pytest.raises(
        ActivationEpochError,
        match="activation_confirmation_epoch_binding_changed",
    ):
        store.transition_activation_epoch(
            epoch_id="rca-release-20260712",
            expected_state="bounded_active",
            target_state="confirmed",
            partition_end_fence={TOPIC: {"2": 21}},
            production_fingerprint="3" * 64,
            production_gate_receipt_sha256="4" * 64,
            expected_config_sha256="9" * 64,
            expected_db_logical_identity_sha256=bounded[
                "db_logical_identity_sha256"
            ],
            expected_partition_start_fence_sha256=bounded[
                "partition_start_fence_sha256"
            ],
            expected_release_binding_sha256=confirmation_preconditions[
                "expected_release_binding_sha256"
            ],
            operator="release-test",
            reason="reject a confirmation capsule for another config",
        )

    confirmed = store.transition_activation_epoch(
        epoch_id="rca-release-20260712",
        expected_state="bounded_active",
        target_state="confirmed",
        partition_end_fence={TOPIC: {"2": 21}},
        production_fingerprint="3" * 64,
        production_gate_receipt_sha256="4" * 64,
        **confirmation_preconditions,
        operator="release-test",
        reason="bind the exact passing production release receipt",
    )
    assert confirmed["state"] == "confirmed"
    assert confirmed["production_fingerprint"] == "3" * 64
    with pytest.raises(
        ActivationEpochError, match="activation_confirmation_binding_conflict"
    ):
        store.transition_activation_epoch(
            epoch_id="rca-release-20260712",
            target_state="confirmed",
            partition_end_fence={TOPIC: {"2": 21}},
            production_fingerprint="5" * 64,
            production_gate_receipt_sha256="4" * 64,
            **confirmation_preconditions,
            operator="release-test",
            reason="conflicting retry must fail closed",
        )
    steady = store.transition_activation_epoch(
        epoch_id="rca-release-20260712",
        expected_state="confirmed",
        target_state="steady_active",
        operator="release-test",
        reason="all governed shadow backlog is empty",
    )
    assert steady["state"] == "steady_active"
    assert [
        (row["from_state"], row["to_state"])
        for row in store.list_rows("rca_activation_transition_audit")
    ] == [
        ("none", "safe_off"),
        ("safe_off", "preauthorized"),
        ("preauthorized", "bounded_active"),
        ("bounded_active", "confirmed"),
        ("confirmed", "steady_active"),
    ]


def test_direct_steady_ignores_historical_shadow_and_has_no_slots(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    _begin_bounded_activation(store, epoch_id="rca-old-bounded-20260712")
    accepted = store.ingest_record(
        _record(offset=20),
        policy=_policy(),
        submit_enabled=True,
        activation_required=True,
    )
    [shadow] = store.list_rows("rca_outbox")
    assert shadow["status"] == "shadow"
    store.transition_activation_epoch(
        epoch_id="rca-old-bounded-20260712",
        expected_state="bounded_active",
        target_state="aborted",
        operator="release-test",
        reason="retire the predecessor without settling historical shadow",
    )

    direct = _activate_direct_steady_epoch(
        store,
        epoch_id="rca-direct-20260712",
        start_offset=21,
    )

    assert direct["state"] == "steady_active"
    assert store.activation_epoch() == direct
    assert [
        row
        for row in store.list_rows("rca_activation_budget_slots")
        if row["epoch_id"] == direct["epoch_id"]
    ] == []
    assert [
        (row["from_state"], row["to_state"])
        for row in store.list_rows("rca_activation_transition_audit")
    ][-1:] == [("direct_release", "steady_active")]

    request = _manual_request(
        "om-direct-steady",
        issue_url="https://project.feishu.cn/g1q3/issue/detail/7041712816",
    )
    admitted = store.admit_manual_trigger(
        request,
        allowed_chat_ids={"oc_allowed"},
        submit_enabled=True,
        active_policy=_policy(),
        activation_required=True,
    )
    assert admitted.outcome == "created"
    assert admitted.state == "pending"
    [ledger] = [
        row
        for row in store.list_rows("rca_activation_admission_ledger")
        if row["epoch_id"] == direct["epoch_id"]
    ]
    assert ledger["decision"] == "admit"
    assert accepted.event_uid


def test_direct_steady_is_idempotent_and_rejects_binding_conflicts(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    first = _activate_direct_steady_epoch(store)
    second = _activate_direct_steady_epoch(store)
    assert second == first
    assert len(store.list_rows("rca_activation_transition_audit")) == 1

    with pytest.raises(
        ActivationEpochError, match="activation_direct_steady_binding_conflict"
    ):
        _activate_direct_steady_epoch(store, config_sha256="9" * 64)


def test_direct_steady_rejects_active_predecessor_and_replaces_aborted_one(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    _activate_direct_steady_epoch(store, epoch_id="rca-direct-first-20260712")
    with pytest.raises(
        ActivationEpochError, match="activation_current_epoch_exists"
    ):
        _activate_direct_steady_epoch(store, epoch_id="rca-direct-second-20260712")

    store.transition_activation_epoch(
        epoch_id="rca-direct-first-20260712",
        expected_state="steady_active",
        target_state="aborted",
        operator="release-test",
        reason="retire direct predecessor",
    )
    replacement = _activate_direct_steady_epoch(
        store,
        epoch_id="rca-direct-second-20260712",
        start_offset=30,
    )
    assert replacement["state"] == "steady_active"
    assert replacement["epoch_id"] == "rca-direct-second-20260712"


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
    assert not [
        row
        for row in store.list_rows("rca_activation_budget_slots")
        if row["epoch_id"] == successor["epoch_id"]
    ]
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
        activation_slot_kind="",
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


def test_direct_steady_successor_ignores_historical_epoch_inflight(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    historical = _activate_direct_steady_epoch(
        store,
        epoch_id="rca-direct-historical-20260712",
    )
    store.admit_manual_trigger(
        _manual_request("om-direct-historical-inflight"),
        allowed_chat_ids={"oc_allowed"},
        submit_enabled=True,
        active_policy=_policy(),
        activation_required=True,
    )
    store.transition_activation_epoch(
        epoch_id=historical["epoch_id"],
        expected_state="steady_active",
        target_state="aborted",
        operator="release-test",
        reason="retire predecessor while preserving historical evidence",
    )
    current = _activate_direct_steady_epoch(
        store,
        epoch_id="rca-direct-current-20260712",
        start_offset=30,
    )
    predecessor_binding = store.direct_steady_predecessor()
    assert predecessor_binding is not None
    assert predecessor_binding["epoch_id"] == current["epoch_id"]
    assert predecessor_binding["inflight"]["total"] == 0

    successor = _activate_direct_steady_epoch(
        store,
        epoch_id="rca-direct-successor-20260712",
        start_offset=40,
        expected_predecessor=predecessor_binding,
    )

    assert store.activation_epoch() == successor
    [historical_outbox] = store.list_rows("rca_outbox")
    assert historical_outbox["activation_epoch_id"] == historical["epoch_id"]
    assert historical_outbox["status"] == "pending"


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


def test_direct_steady_successor_rolls_back_predecessor_supersede_on_insert_error(
    tmp_path,
):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    duplicate_id = "rca-direct-duplicate-20260712"
    _activate_direct_steady_epoch(store, epoch_id=duplicate_id)
    store.transition_activation_epoch(
        epoch_id=duplicate_id,
        expected_state="steady_active",
        target_state="aborted",
        operator="release-test",
        reason="make duplicate identity historical",
    )
    predecessor = _activate_direct_steady_epoch(
        store,
        epoch_id="rca-direct-current-20260712",
        start_offset=30,
    )
    predecessor_binding = store.direct_steady_predecessor()
    assert predecessor_binding is not None

    with pytest.raises(sqlite3.IntegrityError):
        _activate_direct_steady_epoch(
            store,
            epoch_id=duplicate_id,
            start_offset=40,
            expected_predecessor=predecessor_binding,
        )

    assert store.activation_epoch() == predecessor
    [predecessor_row] = [
        row
        for row in store.list_rows("rca_activation_epochs")
        if row["epoch_id"] == predecessor["epoch_id"]
    ]
    assert predecessor_row["is_current"] == 1
    assert predecessor_row["superseded_at"] is None


def test_activation_confirmation_rejects_consumed_but_unbound_slots(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    identities = _begin_bounded_activation(store)
    requests = (
        ("manual_success", "manual_admit", "manual"),
        ("manual_terminal_failure", "manual_admit", "manual"),
    )
    for generation, (slot, entrypoint, source_kind) in enumerate(requests, start=1):
        decision = _adjudicate_activation(
            store,
            entrypoint=entrypoint,
            source_kind=source_kind,
            source_identity=identities[slot][1],
            business_key=f"unbound-business-{generation}",
            submission_key=f"unbound-submission-{generation}",
            generation=generation,
            new_execution=True,
            requested_slot_kind=slot,
        )
        assert decision.decision == "admit"

    with pytest.raises(
        ActivationEpochError, match="activation_bounded_execution_unbound"
    ):
        store.transition_activation_epoch(
            epoch_id="rca-release-20260712",
            expected_state="bounded_active",
            target_state="confirmed",
            partition_end_fence={TOPIC: {"2": 21}},
            production_fingerprint="3" * 64,
            production_gate_receipt_sha256="4" * 64,
            **_confirmation_preconditions(
                store,
                {TOPIC: {"2": 21}},
                release_binding_sha256="9" * 64,
            ),
            operator="release-test",
            reason="unbound ledger reservations are not canary evidence",
        )


def test_bounded_slot_consume_is_atomic_idempotent_and_survives_restart(tmp_path):
    path = tmp_path / "control.sqlite3"
    store = RcaControlStore(path, busy_timeout_ms=10_000)
    identities = _begin_bounded_activation(store)

    def compete(index: int):
        return _adjudicate_activation(
            store,
            entrypoint="manual_admit",
            source_kind="manual",
            source_identity=identities["manual_success"][1],
            business_key=f"business-{index}",
            submission_key=f"submission-{index}",
            generation=1,
            new_execution=True,
            requested_slot_kind="manual_success",
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(compete, range(2)))

    winner_index, winner = next(
        (index, result)
        for index, result in enumerate(results)
        if result.decision == "admit"
    )
    [loser] = [result for result in results if result.decision == "reject"]
    assert winner.consumed_slot is True
    assert loser.reason == "activation_bounded_slot_consumed"
    restarted = RcaControlStore(path)
    replay = compete(winner_index)
    assert replay.decision == "admit"
    assert replay.consumed_slot is False
    assert replay.reason == "activation_admission_idempotent"
    [slot] = [
        row
        for row in restarted.list_rows("rca_activation_budget_slots")
        if row["slot_kind"] == "manual_success"
    ]
    assert slot["consumed_ledger_id"] == winner.ledger_id
    assert len(
        [
            row
            for row in restarted.list_rows("rca_activation_admission_ledger")
            if row["decision"] == "admit"
        ]
    ) == 1


def test_bounded_kafka_identity_must_be_inside_start_fence(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    _create_activation_epoch(store, start_offset=20)
    with pytest.raises(
        ActivationEpochError, match="activation_kafka_before_start_fence"
    ):
        store.authorize_activation_slot(
            epoch_id="rca-release-20260712",
            slot_kind="kafka_success",
            source_kind="kafka",
            source_identity={"event_uid": f"{TOPIC}:2:19"},
            operator="release-test",
            reason="authorization must enforce the start fence",
        )

    [slot] = [
        row
        for row in store.list_rows("rca_activation_budget_slots")
        if row["slot_kind"] == "kafka_success"
    ]
    assert slot["consumed_ledger_id"] is None


def test_preauthorized_enabled_kafka_is_deferred_and_manual_is_rejected(
    tmp_path,
):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    _create_activation_epoch(store)
    _authorize_activation_slots(store)

    with pytest.raises(
        ActivationIngressDeferredError, match="activation_ingress_unavailable"
    ):
        store.ingest_record(
            _record(offset=20),
            policy=_policy(),
            submit_enabled=True,
            activation_required=True,
            activation_slot_kind="kafka_success",
        )

    assert store.list_rows("kafka_inbox") == []
    assert store.list_rows("rca_outbox") == []
    with pytest.raises(
        ManualRcaAdmissionError,
        match="activation_epoch_rejected_preauthorized",
    ):
        store.admit_manual_trigger(
            _manual_request("om_manual_success"),
            allowed_chat_ids={"oc_allowed"},
            submit_enabled=True,
            active_policy=_policy(),
            activation_required=True,
            activation_slot_kind="manual_success",
        )
    assert [
        row
        for row in store.list_rows("rca_trigger_sources")
        if row["source_kind"] == "feishu_group_manual"
    ] == []


def test_bounded_passive_kafka_shadow_cannot_consume_release_slot(
    tmp_path,
):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    _create_activation_epoch(store)
    _authorize_activation_slots(store)
    store.transition_activation_epoch(
        epoch_id="rca-release-20260712",
        target_state="bounded_active",
        operator="release-test",
        reason="open exact canaries before observing bounded shadow",
    )
    accepted = store.ingest_record(
        _record(offset=20),
        policy=_policy(),
        submit_enabled=True,
        activation_required=True,
    )

    [held_outbox] = store.list_rows("rca_outbox")
    assert held_outbox["status"] == "shadow"
    kafka_slot = next(
        row
        for row in store.list_rows("rca_activation_budget_slots")
        if row["slot_kind"] == "kafka_success"
    )
    assert kafka_slot["consumed_ledger_id"] is None

    with pytest.raises(
        ShadowPromotionError,
        match="activation_bounded_identity_not_authorized",
    ):
        store.promote_shadow_event(
            accepted.event_uid,
            operator="release-test",
            reason="wrong slot must not promote",
            activation_required=True,
            activation_slot_kind="manual_success",
        )

    with pytest.raises(
        ShadowPromotionError,
        match="activation_bounded_identity_not_authorized",
    ):
        store.promote_shadow_event(
            accepted.event_uid,
            operator="release-test",
            reason="passive Kafka cannot consume a bounded release slot",
            activation_required=True,
            activation_slot_kind="kafka_success",
        )

    [outbox] = store.list_rows("rca_outbox")
    [trigger] = store.list_rows("business_triggers")
    assert outbox["status"] == trigger["state"] == "shadow"
    kafka_slot = next(
        row
        for row in store.list_rows("rca_activation_budget_slots")
        if row["slot_kind"] == "kafka_success"
    )
    assert kafka_slot["consumed_ledger_id"] is None
    ledger = next(
        row
        for row in store.list_rows("rca_activation_admission_ledger")
        if row["ledger_id"] == outbox["activation_ledger_id"]
    )
    assert ledger["decision"] == "reject"


def test_confirmed_reconciles_only_exact_fenced_shadow_before_steady(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    _create_activation_epoch(store)
    identities = _authorize_activation_slots(store)
    store.transition_activation_epoch(
        epoch_id="rca-release-20260712",
        expected_state="preauthorized",
        target_state="bounded_active",
        operator="release-test",
        reason="open exact canaries while holding ordinary Kafka work",
    )
    catchup = store.ingest_record(
        _record(offset=20),
        policy=_policy(),
        submit_enabled=True,
        activation_required=True,
    )
    for slot_kind in ("manual_success", "manual_terminal_failure"):
        identity = identities[slot_kind][1]
        store.admit_manual_trigger(
            _manual_request(
                identity["message_id"],
                mode=identity["mode"],
                issue_url=identity["issue_url"],
                thread_id=identity["thread_id"],
            ),
            allowed_chat_ids={"oc_allowed"},
            submit_enabled=True,
            operator_authorized=slot_kind == "manual_terminal_failure",
            active_policy=_policy(),
            activation_required=True,
        )
    claim = store.claim_outbox(
        lease_owner="confirmed-success-canary",
        activation_required=True,
    )
    assert claim is not None
    success_submission_key = next(
        row["submission_key"]
        for row in store.list_rows("rca_activation_admission_ledger")
        if row["slot_kind"] == "manual_success"
    )
    terminal_submission_key = next(
        row["submission_key"]
        for row in store.list_rows("rca_activation_admission_ledger")
        if row["slot_kind"] == "manual_terminal_failure"
    )
    assert claim.submission_key == success_submission_key
    store.complete_outbox(
        outbox_id=claim.outbox_id,
        lease_token=claim.lease_token,
        result={"outcome": "confirmed_canary_recorded"},
    )
    _terminalize_permanent(store, terminal_submission_key)
    late = store.ingest_record(
        _record(offset=21, value=_value(work_item_id=7041712817)),
        policy=_policy(),
        submit_enabled=True,
        activation_required=True,
    )
    stale_confirmation_preconditions = _confirmation_preconditions(
        store, {TOPIC: {"2": 21}}
    )
    with pytest.raises(
        ActivationEpochError, match="activation_shadow_outside_end_fence"
    ):
        store.transition_activation_epoch(
            epoch_id="rca-release-20260712",
            expected_state="bounded_active",
            target_state="confirmed",
            partition_end_fence={TOPIC: {"2": 21}},
            production_fingerprint="3" * 64,
            production_gate_receipt_sha256="4" * 64,
            **stale_confirmation_preconditions,
            operator="release-test",
            reason="stale end fence must reject a raced bounded shadow",
        )
    confirmation_preconditions = _confirmation_preconditions(
        store, {TOPIC: {"2": 22}}
    )
    store.transition_activation_epoch(
        epoch_id="rca-release-20260712",
        expected_state="bounded_active",
        target_state="confirmed",
        partition_end_fence={TOPIC: {"2": 22}},
        production_fingerprint="3" * 64,
        production_gate_receipt_sha256="4" * 64,
        **confirmation_preconditions,
        operator="release-test",
        reason="freeze the exact Kafka reconciliation fence",
    )

    with pytest.raises(
        ActivationEpochError, match="activation_confirmed_ingress_deferred"
    ):
        store.ingest_record(
            _record(offset=23, value=_value(work_item_id=7041712818)),
            policy=_policy(),
            submit_enabled=True,
            activation_required=False,
        )
    assert all(
        row["event_uid"] != f"{TOPIC}:2:23"
        for row in store.list_rows("kafka_inbox")
    )
    assert store.claim_outbox(
        lease_owner="confirmed-must-hold", activation_required=True
    ) is None
    with pytest.raises(ActivationEpochError, match="activation_epoch_not_current"):
        store.promote_shadow_event(
            catchup.event_uid,
            operator="release-test",
            reason="stale epoch must not win a promotion race",
            expected_activation_epoch_id="stale-epoch",
            activation_required=True,
        )
    reconciled = store.promote_shadow_event(
        catchup.event_uid,
        operator="release-test",
        reason="reconcile one exact current-epoch fenced shadow",
        expected_activation_epoch_id="rca-release-20260712",
        activation_required=True,
    )
    assert reconciled.promoted is True
    replay = store.promote_shadow_event(
        catchup.event_uid,
        operator="release-test",
        reason="idempotent confirmed reconciliation retry",
        expected_activation_epoch_id="rca-release-20260712",
        activation_required=True,
    )
    assert replay.promoted is False
    assert replay.status == "pending"
    late_reconciled = store.promote_shadow_event(
        late.event_uid,
        operator="release-test",
        reason="reconcile the shadow that raced the first end-fence observation",
        expected_activation_epoch_id="rca-release-20260712",
        activation_required=True,
    )
    assert late_reconciled.promoted is True
    assert store.claim_outbox(
        lease_owner="confirmed-still-held", activation_required=True
    ) is None
    ledger = next(
        row
        for row in store.list_rows("rca_activation_admission_ledger")
        if row["ledger_id"]
        == next(
            outbox["activation_ledger_id"]
            for outbox in store.list_rows("rca_outbox")
            if outbox["submission_key"] == catchup.submission_key
        )
    )
    assert ledger["decision"] == "admit"
    assert ledger["reason"] == "activation_confirmed_shadow_reconciliation"
    steady = store.transition_activation_epoch(
        epoch_id="rca-release-20260712",
        expected_state="confirmed",
        target_state="steady_active",
        operator="release-test",
        reason="all exact current shadow work is reconciled",
    )
    assert steady["state"] == "steady_active"
    claim = store.claim_outbox(
        lease_owner="steady-catchup", activation_required=True
    )
    assert claim is not None
    assert claim.submission_key == catchup.submission_key


def test_activation_health_is_payload_free_and_counts_held_lineage(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    _create_activation_epoch(store)
    _authorize_activation_slots(store)
    store.transition_activation_epoch(
        epoch_id="rca-release-20260712",
        expected_state="preauthorized",
        target_state="bounded_active",
        operator="release-test",
        reason="measure held bounded lineage",
    )
    store.ingest_record(_record(offset=10), policy=_policy(), submit_enabled=True)
    store.ingest_record(
        _record(offset=20, value=_value(work_item_id=7041712813)),
        policy=_policy(),
        submit_enabled=True,
        activation_required=True,
    )

    activation = store.health()["activation"]

    assert activation["configured"] is True
    assert activation["production_active"] is False
    assert activation["ingress_freeze_readiness"]["ready"] is False
    assert (
        activation["ingress_freeze_readiness"]["reason"]
        == "activation_slots_incomplete"
    )
    assert activation["current_epoch"]["epoch_id"] == "rca-release-20260712"
    assert activation["current_epoch"]["state"] == "bounded_active"
    assert activation["slots"] == {
        "kafka_success": {"authorized": False, "consumed": False},
        "manual_success": {"authorized": True, "consumed": False},
        "manual_terminal_failure": {"authorized": True, "consumed": False},
    }
    assert activation["ledger"] == {
        "admit": 0,
        "join": 0,
        "shadow": 2,
        "reject": 0,
    }
    assert activation["backlog"] == {
        "current_admitted": 0,
        "current_held": 2,
        "unadjudicated_shadow": 0,
        "historical_blocked": 0,
        "historical_held": 0,
        "deferred_quarantined": 0,
        "pending_inbox": 0,
        "unbound_ledger": 0,
        "historical_unbound_ledger": 0,
    }
    health = store.health()
    assert health["snapshot_at"].endswith("+00:00")
    assert isinstance(health["sqlite_data_version"], int)
    health_json = json.dumps(health, sort_keys=True)
    assert "ACC braking issue" not in health_json
    assert "om_manual_success" not in health_json


def test_activation_terminal_canary_accepts_settled_silent_internal_route(tmp_path):
    store, _terminal, _status = _silent_terminal_activation(tmp_path)

    assert store.activation_ingress_freeze_readiness() == {
        "epoch_id": "rca-release-20260712",
        "state": "bounded_active",
        "ready": True,
        "reason": "ready",
        "required_slot_count": 2,
        "consumed_slot_count": 2,
        "completed_bound_slot_count": 2,
        "pending_inbox": 0,
        "unbound_ledger": 0,
        "inflight_writes": 0,
    }


def test_activation_delivery_completion_accepts_settled_silent_terminal_route(
    tmp_path,
):
    store, terminal, _status = _silent_terminal_activation(tmp_path)
    row = next(
        item
        for item in store.list_rows("rca_outbox")
        if item["submission_key"] == terminal.submission_key
    )
    conn = store._connect()
    try:
        assert RcaControlStore._activation_delivery_execution_complete_tx(
            conn,
            business_key=row["business_key"],
            submission_key=row["submission_key"],
            generation=int(row["generation"]),
        )
        assert RcaControlStore._activation_bound_delivery_backlog_tx(
            conn,
            epoch_id=store.activation_epoch()["epoch_id"],
        ) == 1
    finally:
        conn.close()


def test_activation_delivery_completion_accepts_exact_taxonomy_gap_issue_only_route(
    tmp_path,
):
    store, terminal, status = _silent_terminal_activation(tmp_path)
    _convert_silent_terminal_to_taxonomy_gap(store, terminal, status)
    row = next(
        item
        for item in store.list_rows("rca_outbox")
        if item["submission_key"] == terminal.submission_key
    )
    conn = store._connect()
    try:
        assert RcaControlStore._activation_delivery_execution_complete_tx(
            conn,
            business_key=row["business_key"],
            submission_key=row["submission_key"],
            generation=int(row["generation"]),
        )
    finally:
        conn.close()


@pytest.mark.parametrize(
    "corruption",
    ("known", "raw_code", "source_conflict", "route_status"),
)
def test_activation_taxonomy_gap_route_rejects_forged_contract(
    tmp_path,
    corruption,
):
    store, terminal, status = _silent_terminal_activation(tmp_path)
    converted = _convert_silent_terminal_to_taxonomy_gap(store, terminal, status)
    taxonomy = converted["failure_taxonomy"]
    if corruption == "known":
        taxonomy["known"] = True
    elif corruption == "raw_code":
        taxonomy["raw_code"] = "different_gap"
    elif corruption == "source_conflict":
        taxonomy["source_conflict"] = True
    else:
        taxonomy["durable_route"]["status"] = "resolved"
    conn = store._connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "UPDATE rca_execution_watch SET last_status_json = ? "
            "WHERE submission_key = ?",
            (control_store_module._canonical_json(converted), terminal.submission_key),
        )
        conn.commit()
    finally:
        conn.close()
    row = next(
        item
        for item in store.list_rows("rca_outbox")
        if item["submission_key"] == terminal.submission_key
    )
    conn = store._connect()
    try:
        assert not RcaControlStore._activation_delivery_execution_complete_tx(
            conn,
            business_key=row["business_key"],
            submission_key=row["submission_key"],
            generation=int(row["generation"]),
        )
    finally:
        conn.close()


@pytest.mark.parametrize(
    "corruption",
    ("external_write", "outlet_effect", "route_identity"),
)
def test_activation_terminal_canary_rejects_forged_silent_settlement(
    tmp_path, corruption
):
    store, terminal, status = _silent_terminal_activation(tmp_path)
    conn = store._connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        if corruption == "external_write":
            status["external_writes"] = True
            conn.execute(
                "UPDATE rca_execution_watch SET last_status_json = ? "
                "WHERE submission_key = ?",
                (json.dumps(status, sort_keys=True, separators=(",", ":")),
                 terminal.submission_key),
            )
        elif corruption == "outlet_effect":
            status["failure_taxonomy"]["durable_route"]["internal_outlet"][
                "external_effects"
            ] = 1
            conn.execute(
                "UPDATE rca_execution_watch SET last_status_json = ? "
                "WHERE submission_key = ?",
                (json.dumps(status, sort_keys=True, separators=(",", ":")),
                 terminal.submission_key),
            )
        else:
            route_key = status["failure_taxonomy"]["durable_route"]["route_key"]
            conn.execute(
                "UPDATE rca_failure_routes SET task_id = ? WHERE route_key = ?",
                ("different-task", route_key),
            )
        conn.commit()
    finally:
        conn.close()

    row = next(
        item
        for item in store.list_rows("rca_outbox")
        if item["submission_key"] == terminal.submission_key
    )
    conn = store._connect()
    try:
        assert not RcaControlStore._activation_delivery_execution_complete_tx(
            conn,
            business_key=row["business_key"],
            submission_key=row["submission_key"],
            generation=int(row["generation"]),
        )
        assert RcaControlStore._activation_bound_delivery_backlog_tx(
            conn,
            epoch_id=store.activation_epoch()["epoch_id"],
        ) == 2
    finally:
        conn.close()

    readiness = store.activation_ingress_freeze_readiness()
    assert readiness["ready"] is False
    assert readiness["reason"] == "activation_canary_executions_incomplete"
    assert readiness["completed_bound_slot_count"] == 1


def test_activation_unbound_health_counts_only_current_creating_ledgers(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    identities = _begin_bounded_activation(store)
    rejected = _adjudicate_activation(
        store,
        entrypoint="kafka_ingest",
        source_kind="kafka",
        source_identity={"event_uid": f"{TOPIC}:2:21"},
        business_key="missing-business",
        submission_key="missing-submission",
        generation=1,
        new_execution=False,
    )
    assert rejected.decision == "reject"
    assert store.health()["activation"]["backlog"]["unbound_ledger"] == 0

    admitted = _adjudicate_activation(
        store,
        entrypoint="manual_admit",
        source_kind="manual",
        source_identity=identities["manual_success"][1],
        business_key="unbound-business",
        submission_key="unbound-submission",
        generation=1,
        new_execution=True,
        requested_slot_kind="manual_success",
    )
    assert admitted.decision == "admit"
    backlog = store.health()["activation"]["backlog"]
    assert backlog["unbound_ledger"] == 1
    assert backlog["historical_unbound_ledger"] == 0

    store.transition_activation_epoch(
        epoch_id="rca-release-20260712",
        expected_state="bounded_active",
        target_state="aborted",
        operator="release-test",
        reason="isolate historical unbound metric from replacement epoch",
    )
    _create_activation_epoch(store, epoch_id="rca-release-replacement")
    replacement = store.health()["activation"]["backlog"]
    assert replacement["unbound_ledger"] == 0
    assert replacement["historical_unbound_ledger"] == 1


def test_bounded_manual_slots_auto_resolve_two_canaries_without_restart(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    success_url = "https://project.feishu.cn/g1q3/issue/detail/7041712814"
    terminal_url = "https://project.feishu.cn/g1q3/issue/detail/7041712815"
    _create_activation_epoch(store)
    store.authorize_activation_slot(
        epoch_id="rca-release-20260712",
        slot_kind="manual_success",
        source_kind="manual",
        source_identity=_manual_activation_identity(
            "om_auto_success", issue_url=success_url
        ),
        operator="release-test",
        reason="authorize manual success canary",
    )
    store.authorize_activation_slot(
        epoch_id="rca-release-20260712",
        slot_kind="manual_terminal_failure",
        source_kind="manual",
        source_identity=_manual_activation_identity(
            "om_auto_terminal", mode="debug", issue_url=terminal_url
        ),
        operator="release-test",
        reason="authorize manual terminal canary",
    )
    store.transition_activation_epoch(
        epoch_id="rca-release-20260712",
        target_state="bounded_active",
        operator="release-test",
        reason="activate both exact manual canaries without config restart",
    )

    success = store.admit_manual_trigger(
        _manual_request("om_auto_success", issue_url=success_url),
        allowed_chat_ids={"oc_allowed"},
        submit_enabled=True,
        active_policy=_policy(),
        activation_required=True,
    )
    terminal = store.admit_manual_trigger(
        _manual_request(
            "om_auto_terminal", mode="debug", issue_url=terminal_url
        ),
        allowed_chat_ids={"oc_allowed"},
        submit_enabled=True,
        operator_authorized=True,
        active_policy=_policy(),
        activation_required=True,
    )
    success_replay = store.admit_manual_trigger(
        _manual_request("om_auto_success", issue_url=success_url),
        allowed_chat_ids={"oc_allowed"},
        submit_enabled=True,
        active_policy=_policy(),
        activation_required=True,
    )
    with pytest.raises(
        ManualRcaAdmissionError,
        match="activation_existing_generation_not_eligible",
    ):
        store.admit_manual_trigger(
            _manual_request("om_unrelated_join", issue_url=success_url),
            allowed_chat_ids={"oc_allowed"},
            submit_enabled=True,
            active_policy=_policy(),
            activation_required=True,
        )

    assert success.outcome == terminal.outcome == "created"
    assert success_replay.submission_key == success.submission_key
    assert success_replay.generation == success.generation
    assert success.submission_key != terminal.submission_key
    consumed = {
        row["slot_kind"]: row["consumed_ledger_id"] is not None
        for row in store.list_rows("rca_activation_budget_slots")
    }
    assert consumed == {
        "kafka_success": False,
        "manual_success": True,
        "manual_terminal_failure": True,
    }
    outboxes = store.list_rows("rca_outbox")
    assert len(outboxes) == 2
    assert all(
        row["activation_epoch_id"] == "rca-release-20260712"
        and row["activation_ledger_id"] is not None
        for row in outboxes
    )


def test_manual_external_write_authority_is_exact_and_revocation_sensitive(
    tmp_path,
):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    issue_url = "https://project.feishu.cn/g1q3/issue/detail/7041712814"
    _begin_bounded_activation(store)
    admitted = store.admit_manual_trigger(
        _manual_request("om_manual_success", issue_url=issue_url),
        allowed_chat_ids={"oc_allowed"},
        submit_enabled=True,
        active_policy=_policy(),
        activation_required=True,
    )

    authority = store.validate_manual_external_write_admission(
        admitted.to_dict(),
        expected_chat_id="oc_allowed",
        expected_thread_id="topic:om_root",
        expected_message_id="om_manual_success",
        expected_requester_id="ou_operator",
    )

    assert authority["epoch_id"] == "rca-release-20260712"
    assert authority["state"] == "bounded_active"
    assert authority["decision"] == "admit"
    assert authority["business_key"] == admitted.business_key
    assert authority["submission_key"] == admitted.submission_key
    assert authority["generation"] == admitted.generation
    with pytest.raises(RecordConflictError, match="source_identity_mismatch"):
        store.validate_manual_external_write_admission(
            admitted.to_dict(),
            expected_chat_id="oc_other",
            expected_thread_id="topic:om_root",
            expected_message_id="om_manual_success",
            expected_requester_id="ou_operator",
        )

    store.transition_activation_epoch(
        epoch_id="rca-release-20260712",
        expected_state="bounded_active",
        target_state="aborted",
        operator="release-test",
        reason="inject provider-bound revocation",
    )
    with pytest.raises(RecordConflictError, match="activation_epoch_not_current"):
        store.validate_manual_external_write_admission(
            admitted.to_dict(),
            expected_chat_id="oc_allowed",
            expected_thread_id="topic:om_root",
            expected_message_id="om_manual_success",
            expected_requester_id="ou_operator",
        )


def test_operator_issue_only_external_write_authority_uses_operator_ledger_identity(
    tmp_path,
):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    _register_policy_without_classifying(store)
    _activate_direct_steady_epoch(store)
    request = _operator_request(
        "batch-operator-7041712812-try-1",
        requester_id="automation:rca-batch-rerun",
    )
    admitted = store.admit_manual_trigger(
        request,
        allowed_chat_ids=set(),
        submit_enabled=True,
        operator_authorized=True,
        active_policy=_policy(),
        activation_required=True,
    )

    authority = store.validate_manual_external_write_admission(
        admitted.to_dict(),
        expected_chat_id="",
        expected_thread_id="",
        expected_message_id=request.message_id,
        expected_requester_id=request.requester_id,
    )

    assert authority["state"] == "steady_active"
    assert authority["decision"] == "admit"
    assert authority["chat_id"] == ""
    assert authority["thread_id"] == ""
    with pytest.raises(RecordConflictError, match="source_identity_mismatch"):
        store.validate_manual_external_write_admission(
            admitted.to_dict(),
            expected_chat_id="oc_forbidden",
            expected_thread_id="topic:forbidden",
            expected_message_id=request.message_id,
            expected_requester_id=request.requester_id,
        )


def test_bounded_manual_auto_slot_rejects_ambiguous_exact_identity(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    identity = _manual_activation_identity("om_ambiguous")
    _create_activation_epoch(store)
    store.authorize_activation_slot(
        epoch_id="rca-release-20260712",
        slot_kind="kafka_success",
        source_kind="kafka",
        source_identity={"event_uid": f"{TOPIC}:2:20"},
        operator="release-test",
        reason="authorize Kafka canary",
    )
    store.authorize_activation_slot(
        epoch_id="rca-release-20260712",
        slot_kind="manual_success",
        source_kind="manual",
        source_identity=identity,
        operator="release-test",
        reason="authorize one exact manual identity",
    )
    with pytest.raises(
        ActivationEpochError, match="activation_slot_identity_reused"
    ):
        store.authorize_activation_slot(
            epoch_id="rca-release-20260712",
            slot_kind="manual_terminal_failure",
            source_kind="manual",
            source_identity=identity,
            operator="release-test",
            reason="duplicate identity must fail at authorization",
        )

    assert not any(
        row["consumed_ledger_id"] is not None
        for row in store.list_rows("rca_activation_budget_slots")
    )
    assert [
        row
        for row in store.list_rows("rca_trigger_sources")
        if row["source_kind"] == "feishu_group_manual"
    ] == []


def test_safe_off_epoch_claims_no_work_and_does_not_age_history(
    tmp_path,
):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    legacy = store.ingest_record(
        _record(offset=10), policy=_policy(), submit_enabled=True
    )
    _create_activation_epoch(store, preauthorize=False)
    _create_activation_epoch(store)

    evidence = store.activation_historical_outbox_hold_evidence(
        epoch_id="rca-release-20260712"
    )
    assert evidence["sealed_count"] == evidence["current_count"] == 1
    assert evidence["sealed_sha256"] == evidence["current_sha256"]
    assert evidence["matches"] is True
    assert store.dispatch_backlog_count() == 0

    claim = store.claim_outbox(
        lease_owner="activation-dispatcher",
        activation_required=False,
        max_age_seconds=1,
    )

    assert claim is None
    legacy_outbox = next(
        row
        for row in store.list_rows("rca_outbox")
        if row["submission_key"] == legacy.submission_key
    )
    assert legacy_outbox["status"] == "pending"


def test_historical_hold_accepts_retry_audit_without_active_lease(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    legacy = store.ingest_record(
        _record(offset=10), policy=_policy(), submit_enabled=True
    )
    claimed = store.claim_outbox(
        lease_owner="historical-retry-test",
        activation_required=False,
    )
    assert claimed is not None
    assert claimed.submission_key == legacy.submission_key

    retried = store.retry_outbox(
        outbox_id=claimed.outbox_id,
        lease_token=claimed.lease_token,
        error_code="production_admission_held",
        delay_seconds=60,
    )
    assert retried.status == "pending"
    [pending] = store.list_rows("rca_outbox")
    assert pending["claimed_at"] is not None
    assert pending["lease_token"] is None
    assert pending["lease_owner"] is None
    assert pending["lease_expires_at"] is None

    _create_activation_epoch(store, preauthorize=False)
    _create_activation_epoch(store)
    evidence = store.activation_historical_outbox_hold_evidence(
        epoch_id="rca-release-20260712"
    )

    assert evidence["sealed_count"] == evidence["current_count"] == 1
    assert evidence["matches"] is True


@pytest.mark.parametrize(
    ("mutation", "error"),
    [
        (
            "UPDATE rca_outbox SET status = 'claimed'",
            "activation_historical_hold_outbox_claimed",
        ),
        (
            "UPDATE rca_outbox SET status = 'shadow'",
            "activation_historical_hold_outbox_shadow",
        ),
        (
            "UPDATE rca_outbox SET lease_token = 'stale-lease'",
            "activation_historical_hold_outbox_leased",
        ),
        (
            "UPDATE rca_outbox SET source_topic = NULL, "
            "source_partition = NULL, source_offset = NULL",
            "activation_historical_hold_outbox_manual",
        ),
        (
            "UPDATE rca_outbox SET source_topic = 'unfenced-topic'",
            "activation_historical_hold_outbox_unfenced",
        ),
        (
            "UPDATE rca_outbox SET source_offset = 20",
            "activation_historical_hold_outbox_at_or_after_start_fence",
        ),
        (
            "UPDATE rca_outbox SET activation_epoch_id = 'other-epoch'",
            "activation_historical_hold_outbox_activation_bound",
        ),
    ],
)
def test_historical_hold_preauthorization_rejects_inexact_active_rows(
    tmp_path,
    mutation,
    error,
):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    store.ingest_record(_record(offset=10), policy=_policy(), submit_enabled=True)
    _create_activation_epoch(store, preauthorize=False)
    conn = store._connect()
    try:
        conn.execute(mutation)
    finally:
        conn.close()

    with pytest.raises(ActivationEpochError, match=error):
        _create_activation_epoch(store)

    assert store.list_rows("rca_activation_historical_outbox_holds") == []


def test_historical_hold_requires_drained_inbox_and_empty_current_ledger(tmp_path):
    pending_store = RcaControlStore(tmp_path / "pending.sqlite3")
    pending_store.ingest_record(
        _record(offset=10), policy=_policy(), submit_enabled=True
    )
    _create_activation_epoch(pending_store, preauthorize=False)
    conn = pending_store._connect()
    try:
        conn.execute("UPDATE kafka_inbox SET decision = 'pending'")
    finally:
        conn.close()
    with pytest.raises(
        ActivationEpochError,
        match="activation_historical_hold_pending_inbox",
    ):
        _create_activation_epoch(pending_store)

    ledger_store = RcaControlStore(tmp_path / "ledger.sqlite3")
    _create_activation_epoch(ledger_store, preauthorize=False)
    held = _adjudicate_activation(
        ledger_store,
        entrypoint="kafka_ingest",
        source_kind="kafka",
        source_identity={"event_uid": f"{TOPIC}:2:20"},
        business_key="held-business",
        submission_key="held-submission",
        generation=1,
        new_execution=True,
        activation_required=True,
    )
    assert held.decision == "shadow"
    with pytest.raises(
        ActivationEpochError,
        match="activation_historical_hold_current_ledger",
    ):
        _create_activation_epoch(ledger_store)


def test_historical_hold_is_immutable_and_idempotent_revalidation_detects_addition(
    tmp_path,
):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    store.ingest_record(
        _record(offset=10, value=_value(work_item_id=7041712800)),
        policy=_policy(),
        submit_enabled=True,
    )
    first = _create_activation_epoch(store)
    original = store.list_rows("rca_outbox")[0]

    evidence = store.activation_historical_outbox_hold_evidence(
        epoch_id=first["epoch_id"]
    )
    assert evidence["sealed_count"] == evidence["current_count"] == 1
    assert evidence["matches"] is True
    assert "7041712800" not in json.dumps(evidence, sort_keys=True)

    conn = store._connect()
    try:
        with pytest.raises(
            sqlite3.IntegrityError,
            match="activation_historical_outbox_update_forbidden",
        ):
            conn.execute(
                "UPDATE rca_outbox SET updated_at = updated_at WHERE outbox_id = ?",
                (original["outbox_id"],),
            )
        with pytest.raises(
            sqlite3.IntegrityError,
            match="activation_historical_outbox_delete_forbidden",
        ):
            conn.execute(
                "DELETE FROM rca_outbox WHERE outbox_id = ?",
                (original["outbox_id"],),
            )

        clone = dict(original)
        clone.pop("outbox_id")
        clone["submission_key"] = f"{original['submission_key']}-added"
        columns = tuple(clone)
        conn.execute(
            f"INSERT INTO rca_outbox({', '.join(columns)}) "
            f"VALUES ({', '.join('?' for _ in columns)})",
            tuple(clone[column] for column in columns),
        )
    finally:
        conn.close()

    drift = store.activation_historical_outbox_hold_evidence(epoch_id=first["epoch_id"])
    assert drift["sealed_count"] == 1
    assert drift["current_count"] == 2
    assert drift["matches"] is False
    with pytest.raises(
        ActivationEpochError,
        match="activation_historical_hold_cohort_changed",
    ):
        _create_activation_epoch(store)


def test_historical_hold_rejects_forged_current_epoch_binding_before_bounded(
    tmp_path,
):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    store.ingest_record(
        _record(offset=10, value=_value(work_item_id=7041712800)),
        policy=_policy(),
        submit_enabled=True,
    )
    epoch = _create_activation_epoch(store)
    original = store.list_rows("rca_outbox")[0]
    conn = store._connect()
    try:
        clone = dict(original)
        clone.pop("outbox_id")
        clone["submission_key"] = f"{original['submission_key']}-forged-current"
        clone["activation_epoch_id"] = epoch["epoch_id"]
        clone["activation_ledger_id"] = 999_999
        columns = tuple(clone)
        conn.execute(
            f"INSERT INTO rca_outbox({', '.join(columns)}) "
            f"VALUES ({', '.join('?' for _ in columns)})",
            tuple(clone[column] for column in columns),
        )
    finally:
        conn.close()

    with pytest.raises(
        ActivationEpochError,
        match="activation_historical_hold_current_epoch_binding_invalid",
    ):
        store.activation_historical_outbox_hold_evidence(epoch_id=epoch["epoch_id"])
    _authorize_activation_slots(store)
    with pytest.raises(
        ActivationEpochError,
        match="activation_historical_hold_current_epoch_binding_invalid",
    ):
        store.transition_activation_epoch(
            epoch_id=epoch["epoch_id"],
            expected_state="preauthorized",
            target_state="bounded_active",
            operator="release-test",
            reason="forged current binding must not cross the bounded gate",
        )


def test_historical_hold_schema_rejects_same_name_noop_trigger(tmp_path):
    path = tmp_path / "control.sqlite3"
    store = RcaControlStore(path)
    conn = store._connect()
    try:
        conn.execute("DROP TRIGGER trg_activation_historical_outbox_no_update")
        conn.execute(
            "CREATE TRIGGER trg_activation_historical_outbox_no_update "
            "BEFORE UPDATE ON rca_outbox BEGIN SELECT 1; END"
        )
    finally:
        conn.close()

    with pytest.raises(
        RuntimeError,
        match="incompatible_control_store_schema:historical_outbox_hold_trigger_sql",
    ):
        RcaControlStore(path, require_current=True)


@pytest.mark.parametrize(
    ("insert_sql", "error"),
    [
        (
            "INSERT INTO rca_activation_historical_outbox_hold_items("
            "epoch_id, outbox_id, row_sha256, immutable_row_sha256"
            ") SELECT 'missing-epoch', outbox_id, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', "
            "'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb' "
            "FROM rca_outbox LIMIT 1",
            "historical_outbox_hold_item_orphan",
        ),
        (
            "INSERT INTO rca_activation_historical_outbox_disposition_items("
            "disposition_id, outbox_id, row_sha256, immutable_row_sha256"
            ") SELECT 'missing-disposition', outbox_id, "
            "'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', "
            "'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb' "
            "FROM rca_outbox LIMIT 1",
            "historical_outbox_disposition_item_orphan",
        ),
    ],
)
def test_historical_hold_schema_rejects_orphan_items(
    tmp_path,
    insert_sql,
    error,
):
    path = tmp_path / "control.sqlite3"
    store = RcaControlStore(path)
    store.ingest_record(_record(offset=10), policy=_policy(), submit_enabled=True)
    conn = sqlite3.connect(path)
    try:
        conn.execute("PRAGMA foreign_keys=OFF")
        conn.execute(insert_sql)
        conn.commit()
    finally:
        conn.close()

    with pytest.raises(
        RuntimeError,
        match=f"incompatible_control_store_schema:{error}",
    ):
        RcaControlStore(path, require_current=True)


def test_historical_hold_row_hash_ignores_future_additive_columns(tmp_path):
    path = tmp_path / "control.sqlite3"
    store = RcaControlStore(path)
    store.ingest_record(
        _record(offset=10), policy=_policy(), submit_enabled=True
    )
    epoch = _create_activation_epoch(store)
    before = store.activation_historical_outbox_hold_evidence(
        epoch_id=epoch["epoch_id"]
    )
    conn = store._connect()
    try:
        conn.execute("ALTER TABLE rca_outbox ADD COLUMN future_additive_field TEXT")
    finally:
        conn.close()

    restarted = RcaControlStore(path, require_current=True)
    after = restarted.activation_historical_outbox_hold_evidence(
        epoch_id=epoch["epoch_id"]
    )
    assert after == before


def test_historical_hold_steady_allowlist_never_selects_sealed_rows(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    legacy = store.ingest_record(
        _record(offset=10), policy=_policy(), submit_enabled=True
    )
    epoch = _create_activation_epoch(store)
    conn = store._connect()
    try:
        conn.execute(
            "UPDATE rca_activation_epochs SET state = 'steady_active' "
            "WHERE epoch_id = ?",
            (epoch["epoch_id"],),
        )
    finally:
        conn.close()

    assert store.preview_dispatchable(
        activation_required=True,
        historical_submission_allowlist=[legacy.submission_key],
    ) == []
    assert store.claim_outbox(
        lease_owner="held-allowlist-test",
        activation_required=True,
        historical_submission_allowlist=[legacy.submission_key],
    ) is None


def test_historical_hold_owner_disposition_is_exact_immutable_and_non_replayable(
    tmp_path,
):
    path = tmp_path / "control.sqlite3"
    store = RcaControlStore(path)
    legacy = store.ingest_record(
        _record(offset=10), policy=_policy(), submit_enabled=True
    )
    epoch = _create_activation_epoch(store)
    evidence = store.activation_historical_outbox_hold_evidence(
        epoch_id=epoch["epoch_id"]
    )
    kwargs = {
        "epoch_id": epoch["epoch_id"],
        "expected_cohort_count": evidence["sealed_count"],
        "expected_cohort_sha256": evidence["sealed_sha256"],
        "owner_authorized": True,
        "operator": "owner-ou-test",
        "reason": "owner-approved exact historical isolation",
        "now": datetime(2026, 7, 27, 5, 0, tzinfo=timezone.utc),
    }
    with pytest.raises(
        ActivationEpochError,
        match="activation_historical_disposition_epoch_not_aborted",
    ):
        store.dispose_activation_historical_outbox_hold(**kwargs)
    store.transition_activation_epoch(
        epoch_id=epoch["epoch_id"],
        expected_state="preauthorized",
        target_state="aborted",
        operator="release-test",
        reason="abort before governed historical disposition",
    )
    with pytest.raises(
        ActivationEpochError,
        match="activation_historical_disposition_owner_authorization_required",
    ):
        store.dispose_activation_historical_outbox_hold(
            **{**kwargs, "owner_authorized": False}
        )
    with pytest.raises(
        ActivationEpochError,
        match="activation_historical_disposition_cohort_binding_changed",
    ):
        store.dispose_activation_historical_outbox_hold(
            **{**kwargs, "expected_cohort_count": evidence["sealed_count"] + 1}
        )
    with pytest.raises(
        ActivationEpochError,
        match="activation_historical_disposition_cohort_binding_changed",
    ):
        store.dispose_activation_historical_outbox_hold(
            **{**kwargs, "expected_cohort_sha256": "f" * 64}
        )

    disposed = store.dispose_activation_historical_outbox_hold(**kwargs)
    assert disposed == store.dispose_activation_historical_outbox_hold(**kwargs)
    assert disposed["cohort_count"] == 1
    assert disposed["cohort_sha256"] == evidence["sealed_sha256"]
    assert disposed["epoch_state"] == "aborted"
    assert disposed["owner_authorized"] is True
    [outbox] = store.list_rows("rca_outbox")
    assert outbox["submission_key"] == legacy.submission_key
    assert outbox["status"] == "quarantined"
    assert outbox["last_error_code"] == "activation_historical_hold_owner_disposed"
    [trigger] = store.list_rows("business_triggers")
    assert trigger["state"] == "quarantined"
    [audit] = store.list_rows("rca_activation_historical_outbox_dispositions")
    [audit_item] = store.list_rows(
        "rca_activation_historical_outbox_disposition_items"
    )
    [hold_item] = store.list_rows("rca_activation_historical_outbox_hold_items")
    assert audit["disposition_id"] == audit_item["disposition_id"]
    assert audit_item["outbox_id"] == hold_item["outbox_id"]
    assert audit_item["row_sha256"] == hold_item["row_sha256"]

    conn = store._connect()
    try:
        with pytest.raises(
            sqlite3.IntegrityError,
            match="activation_historical_disposed_outbox_update_forbidden",
        ):
            conn.execute(
                "UPDATE rca_outbox SET status = 'pending' WHERE outbox_id = ?",
                (outbox["outbox_id"],),
            )
        with pytest.raises(
            sqlite3.IntegrityError,
            match="activation_historical_outbox_delete_forbidden",
        ):
            conn.execute(
                "DELETE FROM rca_outbox WHERE outbox_id = ?",
                (outbox["outbox_id"],),
            )
        with pytest.raises(
            sqlite3.IntegrityError,
            match="activation_historical_disposition_update_forbidden",
        ):
            conn.execute(
                "UPDATE rca_activation_historical_outbox_dispositions "
                "SET reason = reason"
            )
        with pytest.raises(
            sqlite3.IntegrityError,
            match="activation_historical_disposition_item_update_forbidden",
        ):
            conn.execute(
                "UPDATE rca_activation_historical_outbox_disposition_items "
                "SET row_sha256 = row_sha256"
            )
        with pytest.raises(
            sqlite3.IntegrityError,
            match="activation_historical_disposition_item_delete_forbidden",
        ):
            conn.execute(
                "DELETE FROM rca_activation_historical_outbox_disposition_items"
            )
    finally:
        conn.close()
    with pytest.raises(
        ActivationEpochError,
        match="activation_historical_disposition_binding_conflict",
    ):
        store.dispose_activation_historical_outbox_hold(
            **{**kwargs, "reason": "conflicting owner disposition retry"}
        )
    restarted = RcaControlStore(path, require_current=True)
    assert restarted.list_rows("rca_outbox")[0]["status"] == "quarantined"
    assert restarted.claim_outbox(
        lease_owner="disposed-must-never-replay", activation_required=True
    ) is None


def test_empty_historical_hold_can_be_owner_disposed_after_abort(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    epoch = _create_activation_epoch(store)
    evidence = store.activation_historical_outbox_hold_evidence(
        epoch_id=epoch["epoch_id"]
    )
    store.transition_activation_epoch(
        epoch_id=epoch["epoch_id"],
        expected_state="preauthorized",
        target_state="aborted",
        operator="release-test",
        reason="abort empty historical cohort",
    )
    disposed = store.dispose_activation_historical_outbox_hold(
        epoch_id=epoch["epoch_id"],
        expected_cohort_count=0,
        expected_cohort_sha256=evidence["sealed_sha256"],
        owner_authorized=True,
        operator="owner-ou-test",
        reason="close empty historical cohort",
    )
    assert disposed["cohort_count"] == 0
    assert store.list_rows(
        "rca_activation_historical_outbox_disposition_items"
    ) == []


def test_owner_disposition_covers_identical_holds_from_superseded_aborted_epochs(
    tmp_path,
):
    path = tmp_path / "control.sqlite3"
    store = RcaControlStore(path)
    store.ingest_record(_record(offset=10), policy=_policy(), submit_enabled=True)
    first = _create_activation_epoch(store)
    store.transition_activation_epoch(
        epoch_id=first["epoch_id"],
        expected_state="preauthorized",
        target_state="aborted",
        operator="release-test",
        reason="abort first exact hold",
    )
    second = _create_activation_epoch(
        store,
        epoch_id="rca-release-20260712-retry",
    )
    second_evidence = store.activation_historical_outbox_hold_evidence(
        epoch_id=second["epoch_id"]
    )
    store.transition_activation_epoch(
        epoch_id=second["epoch_id"],
        expected_state="preauthorized",
        target_state="aborted",
        operator="release-test",
        reason="abort second exact hold",
    )

    disposed = store.dispose_activation_historical_outbox_hold(
        epoch_id=second["epoch_id"],
        expected_cohort_count=second_evidence["sealed_count"],
        expected_cohort_sha256=second_evidence["sealed_sha256"],
        owner_authorized=True,
        operator="owner-ou-test",
        reason="isolate identical historical cohort after both epochs aborted",
    )

    assert disposed["cohort_count"] == 1
    dispositions = store.list_rows(
        "rca_activation_historical_outbox_dispositions"
    )
    assert {row["epoch_id"] for row in dispositions} == {
        first["epoch_id"],
        second["epoch_id"],
    }
    assert {row["epoch_state"] for row in dispositions} == {"aborted"}
    disposition_items = store.list_rows(
        "rca_activation_historical_outbox_disposition_items"
    )
    assert len(disposition_items) == 2
    assert len({row["outbox_id"] for row in disposition_items}) == 1
    assert len({row["row_sha256"] for row in disposition_items}) == 1
    assert len({row["immutable_row_sha256"] for row in disposition_items}) == 1
    restarted = RcaControlStore(path, require_current=True)
    assert restarted.list_rows("rca_outbox")[0]["status"] == "quarantined"

    conn = restarted._connect()
    try:
        restarted._drop_v13_historical_outbox_hold_triggers(conn)
        first_disposition_id = next(
            row["disposition_id"]
            for row in dispositions
            if row["epoch_id"] == first["epoch_id"]
        )
        conn.execute(
            "DELETE FROM rca_activation_historical_outbox_disposition_items "
            "WHERE disposition_id = ?",
            (first_disposition_id,),
        )
        conn.execute(
            "DELETE FROM rca_activation_historical_outbox_dispositions "
            "WHERE disposition_id = ?",
            (first_disposition_id,),
        )
        restarted._create_v13_historical_outbox_hold_schema(conn)
    finally:
        conn.close()
    with pytest.raises(
        RuntimeError,
        match="incompatible_control_store_schema:historical_outbox_hold_row_binding",
    ):
        RcaControlStore(path, require_current=True)


@pytest.mark.parametrize(
    ("mutation", "error"),
    [
        (
            "UPDATE rca_outbox SET payload_json = 'offline-corruption'",
            "historical_outbox_disposition_row_binding",
        ),
        (
            "UPDATE rca_outbox SET next_attempt_at = '2026-07-28T00:00:00+00:00'",
            "historical_outbox_disposition_row_binding",
        ),
        (
            "UPDATE rca_activation_epochs SET state = 'steady_active' "
            "WHERE epoch_id = 'rca-release-20260712'",
            "historical_outbox_disposition_epoch_state",
        ),
    ],
)
def test_disposed_historical_hold_restart_rejects_audit_drift(
    tmp_path,
    mutation,
    error,
):
    path = tmp_path / "control.sqlite3"
    store = RcaControlStore(path)
    store.ingest_record(_record(offset=10), policy=_policy(), submit_enabled=True)
    epoch = _create_activation_epoch(store)
    evidence = store.activation_historical_outbox_hold_evidence(
        epoch_id=epoch["epoch_id"]
    )
    store.transition_activation_epoch(
        epoch_id=epoch["epoch_id"],
        expected_state="preauthorized",
        target_state="aborted",
        operator="release-test",
        reason="abort before audit-drift test",
    )
    store.dispose_activation_historical_outbox_hold(
        epoch_id=epoch["epoch_id"],
        expected_cohort_count=evidence["sealed_count"],
        expected_cohort_sha256=evidence["sealed_sha256"],
        owner_authorized=True,
        operator="owner-ou-test",
        reason="seal exact disposition before audit-drift test",
    )
    conn = store._connect()
    try:
        store._drop_v13_historical_outbox_hold_triggers(conn)
        conn.execute(mutation)
        store._create_v13_historical_outbox_hold_schema(conn)
    finally:
        conn.close()

    with pytest.raises(
        RuntimeError,
        match=f"incompatible_control_store_schema:{error}",
    ):
        RcaControlStore(path, require_current=True)


def test_held_epoch_claim_has_no_historical_age_quarantine_side_effect(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    legacy = store.ingest_record(
        _record(offset=10), policy=_policy(), submit_enabled=True
    )
    _create_activation_epoch(store, preauthorize=False)

    claim = store.claim_outbox(
        lease_owner="held-dispatcher",
        activation_required=True,
        max_age_seconds=1,
        now=datetime.now(timezone.utc) + timedelta(days=2),
    )

    assert claim is None
    [outbox] = store.list_rows("rca_outbox")
    assert outbox["submission_key"] == legacy.submission_key
    assert outbox["status"] == "pending"
    [trigger] = store.list_rows("business_triggers")
    assert trigger["state"] == "pending"


def test_activation_required_without_epoch_blocks_before_persist_or_commit(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")

    with pytest.raises(ActivationEpochError, match="activation_ingress_unavailable"):
        store.ingest_record(
            _record(offset=20),
            policy=_policy(),
            submit_enabled=True,
            activation_required=True,
            activation_slot_kind="kafka_success",
        )

    assert store.list_rows("kafka_inbox") == []
    assert store.list_rows("rca_outbox") == []
    assert store.claim_outbox(
        lease_owner="required-dispatcher", activation_required=True
    ) is None


def test_kafka_ingress_lineage_reuses_original_epoch_and_rejects_intent_change(
    tmp_path,
):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    _create_activation_epoch(store, epoch_id="rca-release-original")
    _authorize_activation_slots(store, epoch_id="rca-release-original")
    store.transition_activation_epoch(
        epoch_id="rca-release-original",
        expected_state="preauthorized",
        target_state="bounded_active",
        operator="release-test",
        reason="create one bounded shadow for lineage recovery",
    )
    first = store.persist_raw(
        _record(offset=21),
        policy=_policy(),
        submit_enabled=True,
        activation_required=True,
        activation_slot_kind="",
    )
    with pytest.raises(
        ActivationEpochError, match="activation_pending_inbox_not_drained"
    ):
        store.transition_activation_epoch(
            epoch_id="rca-release-original",
            target_state="aborted",
            operator="release-test",
            reason="pending ingress must be resolved before abort",
        )
    processed = store.process_event(_record(offset=21).event_uid)
    assert processed.decision == "accepted"
    assert processed.reason == "activation_bounded_slot_required"
    with pytest.raises(RecordConflictError, match="changed ingress intent"):
        store.persist_raw(
            _record(offset=21),
            policy=_policy(),
            submit_enabled=True,
            activation_required=False,
            activation_slot_kind="",
        )
    store.transition_activation_epoch(
        epoch_id="rca-release-original",
        target_state="aborted",
        operator="release-test",
        reason="replace the candidate without rebinding durable ingress",
    )
    with pytest.raises(
        ActivationEpochError, match="activation_shadow_backlog_not_disposed"
    ):
        _create_activation_epoch(store, epoch_id="rca-release-replacement")
    deferred = store.defer_activation_event(
        _record(offset=21).event_uid,
        expected_activation_epoch_id="rca-release-original",
        operator="release-test",
        reason="reviewed recovery will retrigger this issue after replacement",
    )
    assert deferred.prior_status == "shadow"
    assert deferred.status == "quarantined"
    _create_activation_epoch(store, epoch_id="rca-release-replacement")

    with pytest.raises(
        ActivationIngressDeferredError, match="activation_ingress_unavailable"
    ):
        store.persist_raw(
            _record(offset=21),
            policy=_policy(),
            submit_enabled=True,
            activation_required=True,
            activation_slot_kind="",
        )

    assert first.inserted is True
    [inbox] = store.list_rows("kafka_inbox")
    assert inbox["activation_epoch_id"] == "rca-release-original"
    assert inbox["activation_ingress_state"] == "bounded_active"
    [outbox] = store.list_rows("rca_outbox")
    assert outbox["status"] == "quarantined"
    assert outbox["last_error_code"] == "activation_epoch_deferred"
    assert store.health()["activation"]["backlog"]["deferred_quarantined"] == 1
    assert store.list_rows("rca_shadow_promotion_audit")[-1]["outcome"] == "deferred"


def test_activation_replacement_requires_bound_pending_outbox_to_be_deferred(
    tmp_path,
):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    _create_activation_epoch(
        store,
        epoch_id="rca-release-original-pending",
        start_offset=22,
    )
    conn = store._connect()
    try:
        conn.execute(
            "UPDATE rca_activation_epochs SET state = 'steady_active' "
            "WHERE epoch_id = 'rca-release-original-pending'"
        )
    finally:
        conn.close()
    pending = store.ingest_record(
        _record(offset=22),
        policy=_policy(),
        submit_enabled=True,
        activation_required=True,
    )
    assert pending.decision == "accepted"
    [outbox] = store.list_rows("rca_outbox")
    assert outbox["status"] == "pending"
    assert outbox["activation_epoch_id"] == "rca-release-original-pending"

    store.transition_activation_epoch(
        epoch_id="rca-release-original-pending",
        expected_state="steady_active",
        target_state="aborted",
        operator="release-test",
        reason="abort before reviewed replacement",
    )
    with pytest.raises(
        ActivationEpochError,
        match="activation_bound_outbox_backlog_not_drained",
    ):
        _create_activation_epoch(
            store,
            epoch_id="rca-release-replacement-pending",
            start_offset=23,
            preauthorize=False,
        )
    assert store.activation_epoch()["epoch_id"] == "rca-release-original-pending"

    deferred = store.defer_activation_event(
        _record(offset=22).event_uid,
        expected_activation_epoch_id="rca-release-original-pending",
        operator="release-test",
        reason="reviewed retry after replacement",
    )
    assert deferred.prior_status == "pending"
    assert deferred.status == "quarantined"
    replacement = _create_activation_epoch(
        store,
        epoch_id="rca-release-replacement-pending",
        start_offset=23,
        preauthorize=False,
    )
    assert replacement["state"] == "safe_off"
    assert replacement["epoch_id"] == "rca-release-replacement-pending"


def test_activation_replacement_can_defer_one_exact_manual_message_id(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    identities = _begin_bounded_activation(store)
    manual = identities["manual_success"][1]
    admitted = store.admit_manual_trigger(
        _manual_request(
            manual["message_id"],
            issue_url=manual["issue_url"],
            thread_id=manual["thread_id"],
        ),
        allowed_chat_ids={"oc_allowed"},
        submit_enabled=True,
        active_policy=_policy(),
        activation_required=True,
    )
    store.transition_activation_epoch(
        epoch_id="rca-release-20260712",
        expected_state="bounded_active",
        target_state="aborted",
        operator="release-test",
        reason="reviewed successor replacement for exact manual canary",
    )

    deferred = store.defer_activation_event(
        manual["message_id"],
        expected_activation_epoch_id="rca-release-20260712",
        operator="release-test",
        reason="exact manual canary will be re-sent in successor epoch",
    )
    repeated = store.defer_activation_event(
        manual["message_id"],
        expected_activation_epoch_id="rca-release-20260712",
        operator="release-test",
        reason="idempotent repeat of exact manual deferral",
    )

    assert deferred.prior_status == "pending"
    assert deferred.status == "quarantined"
    assert repeated.prior_status == "quarantined"
    assert repeated.status == "quarantined"
    assert deferred.outbox_id == repeated.outbox_id
    outbox = {
        row["outbox_id"]: row for row in store.list_rows("rca_outbox")
    }[deferred.outbox_id]
    assert outbox["status"] == "quarantined"
    assert outbox["last_error_code"] == "activation_epoch_deferred"
    [trigger] = [
        row
        for row in store.list_rows("business_triggers")
        if row["business_key"] == admitted.business_key
    ]
    assert trigger["state"] == "quarantined"
    assert RcaDeliveryStore(store.db_path).list_rows("rca_delivery_jobs") == []


def test_activation_replacement_requires_bound_quarantine_delivery_to_settle(
    tmp_path,
):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    _create_activation_epoch(
        store,
        epoch_id="rca-release-original-quarantined",
        start_offset=22,
    )
    conn = store._connect()
    try:
        conn.execute(
            "UPDATE rca_activation_epochs SET state = 'steady_active' "
            "WHERE epoch_id = 'rca-release-original-quarantined'"
        )
    finally:
        conn.close()
    pending = store.ingest_record(
        _record(offset=22),
        policy=_policy(),
        submit_enabled=True,
        activation_required=True,
    )
    assert pending.decision == "accepted"
    claim = store.claim_outbox(lease_owner="quarantine-drain-test")
    assert claim is not None
    store.quarantine_outbox(
        outbox_id=claim.outbox_id,
        lease_token=claim.lease_token,
        error_code="business_profile_unsupported",
        error_detail="profile terminal must be materialized before cutover",
    )
    store.transition_activation_epoch(
        epoch_id="rca-release-original-quarantined",
        expected_state="steady_active",
        target_state="aborted",
        operator="release-test",
        reason="abort before delivery settlement",
    )
    with pytest.raises(
        ActivationEpochError,
        match="activation_bound_delivery_backlog_not_drained",
    ):
        _create_activation_epoch(
            store,
            epoch_id="rca-release-replacement-quarantined",
            start_offset=23,
            preauthorize=False,
        )
    assert store.activation_epoch()["epoch_id"] == (
        "rca-release-original-quarantined"
    )


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
    assert mutation.status == "quarantined"
    return claim.outbox_id, window_started


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


def _silent_terminal_activation(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    identities = _begin_bounded_activation(store)
    success_identity = identities["manual_success"][1]
    terminal_identity = identities["manual_terminal_failure"][1]
    success = store.admit_manual_trigger(
        _manual_request(
            success_identity["message_id"],
            issue_url=success_identity["issue_url"],
            thread_id=success_identity["thread_id"],
        ),
        allowed_chat_ids={"oc_allowed"},
        submit_enabled=True,
        active_policy=_policy(),
        activation_required=True,
    )
    terminal = store.admit_manual_trigger(
        _manual_request(
            terminal_identity["message_id"],
            mode=terminal_identity["mode"],
            issue_url=terminal_identity["issue_url"],
            thread_id=terminal_identity["thread_id"],
        ),
        allowed_chat_ids={"oc_allowed"},
        submit_enabled=True,
        operator_authorized=True,
        active_policy=_policy(),
        activation_required=True,
    )
    current = datetime.now(timezone.utc)
    for index, expected in enumerate((success, terminal), start=1):
        claim = store.claim_outbox(
            lease_owner=f"activation-silent-terminal-{index}",
            activation_required=True,
            now=current + timedelta(seconds=index),
        )
        assert claim is not None and claim.submission_key == expected.submission_key
        store.complete_outbox(
            outbox_id=claim.outbox_id,
            lease_token=claim.lease_token,
            result={
                "success": True,
                "submission_key": claim.submission_key,
                "task_id": claim.submission_key,
            },
            now=current + timedelta(seconds=index, milliseconds=100),
        )

    delivery = RcaDeliveryStore(store.db_path)
    assert delivery.backfill_completed_submissions(
        now=current + timedelta(seconds=3), activation_required=True
    ) == 2
    conn = store._connect()
    try:
        conn.execute(
            "UPDATE rca_execution_watch SET next_poll_at = ? "
            "WHERE submission_key = ?",
            ((current + timedelta(days=1)).isoformat(), success.submission_key),
        )
        conn.commit()
    finally:
        conn.close()
    watch_claim = delivery.claim_due_watch(
        lease_owner="activation-silent-terminal-collector",
        now=current + timedelta(seconds=4),
        activation_required=True,
    )
    assert watch_claim is not None
    assert watch_claim.submission_key == terminal.submission_key
    error_code = "rca_work_deadline_exceeded"
    route = delivery.upsert_failure_route(
        claim=watch_claim,
        terminal_error_code=error_code,
        lane="hard_defect",
        route_kind="internal_alert",
        owner="rca-engineering",
        work_started_at=watch_claim.work_started_at,
        deadline_at=(current + timedelta(minutes=30)).isoformat(),
        audit={
            "schema_version": "pnc_rca_failure_route_audit_v1",
            "taxonomy_audit": {},
            "contract_errors": [],
            "source": "before_admission",
            "receipt": {},
        },
        route_payload={
            "schema_version": "pnc_rca_failure_route_payload_v1",
            "decision": {},
            "remediation": {},
            "blocker": {"kind": error_code},
        },
        now=current + timedelta(seconds=4),
    )
    fallback = {
        "schema_version": "pnc_rca_bounded_terminal_fallback_v1",
        "route_key": route.route_key,
        "route_kind": "internal_alert",
        "route_owner": "rca-engineering",
        "terminal_class": "honest_non_attribution",
        "confidence_tier": "low",
        "work_started_at": watch_claim.work_started_at,
        "deadline_at": (current + timedelta(minutes=30)).isoformat(),
        "elapsed_seconds": 1800,
    }
    status = {
        "external_writes": False,
        "failure_taxonomy": {
            "known": True,
            "retryable": False,
            "lane": "hard_defect",
            "internal_route": "internal_alert",
            "terminal_error_code": error_code,
            "terminal_fallback": fallback,
            "durable_route": {
                "route_key": route.route_key,
                "status": route.status,
                "owner": route.owner,
                "internal_outlet": {
                    "route_key": route.route_key,
                    "status": "settled",
                    "attempt": 1,
                    "external_effects": 0,
                },
            },
        },
        "terminal_delivery_policy": "silent_internal_alert_only",
    }
    delivery.terminal_failure(
        submission_key=watch_claim.submission_key,
        lease_token=watch_claim.lease_token,
        status=status,
        error_code=error_code,
        error_detail="RCA work did not produce a deliverable result within 30 minutes",
        now=current + timedelta(seconds=5),
    )
    return store, terminal, status


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


def test_official_business_profile_option_change_creates_next_generation(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    policy = _profile_snapshot_policy()
    first = store.ingest_record(
        _profile_snapshot_record(101, "6670325063"),
        policy=policy,
        submit_enabled=True,
    )
    assert first.decision == "accepted"
    assert first.generation == 1
    _terminalize_permanent(store, first.submission_key)

    unchanged = store.ingest_record(
        _profile_snapshot_record(102, "6670325063"),
        policy=policy,
        submit_enabled=True,
    )
    assert unchanged.generation == 1
    assert unchanged.submission_key == first.submission_key

    changed = store.ingest_record(
        _profile_snapshot_record(103, "7019637554"),
        policy=policy,
        submit_enabled=True,
    )
    assert changed.decision == "accepted"
    assert changed.generation == 2
    assert changed.business_key == first.business_key
    assert changed.submission_key != first.submission_key
    latest = max(store.list_rows("business_triggers"), key=lambda row: row["generation"])
    normalized = json.loads(latest["normalized_json"])
    assert normalized["business_profile_resolution"]["profile_id"] == "mdrive4"
    [source] = [
        row
        for row in store.list_rows("rca_trigger_sources")
        if row["source_id"] == latest["origin_source_id"]
    ]
    assert source["source_kind"] == "kafka_workflow_event"


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
    store = RcaControlStore(path)
    store.ingest_record(_record(), policy=_policy(), submit_enabled=True)
    conn = store._connect()
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
    store = RcaControlStore(path)
    store.ingest_record(_record(), policy=_policy(), submit_enabled=True)
    conn = store._connect()
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
    store = RcaControlStore(path)
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
    store = RcaControlStore(tmp_path / "control.sqlite3")
    first = store.ingest_record(
        _record(10), policy=_policy(), submit_enabled=True
    )
    original_outbox_id, original_window = _quarantine_for_input_wait(store)
    quarantined = store.list_rows("rca_outbox")[0]
    prior_fence = quarantined["fence"]
    assert quarantined["attempt"] == 1

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
        "prior_attempt": 1,
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


def test_settled_input_wait_terminal_creates_canonical_next_generation(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    first = store.ingest_record(
        _record(10), policy=_policy(), submit_enabled=True
    )
    _terminalize_input_wait(store, first.submission_key)
    delivery = RcaDeliveryStore(store.db_path)
    [old_trigger_before] = store.list_rows("business_triggers")
    [old_outbox_before] = store.list_rows("rca_outbox")
    [old_watch_before] = delivery.list_rows("rca_execution_watch")
    [old_job_before] = delivery.list_rows("rca_delivery_jobs")
    old_effects_before = delivery.list_rows("rca_delivery_effects")
    old_source_id = old_trigger_before["origin_source_id"]
    [old_binding_before] = [
        row
        for row in store.list_rows("rca_trigger_bindings")
        if row["source_id"] == old_source_id
    ]

    replacement = store.ingest_record(
        _record(11, value=_value(updated_at=1783659999999)),
        policy=_policy(),
        submit_enabled=True,
    )
    expected = build_rca_admission(
        project_key="project-key",
        project_simple_name="g1q3",
        work_item_type_key="problem-type",
        work_item_id="7041712812",
        rule_version="issue-created-v1",
        trigger_kind="kafka_retrigger",
        generation=2,
        topic=TOPIC,
        partition=2,
        offset=11,
    )

    assert replacement.decision == "accepted"
    assert replacement.reason == INPUT_WAIT_TERMINAL_NEW_GENERATION_REASON
    assert replacement.rearm_reason == INPUT_WAIT_TERMINAL_NEW_GENERATION_REASON
    assert replacement.outbox_rearmed is False
    assert replacement.trigger_created is replacement.outbox_created is True
    assert replacement.business_key == first.business_key == expected.business_key
    assert replacement.submission_key == expected.submission_key
    assert replacement.submission_key != first.submission_key
    assert replacement.generation == 2

    triggers = store.list_rows("business_triggers")
    outboxes = store.list_rows("rca_outbox")
    assert [row["generation"] for row in triggers] == [1, 2]
    assert [row["generation"] for row in outboxes] == [1, 2]
    assert triggers[0] == old_trigger_before
    assert outboxes[0] == old_outbox_before
    assert delivery.list_rows("rca_execution_watch") == [old_watch_before]
    assert delivery.list_rows("rca_delivery_jobs") == [old_job_before]
    assert delivery.list_rows("rca_delivery_effects") == old_effects_before

    new_trigger = triggers[1]
    new_outbox = outboxes[1]
    assert new_trigger["origin_source_id"] != old_source_id
    assert new_trigger["source_event_id"] == replacement.event_uid
    assert new_trigger["source_offset"] == 11
    assert new_outbox["origin_source_id"] == new_trigger["origin_source_id"]
    assert new_outbox["status"] == "pending"
    [new_source] = [
        row
        for row in store.list_rows("rca_trigger_sources")
        if row["source_id"] == new_trigger["origin_source_id"]
    ]
    assert new_source["source_kind"] == "kafka_workflow_event"
    assert new_source["mode"] == "kafka_retrigger"
    assert new_source["kafka_event_uid"] is None
    assert new_source["source_dedupe_key"].endswith(":generation:2")
    new_payload = json.loads(new_outbox["payload_json"])
    assert new_payload["admission"]["trigger_kind"] == "kafka_retrigger"
    assert new_payload["admission"] == expected.to_dict()
    assert new_payload["source_event_id"] == replacement.event_uid
    assert new_payload["offset"] == 11

    bindings = store.list_rows("rca_trigger_bindings")
    assert next(row for row in bindings if row["source_id"] == old_source_id) == (
        old_binding_before
    )
    [new_binding] = [
        row
        for row in bindings
        if row["source_id"] == new_trigger["origin_source_id"]
    ]
    assert new_binding["business_key"] == first.business_key
    assert new_binding["generation"] == 2
    assert new_binding["role"] == "origin"


def test_same_kafka_event_uses_generation_bound_retrigger_source_identity(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    first = store.ingest_record(
        _record(10), policy=_policy(), submit_enabled=True
    )
    _terminalize_input_wait(store, first.submission_key)
    [original_trigger] = store.list_rows("business_triggers")
    [original_outbox] = store.list_rows("rca_outbox")
    [original_source] = store.list_rows("rca_trigger_sources")
    [original_binding] = store.list_rows("rca_trigger_bindings")

    second = store.ingest_record(
        _record(10), policy=_policy(), submit_enabled=True
    )
    expected = build_rca_admission(
        project_key="project-key",
        project_simple_name="g1q3",
        work_item_type_key="problem-type",
        work_item_id="7041712812",
        rule_version="issue-created-v1",
        trigger_kind="kafka_retrigger",
        generation=2,
        topic=TOPIC,
        partition=2,
        offset=10,
    )

    assert second.decision == "accepted"
    assert second.reason == INPUT_WAIT_TERMINAL_NEW_GENERATION_REASON
    assert second.rearm_reason == INPUT_WAIT_TERMINAL_NEW_GENERATION_REASON
    assert second.transport_duplicate is True
    assert second.raw_inserted is False
    assert second.trigger_created is second.outbox_created is True
    assert second.business_key == first.business_key == expected.business_key
    assert second.submission_key == expected.submission_key
    assert second.generation == 2

    sources = store.list_rows("rca_trigger_sources")
    bindings = store.list_rows("rca_trigger_bindings")
    triggers = store.list_rows("business_triggers")
    outboxes = store.list_rows("rca_outbox")
    assert triggers[0] == original_trigger
    assert outboxes[0] == original_outbox
    assert next(
        row for row in sources if row["source_id"] == original_source["source_id"]
    ) == original_source
    assert next(
        row for row in bindings if row["source_id"] == original_binding["source_id"]
    ) == original_binding
    derived_source_id = triggers[1]["origin_source_id"]
    assert derived_source_id != original_source["source_id"]
    assert outboxes[1]["origin_source_id"] == derived_source_id
    assert triggers[1]["source_event_id"] == first.event_uid
    assert outboxes[1]["source_event_id"] == first.event_uid
    derived_source = next(
        row for row in sources if row["source_id"] == derived_source_id
    )
    derived_binding = next(
        row for row in bindings if row["source_id"] == derived_source_id
    )
    assert derived_source["source_dedupe_key"].endswith(":generation:2")
    assert derived_source["kafka_event_uid"] is None
    assert derived_source["mode"] == "kafka_retrigger"
    assert derived_source["payload_sha256"] == original_source["payload_sha256"]
    assert derived_binding["generation"] == 2
    assert derived_binding["role"] == "origin"
    payload = json.loads(outboxes[1]["payload_json"])
    assert payload["admission"] == expected.to_dict()
    assert payload["origin_source_id"] == derived_source_id
    assert payload["source_event_id"] == first.event_uid
    assert (payload["topic"], payload["partition"], payload["offset"]) == (
        TOPIC,
        2,
        10,
    )

    third = store.ingest_record(
        _record(10), policy=_policy(), submit_enabled=True
    )
    assert third.transport_duplicate is True
    assert third.raw_inserted is False
    assert third.trigger_created is third.outbox_created is False
    assert third.submission_key == second.submission_key
    assert third.generation == 2
    assert len(store.list_rows("rca_trigger_sources")) == 2
    assert len(store.list_rows("rca_trigger_bindings")) == 2
    assert len(store.list_rows("business_triggers")) == 2
    assert len(store.list_rows("rca_outbox")) == 2

    _terminalize_input_wait(store, second.submission_key)
    terminal_triggers = store.list_rows("business_triggers")
    terminal_outboxes = store.list_rows("rca_outbox")
    fourth = store.ingest_record(
        _record(10), policy=_policy(), submit_enabled=True
    )
    assert fourth.transport_duplicate is True
    assert fourth.generation == 2
    assert fourth.submission_key == second.submission_key
    assert fourth.trigger_created is fourth.outbox_created is False
    assert store.list_rows("business_triggers") == terminal_triggers
    assert store.list_rows("rca_outbox") == terminal_outboxes
    with store._connect() as check:
        assert check.execute("PRAGMA foreign_key_check").fetchone() is None


def test_kafka_origin_contract_rederives_complete_source_identity(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    first = store.ingest_record(
        _record(10), policy=_policy(), submit_enabled=True
    )
    _terminalize_input_wait(store, first.submission_key)
    second = store.ingest_record(
        _record(10), policy=_policy(), submit_enabled=True
    )
    assert second.generation == 2

    with store._connect() as conn:
        row = store._select_latest_kafka_issue_generation_tx(
            conn,
            project_key="project-key",
            work_item_type_key="problem-type",
            work_item_id="7041712812",
        )
    assert row is not None
    assert store._kafka_generation_origin_contract_valid(row) is True

    forged = dict(row)
    forged_source_id = "g1q3-rca-source-v1-" + "f" * 64
    for field in (
        "origin_source_id",
        "kafka_origin_source_id",
        "outbox_origin_source_id",
    ):
        forged[field] = forged_source_id
    payload = json.loads(forged["outbox_payload_json"])
    payload["origin_source_id"] = forged_source_id
    forged["outbox_payload_json"] = json.dumps(payload)

    assert store._kafka_generation_origin_contract_valid(forged) is False


def test_same_kafka_event_with_pruned_raw_never_creates_retrigger(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    first = store.ingest_record(
        _record(10), policy=_policy(), submit_enabled=True
    )
    _terminalize_input_wait(store, first.submission_key)
    current = datetime.now(timezone.utc).isoformat()
    conn = store._connect()
    try:
        conn.execute(
            "UPDATE kafka_inbox SET raw_value=X'', raw_pruned_at=? WHERE event_uid=?",
            (current, first.event_uid),
        )
    finally:
        conn.close()
    terminal_triggers = store.list_rows("business_triggers")
    terminal_outboxes = store.list_rows("rca_outbox")
    terminal_inbox = store.get_inbox(first.event_uid)

    replay = store.ingest_record(
        _record(10), policy=_policy(), submit_enabled=True
    )

    assert replay.transport_duplicate is True
    assert replay.generation == 1
    assert replay.submission_key == first.submission_key
    assert replay.trigger_created is replay.outbox_created is False
    assert store.list_rows("business_triggers") == terminal_triggers
    assert store.list_rows("rca_outbox") == terminal_outboxes
    assert store.get_inbox(first.event_uid) == terminal_inbox


def test_unsettled_input_wait_terminal_blocks_rearm_and_next_generation(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    first = store.ingest_record(
        _record(10), policy=_policy(), submit_enabled=True
    )
    _terminalize_input_wait(store, first.submission_key, settle=False)
    delivery = RcaDeliveryStore(store.db_path)
    old_trigger = dict(store.list_rows("business_triggers")[0])
    old_outbox = dict(store.list_rows("rca_outbox")[0])

    replacement = store.ingest_record(
        _record(11, value=_value(updated_at=1783659999999)),
        policy=_policy(),
        submit_enabled=True,
    )

    assert replacement.decision == "deduped"
    assert replacement.submission_key == first.submission_key
    assert replacement.generation == 1
    assert replacement.outbox_rearmed is False
    assert replacement.rearm_reason == INPUT_WAIT_EXECUTION_WATCH_PRESENT_REASON
    assert store.list_rows("business_triggers") == [old_trigger]
    assert store.list_rows("rca_outbox") == [old_outbox]
    assert {row["status"] for row in delivery.list_rows("rca_delivery_effects")} == {
        "pending"
    }


def test_concurrent_terminal_replacements_create_one_generation_and_replay_exactly(
    tmp_path,
):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    first = store.ingest_record(
        _record(10), policy=_policy(), submit_enabled=True
    )
    _terminalize_input_wait(store, first.submission_key)
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

    creators = [result for result in results if result.trigger_created]
    assert len(creators) == 1
    creator = creators[0]
    assert creator.rearm_reason == INPUT_WAIT_TERMINAL_NEW_GENERATION_REASON
    assert {result.generation for result in results} == {2}
    assert len({result.submission_key for result in results}) == 1
    assert creator.submission_key != first.submission_key
    assert [row["generation"] for row in store.list_rows("business_triggers")] == [
        1,
        2,
    ]
    assert len(store.list_rows("rca_outbox")) == 2

    creator_offset = int(creator.event_uid.rsplit(":", 1)[1])
    replay = store.ingest_record(
        _record(creator_offset, value=payloads[creator_offset]),
        policy=_policy(),
        submit_enabled=True,
    )
    assert replay.transport_duplicate is True
    assert replay.generation == 2
    assert replay.submission_key == creator.submission_key
    assert replay.rearm_reason == INPUT_WAIT_TERMINAL_NEW_GENERATION_REASON
    assert len(store.list_rows("business_triggers")) == 2
    assert len(store.list_rows("rca_outbox")) == 2


def test_terminal_replacement_transaction_rolls_back_and_retries_cleanly(
    tmp_path, monkeypatch
):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    first = store.ingest_record(
        _record(10), policy=_policy(), submit_enabled=True
    )
    _terminalize_input_wait(store, first.submission_key)
    old_trigger = dict(store.list_rows("business_triggers")[0])
    old_outbox = dict(store.list_rows("rca_outbox")[0])
    replacement_record = _record(11, value=_value(updated_at=1783659999999))
    raw = store.persist_raw(
        replacement_record, policy=_policy(), submit_enabled=True
    )
    original_insert = store._insert_issue_subscription_tx

    def fail_after_generation_created(*_args, **_kwargs):
        raise RuntimeError("injected_after_generation_create")

    monkeypatch.setattr(
        store, "_insert_issue_subscription_tx", fail_after_generation_created
    )
    with pytest.raises(RecordProcessingBlockedError):
        store.process_event_resilient(raw.event_uid)

    assert store.list_rows("business_triggers") == [old_trigger]
    assert store.list_rows("rca_outbox") == [old_outbox]
    assert len(store.list_rows("rca_trigger_sources")) == 1
    failed_inbox = store.get_inbox(raw.event_uid)
    assert failed_inbox["decision"] == "pending"
    assert failed_inbox["processing_attempts"] == 1

    monkeypatch.setattr(store, "_insert_issue_subscription_tx", original_insert)
    recovered = store.process_event_resilient(raw.event_uid)
    assert recovered.trigger_created is recovered.outbox_created is True
    assert recovered.generation == 2
    assert recovered.rearm_reason == INPUT_WAIT_TERMINAL_NEW_GENERATION_REASON
    assert len(store.list_rows("business_triggers")) == 2
    assert len(store.list_rows("rca_outbox")) == 2


def test_terminal_replacement_kafka_is_held_during_bounded_activation(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    first = store.ingest_record(
        _record(10), policy=_policy(), submit_enabled=True
    )
    _terminalize_input_wait(store, first.submission_key)
    _begin_bounded_activation(store, kafka_offset=20)

    replacement = store.ingest_record(
        _record(20, value=_value(updated_at=1783659999999)),
        policy=_policy(),
        submit_enabled=True,
        activation_required=True,
        activation_slot_kind="kafka_success",
    )

    assert replacement.generation == 0
    assert replacement.outbox_created is False
    assert replacement.reason == "activation_existing_generation_not_eligible"
    assert len(store.list_rows("rca_outbox")) == 1
    kafka_slot = next(
        row
        for row in store.list_rows("rca_activation_budget_slots")
        if row["slot_kind"] == "kafka_success"
    )
    assert kafka_slot["consumed_ledger_id"] is None


def test_failed_next_generation_advances_again_without_rearming_old_baseline(
    tmp_path,
):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    first = store.ingest_record(
        _record(10), policy=_policy(), submit_enabled=True
    )
    _terminalize_input_wait(store, first.submission_key)
    second = store.ingest_record(
        _record(11, value=_value(updated_at=1783659999999)),
        policy=_policy(),
        submit_enabled=True,
    )
    assert second.generation == 2
    generation_one_trigger = dict(store.list_rows("business_triggers")[0])
    generation_one_outbox = dict(store.list_rows("rca_outbox")[0])
    delivery = RcaDeliveryStore(store.db_path)
    generation_one_watch = dict(delivery.list_rows("rca_execution_watch")[0])
    generation_one_job = dict(delivery.list_rows("rca_delivery_jobs")[0])

    _quarantine_for_input_wait(store, submission_key=second.submission_key)
    assert delivery.backfill_completed_submissions() == 1
    _settle_delivery(store, second.submission_key)
    third = store.ingest_record(
        _record(12, value=_value(updated_at=1783660000000)),
        policy=_policy(),
        submit_enabled=True,
    )

    assert third.generation == 3
    assert third.submission_key not in {
        first.submission_key,
        second.submission_key,
    }
    assert third.rearm_reason == INPUT_WAIT_TERMINAL_NEW_GENERATION_REASON
    assert [row["generation"] for row in store.list_rows("business_triggers")] == [
        1,
        2,
        3,
    ]
    assert store.list_rows("business_triggers")[0] == generation_one_trigger
    assert store.list_rows("rca_outbox")[0] == generation_one_outbox
    assert delivery.list_rows("rca_execution_watch")[0] == generation_one_watch
    assert delivery.list_rows("rca_delivery_jobs")[0] == generation_one_job
    generation_two_outbox = store.list_rows("rca_outbox")[1]
    assert generation_two_outbox["status"] == "quarantined"
    assert generation_two_outbox["submission_key"] == second.submission_key


def test_kafka_update_binds_latest_kafka_origin_after_newer_manual_generation(
    tmp_path,
):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    first = store.ingest_record(
        _record(10), policy=_policy(), submit_enabled=True
    )
    _terminalize_input_wait(store, first.submission_key)
    second = store.ingest_record(
        _record(11, value=_value(updated_at=1783659999999)),
        policy=_policy(),
        submit_enabled=True,
    )
    assert second.generation == 2
    _terminalize_permanent(store, second.submission_key)
    manual = store.admit_manual_trigger(
        _manual_request(
            "om_generation_three",
            mode="rerun",
            thread_id="topic:om_generation_three_root",
        ),
        allowed_chat_ids={"oc_allowed"},
        submit_enabled=True,
        operator_authorized=True,
    )
    assert manual.generation == 3

    observed = store.ingest_record(
        _record(12, value=_value(updated_at=1783660000000)),
        policy=_policy(),
        submit_enabled=True,
    )

    assert observed.decision == "deduped"
    assert observed.generation == 2
    assert observed.submission_key == second.submission_key
    observed_source = next(
        row
        for row in store.list_rows("rca_trigger_sources")
        if row["source_dedupe_key"]
        == f"{observed.event_uid}:generation:{second.generation}"
    )
    observed_binding = next(
        row
        for row in store.list_rows("rca_trigger_bindings")
        if row["source_id"] == observed_source["source_id"]
    )
    assert observed_source["mode"] == "kafka_retrigger"
    assert observed_binding["generation"] == 2
    assert observed_binding["role"] == "observer"
    assert [row["generation"] for row in store.list_rows("business_triggers")] == [
        1,
        2,
        3,
    ]


def test_legacy_kafka_manual_retrigger_generation_requires_explicit_migration(
    tmp_path,
):
    path = tmp_path / "control.sqlite3"
    store = RcaControlStore(path)
    first = store.ingest_record(
        _record(10), policy=_policy(), submit_enabled=True
    )
    _terminalize_input_wait(store, first.submission_key)
    second = store.ingest_record(
        _record(11, value=_value(updated_at=1783659999999)),
        policy=_policy(),
        submit_enabled=True,
    )
    [generation_two] = [
        row for row in store.list_rows("rca_outbox") if row["generation"] == 2
    ]
    payload = json.loads(generation_two["payload_json"])
    payload["admission"]["trigger_kind"] = "manual_retrigger"
    conn = store._connect()
    try:
        conn.execute(
            "UPDATE rca_outbox SET payload_json=? WHERE outbox_id=?",
            (
                json.dumps(payload, sort_keys=True, separators=(",", ":")),
                generation_two["outbox_id"],
            ),
        )
    finally:
        conn.close()

    sources_before = store.list_rows("rca_trigger_sources")
    conn = store._connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        with pytest.raises(RecordConflictError, match="explicit migration"):
            store._backfill_kafka_sources_and_subscriptions(conn)
        conn.rollback()
    finally:
        conn.close()
    assert store.list_rows("rca_trigger_sources") == sources_before

    observed = store.ingest_record(
        _record(12, value=_value(updated_at=1783660000000)),
        policy=_policy(),
        submit_enabled=True,
    )
    assert observed.decision == "invalid"
    assert observed.reason == LEGACY_KAFKA_GENERATION_REASON
    assert observed.generation == 0
    assert observed.submission_key == ""
    assert [row["generation"] for row in store.list_rows("business_triggers")] == [
        1,
        2,
    ]
    [dead_letter] = [
        row
        for row in store.list_rows("kafka_dead_letters")
        if row["source_event_id"] == observed.event_uid
    ]
    assert dead_letter["error_code"] == LEGACY_KAFKA_GENERATION_REASON


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
    store = RcaControlStore(tmp_path / "control.sqlite3")
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
    store = RcaControlStore(tmp_path / "control.sqlite3")
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
    store = RcaControlStore(tmp_path / "control.sqlite3")
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
    store = RcaControlStore(tmp_path / "control.sqlite3")
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


def test_exact_shadow_event_promotion_is_atomic_and_audited(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    result = store.ingest_record(
        _record(), policy=_policy(), submit_enabled=False
    )

    promoted = store.promote_shadow_event(
        result.event_uid,
        operator="release-owner",
        reason="canary issue approved",
    )

    assert promoted.promoted is True
    assert promoted.status == "pending"
    assert store.get_inbox(result.event_uid)["submission_mode"] == "pending"
    assert store.list_rows("rca_outbox")[0]["status"] == "pending"
    assert store.list_rows("business_triggers")[0]["state"] == "pending"
    audit = store.list_rows("rca_shadow_promotion_audit")
    assert audit[0]["event_uid"] == result.event_uid
    assert audit[0]["operator"] == "release-owner"
    assert audit[0]["reason"] == "canary issue approved"
    assert audit[0]["outcome"] == "promoted"


def test_concurrent_shadow_promotion_has_one_mutation_and_idempotent_repeat(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    event_uid = store.ingest_record(
        _record(), policy=_policy(), submit_enabled=False
    ).event_uid

    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(
            pool.map(
                lambda index: store.promote_shadow_event(
                    event_uid,
                    operator=f"owner-{index}",
                    reason="same canary approval",
                ),
                range(4),
            )
        )

    assert sum(item.promoted for item in results) == 1
    assert {item.status for item in results} == {"pending"}
    audits = store.list_rows("rca_shadow_promotion_audit")
    assert [item["outcome"] for item in audits].count("promoted") == 1
    assert [item["outcome"] for item in audits].count("already_promoted") == 3
    assert store.list_rows("rca_outbox")[0]["status"] == "pending"


def test_shadow_promotion_rejects_unknown_and_non_shadow_events_with_audit(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    pending_event = store.ingest_record(
        _record(), policy=_policy(), submit_enabled=True
    ).event_uid

    with pytest.raises(ShadowPromotionError, match="event_not_found"):
        store.promote_shadow_event(
            pending_event + ":prefix-not-exact",
            operator="release-owner",
            reason="invalid fuzzy attempt",
        )
    with pytest.raises(
        ShadowPromotionError, match="event_was_not_promoted_from_shadow"
    ):
        store.promote_shadow_event(
            pending_event,
            operator="release-owner",
            reason="cannot relabel a live event as canary",
        )

    audits = store.list_rows("rca_shadow_promotion_audit")
    assert [item["outcome"] for item in audits] == ["denied", "denied"]
    assert store.list_rows("rca_outbox")[0]["status"] == "pending"


def test_concurrent_outbox_claim_has_one_winner(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")
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
    store = RcaControlStore(tmp_path / "control.sqlite3")
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
    store = RcaControlStore(tmp_path / "control.sqlite3")
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


def test_activation_transition_fences_claimed_worker_and_renewal_rechecks_state(
    tmp_path,
):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    identities = _begin_bounded_activation(store)
    manual = identities["manual_success"][1]
    store.admit_manual_trigger(
        _manual_request(
            manual["message_id"],
            issue_url=manual["issue_url"],
            thread_id=manual["thread_id"],
        ),
        allowed_chat_ids={"oc_allowed"},
        submit_enabled=True,
        active_policy=_policy(),
        activation_required=True,
    )
    current = datetime.now(timezone.utc) + timedelta(seconds=1)
    claim = store.claim_outbox(
        lease_owner="activation-worker",
        lease_seconds=60,
        activation_required=True,
        now=current,
    )
    assert claim is not None
    with pytest.raises(
        ActivationEpochError, match="activation_inflight_writes_not_drained"
    ):
        store.transition_activation_epoch(
            epoch_id="rca-release-20260712",
            expected_state="bounded_active",
            target_state="aborted",
            operator="release-test",
            reason="claimed worker must fence every epoch transition",
        )

    conn = store._connect()
    try:
        conn.execute(
            "UPDATE rca_activation_epochs SET state = 'confirmed' "
            "WHERE epoch_id = 'rca-release-20260712'"
        )
    finally:
        conn.close()
    with pytest.raises(StaleOutboxLeaseError):
        store.extend_outbox_lease(
            outbox_id=claim.outbox_id,
            lease_token=claim.lease_token,
            lease_owner="activation-worker",
            lease_seconds=60,
            activation_required=True,
            now=current + timedelta(seconds=10),
        )


def test_expired_owner_cannot_complete_or_retry_before_reclaim(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")
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
    store = RcaControlStore(tmp_path / "control.sqlite3")
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
    store = RcaControlStore(tmp_path / "control.sqlite3")
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


def test_shadow_outbox_never_auto_promotes_on_enabled_restart(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    first = store.ingest_record(_record(), policy=_policy(), submit_enabled=False)

    replay = store.ingest_record(_record(), policy=_policy(), submit_enabled=True)

    assert replay.transport_duplicate is True
    assert replay.submission_key == first.submission_key
    assert store.list_rows("business_triggers")[0]["state"] == "shadow"
    assert store.list_rows("rca_outbox")[0]["status"] == "shadow"


def test_health_contains_only_counts_and_no_raw_payload(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    store.ingest_record(_record(value=b"raw-secret-payload"), policy=_policy())

    health_json = json.dumps(store.health(), sort_keys=True)

    assert "raw-secret-payload" not in health_json
    assert store.health()["inbox"] == {"invalid": 1}


def test_manual_first_then_kafka_joins_one_generation_without_rewriting_origin(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")
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
    store = RcaControlStore(tmp_path / "control.sqlite3")
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
    store = RcaControlStore(tmp_path / "control.sqlite3")
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


@pytest.mark.parametrize("mode", ["run_or_join", "rerun", "debug"])
def test_manual_promotes_exact_unsubmitted_kafka_shadow_without_rewriting_origin(
    tmp_path, mode
):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    kafka = store.ingest_record(_record(), policy=_policy(), submit_enabled=False)
    [before_outbox] = store.list_rows("rca_outbox")
    before_payload = before_outbox["payload_json"]
    before_origin = before_outbox["origin_source_id"]

    manual = store.admit_manual_trigger(
        _manual_request("om_promote_shadow", mode=mode),
        allowed_chat_ids={"oc_allowed"},
        submit_enabled=True,
        operator_authorized=mode in {"rerun", "debug"},
    )

    assert manual.outcome == "rearmed"
    assert manual.reason == "manual_shadow_promoted"
    assert manual.submission_key == kafka.submission_key
    assert store.get_inbox(kafka.event_uid)["submission_mode"] == "pending"
    [outbox] = store.list_rows("rca_outbox")
    [trigger] = store.list_rows("business_triggers")
    assert outbox["status"] == trigger["state"] == "pending"
    assert outbox["payload_json"] == before_payload
    assert outbox["origin_source_id"] == trigger["origin_source_id"] == before_origin
    manual_binding = next(
        row
        for row in store.list_rows("rca_trigger_bindings")
        if row["source_id"] == manual.source_id
    )
    assert manual_binding["role"] == "observer"
    [audit] = store.list_rows("rca_shadow_promotion_audit")
    assert audit["event_uid"] == kafka.event_uid
    assert audit["outcome"] == "manual_shadow_promoted"
    assert audit["operator"] == "manual:ou_operator"
    assert audit["from_status"] == "shadow"
    assert audit["to_status"] == "pending"


def test_manual_shadow_promotion_replay_is_idempotent_and_kafka_replay_is_inert(
    tmp_path,
):
    path = tmp_path / "control.sqlite3"
    store = RcaControlStore(path)
    kafka = store.ingest_record(_record(), policy=_policy(), submit_enabled=False)
    request = _manual_request("om_promote_replay")

    first = store.admit_manual_trigger(
        request,
        allowed_chat_ids={"oc_allowed"},
        submit_enabled=True,
    )
    replay = RcaControlStore(path).admit_manual_trigger(
        request,
        allowed_chat_ids={"oc_allowed"},
        submit_enabled=True,
    )
    kafka_replay = store.ingest_record(
        _record(), policy=_policy(), submit_enabled=True
    )

    assert first.outcome == replay.outcome == "rearmed"
    assert first.submission_key == replay.submission_key == kafka.submission_key
    assert replay.reason == "idempotent_source_replay"
    assert kafka_replay.transport_duplicate is True
    assert len(store.list_rows("business_triggers")) == 1
    assert len(store.list_rows("rca_outbox")) == 1
    assert len(store.list_rows("rca_shadow_promotion_audit")) == 1


def test_concurrent_manual_and_kafka_replays_promote_shadow_once(tmp_path):
    path = tmp_path / "control.sqlite3"
    store = RcaControlStore(path)
    kafka = store.ingest_record(_record(), policy=_policy(), submit_enabled=False)

    def invoke(index):
        if index % 3 == 0:
            return store.ingest_record(
                _record(), policy=_policy(), submit_enabled=True
            )
        return RcaControlStore(path).admit_manual_trigger(
            _manual_request(f"om_promote_concurrent_{index}"),
            allowed_chat_ids={"oc_allowed"},
            submit_enabled=True,
        )

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(invoke, range(12)))

    manual_results = [item for item in results if hasattr(item, "source_id")]
    assert [item.outcome for item in manual_results].count("rearmed") == 1
    assert {item.submission_key for item in manual_results} == {kafka.submission_key}
    assert len(store.list_rows("business_triggers")) == 1
    assert len(store.list_rows("rca_outbox")) == 1
    assert len(store.list_rows("rca_shadow_promotion_audit")) == 1
    assert store.list_rows("rca_outbox")[0]["status"] == "pending"


def test_manual_active_policy_snapshot_bootstraps_empty_store(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")

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
    store = RcaControlStore(tmp_path / "control.sqlite3")
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
    store = RcaControlStore(path)
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
        ("shadow", "run_or_join", "rearmed", 1, 1),
        ("shadow", "rerun", "rearmed", 1, 1),
        ("shadow", "debug", "rearmed", 1, 1),
        ("terminal", "run_or_join", "catchup_attached", 1, 1),
        ("terminal", "rerun", "created", 2, 2),
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
    store = RcaControlStore(tmp_path / "control.sqlite3")
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
    store = RcaControlStore(tmp_path / "control.sqlite3")
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
    store = RcaControlStore(tmp_path / "control.sqlite3")
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
    store = RcaControlStore(tmp_path / "control.sqlite3")
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
    RcaControlStore(path)

    def admit(index):
        version = f"issue-created-v{index + 1}"
        return RcaControlStore(path).admit_manual_trigger(
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
    store = RcaControlStore(path)
    assert len(store.list_rows("business_triggers")) == 1
    assert len(store.list_rows("rca_outbox")) == 1


def test_concurrent_cross_rule_kafka_and_manual_create_one_issue_chain(tmp_path):
    path = tmp_path / "control.sqlite3"
    store = RcaControlStore(path)

    def kafka():
        return RcaControlStore(path).ingest_record(
            _record(),
            policy=_policy(policy_version="issue-created-v1"),
            submit_enabled=True,
        )

    def manual():
        return RcaControlStore(path).admit_manual_trigger(
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
    store = RcaControlStore(tmp_path / "control.sqlite3")
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
    store = RcaControlStore(tmp_path / "control.sqlite3")
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
    store = RcaControlStore(tmp_path / "control.sqlite3")
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


def test_manual_high_water_excludes_historical_null_epoch_backlog(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    for index in range(85):
        result = store.ingest_record(
            _record(
                offset=100 + index,
                value=_value(work_item_id=7041720000 + index),
            ),
            policy=_policy(),
            submit_enabled=True,
        )
        assert result.decision == "accepted"

    _activate_direct_steady_epoch(store, start_offset=185)
    for index in range(16):
        admitted = store.admit_manual_trigger(
            _manual_request(
                f"om_current_epoch_capacity_{index}",
                thread_id=f"topic:om_current_epoch_capacity_root_{index}",
                issue_url=(
                    "https://project.feishu.cn/g1q3/issue/detail/"
                    f"{7041730000 + index}"
                ),
            ),
            allowed_chat_ids={"oc_allowed"},
            submit_enabled=True,
            active_policy=_policy(),
            outbox_high_watermark=100,
            activation_required=True,
        )
        assert admitted.outcome == "created"

    rows = store.list_rows("rca_outbox")
    assert sum(row["activation_epoch_id"] is None for row in rows) == 85
    assert sum(row["activation_epoch_id"] is not None for row in rows) == 16

    for index in range(84):
        result = store.ingest_record(
            _record(
                offset=200 + index,
                value=_value(work_item_id=7041740000 + index),
            ),
            policy=_policy(),
            submit_enabled=True,
            activation_required=True,
        )
        assert result.decision == "accepted"

    assert len(store.preview_dispatchable(limit=200, activation_required=True)) == 100
    with pytest.raises(
        ManualRcaAdmissionError, match="manual_outbox_high_watermark_reached"
    ):
        store.admit_manual_trigger(
            _manual_request(
                "om_current_epoch_capacity_rejected",
                thread_id="topic:om_current_epoch_capacity_rejected_root",
                issue_url=(
                    "https://project.feishu.cn/g1q3/issue/detail/7041750000"
                ),
            ),
            allowed_chat_ids={"oc_allowed"},
            submit_enabled=True,
            active_policy=_policy(),
            outbox_high_watermark=100,
            activation_required=True,
        )


def test_manual_source_quota_reserves_shared_outbox_capacity_for_kafka(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")
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
    store = RcaControlStore(tmp_path / "control.sqlite3")
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
    RcaControlStore(path)

    def admit(index):
        try:
            return RcaControlStore(path).admit_manual_trigger(
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
    store = RcaControlStore(path)
    assert len(store.list_rows("business_triggers")) == 1
    assert len(store.list_rows("rca_outbox")) == 1
    assert len(store.list_rows("rca_trigger_sources")) == 1


def test_manual_high_water_blocks_shadow_promotion_and_terminal_rerun_atomically(
    tmp_path,
):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    terminal = store.ingest_record(_record(), policy=_policy(), submit_enabled=True)
    _terminalize_permanent(store, terminal.submission_key)
    live = store.ingest_record(
        _record(offset=11, value=_value(work_item_id=7041712813)),
        policy=_policy(),
        submit_enabled=True,
    )
    shadow = store.ingest_record(
        _record(offset=12, value=_value(work_item_id=7041712814)),
        policy=_policy(),
        submit_enabled=False,
    )
    terminal_join = store.admit_manual_trigger(
        _manual_request("om_terminal_join_at_high"),
        allowed_chat_ids={"oc_allowed"},
        submit_enabled=True,
        active_policy=_policy(),
        outbox_high_watermark=1,
    )
    _settle_delivery(store, terminal.submission_key)
    before_sources = len(store.list_rows("rca_trigger_sources"))

    for request in (
        _manual_request("om_rerun_at_high", mode="rerun"),
        _manual_request(
            "om_shadow_at_high",
            issue_url="https://project.feishu.cn/g1q3/issue/detail/7041712814",
        ),
    ):
        with pytest.raises(
            ManualRcaAdmissionError, match="manual_outbox_high_watermark_reached"
        ):
            store.admit_manual_trigger(
                request,
                allowed_chat_ids={"oc_allowed"},
                submit_enabled=True,
                operator_authorized=request.mode in {"rerun", "debug"},
                active_policy=_policy(),
                outbox_high_watermark=1,
            )

    assert terminal_join.outcome == "catchup_attached"
    assert terminal_join.submission_key == terminal.submission_key
    assert live.submission_key != shadow.submission_key
    assert len(store.list_rows("business_triggers")) == 3
    assert len(store.list_rows("rca_outbox")) == 3
    assert len(store.list_rows("rca_trigger_sources")) == before_sources
    shadow_outbox = next(
        row
        for row in store.list_rows("rca_outbox")
        if row["submission_key"] == shadow.submission_key
    )
    assert shadow_outbox["status"] == "shadow"
    assert store.list_rows("rca_shadow_promotion_audit") == []


def test_manual_storage_reserve_blocks_join_without_partial_source_or_subscription(
    tmp_path, monkeypatch
):
    store = RcaControlStore(tmp_path / "control.sqlite3")
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
    store = RcaControlStore(tmp_path / "control.sqlite3")
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
    store = RcaControlStore(tmp_path / "control.sqlite3")
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
    store = RcaControlStore(tmp_path / "control.sqlite3")
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
    store = RcaControlStore(tmp_path / "control.sqlite3")
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
    store = RcaControlStore(tmp_path / "control.sqlite3")
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
    store = RcaControlStore(tmp_path / "control.sqlite3")
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
    store = RcaControlStore(path)
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

    replay = RcaControlStore(path).admit_manual_trigger(
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
    store = RcaControlStore(tmp_path / "control.sqlite3")
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
    store = RcaControlStore(tmp_path / "control.sqlite3")
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


def test_manual_safe_off_debug_authorization_and_no_mdi_payload(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    _register_policy_without_classifying(store)
    with pytest.raises(ManualRcaAdmissionError, match="manual_intake_disabled"):
        store.admit_manual_trigger(
            _manual_request("om_disabled"),
            allowed_chat_ids={"oc_allowed"},
            submit_enabled=False,
        )
    with pytest.raises(ManualRcaAdmissionError, match="manual_operator_not_authorized"):
        store.admit_manual_trigger(
            _manual_request("om_debug_denied", mode="debug"),
            allowed_chat_ids={"oc_allowed"},
            submit_enabled=True,
        )
    with pytest.raises(ManualRcaAdmissionError, match="manual_operator_not_authorized"):
        store.admit_manual_trigger(
            _manual_request("om_rerun_denied", mode="rerun"),
            allowed_chat_ids={"oc_allowed"},
            submit_enabled=True,
        )

    result = store.admit_manual_trigger(
        _manual_request("om_debug_allowed", mode="debug"),
        allowed_chat_ids={"oc_allowed"},
        submit_enabled=True,
        operator_authorized=True,
    )
    [outbox] = store.list_rows("rca_outbox")
    payload = json.loads(outbox["payload_json"])
    serialized = json.dumps(payload, sort_keys=True).lower()
    assert result.outcome == "created"
    assert payload["schema_version"] == "pnc_rca_submission_outbox_v2"
    assert payload["origin_source_id"] == result.source_id
    assert "normalized_event" not in payload
    assert "source_event_id" not in payload
    assert "topic" not in payload
    assert "partition" not in payload
    assert "offset" not in payload
    assert "mdi" not in serialized
    assert "agent_backend" not in serialized
    assert "allow_download" not in serialized


def test_operator_rate_limit_is_durable_but_run_or_join_and_replay_remain_open(
    tmp_path,
):
    store = RcaControlStore(tmp_path / "control.sqlite3")
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


def test_capacity_transition_requires_explicit_bootstrap_initialization(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    assert store.capacity_transition_state() is None
    assert store.health()["capacity_transition"] == {
        "configured": False,
        "durable_capacity_mode": None,
        "generation": None,
        "irreversible": False,
        "state": None,
        "audit_count": 0,
        "integrity_ok": True,
        "integrity_error": "",
    }

    initialized = store.initialize_capacity_transition(
        release_id="release-20260713",
        bootstrap_epoch_id="rca-bootstrap-release-20260713",
        now=datetime(2026, 7, 13, tzinfo=timezone.utc),
    )
    replay = store.initialize_capacity_transition(
        release_id="release-20260713",
        bootstrap_epoch_id="rca-bootstrap-release-20260713",
        now=datetime(2026, 7, 14, tzinfo=timezone.utc),
    )

    assert initialized == replay
    assert initialized["state"] == "BOOTSTRAP_PRODUCTION"
    assert initialized["generation"] == 1
    assert initialized["final_ledger_sha256"] is None
    assert len(store.list_rows("rca_capacity_transition_audit")) == 1
    with pytest.raises(
        CapacityTransitionStateError, match="capacity_transition_identity_conflict"
    ):
        store.initialize_capacity_transition(
            release_id="release-other",
            bootstrap_epoch_id="rca-bootstrap-other",
        )


def test_capacity_transition_cas_is_irreversible_bound_and_idempotent(tmp_path):
    path = tmp_path / "control.sqlite3"
    store = RcaControlStore(path)
    store.initialize_capacity_transition(
        release_id="release-20260713",
        bootstrap_epoch_id="rca-bootstrap-release-20260713",
        now=CAPACITY_BOOTSTRAP_NOW,
    )
    kwargs = _capacity_steady_kwargs()

    activated = store.compare_and_set_capacity_steady(**kwargs)
    replay = store.compare_and_set_capacity_steady(**kwargs)
    historical_replay = store.compare_and_set_capacity_steady(
        **_capacity_steady_kwargs(
            now=datetime(2026, 7, 20, 2, 0, tzinfo=timezone.utc)
        )
    )
    reopened = RcaControlStore(path, require_current=True)

    assert activated == replay == historical_replay == reopened.capacity_transition_state()
    assert activated["state"] == "STEADY_ACTIVE"
    assert activated["generation"] == 2
    assert activated["final_ledger_sha256"] == "1" * 64
    health = reopened.health()["capacity_transition"]
    assert health["configured"] is True
    assert health["durable_capacity_mode"] == "steady"
    assert health["generation"] == 2
    assert health["irreversible"] is True
    assert health["state"] == activated
    assert health["audit_count"] == 2
    assert [row["to_state"] for row in reopened.list_rows(
        "rca_capacity_transition_audit"
    )] == ["BOOTSTRAP_PRODUCTION", "STEADY_ACTIVE"]


def test_capacity_transition_rejects_stale_generation_and_binding_conflict(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    store.initialize_capacity_transition(
        release_id="release-20260713",
        bootstrap_epoch_id="rca-bootstrap-release-20260713",
        now=CAPACITY_BOOTSTRAP_NOW,
    )
    with pytest.raises(
        CapacityTransitionStateError, match="capacity_transition_generation_changed"
    ):
        store.compare_and_set_capacity_steady(
            **_capacity_steady_kwargs(expected_generation=2)
        )

    activated = store.compare_and_set_capacity_steady(**_capacity_steady_kwargs())
    with pytest.raises(
        CapacityTransitionStateError, match="capacity_transition_generation_changed"
    ):
        store.compare_and_set_capacity_steady(
            **_capacity_steady_kwargs(expected_generation=2)
        )
    with pytest.raises(
        CapacityTransitionStateError,
        match="capacity_transition_steady_binding_conflict",
    ):
        store.compare_and_set_capacity_steady(
            **_capacity_steady_kwargs(commit_marker_fingerprint="a" * 64)
        )
    assert store.capacity_transition_state() == activated
    assert len(store.list_rows("rca_capacity_transition_audit")) == 2


def test_capacity_transition_validates_hashes_identities_and_time_order(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    with pytest.raises(CapacityTransitionStateError, match="capacity_release_id_invalid"):
        store.initialize_capacity_transition(
            release_id="bad release",
            bootstrap_epoch_id="rca-bootstrap-release-20260713",
        )
    store.initialize_capacity_transition(
        release_id="release-20260713",
        bootstrap_epoch_id="rca-bootstrap-release-20260713",
        now=CAPACITY_BOOTSTRAP_NOW,
    )
    with pytest.raises(
        CapacityTransitionStateError, match="capacity_final_ledger_sha256_invalid"
    ):
        store.compare_and_set_capacity_steady(
            **_capacity_steady_kwargs(final_ledger_sha256="not-a-hash")
        )
    now = datetime(2026, 7, 20, 1, 0, tzinfo=timezone.utc)
    with pytest.raises(
        CapacityTransitionStateError,
        match="capacity_transition_timestamp_order_invalid",
    ):
        store.compare_and_set_capacity_steady(
            **_capacity_steady_kwargs(
                receipt_created_at=(now - timedelta(minutes=4)).isoformat()
            )
        )
    recovered = store.compare_and_set_capacity_steady(
        **_capacity_steady_kwargs(now=now + timedelta(days=1))
    )
    assert recovered["state"] == "STEADY_ACTIVE"
    assert datetime.fromisoformat(recovered["steady_activated_at"]) > datetime.fromisoformat(
        recovered["authorization_expires_at"]
    )


def test_capacity_transition_sql_triggers_block_mutation_and_delete(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    store.initialize_capacity_transition(
        release_id="release-20260713",
        bootstrap_epoch_id="rca-bootstrap-release-20260713",
        now=CAPACITY_BOOTSTRAP_NOW,
    )
    conn = store._connect()
    try:
        with pytest.raises(sqlite3.IntegrityError, match="transition_invalid"):
            conn.execute(
                "UPDATE rca_capacity_transition_state SET generation = 2"
            )
        with pytest.raises(sqlite3.IntegrityError, match="delete_forbidden"):
            conn.execute("DELETE FROM rca_capacity_transition_state")
    finally:
        conn.close()

    store.compare_and_set_capacity_steady(**_capacity_steady_kwargs())
    conn = store._connect()
    try:
        with pytest.raises(sqlite3.IntegrityError, match="steady_immutable"):
            conn.execute(
                "UPDATE rca_capacity_transition_state SET updated_at = updated_at"
            )
        with pytest.raises(sqlite3.IntegrityError, match="delete_forbidden"):
            conn.execute("DELETE FROM rca_capacity_transition_state")
        with pytest.raises(sqlite3.IntegrityError, match="update_forbidden"):
            conn.execute(
                "UPDATE rca_capacity_transition_audit SET transitioned_at = transitioned_at"
            )
        with pytest.raises(sqlite3.IntegrityError, match="delete_forbidden"):
            conn.execute("DELETE FROM rca_capacity_transition_audit")
    finally:
        conn.close()


def test_capacity_transition_replace_cannot_downgrade_or_collapse_audit(tmp_path):
    path = tmp_path / "control.sqlite3"
    store = RcaControlStore(path)
    store.initialize_capacity_transition(
        release_id="release-20260713",
        bootstrap_epoch_id="rca-bootstrap-release-20260713",
        now=CAPACITY_BOOTSTRAP_NOW,
    )
    steady = store.compare_and_set_capacity_steady(**_capacity_steady_kwargs())
    bootstrap_audit = store.list_rows("rca_capacity_transition_audit")[0]

    conn = sqlite3.connect(path, isolation_level=None)
    try:
        conn.execute("PRAGMA recursive_triggers=OFF")
        assert conn.execute("PRAGMA recursive_triggers").fetchone()[0] == 0
        with pytest.raises(sqlite3.IntegrityError, match="replace_forbidden"):
            conn.execute(
                """
                INSERT OR REPLACE INTO rca_capacity_transition_audit(
                    audit_id, release_id, bootstrap_epoch_id,
                    from_state, to_state, from_generation, to_generation,
                    transitioned_at
                ) VALUES(
                    2, 'release-20260713',
                    'rca-bootstrap-release-20260713',
                    'UNCONFIGURED', 'BOOTSTRAP_PRODUCTION', 0, 1, ?
                )
                """,
                (bootstrap_audit["transitioned_at"],),
            )
        with pytest.raises(sqlite3.IntegrityError, match="replace_forbidden"):
            conn.execute(
                """
                INSERT OR REPLACE INTO rca_capacity_transition_state(
                    singleton_id, release_id, bootstrap_epoch_id,
                    state, generation, bootstrap_initialized_at, updated_at
                ) VALUES(
                    1, 'release-20260713',
                    'rca-bootstrap-release-20260713',
                    'BOOTSTRAP_PRODUCTION', 1, ?, ?
                )
                """,
                (
                    bootstrap_audit["transitioned_at"],
                    bootstrap_audit["transitioned_at"],
                ),
            )
    finally:
        conn.close()

    assert RcaControlStore(path, require_current=True).capacity_transition_state() == steady
    assert len(store.list_rows("rca_capacity_transition_audit")) == 2


def test_capacity_transition_reader_and_health_fail_closed_on_broken_audit_chain(
    tmp_path,
):
    path = tmp_path / "control.sqlite3"
    store = RcaControlStore(path)
    store.initialize_capacity_transition(
        release_id="release-20260713",
        bootstrap_epoch_id="rca-bootstrap-release-20260713",
        now=CAPACITY_BOOTSTRAP_NOW,
    )
    bindings = _capacity_steady_kwargs()
    activated_at = bindings["now"].isoformat()
    conn = store._connect()
    try:
        conn.execute(
            """
            UPDATE rca_capacity_transition_state
               SET state = 'STEADY_ACTIVE', generation = 2,
                   final_ledger_sha256 = ?,
                   transition_authorization_sha256 = ?,
                   transition_authorization_fingerprint = ?,
                   transition_receipt_sha256 = ?,
                   transition_receipt_fingerprint = ?,
                   commit_marker_sha256 = ?,
                   commit_marker_fingerprint = ?,
                   evidence_bundle_sha256 = ?,
                   evidence_bundle_fingerprint = ?,
                   authorization_issued_at = ?,
                   authorization_expires_at = ?,
                   receipt_created_at = ?, marker_committed_at = ?,
                   steady_activated_at = ?, updated_at = ?
             WHERE singleton_id = 1
            """,
            (
                bindings["final_ledger_sha256"],
                bindings["transition_authorization_sha256"],
                bindings["transition_authorization_fingerprint"],
                bindings["transition_receipt_sha256"],
                bindings["transition_receipt_fingerprint"],
                bindings["commit_marker_sha256"],
                bindings["commit_marker_fingerprint"],
                bindings["evidence_bundle_sha256"],
                bindings["evidence_bundle_fingerprint"],
                bindings["authorization_issued_at"],
                bindings["authorization_expires_at"],
                bindings["receipt_created_at"],
                bindings["marker_committed_at"],
                activated_at,
                activated_at,
            ),
        )
    finally:
        conn.close()

    with pytest.raises(
        RuntimeError,
        match="rca_capacity_transition_integrity:capacity_transition_audit_chain",
    ):
        store.capacity_transition_state()
    with pytest.raises(
        CapacityTransitionStateError,
        match="capacity_transition_integrity_invalid:capacity_transition_audit_chain",
    ):
        store.compare_and_set_capacity_steady(**bindings)
    health = store.health()
    assert health["ok"] is False
    assert health["capacity_transition"]["durable_capacity_mode"] == "blocked"
    assert health["capacity_transition"]["irreversible"] is True
    assert health["capacity_transition"]["integrity_ok"] is False
    assert (
        health["capacity_transition"]["integrity_error"]
        == "capacity_transition_audit_chain"
    )


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


def test_capacity_transition_rolls_back_state_when_audit_insert_fails(tmp_path):
    path = tmp_path / "control.sqlite3"
    store = RcaControlStore(path)
    conn = store._connect()
    try:
        conn.execute(
            """
            CREATE TRIGGER test_rca_capacity_audit_abort
            BEFORE INSERT ON rca_capacity_transition_audit
            BEGIN
                SELECT RAISE(ABORT, 'test_capacity_audit_abort');
            END
            """
        )
    finally:
        conn.close()

    with pytest.raises(sqlite3.IntegrityError, match="test_capacity_audit_abort"):
        store.initialize_capacity_transition(
            release_id="release-20260713",
            bootstrap_epoch_id="rca-bootstrap-release-20260713",
        )
    assert store.capacity_transition_state() is None
    assert store.list_rows("rca_capacity_transition_audit") == []

    conn = store._connect()
    try:
        conn.execute("DROP TRIGGER test_rca_capacity_audit_abort")
    finally:
        conn.close()
    store.initialize_capacity_transition(
        release_id="release-20260713",
        bootstrap_epoch_id="rca-bootstrap-release-20260713",
        now=CAPACITY_BOOTSTRAP_NOW,
    )

    conn = store._connect()
    try:
        conn.execute(
            """
            CREATE TRIGGER test_rca_capacity_audit_abort
            BEFORE INSERT ON rca_capacity_transition_audit
            WHEN NEW.to_state = 'STEADY_ACTIVE'
            BEGIN
                SELECT RAISE(ABORT, 'test_capacity_audit_abort');
            END
            """
        )
    finally:
        conn.close()
    with pytest.raises(sqlite3.IntegrityError, match="test_capacity_audit_abort"):
        store.compare_and_set_capacity_steady(**_capacity_steady_kwargs())
    assert store.capacity_transition_state()["state"] == "BOOTSTRAP_PRODUCTION"
    assert len(store.list_rows("rca_capacity_transition_audit")) == 1


def test_capacity_transition_concurrent_conflicting_cas_has_one_winner(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3", busy_timeout_ms=10_000)
    store.initialize_capacity_transition(
        release_id="release-20260713",
        bootstrap_epoch_id="rca-bootstrap-release-20260713",
        now=CAPACITY_BOOTSTRAP_NOW,
    )
    candidates = [
        _capacity_steady_kwargs(commit_marker_fingerprint="a" * 64),
        _capacity_steady_kwargs(commit_marker_fingerprint="b" * 64),
    ]

    def activate(kwargs):
        try:
            return store.compare_and_set_capacity_steady(**kwargs)
        except CapacityTransitionStateError as exc:
            return exc

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(activate, candidates))

    successes = [value for value in results if isinstance(value, dict)]
    failures = [value for value in results if isinstance(value, Exception)]
    assert len(successes) == len(failures) == 1
    assert failures[0].args == ("capacity_transition_steady_binding_conflict",)
    assert store.capacity_transition_state()["commit_marker_fingerprint"] in {
        "a" * 64,
        "b" * 64,
    }
    assert len(store.list_rows("rca_capacity_transition_audit")) == 2


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
V14_TERMINAL_RERUN_TABLES = {"rca_terminal_rerun_delivery_authorities"}


def _drop_schema_objects(conn, *, tables):
    tables = tuple(tables)
    drop_learning = bool(set(tables) & V12_LEARNING_TABLES)
    drop_historical = bool(set(tables) & V13_HISTORICAL_HOLD_TABLES)
    conn.execute("PRAGMA foreign_keys=OFF")
    for row in conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'trigger'"
    ).fetchall():
        name = str(row["name"])
        if (
            drop_learning and name.startswith("trg_learning_lane_")
        ) or (
            drop_historical and name.startswith("trg_activation_historical_")
        ):
            conn.execute(f"DROP TRIGGER {name}")
    for table in tables:
        conn.execute(f"DROP TABLE IF EXISTS {table}")


def _downgrade_current_store_to_v10(store):
    conn = store._connect()
    try:
        _drop_schema_objects(
            conn,
            tables=(
                *V13_HISTORICAL_HOLD_TABLES,
                *V12_LEARNING_TABLES,
                *V14_TERMINAL_RERUN_TABLES,
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
    conn = store._connect()
    try:
        _drop_schema_objects(
            conn,
            tables=(*V13_HISTORICAL_HOLD_TABLES, *V14_TERMINAL_RERUN_TABLES),
        )
        conn.execute(
            "UPDATE control_meta SET value='pnc_rca_control_store_v12' "
            "WHERE key='schema_version'"
        )
    finally:
        conn.close()


def _downgrade_current_store_to_v11(store):
    conn = store._connect()
    try:
        _drop_schema_objects(
            conn,
            tables=(
                *V13_HISTORICAL_HOLD_TABLES,
                *V12_LEARNING_TABLES,
                *V14_TERMINAL_RERUN_TABLES,
            ),
        )
        conn.execute(
            "UPDATE control_meta SET value='pnc_rca_control_store_v11' "
            "WHERE key='schema_version'"
        )
    finally:
        conn.close()


def _downgrade_current_store_to_v13(store):
    conn = store._connect()
    try:
        _drop_schema_objects(conn, tables=V14_TERMINAL_RERUN_TABLES)
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
    assert all(
        upgraded.list_rows(table) == []
        for table in (*V12_LEARNING_TABLES, *V13_HISTORICAL_HOLD_TABLES)
    )


def test_v12_store_migrates_to_v13_historical_hold_schema(tmp_path):
    path = tmp_path / "control.sqlite3"
    original = RcaControlStore(path)
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
    assert all(
        upgraded.list_rows(table) == [] for table in V13_HISTORICAL_HOLD_TABLES
    )


def test_v13_store_migrates_to_v14_terminal_rerun_authority_schema(tmp_path):
    path = tmp_path / "control.sqlite3"
    _downgrade_current_store_to_v13(RcaControlStore(path))

    with pytest.raises(RuntimeError, match="rca_control_store_schema_not_current"):
        RcaControlStore(path, require_current=True)
    upgraded = RcaControlStore(path)

    assert upgraded.health()["schema_version"] == CONTROL_STORE_SCHEMA_VERSION
    assert upgraded.initialization_observation()["mode"] == "migration"
    assert upgraded.list_rows("rca_terminal_rerun_delivery_authorities") == []


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
    conn = store._connect()
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


def test_v12_prototype_sealed_hold_migrates_without_rehashing_rows(tmp_path):
    path = tmp_path / "control.sqlite3"
    store = RcaControlStore(path)
    store.ingest_record(_record(offset=10), policy=_policy(), submit_enabled=True)
    epoch = _create_activation_epoch(store)
    before = store.activation_historical_outbox_hold_evidence(
        epoch_id=epoch["epoch_id"]
    )
    conn = store._connect()
    try:
        store._drop_v13_historical_outbox_hold_triggers(conn)
        conn.execute(
            "DROP TABLE rca_activation_historical_outbox_disposition_items"
        )
        conn.execute("DROP TABLE rca_activation_historical_outbox_dispositions")
        conn.execute(
            "UPDATE control_meta SET value='pnc_rca_control_store_v12' "
            "WHERE key='schema_version'"
        )
    finally:
        conn.close()

    upgraded = RcaControlStore(path)
    after = upgraded.activation_historical_outbox_hold_evidence(
        epoch_id=epoch["epoch_id"]
    )
    assert upgraded.health()["schema_version"] == CONTROL_STORE_SCHEMA_VERSION
    assert after["sealed_sha256"] == before["sealed_sha256"]
    assert after["matches"] is True


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


def test_v12_to_v13_migration_rejects_incomplete_v12_learning_schema(tmp_path):
    path = tmp_path / "control.sqlite3"
    store = RcaControlStore(path)
    conn = store._connect()
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
            V13_HISTORICAL_HOLD_TABLES,
            "historical_outbox_hold_table_sql",
        ),
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


def test_v9_store_migrates_to_empty_explicit_capacity_latch(tmp_path):
    path = tmp_path / "control.sqlite3"
    store = RcaControlStore(path)
    conn = store._connect()
    try:
        conn.execute("DROP TABLE rca_capacity_transition_state")
        conn.execute("DROP TABLE rca_capacity_transition_audit")
        conn.execute(
            "UPDATE control_meta SET value='pnc_rca_control_store_v9' "
            "WHERE key='schema_version'"
        )
    finally:
        conn.close()

    upgraded = RcaControlStore(path)
    assert upgraded.health()["schema_version"] == CONTROL_STORE_SCHEMA_VERSION
    assert upgraded.capacity_transition_state() is None
    assert upgraded.list_rows("rca_capacity_transition_audit") == []
    assert upgraded.initialization_observation() == {
        "mode": "migration",
        "backfill_runs": 1,
    }


def test_v9_capacity_schema_migration_rolls_back_all_ddl_on_validation_failure(
    tmp_path, monkeypatch
):
    path = tmp_path / "control.sqlite3"
    store = RcaControlStore(path)
    conn = store._connect()
    try:
        conn.execute("DROP TABLE rca_capacity_transition_state")
        conn.execute("DROP TABLE rca_capacity_transition_audit")
        conn.execute(
            "UPDATE control_meta SET value='pnc_rca_control_store_v9' "
            "WHERE key='schema_version'"
        )
    finally:
        conn.close()

    def reject_migrated_schema(_conn, *, integrity_check):
        assert integrity_check is True
        raise RuntimeError("injected_capacity_schema_validation_failure")

    monkeypatch.setattr(
        RcaControlStore,
        "_validate_structural_contract",
        staticmethod(reject_migrated_schema),
    )
    with pytest.raises(
        RuntimeError, match="injected_capacity_schema_validation_failure"
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
    assert marker == "pnc_rca_control_store_v9"
    assert "rca_capacity_transition_state" not in tables
    assert "rca_capacity_transition_audit" not in tables


def test_current_capacity_schema_missing_trigger_is_rejected_read_only(tmp_path):
    path = tmp_path / "control.sqlite3"
    store = RcaControlStore(path)
    conn = store._connect()
    try:
        conn.execute("DROP TRIGGER trg_rca_capacity_state_steady_immutable")
    finally:
        conn.close()

    with pytest.raises(
        RuntimeError,
        match="incompatible_control_store_schema:capacity_transition_triggers",
    ):
        RcaControlStore(path, require_current=True)


def test_current_capacity_schema_rejects_redefined_trigger_sql(tmp_path):
    path = tmp_path / "control.sqlite3"
    store = RcaControlStore(path)
    conn = store._connect()
    try:
        conn.execute("DROP TRIGGER trg_rca_capacity_state_no_replace")
        conn.execute(
            """
            CREATE TRIGGER trg_rca_capacity_state_no_replace
            BEFORE INSERT ON rca_capacity_transition_state
            WHEN 0
            BEGIN
                SELECT RAISE(ABORT, 'rca_capacity_state_replace_forbidden');
            END
            """
        )
    finally:
        conn.close()

    with pytest.raises(
        RuntimeError,
        match="incompatible_control_store_schema:capacity_transition_trigger_sql",
    ):
        RcaControlStore(path, require_current=True)


def test_current_capacity_schema_rejects_orphan_audit(tmp_path):
    path = tmp_path / "control.sqlite3"
    store = RcaControlStore(path)
    conn = store._connect()
    try:
        conn.execute(
            """
            INSERT INTO rca_capacity_transition_audit(
                release_id, bootstrap_epoch_id, from_state, to_state,
                from_generation, to_generation, transitioned_at
            ) VALUES(
                'release-orphan', 'rca-bootstrap-orphan',
                'UNCONFIGURED', 'BOOTSTRAP_PRODUCTION', 0, 1,
                '2026-07-13T00:00:00+00:00'
            )
            """
        )
    finally:
        conn.close()

    with pytest.raises(
        RuntimeError,
        match="incompatible_control_store_schema:capacity_transition_orphan_audit",
    ):
        RcaControlStore(path, require_current=True)
