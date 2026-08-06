from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import sqlite3

import pytest

from gateway.pnc_rca_control_store import (
    MANUAL_TRIGGER_SCHEMA_VERSION,
    ActivationEpochError,
    KafkaRecord,
    ManualRcaTriggerRequest,
    RcaControlStore,
)
from gateway.pnc_rca_delivery_contract import (
    DELIVERY_EFFECT_KIND,
    DELIVERY_EFFECT_SCHEMA_VERSION,
    VerifiedDelivery,
    compute_delivery_effect_key,
    compute_delivery_effect_payload_sha256,
    delivery_effect_marker,
)
from gateway.pnc_rca_delivery_observability import (
    OBSERVATION_SCHEMA_VERSION,
    DeliveryObservationError,
    delivery_observation_id,
)
from gateway.pnc_rca_delivery_store import (
    DELIVERY_STORE_SCHEMA_VERSION,
    OUTBOX_QUARANTINED_PUBLIC_ERROR_CODE,
    OUTBOX_QUARANTINED_TERMINAL_STATE,
    PERMANENT_FAILURE_CIRCUIT_THRESHOLD,
    PRE_W3_EFFECT_DISPOSITION_COMMAND,
    PRE_W3_EFFECT_DISPOSITION_SCHEMA_VERSION,
    DeliveryRecordConflictError,
    RcaDeliveryStore,
    StaleDeliveryEffectLeaseError,
    StaleDeliveryWatchLeaseError,
    _pre_w3_disposition_sha256,
    _pre_w3_effect_disposition_after,
    _pre_w3_effect_disposition_fingerprint,
)
from gateway.pnc_rca_kafka_contract import WorkflowEventPolicy, WorkflowTransition
from scripts.pnc_foxglove_delivery import canonical_viz_mcap_path, foxglove_url


NOW = datetime(2026, 7, 10, 8, 0, tzinfo=timezone.utc)
TOPIC = "feishu-project-workflow-event"


@pytest.fixture(autouse=True)
def _configured_viewer_origin(monkeypatch):
    monkeypatch.setenv("PNC_FOXGLOVE_RENDER_HOST", "https://viewer.internal")


def _policy():
    return WorkflowEventPolicy(
        topic=TOPIC,
        policy_version="issue-created-v1",
        project_keys=frozenset({"t03o4q"}),
        project_simple_names=frozenset({"g1q3"}),
        work_item_type_keys=frozenset({"issue"}),
        status_change_types=frozenset({"Reached"}),
        transitions=(
            WorkflowTransition(state_key="new-problem", pre_status=1, cur_status=2),
        ),
    )


def _record(*, offset: int = 10, issue_id: int = 7041712812):
    return KafkaRecord(
        topic=TOPIC,
        partition=2,
        offset=offset,
        value=json.dumps({
            "id": issue_id,
            "name": "ACC braking issue",
            "nodes": [
                {
                    "state_key": "new-problem",
                    "pre_status": 1,
                    "cur_status": 2,
                }
            ],
            "project_key": "t03o4q",
            "project_simple_name": "g1q3",
            "status_change_type": "Reached",
            "updated_at": 1783650000000,
            "work_item_type_key": "issue",
        }).encode(),
    )


def _delivery_observation(claim=None, **overrides):
    content = ""
    if claim is not None:
        content = str(
            claim.payload.get("comment_content")
            or claim.payload.get("message_content")
            or ""
        )
    value = {
        "schema_version": OBSERVATION_SCHEMA_VERSION,
        "work_item_id": claim.work_item_id if claim is not None else "7041712812",
        "case_key": (
            claim.submission_key or claim.business_key
            if claim is not None
            else "g1q3-rca-s1-" + "a" * 64
        ),
        "delivered_at": NOW.isoformat(),
        "level": "L1_observation",
        "has_attribution": False,
        "viz_published": False,
        "viz_bytes": 0,
        "evidence_channel_msg_count": None,
        "evidence_channel_msg_count_not_measured_reason": "fixture_not_measured",
        "evidence_refs_nonempty": None,
        "evidence_refs_nonempty_not_measured_reason": "fixture_not_measured",
        "evaluator_hit_count": 0,
        "pipeline_elapsed_seconds": 1.0,
        "outcome_content_sha256": (
            hashlib.sha256(content.encode("utf-8")).hexdigest()
            if claim is not None
            else "b" * 64
        ),
        "remote_receipt_id": "comment-observed",
        "release_id": "release-test",
        "inventory_pin": "c" * 64,
    }
    value.update(overrides)
    if "observation_id" not in overrides:
        value["observation_id"] = delivery_observation_id(value)
    return value


def _control(
    tmp_path,
    *,
    completed: bool = True,
    offset: int = 10,
    issue_id: int = 7041712812,
):
    control = RcaControlStore(tmp_path / "control.sqlite3")
    result = control.ingest_record(
        _record(offset=offset, issue_id=issue_id),
        policy=_policy(),
        submit_enabled=True,
    )
    if completed:
        claim = control.claim_outbox(lease_owner="submission-worker", now=NOW)
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
            now=NOW,
        )
    return control, result


def _bind_activation_execution(
    control,
    result,
    *,
    epoch_id="delivery-epoch-1",
    state="steady_active",
    slot_kind="kafka_success",
    start_offset=0,
):
    created = control.create_activation_epoch(
        epoch_id=epoch_id,
        preauthorization_fingerprint="a" * 64,
        preauthorization_gate_receipt_sha256="c" * 64,
        preauthorization_capsule_sha256="d" * 64,
        config_sha256="b" * 64,
        db_logical_identity={"database": "delivery-test"},
        partition_start_fence={TOPIC: {"2": start_offset}},
        operator="delivery-test",
        reason="delivery_activation_test",
        now=NOW,
    )
    if created["state"] == "safe_off":
        control.preauthorize_activation_epoch(
            epoch_id=epoch_id,
            preproduction_fingerprint="e" * 64,
            preproduction_gate_receipt_sha256="f" * 64,
            preproduction_capsule_sha256="1" * 64,
            expected_preauthorization_fingerprint="a" * 64,
            expected_preauthorization_gate_receipt_sha256="c" * 64,
            expected_preauthorization_capsule_sha256="d" * 64,
            expected_config_sha256=created["config_sha256"],
            expected_db_logical_identity_sha256=created["db_logical_identity_sha256"],
            expected_partition_start_fence_sha256=created[
                "partition_start_fence_sha256"
            ],
            operator="delivery-test",
            reason="bind exact preproduction capsule for delivery activation test",
            now=NOW,
        )
    current = NOW.isoformat()
    with sqlite3.connect(control.db_path) as conn:
        conn.execute(
            "UPDATE rca_activation_epochs SET state = ?, updated_at = ? "
            "WHERE epoch_id = ? AND is_current = 1",
            (state, current, epoch_id),
        )
        cursor = conn.execute(
            """
            INSERT INTO rca_activation_admission_ledger(
                epoch_id, admission_key, entrypoint, source_kind,
                source_identity_sha256, slot_kind, decision, reason,
                business_key, submission_key, generation,
                first_adjudicated_at, last_adjudicated_at,
                admitted_at, bound_at
            ) VALUES (?, ?, 'kafka_ingest', 'kafka', ?, ?, 'admit',
                      'delivery_test_exact_admit', ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                epoch_id,
                f"delivery-test:{result.submission_key}",
                "c" * 64,
                slot_kind,
                result.business_key,
                result.submission_key,
                result.generation,
                current,
                current,
                current,
                current,
            ),
        )
        ledger_id = int(cursor.lastrowid)
        conn.execute(
            "UPDATE business_triggers SET activation_epoch_id = ?, "
            "activation_ledger_id = ? WHERE submission_key = ?",
            (epoch_id, ledger_id, result.submission_key),
        )
        conn.execute(
            "UPDATE rca_outbox SET activation_epoch_id = ?, "
            "activation_ledger_id = ? WHERE submission_key = ?",
            (epoch_id, ledger_id, result.submission_key),
        )
        if state == "bounded_active":
            conn.execute(
                "UPDATE rca_activation_budget_slots "
                "SET authorized_source_kind = 'kafka', "
                "authorized_identity_sha256 = ?, authorized_at = ?, "
                "consumed_ledger_id = ?, consumed_at = ? "
                "WHERE epoch_id = ? AND slot_kind = ?",
                ("c" * 64, current, ledger_id, current, epoch_id, slot_kind),
            )
    return ledger_id


def _switch_activation_epoch(control, *, old_epoch, new_epoch):
    with sqlite3.connect(control.db_path) as conn:
        conn.execute(
            "UPDATE rca_activation_epochs SET state = 'aborted', is_current = 0, "
            "aborted_at = ?, superseded_at = ?, updated_at = ? "
            "WHERE epoch_id = ? AND is_current = 1",
            (NOW.isoformat(), NOW.isoformat(), NOW.isoformat(), old_epoch),
        )
    created = control.create_activation_epoch(
        epoch_id=new_epoch,
        preauthorization_fingerprint="d" * 64,
        preauthorization_gate_receipt_sha256="f" * 64,
        preauthorization_capsule_sha256="1" * 64,
        config_sha256="e" * 64,
        db_logical_identity={"database": "delivery-test"},
        partition_start_fence={TOPIC: {"2": 11}},
        operator="delivery-test",
        reason="delivery_activation_switch_test",
        now=NOW + timedelta(seconds=1),
    )
    control.preauthorize_activation_epoch(
        epoch_id=new_epoch,
        preproduction_fingerprint="2" * 64,
        preproduction_gate_receipt_sha256="3" * 64,
        preproduction_capsule_sha256="4" * 64,
        expected_preauthorization_fingerprint="d" * 64,
        expected_preauthorization_gate_receipt_sha256="f" * 64,
        expected_preauthorization_capsule_sha256="1" * 64,
        expected_config_sha256=created["config_sha256"],
        expected_db_logical_identity_sha256=created["db_logical_identity_sha256"],
        expected_partition_start_fence_sha256=created["partition_start_fence_sha256"],
        operator="delivery-test",
        reason="bind exact preproduction capsule for delivery epoch switch",
        now=NOW + timedelta(seconds=1),
    )
    with sqlite3.connect(control.db_path) as conn:
        conn.execute(
            "UPDATE rca_activation_epochs SET state = 'confirmed' "
            "WHERE epoch_id = ? AND is_current = 1",
            (new_epoch,),
        )


def _quarantine_submission(control, *, error_code="internal_submit_failure"):
    claim = control.claim_outbox(lease_owner="submission-worker", now=NOW)
    assert claim is not None
    control.quarantine_outbox(
        outbox_id=claim.outbox_id,
        lease_token=claim.lease_token,
        error_code=error_code,
        error_detail="internal bearer SECRET-MUST-NOT-LEAK",
        now=NOW + timedelta(seconds=1),
    )
    return claim


def _manual_request(message_id, *, mode="run_or_join", thread_root="om_origin_root"):
    return ManualRcaTriggerRequest(
        schema_version=MANUAL_TRIGGER_SCHEMA_VERSION,
        issue_url="https://project.feishu.cn/g1q3/issue/detail/7041712812",
        mode=mode,
        reason="manual_explicit_issue_action",
        platform="feishu",
        chat_id="oc_group123",
        thread_id=f"topic:{thread_root}",
        message_id=message_id,
        requester_id="ou_requester789",
    )


def _completed_cases(tmp_path, count: int) -> RcaDeliveryStore:
    for index in range(count):
        _control(
            tmp_path,
            offset=10 + index,
            issue_id=7041712812 + index,
        )
    store = RcaDeliveryStore(tmp_path / "control.sqlite3")
    assert store.backfill_completed_submissions(now=NOW) == count
    return store


def _delivery(claim):
    artifact_set = "g1q3-rca-artifact-v1-" + "a" * 64
    delivery_id = "g1q3-rca-delivery-v1-" + "b" * 64
    target = f"feishu_project:{claim.project_key}:{claim.work_item_type_key}:{claim.work_item_id}"
    issue_url = f"https://project.feishu.cn/{claim.project_key}/issue/detail/{claim.work_item_id}"
    report_url = "http://192.168.26.174:18081/rca/index.html"
    viz_mcap_vm = canonical_viz_mcap_path(claim.submission_key)
    rendered_foxglove_url = foxglove_url(viz_mcap_vm)
    semantic = {
        "schema_version": DELIVERY_EFFECT_SCHEMA_VERSION,
        "delivery_id": delivery_id,
        "effect_kind": DELIVERY_EFFECT_KIND,
        "target_key": target,
        "project_key": claim.project_key,
        "work_item_type_key": claim.work_item_type_key,
        "work_item_id": claim.work_item_id,
        "issue_url": issue_url,
        "artifact_set_id": artifact_set,
        "report_url": report_url,
        "viz_mcap_vm": viz_mcap_vm,
        "foxglove_url": rendered_foxglove_url,
        "report_status": "html_delivery_ready",
        "requires_human_review": True,
        "conclusion": "本单未能定向\n仅供参考，待确认\n未发现已知异常模式",
    }
    semantic_sha = compute_delivery_effect_payload_sha256(
        semantic, DELIVERY_EFFECT_KIND
    )
    effect_key = compute_delivery_effect_key(
        delivery_id=delivery_id,
        effect_kind=DELIVERY_EFFECT_KIND,
        target_key=target,
        semantic_payload_sha256=semantic_sha,
    )
    marker = delivery_effect_marker(effect_key, artifact_set)
    return VerifiedDelivery(
        delivery_id=delivery_id,
        effect_key=effect_key,
        semantic_payload_sha256=semantic_sha,
        artifact_set_id=artifact_set,
        business_key=claim.business_key,
        submission_key=claim.submission_key,
        generation=claim.generation,
        project_key=claim.project_key,
        work_item_type_key=claim.work_item_type_key,
        work_item_id=claim.work_item_id,
        target_key=target,
        issue_url=issue_url,
        report_url=report_url,
        viz_mcap_vm=viz_mcap_vm,
        foxglove_url=rendered_foxglove_url,
        conclusion="本单未能定向\n仅供参考，待确认\n未发现已知异常模式",
        marker=marker,
        manifest={"schema_version": "delivery_manifest_v2"},
        contract={"schema_version": "g1q3_delivery_contract_v1"},
        artifacts=(),
        effect_payload={
            **semantic,
            "effect_key": effect_key,
            "semantic_payload_sha256": semantic_sha,
            "marker": marker,
            "comment_content": marker + "\nreport ready",
        },
    )


def _claimed_effect(tmp_path):
    _control(tmp_path)
    store = RcaDeliveryStore(tmp_path / "control.sqlite3")
    store.backfill_completed_submissions(now=NOW)
    watch = store.claim_due_watch(lease_owner="collector", now=NOW)
    assert watch is not None
    store.create_delivery(
        claim=watch,
        delivery=_delivery(watch),
        status={"success": True, "state": "completed"},
        now=NOW,
    )
    effect = store.claim_due_effect(lease_owner="dispatcher", now=NOW)
    assert effect is not None
    return store, effect


def _insert_job_outcomes(store, rows):
    with sqlite3.connect(store.db_path) as conn:
        for index, row in enumerate(rows, start=1):
            outcome, created_at, *status_override = row
            delivery_status = status_override[0] if status_override else "delivered"
            token = f"{index:064d}"
            conn.execute(
                """
                INSERT INTO rca_delivery_jobs(
                    delivery_id, submission_key, business_key, generation,
                    artifact_set_id, project_key, work_item_type_key,
                    work_item_id, target_key, issue_url, report_url, outcome,
                    outcome_key, terminal_state, terminal_error_code, status,
                    manifest_json, contract_json, artifacts_json,
                    created_at, updated_at
                ) VALUES (?, ?, ?, 1, ?, 't03o4q', 'issue', ?, ?, ?, '', ?, ?,
                          ?, ?, ?, '{}', '{}', '[]', ?, ?)
                """,
                (
                    f"delivery-{token}",
                    f"submission-{token}",
                    f"business-{token}",
                    f"artifact-{token}",
                    str(7041712800 + index),
                    f"target-{token}",
                    f"https://project.feishu.cn/g1q3/issue/detail/{7041712800 + index}",
                    outcome,
                    f"outcome-{token}" if outcome != "success" else "",
                    "failed" if outcome != "success" else "",
                    "vm_terminal_failed" if outcome != "success" else "",
                    delivery_status,
                    created_at.isoformat(),
                    created_at.isoformat(),
                ),
            )


def _install_subscription_table(store):
    conn = store._connect()
    try:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS rca_delivery_subscriptions (
                subscription_key TEXT PRIMARY KEY,
                business_key TEXT NOT NULL,
                generation INTEGER NOT NULL CHECK(generation >= 1),
                source_id TEXT,
                effect_kind TEXT NOT NULL CHECK(effect_kind IN (
                    'feishu_issue_comment', 'feishu_thread_reply'
                )),
                target_key TEXT NOT NULL,
                target_json TEXT NOT NULL,
                required INTEGER NOT NULL CHECK(required IN (0, 1)),
                status TEXT NOT NULL CHECK(status IN (
                    'pending', 'materialized', 'suppressed', 'quarantined'
                )),
                delivery_id TEXT,
                effect_key TEXT,
                catchup_requested_at TEXT,
                materialized_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(business_key, generation, effect_kind, target_key)
            );
            """
        )
    finally:
        conn.close()


def _insert_subscription(
    store,
    claim,
    *,
    effect_kind,
    invalid_thread=False,
    chat_id="oc_group123",
    thread_root="om_root123",
    source_message_id="om_trigger456",
    requester_id="ou_requester789",
):
    if effect_kind == "feishu_issue_comment":
        target_key = (
            f"feishu_project:{claim.project_key}:{claim.work_item_type_key}:"
            f"{claim.work_item_id}"
        )
        target = {
            "schema_version": "pnc_rca_delivery_target_v1",
            "platform": "feishu_project",
            "project_key": claim.project_key,
            "work_item_type_key": claim.work_item_type_key,
            "work_item_id": claim.work_item_id,
            "output_cap": "L1",
        }
    else:
        target_key = f"feishu_thread:{chat_id}:{thread_root}"
        target = {
            "schema_version": "pnc_rca_delivery_target_v1",
            "platform": "feishu",
            "chat_id": chat_id,
            "thread_id": "topic:om_wrong" if invalid_thread else f"topic:{thread_root}",
            "reply_anchor_message_id": thread_root,
            "source_message_id": source_message_id,
            "requester_id": requester_id,
            "reply_in_thread": True,
            "output_cap": "L1",
        }
    conn = store._connect()
    try:
        conn.execute(
            """
            INSERT INTO rca_delivery_subscriptions(
                subscription_key, business_key, generation, effect_kind,
                target_key, target_json, required, status, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, 1, 'pending', ?, ?)
            """,
            (
                f"subscription:{effect_kind}:{target_key}",
                claim.business_key,
                claim.generation,
                effect_kind,
                target_key,
                json.dumps(target, ensure_ascii=False, sort_keys=True),
                NOW.isoformat(),
                NOW.isoformat(),
            ),
        )
    finally:
        conn.close()


def test_delivery_tables_share_control_db_and_use_durable_pragmas(tmp_path):
    _control(tmp_path)
    store = RcaDeliveryStore(tmp_path / "control.sqlite3")

    settings = store.journal_settings()
    assert settings == {"journal_mode": "wal", "synchronous": 2, "foreign_keys": 1}
    assert store.list_rows("rca_execution_watch") == []
    assert store.list_rows("rca_delivery_jobs") == []


def test_backpressure_snapshot_counts_all_unresolved_effect_states_atomically(
    tmp_path,
):
    store, claim = _claimed_effect(tmp_path)
    conn = store._connect()
    try:
        rows = (
            (1, "pending", "feishu_card_patch"),
            (2, "retry_wait", "feishu_thread_reply"),
            (3, "uncertain", "feishu_field_update"),
        )
        for index, status, effect_kind in rows:
            conn.execute(
                """
                INSERT INTO rca_delivery_effects(
                    effect_key, delivery_id, effect_kind, required,
                    target_key, payload_json, payload_sha256, status,
                    created_at, updated_at
                ) VALUES (?, ?, ?, 1, ?, '{}', ?, ?, ?, ?)
                """,
                (
                    f"g1q3-rca-effect-v1-{index:064d}",
                    claim.delivery_id,
                    effect_kind,
                    f"target-{index}",
                    str(index) * 64,
                    status,
                    NOW.isoformat(),
                    NOW.isoformat(),
                ),
            )
    finally:
        conn.close()

    snapshot = store.backpressure_snapshot(now=NOW)

    assert snapshot.pending == 1
    assert snapshot.claimed == 1
    assert snapshot.retry_wait == 1
    assert snapshot.uncertain == 1
    assert snapshot.unresolved_effects == 4
    assert snapshot.untracked_completed_submissions == 0
    assert snapshot.pending_watches == 0
    assert snapshot.running_watches == 0
    assert snapshot.unresolved_work == 4
    assert snapshot.circuit.state == "closed"
    assert snapshot.public_dict()["effect_counts"] == {
        "pending": 1,
        "claimed": 1,
        "retry_wait": 1,
        "uncertain": 1,
    }
    store.open_delivery_dispatcher_circuit(reason_code="feishu_auth_failed", now=NOW)
    circuit_snapshot = store.backpressure_snapshot(now=NOW)
    assert circuit_snapshot.circuit.is_open is True
    assert circuit_snapshot.circuit.reason_code == "feishu_auth_failed"


def test_delivered_terminal_failures_do_not_fail_delivery_slo(tmp_path):
    _control(tmp_path)
    store = RcaDeliveryStore(tmp_path / "control.sqlite3")
    _insert_job_outcomes(
        store,
        [
            ("terminal_failed", NOW - timedelta(seconds=120)),
            ("quarantined", NOW - timedelta(seconds=60)),
            ("terminal_failed", NOW - timedelta(seconds=10)),
        ],
    )

    health = store.health(now=NOW)
    snapshot = store.backpressure_snapshot(now=NOW)

    slo = health["delivery_outcome_slo"]
    assert slo["healthy"] is True
    assert slo["consecutive_failure_count"] == 0
    assert slo["consecutive_failure_breached"] is False
    assert slo["windows"]["5m"] == {
        "window_seconds": 300,
        "min_samples": 3,
        "max_failure_rate": 0.5,
        "sample_count": 3,
        "failure_count": 0,
        "failure_rate": 0.0,
        "breached": False,
    }
    assert health["business_blockers"]["outcome_slo_breached"] == 0
    assert snapshot.outcome_slo == slo
    assert snapshot.public_dict()["delivery_outcome_slo"]["healthy"] is True


def test_delivery_slo_counts_required_delivery_quarantine_and_resets_on_delivery(
    tmp_path,
):
    _control(tmp_path)
    store = RcaDeliveryStore(tmp_path / "control.sqlite3")
    _insert_job_outcomes(
        store,
        [
            ("success", NOW - timedelta(minutes=61, seconds=3), "quarantined"),
            ("success", NOW - timedelta(minutes=61, seconds=2), "quarantined"),
            ("success", NOW - timedelta(minutes=61, seconds=1), "quarantined"),
            ("success", NOW - timedelta(seconds=50), "quarantined"),
            ("terminal_failed", NOW - timedelta(seconds=40), "quarantined"),
            ("terminal_failed", NOW - timedelta(seconds=30), "delivered"),
            ("success", NOW - timedelta(seconds=20), "partial"),
            ("success", NOW - timedelta(seconds=10), "delivered"),
        ],
    )

    slo = store.health(now=NOW)["delivery_outcome_slo"]

    assert slo["healthy"] is True
    assert slo["consecutive_failure_count"] == 0
    assert slo["windows"]["60m"]["sample_count"] == 5
    assert slo["windows"]["60m"]["failure_count"] == 2
    assert all(not window["breached"] for window in slo["windows"].values())


def test_successful_analyses_with_quarantined_required_delivery_breach_slo(tmp_path):
    _control(tmp_path)
    store = RcaDeliveryStore(tmp_path / "control.sqlite3")
    _insert_job_outcomes(
        store,
        [
            ("success", NOW - timedelta(seconds=30), "quarantined"),
            ("success", NOW - timedelta(seconds=20), "quarantined"),
            ("success", NOW - timedelta(seconds=10), "quarantined"),
        ],
    )

    slo = store.health(now=NOW)["delivery_outcome_slo"]

    assert slo["healthy"] is False
    assert slo["consecutive_failure_count"] == 3
    assert slo["consecutive_failure_breached"] is True
    assert slo["windows"]["5m"]["failure_count"] == 3


def test_backpressure_snapshot_tracks_collector_handoff_without_double_counting(
    tmp_path,
):
    _control(tmp_path)
    store = RcaDeliveryStore(tmp_path / "control.sqlite3")

    before_backfill = store.backpressure_snapshot(now=NOW)
    assert before_backfill.untracked_completed_submissions == 1
    assert before_backfill.pending_watches == 0
    assert before_backfill.unresolved_effects == 0
    assert before_backfill.unresolved_work == 1

    assert store.backfill_completed_submissions(now=NOW) == 1
    after_backfill = store.backpressure_snapshot(now=NOW)
    assert after_backfill.untracked_completed_submissions == 0
    assert after_backfill.pending_watches == 1
    assert after_backfill.unresolved_work == 1


def test_consecutive_permanent_watch_failures_open_circuit_until_manual_reset(
    tmp_path,
):
    store = _completed_cases(tmp_path, PERMANENT_FAILURE_CIRCUIT_THRESHOLD + 1)

    first = store.claim_due_watch(lease_owner="collector", now=NOW)
    assert first is not None
    store.terminal_failure(
        submission_key=first.submission_key,
        lease_token=first.lease_token,
        status={"success": True, "state": "blocked"},
        error_code="vm_terminal_blocked_need_keyframe",
        error_detail="no candidate frame",
        now=NOW,
    )
    assert store.delivery_dispatcher_circuit().is_open is False
    assert store.permanent_failure_circuit_state()["consecutive_failures"] == 1

    second = store.claim_due_watch(lease_owner="collector", now=NOW)
    assert second is not None
    store.quarantine_watch(
        submission_key=second.submission_key,
        lease_token=second.lease_token,
        status={"success": True, "state": "completed"},
        error_code="html_script_execution_unsupported",
        error_detail="executable script is not allowed",
        now=NOW,
    )

    circuit = store.delivery_dispatcher_circuit()
    assert circuit.is_open is True
    assert circuit.reason_code == "delivery_permanent_failure_streak_exceeded"
    state = store.permanent_failure_circuit_state()
    assert state["threshold"] == PERMANENT_FAILURE_CIRCUIT_THRESHOLD
    assert state["consecutive_failures"] == PERMANENT_FAILURE_CIRCUIT_THRESHOLD
    assert state["last_failure"]["subject_key"] == second.submission_key
    assert store.backpressure_snapshot(now=NOW).circuit.is_open is True

    store.close_delivery_dispatcher_circuit(now=NOW + timedelta(seconds=1))
    assert store.delivery_dispatcher_circuit().is_open is False
    assert store.permanent_failure_circuit_state() == {
        "threshold": PERMANENT_FAILURE_CIRCUIT_THRESHOLD,
        "consecutive_failures": 0,
        "last_failure": {},
    }

    third = store.claim_due_watch(
        lease_owner="collector",
        now=NOW + timedelta(seconds=1),
    )
    assert third is not None
    store.terminal_failure(
        submission_key=third.submission_key,
        lease_token=third.lease_token,
        status={"success": True, "state": "failed"},
        error_code="vm_terminal_failed",
        error_detail="task failed",
        now=NOW + timedelta(seconds=1),
    )
    assert store.delivery_dispatcher_circuit().is_open is False
    assert store.permanent_failure_circuit_state()["consecutive_failures"] == 1


def test_missing_work_item_quarantine_does_not_open_pipeline_circuit(tmp_path):
    store, effect = _claimed_effect(tmp_path)
    conn = store._connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        store._record_permanent_failure_in_transaction(
            conn,
            circuit_name=DELIVERY_EFFECT_KIND,
            subject_key="prior-pipeline-failure",
            failure_state="quarantined",
            error_code="report_http_verification_mismatch",
            error_detail="sealed report mismatch",
            current=NOW.isoformat(),
        )
        conn.commit()
    finally:
        conn.close()
    assert store.permanent_failure_circuit_state()["consecutive_failures"] == 1

    store.quarantine_effect(
        claim=effect,
        error_code="feishu_work_item_not_found",
        error_detail="deterministic missing target",
        now=NOW,
    )

    assert store.delivery_dispatcher_circuit().is_open is False
    assert store.permanent_failure_circuit_state() == {
        "threshold": PERMANENT_FAILURE_CIRCUIT_THRESHOLD,
        "consecutive_failures": 0,
        "last_failure": {},
    }
    assert store.list_rows("rca_delivery_effects")[0]["status"] == "quarantined"


def test_successful_required_delivery_breaks_permanent_failure_streak(tmp_path):
    store = _completed_cases(tmp_path, 3)

    failed = store.claim_due_watch(lease_owner="collector", now=NOW)
    assert failed is not None
    store.terminal_failure(
        submission_key=failed.submission_key,
        lease_token=failed.lease_token,
        status={"success": True, "state": "failed"},
        error_code="vm_terminal_failed",
        error_detail="task failed",
        now=NOW,
    )

    delivered = store.claim_due_watch(lease_owner="collector", now=NOW)
    assert delivered is not None
    store.create_delivery(
        claim=delivered,
        delivery=_delivery(delivered),
        status={"success": True, "state": "completed"},
        now=NOW,
    )
    effect = store.claim_due_effect(lease_owner="dispatcher", now=NOW)
    assert effect is not None
    store.complete_effect(
        claim=effect,
        outcome="ack",
        remote_id="comment-1",
        receipt={"remote_id": "comment-1"},
        observation=_delivery_observation(
            effect, remote_receipt_id="comment-1"
        ),
        now=NOW,
    )
    assert store.permanent_failure_circuit_state()["consecutive_failures"] == 0

    next_failure = store.claim_due_watch(lease_owner="collector", now=NOW)
    assert next_failure is not None
    store.quarantine_watch(
        submission_key=next_failure.submission_key,
        lease_token=next_failure.lease_token,
        status={"success": True, "state": "completed"},
        error_code="artifact_hash_mismatch",
        error_detail="sealed artifact mismatch",
        now=NOW,
    )
    assert store.delivery_dispatcher_circuit().is_open is False
    assert store.permanent_failure_circuit_state()["consecutive_failures"] == 1


def test_successful_effect_commits_observation_intent_atomically(tmp_path):
    store, effect = _claimed_effect(tmp_path)
    observation = _delivery_observation(effect)

    store.complete_effect(
        claim=effect,
        outcome="ack",
        remote_id="comment-observed",
        receipt={"remote_id": "comment-observed"},
        observation=observation,
        now=NOW,
    )

    [row] = store.list_rows("rca_delivery_observation_outbox")
    assert row["effect_key"] == effect.effect_key
    assert row["status"] == "pending"
    [intent] = store.list_pending_delivery_observations()
    assert intent.payload == observation
    assert store.pending_delivery_observation_count() == 1
    assert store.mark_delivery_observation_appended(
        observation_id=intent.observation_id,
        payload_sha256=intent.payload_sha256,
        now=NOW + timedelta(seconds=1),
    ) is True
    assert store.pending_delivery_observation_count() == 0
    assert store.list_rows("rca_delivery_observation_outbox")[0]["status"] == "appended"
    assert store.requeue_delivery_observations(
        observations=[(intent.observation_id, intent.payload_sha256)]
    ) == 1
    [requeued] = store.list_delivery_observations()
    assert requeued.status == "pending"
    assert store.pending_delivery_observation_count() == 1


def test_observation_requeue_is_exact_and_atomic(tmp_path):
    store, effect = _claimed_effect(tmp_path)
    observation = _delivery_observation(effect)
    store.complete_effect(
        claim=effect,
        outcome="ack",
        remote_id="comment-observed",
        receipt={"remote_id": "comment-observed"},
        observation=observation,
        now=NOW,
    )
    [intent] = store.list_pending_delivery_observations()
    store.mark_delivery_observation_appended(
        observation_id=intent.observation_id,
        payload_sha256=intent.payload_sha256,
        now=NOW + timedelta(seconds=1),
    )

    with pytest.raises(
        DeliveryRecordConflictError,
        match="delivery_observation_requeue_identity_mismatch",
    ):
        store.requeue_delivery_observations(
            observations=[
                (intent.observation_id, "f" * 64),
                ("e" * 64, "e" * 64),
            ]
        )

    [unchanged] = store.list_delivery_observations()
    assert unchanged.status == "appended"


def test_successful_effect_requires_observation_intent(tmp_path):
    store, effect = _claimed_effect(tmp_path)

    with pytest.raises(
        DeliveryRecordConflictError, match="delivery_observation_required"
    ):
        store.complete_effect(
            claim=effect,
            outcome="ack",
            remote_id="comment-required",
            receipt={"remote_id": "comment-required"},
            now=NOW,
        )

    assert store.list_rows("rca_delivery_effects")[0]["status"] == "claimed"
    assert store.list_rows("rca_delivery_observation_outbox") == []


@pytest.mark.parametrize(
    ("overrides", "expected_code"),
    [
        (
            {"remote_receipt_id": "comment-other"},
            "delivery_observation_remote_receipt_id_mismatch",
        ),
        (
            {"work_item_id": "7041712999"},
            "delivery_observation_work_item_id_mismatch",
        ),
        (
            {"case_key": "g1q3-rca-s1-" + "f" * 64},
            "delivery_observation_case_key_mismatch",
        ),
        (
            {"outcome_content_sha256": "f" * 64},
            "delivery_observation_outcome_content_sha256_mismatch",
        ),
    ],
)
def test_successful_effect_rejects_self_consistent_observation_for_other_effect(
    tmp_path,
    overrides,
    expected_code,
):
    store, effect = _claimed_effect(tmp_path)
    observation = _delivery_observation(
        effect,
        **{"remote_receipt_id": "comment-bound", **overrides},
    )

    with pytest.raises(DeliveryRecordConflictError, match=expected_code):
        store.complete_effect(
            claim=effect,
            outcome="ack",
            remote_id="comment-bound",
            receipt={"remote_id": "comment-bound"},
            observation=observation,
            now=NOW,
        )

    assert store.list_rows("rca_delivery_effects")[0]["status"] == "claimed"
    assert store.list_rows("rca_delivery_observation_outbox") == []


def test_observation_intent_conflict_rolls_back_successful_settlement(tmp_path):
    store, effect = _claimed_effect(tmp_path)
    existing = _delivery_observation(
        effect, remote_receipt_id="existing-comment"
    )
    payload_json = json.dumps(existing, sort_keys=True, separators=(",", ":"))
    with sqlite3.connect(store.db_path) as conn:
        conn.execute(
            "INSERT INTO rca_delivery_observation_outbox("
            "observation_id, effect_key, payload_json, payload_sha256, status, "
            "created_at, appended_at) VALUES(?, ?, ?, ?, 'pending', ?, NULL)",
            (
                existing["observation_id"],
                effect.effect_key,
                payload_json,
                hashlib.sha256(payload_json.encode()).hexdigest(),
                NOW.isoformat(),
            ),
        )

    with pytest.raises(
        DeliveryRecordConflictError,
        match="delivery_observation_intent_conflict",
    ):
        store.complete_effect(
            claim=effect,
            outcome="ack",
            remote_id="comment-conflict",
            receipt={"remote_id": "comment-conflict"},
            observation=_delivery_observation(
                effect, remote_receipt_id="comment-conflict"
            ),
            now=NOW,
        )

    [effect_row] = store.list_rows("rca_delivery_effects")
    assert effect_row["status"] == "claimed"


@pytest.mark.parametrize(
    ("mutation", "expected_code"),
    [
        ("missing_level", "observation_required_field_missing"),
        ("identity_mismatch", "observation_identity_mismatch"),
    ],
)
def test_malformed_observation_rolls_back_successful_settlement(
    tmp_path,
    mutation,
    expected_code,
):
    store, effect = _claimed_effect(tmp_path)
    observation = _delivery_observation(
        effect, remote_receipt_id="comment-malformed"
    )
    if mutation == "missing_level":
        observation.pop("level")
    else:
        observation["remote_receipt_id"] = "identity-was-not-recomputed"
    before_attempts = store.list_rows("rca_delivery_attempts")

    with pytest.raises(DeliveryObservationError) as raised:
        store.complete_effect(
            claim=effect,
            outcome="ack",
            remote_id="comment-malformed",
            receipt={"remote_id": "comment-malformed"},
            observation=observation,
            now=NOW,
        )

    assert raised.value.code == expected_code
    [effect_row] = store.list_rows("rca_delivery_effects")
    assert effect_row["status"] == "claimed"
    assert store.list_rows("rca_delivery_attempts") == before_attempts
    assert store.list_rows("rca_delivery_observation_outbox") == []


def test_effect_claim_uses_business_trigger_acceptance_timestamp(tmp_path):
    store, effect = _claimed_effect(tmp_path)
    with sqlite3.connect(store.db_path) as conn:
        accepted_at = conn.execute(
            "SELECT created_at FROM business_triggers "
            "WHERE business_key = ? AND generation = ?",
            (effect.business_key, effect.generation),
        ).fetchone()[0]

    assert effect.business_accepted_at == accepted_at


@pytest.mark.parametrize("corruption", ["missing", "empty", "naive"])
def test_effect_claim_fails_closed_on_invalid_business_acceptance_timestamp(
    tmp_path,
    corruption,
):
    control, _result = _control(tmp_path)
    store = RcaDeliveryStore(control.db_path)
    store.backfill_completed_submissions(now=NOW)
    watch = store.claim_due_watch(lease_owner="collector", now=NOW)
    assert watch is not None
    store.create_delivery(
        claim=watch,
        delivery=_delivery(watch),
        status={"success": True, "state": "completed"},
        now=NOW,
    )
    with sqlite3.connect(store.db_path) as conn:
        if corruption == "missing":
            conn.execute(
                "DELETE FROM business_triggers WHERE business_key = ? AND generation = ?",
                (watch.business_key, watch.generation),
            )
        else:
            conn.execute(
                "UPDATE business_triggers SET created_at = ? "
                "WHERE business_key = ? AND generation = ?",
                (
                    "" if corruption == "empty" else "2026-07-31T10:00:00",
                    watch.business_key,
                    watch.generation,
                ),
            )
    before_effects = store.list_rows("rca_delivery_effects")
    before_attempts = store.list_rows("rca_delivery_attempts")

    with pytest.raises(
        DeliveryRecordConflictError,
        match="delivery_business_acceptance_timestamp_invalid",
    ):
        store.claim_due_effect(lease_owner="dispatcher", now=NOW)

    assert store.list_rows("rca_delivery_effects") == before_effects
    assert store.list_rows("rca_delivery_attempts") == before_attempts


def test_permanent_failure_and_circuit_open_roll_back_atomically(
    tmp_path,
    monkeypatch,
):
    store = _completed_cases(tmp_path, PERMANENT_FAILURE_CIRCUIT_THRESHOLD)
    first = store.claim_due_watch(lease_owner="collector", now=NOW)
    assert first is not None
    store.terminal_failure(
        submission_key=first.submission_key,
        lease_token=first.lease_token,
        status={"success": True, "state": "failed"},
        error_code="vm_terminal_failed",
        error_detail="task failed",
        now=NOW,
    )
    second = store.claim_due_watch(lease_owner="collector", now=NOW)
    assert second is not None

    def fail_circuit_write(*_args, **_kwargs):
        raise RuntimeError("simulated circuit write failure")

    monkeypatch.setattr(
        store,
        "_open_delivery_dispatcher_circuit_in_transaction",
        fail_circuit_write,
    )
    with pytest.raises(RuntimeError, match="simulated circuit write failure"):
        store.quarantine_watch(
            submission_key=second.submission_key,
            lease_token=second.lease_token,
            status={"success": True, "state": "completed"},
            error_code="artifact_hash_mismatch",
            error_detail="sealed artifact mismatch",
            now=NOW,
        )

    rows = {
        row["submission_key"]: row for row in store.list_rows("rca_execution_watch")
    }
    assert rows[second.submission_key]["state"] == "pending"
    assert rows[second.submission_key]["lease_token"] == second.lease_token
    assert store.permanent_failure_circuit_state()["consecutive_failures"] == 1
    assert store.delivery_dispatcher_circuit().is_open is False


def test_read_existing_backpressure_snapshot_never_creates_missing_database(
    tmp_path,
):
    path = tmp_path / "missing.sqlite3"

    with pytest.raises(RuntimeError, match="file_missing"):
        RcaDeliveryStore.read_existing_backpressure_snapshot(path, now=NOW)

    assert path.exists() is False


def test_current_store_has_long_lived_health_query_indexes(tmp_path):
    store = RcaDeliveryStore(tmp_path / "delivery.sqlite3")
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
        "idx_delivery_jobs_updated_at",
        "idx_delivery_attempts_outcome_started",
    }.issubset(indexes)


def test_future_delivery_schema_is_rejected_before_tables_are_changed(tmp_path):
    path = tmp_path / "control.sqlite3"
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            "CREATE TABLE rca_delivery_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
        )
        conn.execute(
            "INSERT INTO rca_delivery_meta VALUES "
            "('schema_version', 'pnc_rca_delivery_store_v999')"
        )
        conn.execute("CREATE TABLE future_delivery_only (id INTEGER PRIMARY KEY)")
        conn.commit()
        before = conn.execute(
            "SELECT name, sql FROM sqlite_master ORDER BY name"
        ).fetchall()
    finally:
        conn.close()

    with pytest.raises(
        RuntimeError, match="incompatible_delivery_store_schema:version"
    ):
        RcaDeliveryStore(path)

    conn = sqlite3.connect(path)
    try:
        after = conn.execute(
            "SELECT name, sql FROM sqlite_master ORDER BY name"
        ).fetchall()
    finally:
        conn.close()
    assert after == before


def test_require_current_delivery_store_never_creates_missing_path(tmp_path):
    path = tmp_path / "missing-parent" / "control.sqlite3"

    with pytest.raises(RuntimeError, match="rca_delivery_store_existing_path_missing"):
        RcaDeliveryStore(path, require_current=True)

    assert path.parent.exists() is False


def test_require_current_delivery_store_never_migrates_predecessor(tmp_path):
    path = tmp_path / "control.sqlite3"
    RcaDeliveryStore(path)
    with sqlite3.connect(path) as conn:
        conn.execute(
            "UPDATE rca_delivery_meta SET value='pnc_rca_delivery_store_v5' "
            "WHERE key='schema_version'"
        )
        before = conn.execute(
            "SELECT name, sql FROM sqlite_master ORDER BY name"
        ).fetchall()

    with pytest.raises(RuntimeError, match="rca_delivery_store_schema_not_current"):
        RcaDeliveryStore(path, require_current=True)

    with sqlite3.connect(path) as conn:
        after = conn.execute(
            "SELECT name, sql FROM sqlite_master ORDER BY name"
        ).fetchall()
        marker = conn.execute(
            "SELECT value FROM rca_delivery_meta WHERE key='schema_version'"
        ).fetchone()[0]
    assert after == before
    assert marker == "pnc_rca_delivery_store_v5"


def test_require_current_delivery_store_rejects_v10_without_outbox_unchanged(
    tmp_path,
):
    path = tmp_path / "control.sqlite3"
    RcaDeliveryStore(path)
    with sqlite3.connect(path) as conn:
        conn.executescript(
            """
            DROP INDEX idx_delivery_observation_outbox_status;
            DROP TABLE rca_delivery_observation_outbox;
            UPDATE rca_delivery_meta
               SET value = 'pnc_rca_delivery_store_v10'
             WHERE key = 'schema_version';
            """
        )
        before = conn.execute(
            "SELECT type, name, tbl_name, sql FROM sqlite_master ORDER BY type, name"
        ).fetchall()

    with pytest.raises(RuntimeError, match="rca_delivery_store_schema_not_current"):
        RcaDeliveryStore(path, require_current=True)

    with sqlite3.connect(path) as conn:
        after = conn.execute(
            "SELECT type, name, tbl_name, sql FROM sqlite_master ORDER BY type, name"
        ).fetchall()
        marker = conn.execute(
            "SELECT value FROM rca_delivery_meta WHERE key = 'schema_version'"
        ).fetchone()[0]
    assert after == before
    assert marker == "pnc_rca_delivery_store_v10"


def test_delivery_store_migrates_real_v7_shape_before_comment_slot_index(tmp_path):
    path = tmp_path / "control.sqlite3"
    RcaDeliveryStore(path)
    with sqlite3.connect(path) as conn:
        conn.executescript(
            """
            DROP INDEX idx_delivery_effects_comment_slot;
            DROP TABLE rca_conclusion_adjudication_repairs;
            DROP TABLE rca_conclusion_adjudications;
            DROP TABLE rca_failure_routes;
            ALTER TABLE rca_delivery_effects
                DROP COLUMN adjudication_comment_attempted_at;
            ALTER TABLE rca_delivery_effects
                DROP COLUMN adjudication_comment_attempt_count;
            ALTER TABLE rca_delivery_effects
                DROP COLUMN comment_slot_budget_exempt;
            ALTER TABLE rca_delivery_effects
                DROP COLUMN comment_slot_revision;
            ALTER TABLE rca_delivery_effects
                DROP COLUMN comment_slot_generation;
            ALTER TABLE rca_delivery_effects
                DROP COLUMN comment_slot_kind;
            ALTER TABLE rca_delivery_effects
                DROP COLUMN comment_slot_key;
            ALTER TABLE rca_delivery_effects
                DROP COLUMN comment_slot_schema_version;
            UPDATE rca_delivery_meta
               SET value='pnc_rca_delivery_store_v7'
             WHERE key='schema_version';
            """
        )

    RcaDeliveryStore(path)

    with sqlite3.connect(path) as conn:
        marker = conn.execute(
            "SELECT value FROM rca_delivery_meta WHERE key='schema_version'"
        ).fetchone()[0]
        columns = {
            row[1] for row in conn.execute("PRAGMA table_info(rca_delivery_effects)")
        }
        index = conn.execute(
            "SELECT sql FROM sqlite_master "
            "WHERE type='index' AND name='idx_delivery_effects_comment_slot'"
        ).fetchone()
    assert marker == DELIVERY_STORE_SCHEMA_VERSION
    assert "comment_slot_key" in columns
    assert index is not None
    assert "comment_slot_key" in index[0]


def test_require_current_delivery_store_opens_current_regular_file(tmp_path):
    path = tmp_path / "control.sqlite3"
    RcaDeliveryStore(path)

    reopened = RcaDeliveryStore(path, require_current=True)

    assert reopened.db_path == path


@pytest.mark.parametrize("mutation", ["drop_index", "weaken_check"])
def test_require_current_rejects_noncanonical_observation_outbox_schema(
    tmp_path,
    mutation,
):
    path = tmp_path / "control.sqlite3"
    RcaDeliveryStore(path)
    with sqlite3.connect(path) as conn:
        before_columns = [
            row[1]
            for row in conn.execute(
                "PRAGMA table_info(rca_delivery_observation_outbox)"
            )
        ]
        if mutation == "drop_index":
            conn.execute("DROP INDEX idx_delivery_observation_outbox_status")
        else:
            conn.execute("PRAGMA writable_schema=ON")
            conn.execute(
                "UPDATE sqlite_master SET sql = REPLACE(sql, ?, ?) "
                "WHERE type = 'table' "
                "AND name = 'rca_delivery_observation_outbox'",
                (
                    "status IN ('pending', 'appended')",
                    "status IN ('pending', 'appended', 'forged')",
                ),
            )
            schema_version = conn.execute("PRAGMA schema_version").fetchone()[0]
            conn.execute(f"PRAGMA schema_version = {schema_version + 1}")
            conn.execute("PRAGMA writable_schema=OFF")

    with sqlite3.connect(path) as conn:
        after_columns = [
            row[1]
            for row in conn.execute(
                "PRAGMA table_info(rca_delivery_observation_outbox)"
            )
        ]
    assert after_columns == before_columns

    with pytest.raises(
        RuntimeError,
        match="incompatible_delivery_store_schema:delivery_observation_outbox",
    ):
        RcaDeliveryStore(path, require_current=True)


def test_require_current_rejects_v8_marker_without_failure_route_sink(tmp_path):
    path = tmp_path / "control.sqlite3"
    RcaDeliveryStore(path)
    with sqlite3.connect(path) as conn:
        conn.execute("DROP TABLE rca_failure_routes")

    with pytest.raises(
        RuntimeError, match="incompatible_delivery_store_schema:failure_routes"
    ):
        RcaDeliveryStore(path, require_current=True)


def test_require_current_delivery_store_rejects_symlink(tmp_path):
    path = tmp_path / "control.sqlite3"
    RcaDeliveryStore(path)
    alias = tmp_path / "delivery-alias.sqlite3"
    alias.symlink_to(path)

    with pytest.raises(RuntimeError, match="rca_delivery_store_existing_path_invalid"):
        RcaDeliveryStore(alias, require_current=True)


@pytest.mark.parametrize(
    "suffix",
    [".pnc-rca-maintenance", ".pnc-rca-tombstone"],
)
def test_require_current_delivery_store_rejects_runtime_fence(tmp_path, suffix):
    path = tmp_path / "control.sqlite3"
    RcaDeliveryStore(path)
    marker = Path(f"{path}{suffix}")
    marker.write_text("fenced\n", encoding="utf-8")

    with pytest.raises(RuntimeError, match="rca_delivery_store_runtime_fenced"):
        RcaDeliveryStore(path, require_current=True)


def test_open_delivery_store_rechecks_runtime_fence_before_each_connection(tmp_path):
    path = tmp_path / "control.sqlite3"
    RcaDeliveryStore(path)
    store = RcaDeliveryStore(path, require_current=True)
    Path(f"{path}.pnc-rca-maintenance").write_text("fenced\n", encoding="utf-8")

    with pytest.raises(RuntimeError, match="rca_delivery_store_runtime_fenced"):
        store._connect()


def test_only_completed_submission_outbox_rows_are_backfilled(tmp_path):
    _control(tmp_path, completed=False)
    store = RcaDeliveryStore(tmp_path / "control.sqlite3")
    assert store.backfill_completed_submissions(now=NOW) == 0

    control = RcaControlStore(tmp_path / "control.sqlite3")
    claim = control.claim_outbox(lease_owner="submission-worker", now=NOW)
    assert claim is not None
    control.complete_outbox(
        outbox_id=claim.outbox_id,
        lease_token=claim.lease_token,
        result={
            "success": True,
            "submission_key": claim.submission_key,
            "task_id": claim.submission_key,
        },
        now=NOW,
    )
    assert store.backfill_completed_submissions(now=NOW) == 1
    watch = store.list_rows("rca_execution_watch")[0]
    assert watch["task_id"] == claim.submission_key


def test_quarantined_kafka_outbox_backfills_public_issue_terminal_atomically(
    tmp_path,
):
    control, result = _control(tmp_path, completed=False)
    _quarantine_submission(
        control,
        error_code="private_submit_error_with_sensitive_context",
    )
    store = RcaDeliveryStore(tmp_path / "control.sqlite3")

    before = store.backpressure_snapshot(now=NOW + timedelta(seconds=2))
    assert before.untracked_completed_submissions == 1
    assert before.unresolved_work == 1
    assert store.backfill_completed_submissions(now=NOW + timedelta(seconds=2)) == 1
    assert store.backfill_completed_submissions(now=NOW + timedelta(seconds=3)) == 0

    [watch] = store.list_rows("rca_execution_watch")
    [job] = store.list_rows("rca_delivery_jobs")
    [effect] = store.list_rows("rca_delivery_effects")
    [subscription] = store.list_rows("rca_delivery_subscriptions")
    assert watch["submission_key"] == result.submission_key
    assert watch["task_id"] is None
    assert watch["state"] == "delivery_created"
    assert watch["last_error_code"] == "private_submit_error_with_sensitive_context"
    assert "SECRET-MUST-NOT-LEAK" in watch["last_error_detail"]
    assert job["outcome"] == "quarantined"
    assert job["terminal_state"] == OUTBOX_QUARANTINED_TERMINAL_STATE
    assert job["terminal_error_code"] == OUTBOX_QUARANTINED_PUBLIC_ERROR_CODE
    assert job["report_url"] == ""
    assert json.loads(job["artifacts_json"]) == []
    assert effect["effect_kind"] == "feishu_issue_comment"
    assert effect["required"] == 1
    payload = json.loads(effect["payload_json"])
    assert payload["error_code"] == OUTBOX_QUARANTINED_PUBLIC_ERROR_CODE
    assert "private_submit_error" not in effect["payload_json"]
    assert "SECRET-MUST-NOT-LEAK" not in effect["payload_json"]
    assert subscription["effect_kind"] == "feishu_issue_comment"
    assert subscription["status"] == "materialized"
    assert subscription["effect_key"] == effect["effect_key"]
    assert all(
        item["state"] == "closed"
        for item in store.health(now=NOW + timedelta(seconds=2))[
            "delivery_dispatcher_circuits"
        ].values()
    )
    assert store.permanent_failure_circuit_state()["consecutive_failures"] == 0
    after = store.backpressure_snapshot(now=NOW + timedelta(seconds=3))
    assert after.untracked_completed_submissions == 0
    assert after.unresolved_effects == 1
    assert after.unresolved_work == 1


def test_current_epoch_pre_w3_quarantine_backfill_is_silent_and_idempotent(
    tmp_path,
):
    control, result = _control(tmp_path, completed=False)
    _quarantine_submission(control)
    _bind_activation_execution(control, result, state="steady_active")
    store = RcaDeliveryStore(control.db_path)

    subscriptions_before = store.list_rows("rca_delivery_subscriptions")
    before = store.backpressure_snapshot(now=NOW + timedelta(seconds=2))
    assert before.untracked_completed_submissions == 1
    assert store.backfill_completed_submissions(now=NOW + timedelta(seconds=2)) == 1
    assert store.backfill_completed_submissions(now=NOW + timedelta(seconds=3)) == 0

    [watch] = store.list_rows("rca_execution_watch")
    assert watch["state"] == "quarantined"
    assert watch["task_id"] is None
    assert watch["delivery_id"] is None
    status = json.loads(watch["last_status_json"])
    assert status["external_writes"] is False
    assert status["terminal_delivery_policy"] == "silent_internal_alert_only"
    assert status["error_code"] == "w3_execution_snapshot_missing"
    assert store.list_rows("rca_delivery_jobs") == []
    assert store.list_rows("rca_delivery_effects") == []
    subscriptions_after = store.list_rows("rca_delivery_subscriptions")
    assert len(subscriptions_after) == len(subscriptions_before)
    assert all(row["status"] == "quarantined" for row in subscriptions_after)
    assert all(
        row["effect_key"] is None and row["delivery_id"] is None
        for row in subscriptions_after
    )
    after = store.backpressure_snapshot(now=NOW + timedelta(seconds=3))
    assert after.untracked_completed_submissions == 0
    assert after.unresolved_effects == 0
    assert after.unresolved_work == 0
    assert all(
        item["state"] == "closed"
        for item in store.health(now=NOW + timedelta(seconds=3))[
            "delivery_dispatcher_circuits"
        ].values()
    )


def _current_epoch_valid_w3_quarantined_control(tmp_path):
    from tests.gateway.test_pnc_rca_control_store import _create_activation_epoch
    from tests.gateway.test_pnc_rca_w3_snapshot import _admit_manual_w3

    control = RcaControlStore(tmp_path / "control.sqlite3")
    epoch = _create_activation_epoch(control)
    current = datetime.now(timezone.utc)
    with sqlite3.connect(control.db_path) as conn:
        conn.execute(
            "UPDATE rca_activation_epochs SET state = 'steady_active', "
            "updated_at = ? WHERE epoch_id = ? AND is_current = 1",
            (current.isoformat(), epoch["epoch_id"]),
        )
    admitted = _admit_manual_w3(control)
    [snapshot_row] = control.list_rows("rca_admission_snapshots")
    snapshot = json.loads(snapshot_row["admission_snapshot_json"])
    assert snapshot["write_fence"]["state"] == "issued"
    claim = control.claim_outbox(
        lease_owner="valid-w3-quarantine-test",
        now=current + timedelta(seconds=1),
    )
    assert claim is not None
    assert claim.submission_key == admitted.submission_key
    control.quarantine_outbox(
        outbox_id=claim.outbox_id,
        lease_token=claim.lease_token,
        error_code="internal_submit_failure",
        error_detail="private submit failure",
        now=current + timedelta(seconds=2),
    )
    return control, current, snapshot


def test_current_epoch_valid_w3_quarantine_keeps_fenced_terminal_effect(tmp_path):
    control, current, snapshot = _current_epoch_valid_w3_quarantined_control(
        tmp_path
    )
    expected_issue_url = snapshot["canonical_request"]["ticket"]["issue_url"]
    store = RcaDeliveryStore(control.db_path)

    assert store.backfill_completed_submissions(
        now=current + timedelta(seconds=3)
    ) == 1

    [watch] = store.list_rows("rca_execution_watch")
    [job] = store.list_rows("rca_delivery_jobs")
    effects = store.list_rows("rca_delivery_effects")
    binding = json.loads(job["contract_json"])["w3_execution_snapshot"]
    assert watch["state"] == "delivery_created"
    assert job["issue_url"] == expected_issue_url
    assert binding["write_fence"]["state"] == "issued"
    assert len(binding["snapshot_core_sha256"]) == 64
    assert any(row["effect_kind"] == "feishu_issue_comment" for row in effects)
    assert all(row["status"] == "pending" for row in effects)
    effect = store.claim_due_effect(
        lease_owner="valid-w3-effect-target-test",
        now=current + timedelta(seconds=4),
        activation_required=True,
    )
    assert effect is not None
    assert effect.issue_url == expected_issue_url


def test_current_epoch_profile_terminal_without_w3_keeps_public_issue_target(
    tmp_path,
):
    from tests.gateway.test_pnc_rca_control_store import (
        _create_activation_epoch,
        _profile_snapshot_policy,
        _profile_snapshot_record,
    )

    control = RcaControlStore(tmp_path / "control.sqlite3")
    epoch = _create_activation_epoch(control, start_offset=20)
    with sqlite3.connect(control.db_path) as conn:
        conn.execute(
            "UPDATE rca_activation_epochs SET state='steady_active' "
            "WHERE epoch_id=? AND is_current=1",
            (epoch["epoch_id"],),
        )
    result = control.ingest_record(
        _profile_snapshot_record(20, "6841983153"),
        policy=_profile_snapshot_policy(),
        submit_enabled=True,
        activation_required=True,
        activation_slot_kind="kafka_success",
    )
    assert result.decision == "accepted"
    assert control.list_rows("rca_admission_snapshots") == []
    claim = control.claim_outbox(
        lease_owner="profile-terminal-quarantine-test",
        now=NOW,
    )
    assert claim is not None
    control.quarantine_outbox(
        outbox_id=claim.outbox_id,
        lease_token=claim.lease_token,
        error_code="business_profile_unsupported",
        error_detail="official project option is not registered",
        now=NOW + timedelta(seconds=1),
    )

    store = RcaDeliveryStore(control.db_path)
    assert (
        store.backfill_completed_submissions(
            now=NOW + timedelta(seconds=2),
            activation_required=True,
        )
        == 1
    )
    [watch] = store.list_rows("rca_execution_watch")
    [job] = store.list_rows("rca_delivery_jobs")
    assert watch["state"] == "delivery_created"
    assert job["issue_url"].endswith("/t03o4q/issue/detail/7041712812")
    assert job["terminal_error_code"] == "business_profile_unsupported"
    contract = json.loads(job["contract_json"])
    assert contract["diagnostic_code"] == "business_route_unsupported"
    assert "w3_execution_snapshot" not in contract
    effect = store.claim_due_effect(
        lease_owner="profile-terminal-fence-test",
        now=NOW + timedelta(seconds=3),
        activation_required=True,
    )
    assert effect is not None
    binding = store.validate_profile_terminal_external_write_binding(
        effect_key=effect.effect_key,
        delivery_id=effect.delivery_id,
        lease_token=effect.lease_token,
        lease_fence=effect.fence,
        operation="feishu_issue_comment",
        issue_url=effect.issue_url,
        target_key=effect.target_key,
        business_key=effect.business_key,
        submission_key=effect.submission_key,
        generation=effect.generation,
        require_write_started=False,
        now=NOW + timedelta(seconds=3),
    )
    assert binding["source_error_code"] == "business_profile_unsupported"
    with pytest.raises(
        RuntimeError,
        match="external_write_fence_operation_denied",
    ):
        store.validate_profile_terminal_external_write_binding(
            effect_key=effect.effect_key,
            delivery_id=effect.delivery_id,
            lease_token=effect.lease_token,
            lease_fence=effect.fence,
            operation="feishu_issue_field_update",
            issue_url=effect.issue_url,
            target_key=effect.target_key,
            business_key=effect.business_key,
            submission_key=effect.submission_key,
            generation=effect.generation,
            require_write_started=False,
            now=NOW + timedelta(seconds=3),
        )
    store.mark_effect_write_started(
        claim=effect,
        now=NOW + timedelta(seconds=4),
        activation_required=True,
    )
    started = store.validate_profile_terminal_external_write_binding(
        effect_key=effect.effect_key,
        delivery_id=effect.delivery_id,
        lease_token=effect.lease_token,
        lease_fence=effect.fence,
        operation="feishu_issue_comment",
        issue_url=effect.issue_url,
        target_key=effect.target_key,
        business_key=effect.business_key,
        submission_key=effect.submission_key,
        generation=effect.generation,
        require_write_started=True,
        now=NOW + timedelta(seconds=4),
    )
    assert started["write_phase"] == "write_started"


def test_profile_terminal_provider_claim_rechecks_lease_target_and_epoch(tmp_path, monkeypatch):
    from gateway import pnc_rca_provider_fence as provider_fence
    from gateway.pnc_rca_provider_fence import (
        build_profile_terminal_provider_claim,
    )
    from gateway.pnc_rca_write_fence import ExternalWriteFenceError
    from tests.gateway.test_pnc_rca_control_store import (
        _create_activation_epoch,
        _profile_snapshot_policy,
        _profile_snapshot_record,
    )

    provider_now = datetime.now(timezone.utc)
    control = RcaControlStore(tmp_path / "control.sqlite3")
    epoch = _create_activation_epoch(control, start_offset=20)
    with sqlite3.connect(control.db_path) as conn:
        conn.execute(
            "UPDATE rca_activation_epochs SET state='steady_active' "
            "WHERE epoch_id=? AND is_current=1",
            (epoch["epoch_id"],),
        )
    result = control.ingest_record(
        _profile_snapshot_record(20, "6841983153"),
        policy=_profile_snapshot_policy(),
        submit_enabled=True,
        activation_required=True,
        activation_slot_kind="kafka_success",
    )
    assert result.decision == "accepted"
    outbox = control.claim_outbox(
        lease_owner="profile-terminal-provider-test",
        now=provider_now,
    )
    assert outbox is not None
    control.quarantine_outbox(
        outbox_id=outbox.outbox_id,
        lease_token=outbox.lease_token,
        error_code="business_profile_unsupported",
        error_detail="profile provider claim fixture",
        now=provider_now + timedelta(seconds=1),
    )
    store = RcaDeliveryStore(control.db_path)
    assert store.backfill_completed_submissions(
        now=provider_now + timedelta(seconds=2),
        activation_required=True,
    ) == 1
    effect = store.claim_due_effect(
        lease_owner="profile-terminal-provider-test",
        now=provider_now + timedelta(seconds=3),
        activation_required=True,
    )
    assert effect is not None
    store.mark_effect_write_started(
        claim=effect,
        now=provider_now + timedelta(seconds=4),
        activation_required=True,
    )
    binding = store.validate_profile_terminal_external_write_binding(
        effect_key=effect.effect_key,
        delivery_id=effect.delivery_id,
        lease_token=effect.lease_token,
        lease_fence=effect.fence,
        operation="feishu_issue_comment",
        issue_url=effect.issue_url,
        target_key=effect.target_key,
        business_key=effect.business_key,
        submission_key=effect.submission_key,
        generation=effect.generation,
        require_write_started=True,
        now=provider_now + timedelta(seconds=4),
    )
    claim = build_profile_terminal_provider_claim(
        epoch_id=binding["epoch_id"],
        activation_ledger_id=binding["activation_ledger_id"],
        effect_key=binding["effect_key"],
        delivery_id=binding["delivery_id"],
        lease_token=binding["lease_token"],
        lease_fence=binding["lease_fence"],
        issue_target=binding["issue_url"],
        target_key=binding["target_key"],
        business_key=binding["business_key"],
        submission_key=binding["submission_key"],
        generation=binding["generation"],
        source_error_code=binding["source_error_code"],
    )
    monkeypatch.setattr(provider_fence, "_canonical_store", lambda: control)
    live = provider_fence.revalidate_provider_write_claim(
        claim,
        operation="feishu_issue_comment",
        issue_project_key="t03o4q",
        issue_work_item_id="7041712812",
    )
    assert live["authority_kind"] == "profile_terminal"
    with sqlite3.connect(control.db_path) as conn:
        conn.execute(
            "UPDATE rca_trigger_sources SET source_kind='feishu_group_manual' "
            "WHERE source_id = (SELECT origin_source_id FROM business_triggers "
            "WHERE submission_key = ?)",
            (effect.submission_key,),
        )
    with pytest.raises(
        ExternalWriteFenceError,
        match="external_write_fence_identity_mismatch",
    ):
        provider_fence.revalidate_provider_write_claim(
            claim,
            operation="feishu_issue_comment",
            issue_project_key="t03o4q",
            issue_work_item_id="7041712812",
        )
    with sqlite3.connect(control.db_path) as conn:
        conn.execute(
            "UPDATE rca_trigger_sources SET source_kind='kafka_workflow_event' "
            "WHERE source_id = (SELECT origin_source_id FROM business_triggers "
            "WHERE submission_key = ?)",
            (effect.submission_key,),
        )
        normalized_raw = conn.execute(
            "SELECT normalized_json FROM business_triggers WHERE submission_key = ?",
            (effect.submission_key,),
        ).fetchone()[0]
        normalized = json.loads(normalized_raw)
        normalized["business_profile_resolution"]["project_option_ids"] = [
            "tampered-option"
        ]
        conn.execute(
            "UPDATE business_triggers SET normalized_json = ? "
            "WHERE submission_key = ?",
            (json.dumps(normalized, ensure_ascii=False, sort_keys=True), effect.submission_key),
        )
    with pytest.raises(
        ExternalWriteFenceError,
        match="external_write_fence_identity_mismatch",
    ):
        provider_fence.revalidate_provider_write_claim(
            claim,
            operation="feishu_issue_comment",
            issue_project_key="t03o4q",
            issue_work_item_id="7041712812",
        )
    with sqlite3.connect(control.db_path) as conn:
        conn.execute(
            "UPDATE business_triggers SET normalized_json = ? "
            "WHERE submission_key = ?",
            (normalized_raw, effect.submission_key),
        )
    with pytest.raises(
        ExternalWriteFenceError,
        match="external_write_fence_operation_denied",
    ):
        provider_fence.revalidate_provider_write_claim(
            claim,
            operation="feishu_issue_field_update",
            issue_project_key="t03o4q",
            issue_work_item_id="7041712812",
        )
    assert claim.payload()["authority"]["operation"] == "feishu_issue_comment"
    assert "thread_id" not in claim.payload()["authority"]
    wrong_target = build_profile_terminal_provider_claim(
        epoch_id=binding["epoch_id"],
        activation_ledger_id=binding["activation_ledger_id"],
        effect_key=binding["effect_key"],
        delivery_id=binding["delivery_id"],
        lease_token=binding["lease_token"],
        lease_fence=binding["lease_fence"],
        issue_target="https://project.feishu.cn/t03o4q/issue/detail/9999999999",
        target_key=binding["target_key"],
        business_key=binding["business_key"],
        submission_key=binding["submission_key"],
        generation=binding["generation"],
        source_error_code=binding["source_error_code"],
    )
    with pytest.raises(
        ExternalWriteFenceError,
        match="external_write_fence_target_mismatch",
    ):
        provider_fence.revalidate_provider_write_claim(
            wrong_target,
            operation="feishu_issue_comment",
            issue_project_key="t03o4q",
            issue_work_item_id="7041712812",
        )
    with sqlite3.connect(control.db_path) as conn:
        conn.execute(
            "UPDATE rca_activation_epochs SET state='aborted' "
            "WHERE epoch_id=? AND is_current=1",
            (epoch["epoch_id"],),
        )
    with pytest.raises(
        ExternalWriteFenceError,
        match="external_write_fence_epoch_not_current",
    ):
        provider_fence.revalidate_provider_write_claim(
            claim,
            operation="feishu_issue_comment",
            issue_project_key="t03o4q",
            issue_work_item_id="7041712812",
        )


def test_current_epoch_adapter_pending_terminal_without_w3_is_public(tmp_path):
    from tests.gateway.test_pnc_rca_control_store import (
        _create_activation_epoch,
        _profile_snapshot_policy,
        _profile_snapshot_record,
    )

    control = RcaControlStore(tmp_path / "control.sqlite3")
    epoch = _create_activation_epoch(control, start_offset=21)
    with sqlite3.connect(control.db_path) as conn:
        conn.execute(
            "UPDATE rca_activation_epochs SET state='steady_active' "
            "WHERE epoch_id=? AND is_current=1",
            (epoch["epoch_id"],),
        )
    result = control.ingest_record(
        _profile_snapshot_record(21, "7019637554"),
        policy=_profile_snapshot_policy(),
        submit_enabled=True,
        activation_required=True,
        activation_slot_kind="kafka_success",
    )
    assert result.decision == "accepted"
    claim = control.claim_outbox(
        lease_owner="profile-adapter-terminal-quarantine-test",
        now=NOW,
    )
    assert claim is not None
    control.quarantine_outbox(
        outbox_id=claim.outbox_id,
        lease_token=claim.lease_token,
        error_code="business_profile_adapter_not_ready",
        error_detail="matched profile input adapter is not ready",
        now=NOW + timedelta(seconds=1),
    )

    store = RcaDeliveryStore(control.db_path)
    assert store.backfill_completed_submissions(
        now=NOW + timedelta(seconds=2),
        activation_required=True,
    ) == 1
    [watch] = store.list_rows("rca_execution_watch")
    [job] = store.list_rows("rca_delivery_jobs")
    assert watch["state"] == "delivery_created"
    assert job["terminal_error_code"] == "business_profile_adapter_not_ready"
    contract = json.loads(job["contract_json"])
    assert contract["diagnostic_code"] == "business_adapter_not_ready"
    assert "w3_execution_snapshot" not in contract


@pytest.mark.parametrize(
    "tamper_kind",
    ("creation_rule_version", "resolution_project_key"),
)
def test_forged_profile_terminal_observation_is_silent_and_settled(
    tmp_path,
    tamper_kind,
):
    from tests.gateway.test_pnc_rca_control_store import (
        _create_activation_epoch,
        _profile_snapshot_policy,
        _profile_snapshot_record,
    )

    control = RcaControlStore(tmp_path / "control.sqlite3")
    epoch = _create_activation_epoch(control, start_offset=22)
    with sqlite3.connect(control.db_path) as conn:
        conn.execute(
            "UPDATE rca_activation_epochs SET state='steady_active' "
            "WHERE epoch_id=? AND is_current=1",
            (epoch["epoch_id"],),
        )
    result = control.ingest_record(
        _profile_snapshot_record(22, "6841983153"),
        policy=_profile_snapshot_policy(),
        submit_enabled=True,
        activation_required=True,
        activation_slot_kind="kafka_success",
    )
    assert result.decision == "accepted"
    claim = control.claim_outbox(
        lease_owner="profile-terminal-forged-observation-test",
        now=NOW,
    )
    assert claim is not None
    control.quarantine_outbox(
        outbox_id=claim.outbox_id,
        lease_token=claim.lease_token,
        error_code="business_profile_unsupported",
        error_detail="forged profile observation must remain internal",
        now=NOW + timedelta(seconds=1),
    )
    with sqlite3.connect(control.db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT source_event_id, normalized_json FROM business_triggers "
            "WHERE submission_key = ?",
            (result.submission_key,),
        ).fetchone()
        normalized = json.loads(row["normalized_json"])
        if tamper_kind == "creation_rule_version":
            normalized["creation_rule_version"] = "forged-rule-v1"
        else:
            normalized["business_profile_resolution"]["project_key"] = (
                "forged-project"
            )
        forged = json.dumps(
            normalized,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        conn.execute(
            "UPDATE business_triggers SET normalized_json = ? "
            "WHERE submission_key = ?",
            (forged, result.submission_key),
        )
        conn.execute(
            "UPDATE kafka_inbox SET normalized_json = ? WHERE event_uid = ?",
            (forged, row["source_event_id"]),
        )

    store = RcaDeliveryStore(control.db_path)
    assert store.backfill_completed_submissions(
        now=NOW + timedelta(seconds=2),
        activation_required=True,
    ) == 1
    [watch] = store.list_rows("rca_execution_watch")
    assert watch["state"] == "quarantined"
    assert watch["last_error_code"] == "profile_terminal_binding_invalid"
    assert store.list_rows("rca_delivery_jobs") == []
    assert store.list_rows("rca_delivery_effects") == []


def test_invalid_profile_terminal_binding_is_silent_and_settled(tmp_path):
    from tests.gateway.test_pnc_rca_control_store import (
        _create_activation_epoch,
        _profile_snapshot_policy,
        _profile_snapshot_record,
    )

    control = RcaControlStore(tmp_path / "control.sqlite3")
    epoch = _create_activation_epoch(control, start_offset=22)
    with sqlite3.connect(control.db_path) as conn:
        conn.execute(
            "UPDATE rca_activation_epochs SET state='steady_active' "
            "WHERE epoch_id=? AND is_current=1",
            (epoch["epoch_id"],),
        )
    result = control.ingest_record(
        _profile_snapshot_record(22, "6841983153"),
        policy=_profile_snapshot_policy(),
        submit_enabled=True,
        activation_required=True,
        activation_slot_kind="kafka_success",
    )
    assert result.decision == "accepted"
    claim = control.claim_outbox(
        lease_owner="profile-terminal-invalid-binding-test",
        now=NOW,
    )
    assert claim is not None
    control.quarantine_outbox(
        outbox_id=claim.outbox_id,
        lease_token=claim.lease_token,
        error_code="business_profile_adapter_not_ready",
        error_detail="injected mismatched terminal code",
        now=NOW + timedelta(seconds=1),
    )

    store = RcaDeliveryStore(control.db_path)
    assert store.backfill_completed_submissions(
        now=NOW + timedelta(seconds=2),
        activation_required=True,
    ) == 1
    [watch] = store.list_rows("rca_execution_watch")
    assert watch["state"] == "quarantined"
    assert watch["last_error_code"] == "profile_terminal_binding_invalid"
    assert store.list_rows("rca_delivery_jobs") == []
    assert store.list_rows("rca_delivery_effects") == []
    assert {
        row["status"] for row in store.list_rows("rca_delivery_subscriptions")
    } == {"quarantined"}

    control.transition_activation_epoch(
        epoch_id=epoch["epoch_id"],
        expected_state="steady_active",
        target_state="aborted",
        operator="delivery-test",
        reason="prove silent invalid terminal is fully settled before replacement",
    )
    replacement = _create_activation_epoch(
        control,
        epoch_id="rca-release-after-silent-terminal",
        start_offset=23,
        preauthorize=False,
    )
    assert replacement["state"] == "safe_off"


def test_silent_terminal_without_required_subscriptions_blocks_replacement(
    tmp_path,
):
    from tests.gateway.test_pnc_rca_control_store import (
        _create_activation_epoch,
        _profile_snapshot_policy,
        _profile_snapshot_record,
    )

    control = RcaControlStore(tmp_path / "control.sqlite3")
    epoch = _create_activation_epoch(control, start_offset=23)
    with sqlite3.connect(control.db_path) as conn:
        conn.execute(
            "UPDATE rca_activation_epochs SET state='steady_active' "
            "WHERE epoch_id=? AND is_current=1",
            (epoch["epoch_id"],),
        )
    result = control.ingest_record(
        _profile_snapshot_record(23, "6841983153"),
        policy=_profile_snapshot_policy(),
        submit_enabled=True,
        activation_required=True,
        activation_slot_kind="kafka_success",
    )
    assert result.decision == "accepted"
    claim = control.claim_outbox(
        lease_owner="profile-terminal-missing-subscription-test",
        now=NOW,
    )
    assert claim is not None
    control.quarantine_outbox(
        outbox_id=claim.outbox_id,
        lease_token=claim.lease_token,
        error_code="business_profile_adapter_not_ready",
        error_detail="injected mismatched terminal code",
        now=NOW + timedelta(seconds=1),
    )
    store = RcaDeliveryStore(control.db_path)
    assert store.backfill_completed_submissions(
        now=NOW + timedelta(seconds=2),
        activation_required=True,
    ) == 1
    with sqlite3.connect(control.db_path) as conn:
        conn.execute(
            "DELETE FROM rca_delivery_subscriptions "
            "WHERE business_key=? AND generation=?",
            (result.business_key, result.generation),
        )

    control.transition_activation_epoch(
        epoch_id=epoch["epoch_id"],
        expected_state="steady_active",
        target_state="aborted",
        operator="delivery-test",
        reason="prove missing subscription cannot be mistaken for settlement",
        now=NOW + timedelta(seconds=3),
    )
    with pytest.raises(
        ActivationEpochError,
        match="activation_bound_delivery_backlog_not_drained",
    ):
        _create_activation_epoch(
            control,
            epoch_id="rca-release-after-missing-subscriptions",
            start_offset=24,
            preauthorize=False,
        )


def test_current_epoch_w3_validator_runtime_error_rolls_back_backfill(
    tmp_path, monkeypatch
):
    control, current, _snapshot = _current_epoch_valid_w3_quarantined_control(
        tmp_path
    )
    store = RcaDeliveryStore(control.db_path)
    subscriptions_before = store.list_rows("rca_delivery_subscriptions")

    def validator_crash(*_args, **_kwargs):
        raise RuntimeError("injected_w3_validator_runtime_error")

    monkeypatch.setattr(
        "gateway.pnc_rca_delivery_store.validate_write_fence_source_binding",
        validator_crash,
    )
    with pytest.raises(RuntimeError, match="injected_w3_validator_runtime_error"):
        store.backfill_completed_submissions(now=current + timedelta(seconds=3))

    assert store.list_rows("rca_execution_watch") == []
    assert store.list_rows("rca_delivery_jobs") == []
    assert store.list_rows("rca_delivery_effects") == []
    assert store.list_rows("rca_delivery_subscriptions") == subscriptions_before
    backpressure = store.backpressure_snapshot(now=current + timedelta(seconds=3))
    assert backpressure.untracked_completed_submissions == 1
    assert backpressure.unresolved_effects == 0
    assert backpressure.unresolved_work == 1


def _legacy_pre_w3_pending_effects(tmp_path, *, count=2):
    results = []
    control = None
    for index in range(count):
        control, result = _control(
            tmp_path,
            completed=False,
            offset=40 + index,
            issue_id=7041712900 + index,
        )
        _quarantine_submission(control)
        results.append(result)
    assert control is not None
    store = RcaDeliveryStore(control.db_path)
    assert store.backfill_completed_submissions(now=NOW + timedelta(seconds=2)) == count
    for result in results:
        _bind_activation_execution(control, result, state="steady_active")
    with sqlite3.connect(control.db_path) as conn:
        conn.execute(
            "UPDATE rca_activation_epochs SET production_fingerprint = ?, "
            "production_gate_receipt_sha256 = ? WHERE is_current = 1",
            ("2" * 64, "3" * 64),
        )
    store.open_delivery_dispatcher_circuit(
        reason_code="delivery_permanent_failure_streak_exceeded",
        reason_detail="pre-W3 fixture circuit",
        now=NOW + timedelta(seconds=3),
    )
    keys = sorted(
        row["effect_key"] for row in store.list_rows("rca_delivery_effects")
    )
    assert len(keys) == count
    return control, store, keys


def _pre_w3_disposition_audit(store, snapshot, tmp_path, *, now):
    observed = store.db_path.absolute().lstat()
    receipt_path = (tmp_path / "pre-w3-disposition.json").absolute()
    parent = receipt_path.parent.lstat()
    tool_provenance = {
        "entrypoint_path": str((tmp_path / "dispatcher.py").absolute()),
        "entrypoint_sha256": "a" * 64,
        "delivery_store_path": str((tmp_path / "delivery-store.py").absolute()),
        "delivery_store_sha256": "b" * 64,
        "receipt_helper_path": str((tmp_path / "receipt-helper.py").absolute()),
        "receipt_helper_sha256": "c" * 64,
        "control_store_path": str((tmp_path / "control-store.py").absolute()),
        "control_store_sha256": "d" * 64,
        "bootstrap_path": str((tmp_path / "bootstrap.py").absolute()),
        "bootstrap_sha256": "e" * 64,
    }
    plan_id = _pre_w3_disposition_sha256({
        "before": snapshot["snapshot_sha256"],
        "destination": str(receipt_path),
    })
    disposition_id = _pre_w3_disposition_sha256({"plan_id": plan_id})
    after = _pre_w3_effect_disposition_after(
        snapshot,
        disposition_id=disposition_id,
        recorded_at=now.isoformat(),
    )
    count = len(snapshot["effect_keys"])
    audit = {
        "schema_version": PRE_W3_EFFECT_DISPOSITION_SCHEMA_VERSION,
        "command": PRE_W3_EFFECT_DISPOSITION_COMMAND,
        "disposition_id": disposition_id,
        "plan_id": plan_id,
        "recorded_at": now.isoformat(),
        "operator": "test-operator",
        "reason": "dispose exact pre-W3 effects without provider calls",
        "effect_kind": DELIVERY_EFFECT_KIND,
        "effect_keys": snapshot["effect_keys"],
        "effect_set_sha256": snapshot["effect_set_sha256"],
        "before_snapshot_sha256": snapshot["snapshot_sha256"],
        "control_db_identity": {
            "path": str(store.db_path.absolute()),
            "device": int(observed.st_dev),
            "inode": int(observed.st_ino),
            "size": int(observed.st_size),
            "mtime_ns": int(observed.st_mtime_ns),
        },
        "destination_binding": {
            "path_sha256": hashlib.sha256(
                str(receipt_path).encode("utf-8")
            ).hexdigest(),
            "parent_device": int(parent.st_dev),
            "parent_inode": int(parent.st_ino),
        },
        "backup_binding": {
            "path": str((tmp_path / "control.before.sqlite3").absolute()),
            "sha256": "4" * 64,
            "size_bytes": 4096,
            "device": 10,
            "inode": 11,
            "mtime_ns": 12,
            "source_path": str(store.db_path.absolute()),
            "source_sha256": "4" * 64,
            "source_device": int(observed.st_dev),
            "source_inode": int(observed.st_ino),
            "source_size_bytes": int(observed.st_size),
            "source_mtime_ns": int(observed.st_mtime_ns),
            "journal_mode": "delete",
            "quick_check": "ok",
            "foreign_key_check": "ok",
            "snapshot_sha256": snapshot["snapshot_sha256"],
            "effect_set_sha256": snapshot["effect_set_sha256"],
            "logical_digest_sha256": snapshot["control_db_logical_digest"][
                "sha256"
            ],
        },
        "active_release_binding": {
            "path": str((tmp_path / "active-release-binding.json").absolute()),
            "sha256": "5" * 64,
            "release_id": "rca-test-release",
            "bootstrap_epoch_id": "rca-bootstrap-test",
            "candidate_env_sha256": "6" * 64,
            "live_env_sha256": "7" * 64,
            "live_env_matches_candidate": False,
        },
        "config_binding_sha256": "8" * 64,
        "tool_provenance": tool_provenance,
        "tool_provenance_sha256": _pre_w3_disposition_sha256(tool_provenance),
        "before": snapshot,
        "after": after,
        "effect_delta": {
            "external_effects_triggered": False,
            "provider_calls": 0,
            "control_meta_inserted": 1,
            "attempt_audits_inserted": count,
            "effects_updated": count,
            "jobs_updated": count,
            "quarantine_audits_inserted": 2 * count,
            "total_database_rows": 1 + (5 * count),
        },
        "external_writes_performed": False,
        "provider_calls_performed": 0,
    }
    audit["receipt_fingerprint"] = _pre_w3_effect_disposition_fingerprint(audit)
    return audit


def test_exact_pre_w3_disposition_is_audited_atomic_and_idempotent(tmp_path):
    _control_store, store, keys = _legacy_pre_w3_pending_effects(tmp_path)
    circuit_before = store.delivery_dispatcher_circuit_reset_state()
    snapshot = store.pre_w3_effect_disposition_snapshot(effect_keys=reversed(keys))
    audit = _pre_w3_disposition_audit(
        store,
        snapshot,
        tmp_path,
        now=NOW + timedelta(seconds=4),
    )

    applied_audit, applied = store.quarantine_pre_w3_effects_with_audit(
        audit=audit,
        now=NOW + timedelta(seconds=4),
    )

    assert applied is True
    assert applied_audit == audit
    effects = store.list_rows("rca_delivery_effects")
    assert [row["effect_key"] for row in effects] != []
    assert {row["status"] for row in effects} == {"quarantined"}
    assert {row["write_phase"] for row in effects} == {"settled"}
    assert {row["attempt"] for row in effects} == {0}
    assert {row["fence"] for row in effects} == {0}
    assert all(row["remote_receipt_json"] is None for row in effects)
    assert {row["status"] for row in store.list_rows("rca_delivery_jobs")} == {
        "quarantined"
    }
    attempts = store.list_rows("rca_delivery_attempts")
    assert len(attempts) == len(keys)
    assert {row["outcome"] for row in attempts} == {"quarantined"}
    assert all(row["request_id"].startswith("pre-w3-disposition:") for row in attempts)
    assert not any(row["outcome"] == "started" for row in attempts)
    assert store.delivery_dispatcher_circuit_reset_state() == circuit_before
    assert store.pre_w3_effect_disposition_audit(audit["disposition_id"]) == audit
    with sqlite3.connect(store.db_path) as conn:
        conn.row_factory = sqlite3.Row
        quarantine_audit = [
            dict(row)
            for row in conn.execute(
                "SELECT * FROM rca_delivery_quarantine_mutation_audit "
                "ORDER BY audit_id"
            ).fetchall()
        ]
    assert len(quarantine_audit) == 2 * len(keys)
    assert {row["entity_kind"] for row in quarantine_audit} == {"effect", "job"}

    before_counts = {"rca_delivery_attempts": len(attempts)}
    before_counts["rca_delivery_quarantine_mutation_audit"] = len(quarantine_audit)
    repeated, repeated_applied = store.quarantine_pre_w3_effects_with_audit(
        audit=audit,
        now=NOW + timedelta(seconds=4),
    )
    assert repeated == audit
    assert repeated_applied is False
    with sqlite3.connect(store.db_path) as conn:
        audit_count = conn.execute(
            "SELECT COUNT(*) FROM rca_delivery_quarantine_mutation_audit"
        ).fetchone()[0]
    assert before_counts == {
        "rca_delivery_attempts": len(store.list_rows("rca_delivery_attempts")),
        "rca_delivery_quarantine_mutation_audit": audit_count,
    }


def test_exact_pre_w3_disposition_rolls_back_every_row_on_mid_batch_failure(
    tmp_path, monkeypatch
):
    _control_store, store, keys = _legacy_pre_w3_pending_effects(tmp_path)
    snapshot = store.pre_w3_effect_disposition_snapshot(effect_keys=keys)
    audit = _pre_w3_disposition_audit(
        store,
        snapshot,
        tmp_path,
        now=NOW + timedelta(seconds=4),
    )
    original = store._aggregate_job_status
    calls = 0

    def fail_second(conn, delivery_id, current):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("injected_pre_w3_batch_failure")
        return original(conn, delivery_id, current)

    monkeypatch.setattr(store, "_aggregate_job_status", fail_second)
    with pytest.raises(RuntimeError, match="injected_pre_w3_batch_failure"):
        store.quarantine_pre_w3_effects_with_audit(
            audit=audit,
            now=NOW + timedelta(seconds=4),
        )

    assert {row["status"] for row in store.list_rows("rca_delivery_effects")} == {
        "pending"
    }
    assert {row["status"] for row in store.list_rows("rca_delivery_jobs")} == {
        "ready"
    }
    assert store.list_rows("rca_delivery_attempts") == []
    with sqlite3.connect(store.db_path) as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM rca_delivery_quarantine_mutation_audit"
        ).fetchone()[0] == 0
    assert store.pre_w3_effect_disposition_audit(audit["disposition_id"]) is None


def test_exact_pre_w3_disposition_rejects_row_drift_before_mutation(tmp_path):
    _control_store, store, keys = _legacy_pre_w3_pending_effects(tmp_path, count=1)
    snapshot = store.pre_w3_effect_disposition_snapshot(effect_keys=keys)
    audit = _pre_w3_disposition_audit(
        store,
        snapshot,
        tmp_path,
        now=NOW + timedelta(seconds=4),
    )
    with sqlite3.connect(store.db_path) as conn:
        conn.execute(
            "UPDATE rca_delivery_effects SET last_error_detail = 'drifted' "
            "WHERE effect_key = ?",
            (keys[0],),
        )

    with pytest.raises(
        DeliveryRecordConflictError,
        match="effect_not_eligible|before_changed|logical_state_changed",
    ):
        store.quarantine_pre_w3_effects_with_audit(
            audit=audit,
            now=NOW + timedelta(seconds=4),
        )
    assert store.list_rows("rca_delivery_attempts") == []
    assert store.pre_w3_effect_disposition_audit(audit["disposition_id"]) is None


def test_manual_quarantined_backfill_rolls_back_then_materializes_issue_and_topic(
    tmp_path, monkeypatch
):
    control, _result = _control(tmp_path, completed=False)
    control.admit_manual_trigger(
        _manual_request("om_manual_origin"),
        allowed_chat_ids={"oc_group123"},
        submit_enabled=True,
    )
    _quarantine_submission(control)
    store = RcaDeliveryStore(tmp_path / "control.sqlite3")
    original = store._materialize_delivery_subscriptions_in_transaction

    def crash(*_args, **_kwargs):
        raise RuntimeError("simulated quarantined backfill crash")

    monkeypatch.setattr(
        store, "_materialize_delivery_subscriptions_in_transaction", crash
    )
    with pytest.raises(RuntimeError, match="simulated quarantined backfill crash"):
        store.backfill_completed_submissions(now=NOW + timedelta(seconds=2))
    assert store.list_rows("rca_execution_watch") == []
    assert store.list_rows("rca_delivery_jobs") == []
    assert store.list_rows("rca_delivery_effects") == []
    assert {row["status"] for row in store.list_rows("rca_delivery_subscriptions")} == {
        "pending"
    }

    monkeypatch.setattr(
        store, "_materialize_delivery_subscriptions_in_transaction", original
    )
    assert store.backfill_completed_submissions(now=NOW + timedelta(seconds=3)) == 1
    assert store.backfill_completed_submissions(now=NOW + timedelta(seconds=4)) == 0
    effects = store.list_rows("rca_delivery_effects")
    assert {row["effect_kind"] for row in effects} == {
        "feishu_issue_comment",
        "feishu_thread_reply",
    }
    assert {row["status"] for row in effects} == {"pending"}
    assert {row["status"] for row in store.list_rows("rca_delivery_subscriptions")} == {
        "materialized"
    }
    assert all("SECRET-MUST-NOT-LEAK" not in row["payload_json"] for row in effects)


def test_quarantined_manual_rerun_waits_for_all_required_effects_to_settle(
    tmp_path,
):
    control, _result = _control(tmp_path, completed=False)
    control.admit_manual_trigger(
        _manual_request("om_settlement_origin"),
        allowed_chat_ids={"oc_group123"},
        submit_enabled=True,
    )
    _quarantine_submission(control)
    store = RcaDeliveryStore(tmp_path / "control.sqlite3")
    assert store.backfill_completed_submissions(now=NOW + timedelta(seconds=2)) == 1

    pending = control.admit_manual_trigger(
        _manual_request(
            "om_pending_rerun", mode="rerun", thread_root="om_pending_rerun_root"
        ),
        allowed_chat_ids={"oc_group123"},
        submit_enabled=True,
        operator_authorized=True,
        operator_rate_limit=10,
    )
    assert pending.generation == 1
    store.materialize_pending_subscriptions(now=NOW + timedelta(seconds=3))
    with sqlite3.connect(store.db_path) as conn:
        conn.execute("UPDATE rca_delivery_effects SET status = 'succeeded'")
        conn.execute(
            "UPDATE rca_delivery_effects SET status = 'uncertain' "
            "WHERE effect_kind = 'feishu_thread_reply' AND rowid = ("
            "SELECT MIN(rowid) FROM rca_delivery_effects "
            "WHERE effect_kind = 'feishu_thread_reply')"
        )
        conn.execute("UPDATE rca_delivery_jobs SET status = 'ready'")

    uncertain = control.admit_manual_trigger(
        _manual_request(
            "om_uncertain_debug", mode="debug", thread_root="om_uncertain_debug_root"
        ),
        allowed_chat_ids={"oc_group123"},
        submit_enabled=True,
        operator_authorized=True,
        operator_rate_limit=10,
    )
    assert uncertain.generation == 1
    store.materialize_pending_subscriptions(now=NOW + timedelta(seconds=4))
    conn = store._connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("UPDATE rca_delivery_effects SET status = 'succeeded'")
        [delivery_id] = [
            row["delivery_id"] for row in store.list_rows("rca_delivery_jobs")
        ]
        store._aggregate_job_status(
            conn,
            delivery_id,
            (NOW + timedelta(seconds=5)).isoformat(),
        )
        conn.commit()
    finally:
        conn.close()

    settled = control.admit_manual_trigger(
        _manual_request(
            "om_settled_rerun", mode="rerun", thread_root="om_settled_rerun_root"
        ),
        allowed_chat_ids={"oc_group123"},
        submit_enabled=True,
        operator_authorized=True,
        operator_rate_limit=10,
    )
    assert settled.outcome == "created"
    assert settled.generation == 2
    assert len(control.list_rows("rca_outbox")) == 2


def test_v4_watch_schema_migrates_task_id_to_nullable(tmp_path):
    path = tmp_path / "control.sqlite3"
    with sqlite3.connect(path) as conn:
        conn.execute(
            "CREATE TABLE rca_delivery_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
        )
        conn.execute(
            "INSERT INTO rca_delivery_meta VALUES "
            "('schema_version', 'pnc_rca_delivery_store_v4')"
        )
        conn.execute(
            """
            CREATE TABLE rca_execution_watch (
                submission_key TEXT PRIMARY KEY,
                submission_outbox_id INTEGER NOT NULL UNIQUE,
                business_key TEXT NOT NULL,
                generation INTEGER NOT NULL CHECK (generation >= 1),
                project_key TEXT NOT NULL,
                work_item_type_key TEXT NOT NULL,
                work_item_id TEXT NOT NULL,
                task_id TEXT NOT NULL UNIQUE,
                state TEXT NOT NULL,
                poll_attempt INTEGER NOT NULL DEFAULT 0,
                next_poll_at TEXT NOT NULL,
                last_observed_at TEXT,
                terminal_at TEXT,
                terminal_first_seen_at TEXT,
                fence INTEGER NOT NULL DEFAULT 0,
                lease_token TEXT,
                lease_owner TEXT,
                lease_expires_at TEXT,
                last_status_json TEXT,
                last_error_code TEXT NOT NULL DEFAULT '',
                last_error_detail TEXT NOT NULL DEFAULT '',
                delivery_id TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(business_key, generation)
            )
            """
        )

    RcaDeliveryStore(path)

    with sqlite3.connect(path) as conn:
        columns = {
            row[1]: row
            for row in conn.execute("PRAGMA table_info(rca_execution_watch)")
        }
        failure_route_columns = {
            row[1] for row in conn.execute("PRAGMA table_info(rca_failure_routes)")
        }
        version = conn.execute(
            "SELECT value FROM rca_delivery_meta WHERE key='schema_version'"
        ).fetchone()[0]
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
    assert columns["task_id"][3] == 0
    assert {
        "route_key",
        "dedupe_key",
        "owner",
        "status",
        "observation_count",
        "retry_count",
        "audit_json",
        "remediation_result_json",
    } <= failure_route_columns
    assert version == DELIVERY_STORE_SCHEMA_VERSION


def test_delivery_health_observes_stalled_watch_without_blocking_readiness(tmp_path):
    _control(tmp_path)
    store = RcaDeliveryStore(tmp_path / "control.sqlite3")
    assert store.backfill_completed_submissions(now=NOW) == 1
    conn = store._connect()
    try:
        conn.execute(
            "UPDATE rca_execution_watch SET created_at = ?",
            ((NOW - timedelta(days=2)).isoformat(),),
        )
    finally:
        conn.close()

    health = store.health(now=NOW)
    watch = store.list_rows("rca_execution_watch")[0]

    assert health["process_healthy"] is True
    assert health["business_ready"] is True
    assert health["ok"] is True
    assert health["business_blockers"]["stalled_watches"] == 1
    assert health["production_blockers"] == {
        "activation_schema_unavailable": 0,
        "uncertain_effects": 0,
        "quarantined_jobs": 0,
        "quarantined_effects": 0,
        "quarantined_subscriptions": 0,
        "quarantine_baseline_invalid": 0,
        "pending_delivery_observations": 0,
    }
    assert watch["state"] == "pending"


def test_concurrent_backfill_creates_exactly_one_watch(tmp_path):
    _control(tmp_path)
    db = tmp_path / "control.sqlite3"

    def backfill(_index):
        return RcaDeliveryStore(db).backfill_completed_submissions(now=NOW)

    with ThreadPoolExecutor(max_workers=8) as pool:
        inserted = list(pool.map(backfill, range(8)))

    store = RcaDeliveryStore(db)
    assert sum(inserted) == 1
    assert len(store.list_rows("rca_execution_watch")) == 1


def test_concurrent_claim_has_one_winner(tmp_path):
    _control(tmp_path)
    store = RcaDeliveryStore(tmp_path / "control.sqlite3")
    store.backfill_completed_submissions(now=NOW)

    def claim(index):
        return RcaDeliveryStore(store.db_path).claim_due_watch(
            lease_owner=f"collector-{index}", now=NOW
        )

    with ThreadPoolExecutor(max_workers=8) as pool:
        claims = list(pool.map(claim, range(8)))

    assert sum(item is not None for item in claims) == 1


def test_expired_lease_reclaim_fences_stale_collector(tmp_path):
    _control(tmp_path)
    store = RcaDeliveryStore(tmp_path / "control.sqlite3")
    store.backfill_completed_submissions(now=NOW)
    first = store.claim_due_watch(lease_owner="collector-1", lease_seconds=30, now=NOW)
    assert first is not None
    second = store.claim_due_watch(
        lease_owner="collector-2",
        lease_seconds=30,
        now=NOW + timedelta(seconds=31),
    )
    assert second is not None
    assert second.fence == first.fence + 1

    with pytest.raises(StaleDeliveryWatchLeaseError):
        store.reschedule_watch(
            submission_key=first.submission_key,
            lease_token=first.lease_token,
            observed_state="running",
            status={"state": "running"},
            next_poll_at=NOW + timedelta(seconds=60),
            now=NOW + timedelta(seconds=31),
        )


def test_expired_unreclaimed_lease_cannot_commit(tmp_path):
    _control(tmp_path)
    store = RcaDeliveryStore(tmp_path / "control.sqlite3")
    store.backfill_completed_submissions(now=NOW)
    claim = store.claim_due_watch(lease_owner="collector-1", lease_seconds=30, now=NOW)
    assert claim is not None

    with pytest.raises(StaleDeliveryWatchLeaseError):
        store.reschedule_watch(
            submission_key=claim.submission_key,
            lease_token=claim.lease_token,
            observed_state="running",
            status={"state": "running"},
            next_poll_at=NOW + timedelta(seconds=60),
            now=NOW + timedelta(seconds=31),
        )


def test_verified_delivery_job_and_required_effect_are_one_transaction(tmp_path):
    _control(tmp_path)
    store = RcaDeliveryStore(tmp_path / "control.sqlite3")
    store.backfill_completed_submissions(now=NOW)
    claim = store.claim_due_watch(lease_owner="collector", now=NOW)
    assert claim is not None
    delivery = _delivery(claim)

    created = store.create_delivery(
        claim=claim,
        delivery=delivery,
        status={"success": True, "state": "completed"},
        now=NOW,
    )

    assert created.created is True
    watch = store.list_rows("rca_execution_watch")[0]
    job = store.list_rows("rca_delivery_jobs")[0]
    effect = store.list_rows("rca_delivery_effects")[0]
    assert watch["state"] == "delivery_created"
    assert watch["delivery_id"] == delivery.delivery_id
    assert job["status"] == "ready"
    assert job["artifact_set_id"] == delivery.artifact_set_id
    assert effect["effect_kind"] == "feishu_issue_comment"
    assert effect["required"] == 1
    assert effect["status"] == "pending"
    payload = json.loads(effect["payload_json"])
    assert payload["marker"] == delivery.marker
    assert store.list_rows("rca_delivery_attempts") == []


def test_create_delivery_materializes_issue_and_origin_topic_subscriptions(tmp_path):
    _control(tmp_path)
    store = RcaDeliveryStore(tmp_path / "control.sqlite3")
    _install_subscription_table(store)
    store.backfill_completed_submissions(now=NOW)
    claim = store.claim_due_watch(lease_owner="collector", now=NOW)
    assert claim is not None
    _insert_subscription(store, claim, effect_kind="feishu_thread_reply")

    store.create_delivery(
        claim=claim,
        delivery=_delivery(claim),
        status={"success": True, "state": "completed"},
        now=NOW,
    )

    effects = store.list_rows("rca_delivery_effects")
    assert [row["effect_kind"] for row in effects] == [
        "feishu_issue_comment",
        "feishu_thread_reply",
    ]
    thread_payload = json.loads(effects[1]["payload_json"])
    assert thread_payload["thread_id"] == "topic:om_root123"
    assert thread_payload["target_key"] == "feishu_thread:oc_group123:om_root123"
    assert thread_payload["idempotency_uuid"]
    subscriptions = store.list_rows("rca_delivery_subscriptions")
    assert {row["status"] for row in subscriptions} == {"materialized"}
    assert all(row["delivery_id"] for row in subscriptions)
    assert all(row["effect_key"] for row in subscriptions)


def test_terminal_delivery_transaction_rolls_back_and_retries_after_crash(
    tmp_path, monkeypatch
):
    _control(tmp_path)
    store = RcaDeliveryStore(tmp_path / "control.sqlite3")
    store.backfill_completed_submissions(now=NOW)
    claim = store.claim_due_watch(
        lease_owner="collector-before-crash", lease_seconds=60, now=NOW
    )
    assert claim is not None
    original = store._materialize_delivery_subscriptions_in_transaction

    def crash(*_args, **_kwargs):
        raise RuntimeError("simulated collector crash")

    monkeypatch.setattr(
        store, "_materialize_delivery_subscriptions_in_transaction", crash
    )
    with pytest.raises(RuntimeError, match="simulated collector crash"):
        store.create_terminal_delivery(
            claim=claim,
            status={"success": True, "state": "failed"},
            outcome="terminal_failed",
            terminal_state="failed",
            error_code="vm_terminal_failed",
            error_detail="internal detail",
            now=NOW,
        )
    assert store.list_rows("rca_delivery_jobs") == []
    assert store.list_rows("rca_delivery_effects") == []
    assert store.list_rows("rca_execution_watch")[0]["state"] == "pending"

    monkeypatch.setattr(
        store, "_materialize_delivery_subscriptions_in_transaction", original
    )
    retried = store.claim_due_watch(
        lease_owner="collector-after-crash",
        lease_seconds=60,
        now=NOW + timedelta(seconds=61),
    )
    assert retried is not None
    result = store.create_terminal_delivery(
        claim=retried,
        status={"success": True, "state": "failed"},
        outcome="terminal_failed",
        terminal_state="failed",
        error_code="vm_terminal_failed",
        error_detail="internal detail",
        now=NOW + timedelta(seconds=61),
    )
    assert result.created is True
    assert len(store.list_rows("rca_delivery_effects")) == 1


def test_late_topic_subscription_reopens_delivered_job_for_catchup(tmp_path):
    _control(tmp_path)
    store = RcaDeliveryStore(tmp_path / "control.sqlite3")
    _install_subscription_table(store)
    store.backfill_completed_submissions(now=NOW)
    claim = store.claim_due_watch(lease_owner="collector", now=NOW)
    assert claim is not None
    store.create_delivery(
        claim=claim,
        delivery=_delivery(claim),
        status={"success": True, "state": "completed"},
        now=NOW,
    )
    issue_claim = store.claim_due_effect(lease_owner="dispatcher", now=NOW)
    assert issue_claim is not None
    store.complete_effect(
        claim=issue_claim,
        outcome="ack",
        remote_id="comment-1",
        receipt={"remote_id": "comment-1"},
        observation=_delivery_observation(
            issue_claim, remote_receipt_id="comment-1"
        ),
        now=NOW,
    )
    assert store.list_rows("rca_delivery_jobs")[0]["status"] == "delivered"

    _insert_subscription(store, claim, effect_kind="feishu_thread_reply")
    result = store.materialize_pending_subscriptions(now=NOW)

    assert result.materialized == 1
    assert result.quarantined == 0
    assert store.list_rows("rca_delivery_jobs")[0]["status"] == "ready"
    thread_effect = store.list_rows("rca_delivery_effects")[1]
    assert thread_effect["effect_kind"] == "feishu_thread_reply"
    assert thread_effect["status"] == "pending"


def test_suppressed_required_subscription_terminates_job_as_partial(tmp_path):
    _control(tmp_path)
    store = RcaDeliveryStore(tmp_path / "control.sqlite3")
    _install_subscription_table(store)
    store.backfill_completed_submissions(now=NOW)
    claim = store.claim_due_watch(lease_owner="collector", now=NOW)
    assert claim is not None
    _insert_subscription(store, claim, effect_kind="feishu_thread_reply")
    with sqlite3.connect(store.db_path) as conn:
        conn.execute(
            "UPDATE rca_delivery_subscriptions "
            "SET status = 'suppressed', reason = 'owner_delivery_suppressed' "
            "WHERE effect_kind = 'feishu_thread_reply'"
        )

    store.create_delivery(
        claim=claim,
        delivery=_delivery(claim),
        status={"success": True, "state": "completed"},
        now=NOW,
    )
    issue_claim = store.claim_due_effect(lease_owner="dispatcher", now=NOW)
    assert issue_claim is not None
    mutation = store.complete_effect(
        claim=issue_claim,
        outcome="ack",
        remote_id="comment-1",
        receipt={"remote_id": "comment-1"},
        observation=_delivery_observation(
            issue_claim, remote_receipt_id="comment-1"
        ),
        now=NOW,
    )

    assert mutation.job_status == "partial"
    assert store.list_rows("rca_delivery_jobs")[0]["status"] == "partial"
    [subscription] = [
        row
        for row in store.list_rows("rca_delivery_subscriptions")
        if row["effect_kind"] == "feishu_thread_reply"
    ]
    assert subscription["reason"] == "owner_delivery_suppressed"
    events = [
        row
        for row in store.list_rows("rca_delivery_subscription_events")
        if row["subscription_key"] == subscription["subscription_key"]
    ]
    assert [row["new_status"] for row in events] == ["pending", "suppressed"]
    assert events[-1]["reason"] == "owner_delivery_suppressed"


def test_reconcile_delivery_job_status_repairs_stale_ready_status(tmp_path):
    store, effect = _claimed_effect(tmp_path)
    store.complete_effect(
        claim=effect,
        outcome="ack",
        remote_id="comment-1",
        receipt={"remote_id": "comment-1"},
        observation=_delivery_observation(
            effect, remote_receipt_id="comment-1"
        ),
        now=NOW,
    )
    [job] = store.list_rows("rca_delivery_jobs")
    with sqlite3.connect(store.db_path) as conn:
        conn.execute(
            "UPDATE rca_delivery_jobs SET status = 'ready' WHERE delivery_id = ?",
            (job["delivery_id"],),
        )

    status = store.reconcile_delivery_job_status(
        delivery_id=job["delivery_id"],
        now=NOW + timedelta(seconds=1),
    )

    assert status == "delivered"
    assert store.list_rows("rca_delivery_jobs")[0]["status"] == "delivered"


def test_concurrent_late_subscription_materialization_creates_one_effect(tmp_path):
    _control(tmp_path)
    store = RcaDeliveryStore(tmp_path / "control.sqlite3")
    store.backfill_completed_submissions(now=NOW)
    claim = store.claim_due_watch(lease_owner="collector", now=NOW)
    assert claim is not None
    store.create_delivery(
        claim=claim,
        delivery=_delivery(claim),
        status={"success": True, "state": "completed"},
        now=NOW,
    )
    _insert_subscription(store, claim, effect_kind="feishu_thread_reply")

    def materialize(_index):
        return RcaDeliveryStore(store.db_path).materialize_pending_subscriptions(
            now=NOW
        )

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(materialize, range(8)))

    assert sum(result.materialized for result in results) == 1
    effects = store.list_rows("rca_delivery_effects")
    assert [row["effect_kind"] for row in effects].count("feishu_thread_reply") == 1
    subscriptions = store.list_rows("rca_delivery_subscriptions")
    assert subscriptions[-1]["status"] == "materialized"


def test_invalid_topic_subscription_quarantines_without_fallback_effect(tmp_path):
    _control(tmp_path)
    store = RcaDeliveryStore(tmp_path / "control.sqlite3")
    _install_subscription_table(store)
    store.backfill_completed_submissions(now=NOW)
    claim = store.claim_due_watch(lease_owner="collector", now=NOW)
    assert claim is not None
    _insert_subscription(
        store,
        claim,
        effect_kind="feishu_thread_reply",
        invalid_thread=True,
    )

    store.create_delivery(
        claim=claim,
        delivery=_delivery(claim),
        status={"success": True, "state": "completed"},
        now=NOW,
    )

    effects = store.list_rows("rca_delivery_effects")
    assert [row["effect_kind"] for row in effects] == ["feishu_issue_comment"]
    subscriptions = store.list_rows("rca_delivery_subscriptions")
    assert [row["status"] for row in subscriptions] == [
        "materialized",
        "quarantined",
    ]
    assert store.list_rows("rca_delivery_jobs")[0]["status"] == "quarantined"


def test_thread_circuit_does_not_block_issue_comment_effect(tmp_path):
    _control(tmp_path)
    store = RcaDeliveryStore(tmp_path / "control.sqlite3")
    store.backfill_completed_submissions(now=NOW)
    claim = store.claim_due_watch(lease_owner="collector", now=NOW)
    assert claim is not None
    _insert_subscription(store, claim, effect_kind="feishu_thread_reply")
    store.create_delivery(
        claim=claim,
        delivery=_delivery(claim),
        status={"success": True, "state": "completed"},
        now=NOW,
    )
    store.open_delivery_dispatcher_circuit(
        effect_kind="feishu_thread_reply",
        reason_code="feishu_thread_read_unavailable",
        now=NOW,
    )

    issue_claim = store.claim_due_effect(lease_owner="dispatcher", now=NOW)

    assert issue_claim is not None
    assert issue_claim.effect_kind == "feishu_issue_comment"
    assert store.delivery_dispatcher_circuit("feishu_issue_comment").is_open is False
    assert store.delivery_dispatcher_circuit("feishu_thread_reply").is_open is True


def test_thread_only_open_circuit_blocks_aggregate_backpressure(tmp_path):
    _control(tmp_path)
    store = RcaDeliveryStore(tmp_path / "control.sqlite3")
    store.open_delivery_dispatcher_circuit(
        effect_kind="feishu_thread_reply",
        reason_code="feishu_thread_read_unavailable",
        now=NOW,
    )

    snapshot = store.backpressure_snapshot(now=NOW)
    public = snapshot.public_dict()

    assert snapshot.circuit.is_open is True
    assert snapshot.circuit.reason_code == "feishu_thread_read_unavailable"
    assert snapshot.circuits["feishu_issue_comment"].is_open is False
    assert snapshot.circuits["feishu_thread_reply"].is_open is True
    assert public["delivery_dispatcher_circuit"]["state"] == "open"
    assert (
        public["delivery_dispatcher_circuits"]["feishu_issue_comment"]["state"]
        == "closed"
    )
    assert public["delivery_dispatcher_circuits"]["feishu_thread_reply"] == {
        "state": "open",
        "reason_code": "feishu_thread_read_unavailable",
        "reason_detail": "",
        "opened_at": NOW.isoformat(),
        "updated_at": NOW.isoformat(),
    }


def test_thread_permanent_failure_streak_cannot_open_issue_circuit(tmp_path):
    _control(tmp_path)
    store = RcaDeliveryStore(tmp_path / "control.sqlite3")
    conn = store._connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        for index in range(PERMANENT_FAILURE_CIRCUIT_THRESHOLD):
            store._record_permanent_failure_in_transaction(
                conn,
                circuit_name="feishu_thread_reply",
                subject_key=f"thread-effect-{index}",
                failure_state="quarantined",
                error_code="feishu_thread_root_invalid",
                error_detail="invalid topic root",
                current=NOW.isoformat(),
            )
        conn.commit()
    finally:
        conn.close()

    assert store.delivery_dispatcher_circuit("feishu_thread_reply").is_open is True
    assert store.delivery_dispatcher_circuit("feishu_issue_comment").is_open is False
    assert (
        store.permanent_failure_circuit_state("feishu_thread_reply")[
            "consecutive_failures"
        ]
        == PERMANENT_FAILURE_CIRCUIT_THRESHOLD
    )
    assert (
        store.permanent_failure_circuit_state("feishu_issue_comment")[
            "consecutive_failures"
        ]
        == 0
    )


def test_reschedule_effect_and_open_circuit_commit_together(tmp_path):
    store, claim = _claimed_effect(tmp_path)

    mutation = store.reschedule_effect_and_open_circuit(
        claim=claim,
        error_code="feishu_auth_failed",
        error_detail="token expired",
        delay_seconds=5,
        uncertain=False,
        now=NOW,
    )

    assert mutation.effect_status == "retry_wait"
    assert store.list_rows("rca_delivery_effects")[0]["status"] == "retry_wait"
    assert [row["outcome"] for row in store.list_rows("rca_delivery_attempts")] == [
        "started",
        "nack",
    ]
    circuit = store.delivery_dispatcher_circuit()
    assert circuit.is_open is True
    assert circuit.reason_code == "feishu_auth_failed"


def test_reschedule_effect_and_open_circuit_rolls_back_together(tmp_path, monkeypatch):
    store, claim = _claimed_effect(tmp_path)

    def fail_circuit_write(*_args, **_kwargs):
        raise RuntimeError("simulated circuit write failure")

    monkeypatch.setattr(
        store,
        "_open_delivery_dispatcher_circuit_in_transaction",
        fail_circuit_write,
    )
    with pytest.raises(RuntimeError, match="simulated circuit write failure"):
        store.reschedule_effect_and_open_circuit(
            claim=claim,
            error_code="feishu_auth_failed",
            error_detail="token expired",
            delay_seconds=5,
            uncertain=False,
            now=NOW,
        )

    effect = store.list_rows("rca_delivery_effects")[0]
    assert effect["status"] == "claimed"
    assert effect["lease_token"] == claim.lease_token
    assert [row["outcome"] for row in store.list_rows("rca_delivery_attempts")] == [
        "started"
    ]
    assert store.delivery_dispatcher_circuit().is_open is False


def test_stale_effect_cannot_open_dispatcher_circuit(tmp_path):
    store, claim = _claimed_effect(tmp_path)

    with pytest.raises(StaleDeliveryEffectLeaseError):
        store.reschedule_effect_and_open_circuit(
            claim=claim,
            error_code="feishu_auth_failed",
            error_detail="late worker",
            delay_seconds=5,
            uncertain=False,
            now=NOW + timedelta(seconds=61),
        )

    assert store.delivery_dispatcher_circuit().is_open is False


def test_effect_lease_extension_is_fenced_by_token_fence_owner_and_expiry(
    tmp_path,
):
    store, claim = _claimed_effect(tmp_path)

    expires = store.extend_effect_lease(
        claim=claim,
        lease_seconds=90,
        now=NOW + timedelta(seconds=30),
    )

    assert expires == (NOW + timedelta(seconds=120)).isoformat()
    assert (
        store.claim_due_effect(
            lease_owner="worker-2",
            lease_seconds=90,
            now=NOW + timedelta(seconds=61),
        )
        is None
    )
    stale_claims = (
        replace(claim, lease_token="stale-token"),
        replace(claim, fence=claim.fence + 1),
        replace(claim, lease_owner="wrong-owner"),
    )
    for stale in stale_claims:
        with pytest.raises(StaleDeliveryEffectLeaseError):
            store.extend_effect_lease(
                claim=stale,
                lease_seconds=90,
                now=NOW + timedelta(seconds=62),
            )
    with pytest.raises(StaleDeliveryEffectLeaseError):
        store.extend_effect_lease(
            claim=claim,
            lease_seconds=90,
            now=NOW + timedelta(seconds=121),
        )


def test_recovery_write_requires_grace_multiple_reads_and_rate_limit(tmp_path):
    store, claim = _claimed_effect(tmp_path)
    store.mark_effect_write_started(claim=claim, now=NOW)
    store.extend_effect_lease(claim=claim, lease_seconds=600, now=NOW)

    first = store.record_effect_reconciliation_miss(
        claim=claim,
        visibility_grace_seconds=120,
        minimum_missing_reads=3,
        recovery_interval_seconds=300,
        max_recovery_writes=2,
        now=NOW + timedelta(seconds=30),
    )
    second = store.record_effect_reconciliation_miss(
        claim=claim,
        visibility_grace_seconds=120,
        minimum_missing_reads=3,
        recovery_interval_seconds=300,
        max_recovery_writes=2,
        now=NOW + timedelta(seconds=60),
    )
    third = store.record_effect_reconciliation_miss(
        claim=claim,
        visibility_grace_seconds=120,
        minimum_missing_reads=3,
        recovery_interval_seconds=300,
        max_recovery_writes=2,
        now=NOW + timedelta(seconds=121),
    )

    assert first.recovery_eligible is False
    assert second.recovery_eligible is False
    assert third.recovery_eligible is True
    assert third.missing_read_count == 3
    assert (
        store.authorize_effect_recovery_write(
            claim=claim,
            visibility_grace_seconds=120,
            minimum_missing_reads=3,
            recovery_interval_seconds=300,
            max_recovery_writes=2,
            now=NOW + timedelta(seconds=121),
        )
        == 1
    )

    for seconds in (151, 181, 211):
        state = store.record_effect_reconciliation_miss(
            claim=claim,
            visibility_grace_seconds=120,
            minimum_missing_reads=3,
            recovery_interval_seconds=300,
            max_recovery_writes=2,
            now=NOW + timedelta(seconds=seconds),
        )
    assert state.missing_read_count == 3
    assert state.recovery_interval_elapsed is False
    assert (
        store.authorize_effect_recovery_write(
            claim=claim,
            visibility_grace_seconds=120,
            minimum_missing_reads=3,
            recovery_interval_seconds=300,
            max_recovery_writes=2,
            now=NOW + timedelta(seconds=211),
        )
        is None
    )

    due = store.record_effect_reconciliation_miss(
        claim=claim,
        visibility_grace_seconds=120,
        minimum_missing_reads=3,
        recovery_interval_seconds=300,
        max_recovery_writes=2,
        now=NOW + timedelta(seconds=421),
    )
    assert due.recovery_eligible is True
    assert (
        store.authorize_effect_recovery_write(
            claim=claim,
            visibility_grace_seconds=120,
            minimum_missing_reads=3,
            recovery_interval_seconds=300,
            max_recovery_writes=2,
            now=NOW + timedelta(seconds=421),
        )
        == 2
    )
    for seconds in (451, 481, 541):
        exhausted = store.record_effect_reconciliation_miss(
            claim=claim,
            visibility_grace_seconds=120,
            minimum_missing_reads=3,
            recovery_interval_seconds=300,
            max_recovery_writes=2,
            now=NOW + timedelta(seconds=seconds),
        )
    assert exhausted.recovery_eligible is False
    assert exhausted.recovery_limit_exceeded is True
    assert (
        store.authorize_effect_recovery_write(
            claim=claim,
            visibility_grace_seconds=120,
            minimum_missing_reads=3,
            recovery_interval_seconds=300,
            max_recovery_writes=2,
            now=NOW + timedelta(seconds=541),
        )
        is None
    )
    effect = store.list_rows("rca_delivery_effects")[0]
    assert effect["write_started_at"] == NOW.isoformat()
    assert effect["reconciliation_miss_count"] == 3
    assert effect["recovery_write_count"] == 2
    assert (
        effect["last_recovery_write_at"] == (NOW + timedelta(seconds=421)).isoformat()
    )


def test_terminal_failure_closes_watch_without_delivery_effect(tmp_path):
    _control(tmp_path)
    store = RcaDeliveryStore(tmp_path / "control.sqlite3")
    store.backfill_completed_submissions(now=NOW)
    claim = store.claim_due_watch(lease_owner="collector", now=NOW)
    assert claim is not None

    store.terminal_failure(
        submission_key=claim.submission_key,
        lease_token=claim.lease_token,
        status={"success": True, "state": "blocked"},
        error_code="vm_terminal_blocked_need_keyframe",
        error_detail="need keyframe",
        now=NOW,
    )

    watch = store.list_rows("rca_execution_watch")[0]
    assert watch["state"] == "terminal_failed"
    assert watch["last_error_code"] == "vm_terminal_blocked_need_keyframe"
    assert store.list_rows("rca_delivery_jobs") == []
    assert store.list_rows("rca_delivery_effects") == []


def test_preview_is_read_only_and_does_not_create_watch(tmp_path):
    _control(tmp_path)
    store = RcaDeliveryStore(tmp_path / "control.sqlite3")

    rows = store.preview_unwatched_completed()

    assert len(rows) == 1
    assert rows[0]["submission_key"].startswith("g1q3-rca-s1-")
    assert store.list_rows("rca_execution_watch") == []


def test_current_epoch_holds_preauthorized_even_when_caller_uses_legacy_flag(
    tmp_path,
):
    control, result = _control(tmp_path)
    _bind_activation_execution(control, result, state="preauthorized")
    store = RcaDeliveryStore(control.db_path)

    assert store.backfill_completed_submissions(now=NOW, activation_required=True) == 0
    health = store.health(now=NOW, activation_required=False)
    assert health["activation"]["required"] is True
    assert health["activation"]["current_epoch_state"] == "preauthorized"
    assert health["activation"]["eligible_counts"]["completed_submissions"] == 0
    assert health["activation"]["held_current_counts"]["completed_submissions"] == 1
    assert store.backfill_completed_submissions(now=NOW) == 0


def test_legacy_flag_bypasses_only_when_no_current_epoch_exists(tmp_path):
    _control(tmp_path)
    store = RcaDeliveryStore(tmp_path / "control.sqlite3")

    assert store.backfill_completed_submissions(now=NOW) == 1
    assert store.health(now=NOW)["activation"]["required"] is False


def test_confirmed_epoch_keeps_delivery_held_until_steady_active(tmp_path):
    control, result = _control(tmp_path)
    _bind_activation_execution(control, result, state="confirmed")
    store = RcaDeliveryStore(control.db_path)

    assert store.backfill_completed_submissions(now=NOW) == 0
    activation = store.health(now=NOW)["activation"]
    assert activation["required"] is True
    assert activation["processing_enabled"] is False
    assert activation["eligible_counts"]["completed_submissions"] == 0
    assert activation["held_current_counts"]["completed_submissions"] == 1


def test_bounded_activation_allows_exact_execution_and_reuses_one_budget_slot(
    tmp_path,
):
    control, result = _control(tmp_path)
    ledger_id = _bind_activation_execution(control, result, state="bounded_active")
    store = RcaDeliveryStore(control.db_path)
    assert store.backfill_completed_submissions(now=NOW, activation_required=True) == 1
    claim = store.claim_due_watch(
        lease_owner="activation-collector",
        now=NOW,
        activation_required=True,
    )
    assert claim is not None
    _install_subscription_table(store)
    _insert_subscription(store, claim, effect_kind="feishu_thread_reply")
    store.create_delivery(
        claim=claim,
        delivery=_delivery(claim),
        status={"success": True, "state": "completed"},
        now=NOW,
        activation_required=True,
    )

    first = store.claim_due_effect(
        lease_owner="activation-dispatcher-1",
        now=NOW,
        activation_required=True,
    )
    second = store.claim_due_effect(
        lease_owner="activation-dispatcher-2",
        now=NOW,
        activation_required=True,
    )
    assert first is not None and second is not None
    assert {first.effect_kind, second.effect_kind} == {
        "feishu_issue_comment",
        "feishu_thread_reply",
    }
    with sqlite3.connect(control.db_path) as conn:
        consumed = conn.execute(
            "SELECT consumed_ledger_id FROM rca_activation_budget_slots "
            "WHERE epoch_id = 'delivery-epoch-1' "
            "AND slot_kind = 'kafka_success'"
        ).fetchone()[0]
        ledger_count = conn.execute(
            "SELECT COUNT(*) FROM rca_activation_admission_ledger"
        ).fetchone()[0]
    assert consumed == ledger_id
    assert ledger_count == 1


def test_fully_settled_issue_only_delivery_allows_activation_replacement(tmp_path):
    control, result = _control(tmp_path)
    _bind_activation_execution(control, result, state="steady_active")
    store = RcaDeliveryStore(control.db_path)
    assert store.backfill_completed_submissions(now=NOW) == 1
    watch = store.claim_due_watch(lease_owner="issue-only-collector", now=NOW)
    assert watch is not None
    store.create_delivery(
        claim=watch,
        delivery=_delivery(watch),
        status={"success": True, "state": "completed"},
        now=NOW,
    )
    effect = store.claim_due_effect(
        lease_owner="issue-only-dispatcher",
        now=NOW + timedelta(seconds=1),
    )
    assert effect is not None
    assert effect.effect_kind == "feishu_issue_comment"
    store.complete_effect(
        claim=effect,
        outcome="ack",
        remote_id="comment-issue-only",
        receipt={"remote_id": "comment-issue-only"},
        observation=_delivery_observation(
            effect,
            remote_receipt_id="comment-issue-only",
        ),
        now=NOW + timedelta(seconds=2),
    )
    assert store.list_rows("rca_delivery_jobs")[0]["status"] == "delivered"

    control.transition_activation_epoch(
        epoch_id="delivery-epoch-1",
        expected_state="steady_active",
        target_state="aborted",
        operator="delivery-test",
        reason="replace only after the issue-only effect is settled",
        now=NOW + timedelta(seconds=3),
    )
    replacement = control.create_activation_epoch(
        epoch_id="delivery-epoch-issue-only-successor",
        preauthorization_fingerprint="7" * 64,
        preauthorization_gate_receipt_sha256="8" * 64,
        preauthorization_capsule_sha256="9" * 64,
        config_sha256="a" * 64,
        db_logical_identity={"database": "delivery-test"},
        partition_start_fence={TOPIC: {"2": 11}},
        operator="delivery-test",
        reason="issue-only delivery settlement permits replacement",
        now=NOW + timedelta(seconds=4),
    )
    assert replacement["state"] == "safe_off"


def _claimed_activation_effect(tmp_path):
    control, result = _control(tmp_path)
    _bind_activation_execution(control, result, state="steady_active")
    store = RcaDeliveryStore(control.db_path)
    assert store.backfill_completed_submissions(now=NOW) == 1
    watch = store.claim_due_watch(
        lease_owner="activation-collector",
        now=NOW,
    )
    assert watch is not None
    store.create_delivery(
        claim=watch,
        delivery=_delivery(watch),
        status={"success": True, "state": "completed"},
        now=NOW,
    )
    effect = store.claim_due_effect(
        lease_owner="activation-dispatcher",
        lease_seconds=600,
        now=NOW,
    )
    assert effect is not None
    return control, store, effect


def test_epoch_switch_rejects_prewrite_claim_even_with_legacy_caller_flag(tmp_path):
    control, store, effect = _claimed_activation_effect(tmp_path)
    _switch_activation_epoch(
        control,
        old_epoch="delivery-epoch-1",
        new_epoch="delivery-epoch-2",
    )

    with pytest.raises(StaleDeliveryEffectLeaseError, match="activation changed"):
        store.mark_effect_write_started(
            claim=effect,
            now=NOW + timedelta(seconds=2),
            activation_required=False,
        )
    assert store.list_rows("rca_delivery_effects")[0]["write_phase"] == "prewrite"


def test_epoch_switch_rejects_recovery_write_but_allows_settlement_state(
    tmp_path,
):
    control, store, effect = _claimed_activation_effect(tmp_path)
    store.mark_effect_write_started(
        claim=effect,
        now=NOW,
        activation_required=False,
    )
    for seconds in (30, 60, 121):
        store.record_effect_reconciliation_miss(
            claim=effect,
            visibility_grace_seconds=120,
            minimum_missing_reads=3,
            recovery_interval_seconds=300,
            max_recovery_writes=2,
            now=NOW + timedelta(seconds=seconds),
        )
    _switch_activation_epoch(
        control,
        old_epoch="delivery-epoch-1",
        new_epoch="delivery-epoch-2",
    )

    with pytest.raises(StaleDeliveryEffectLeaseError, match="activation changed"):
        store.authorize_effect_recovery_write(
            claim=effect,
            visibility_grace_seconds=120,
            minimum_missing_reads=3,
            recovery_interval_seconds=300,
            max_recovery_writes=2,
            now=NOW + timedelta(seconds=121),
            activation_required=False,
        )
    row = store.list_rows("rca_delivery_effects")[0]
    assert row["write_phase"] == "write_started"
    assert row["recovery_write_count"] == 0
    settled = store.complete_effect(
        claim=effect,
        outcome="reconciled",
        remote_id="remote-existing-effect",
        receipt={"source": "read_after_epoch_switch"},
        observation=_delivery_observation(
            effect, remote_receipt_id="remote-existing-effect"
        ),
        now=NOW + timedelta(seconds=122),
    )
    assert settled.effect_status == "succeeded"


def test_epoch_switch_blocks_historical_watch_materialization_and_effect_aging(
    tmp_path,
):
    control, result = _control(tmp_path)
    control, pending_result = _control(
        tmp_path,
        offset=11,
        issue_id=7041712813,
    )
    _bind_activation_execution(control, result, state="steady_active")
    _bind_activation_execution(control, pending_result, state="steady_active")
    store = RcaDeliveryStore(control.db_path)
    assert store.backfill_completed_submissions(now=NOW, activation_required=True) == 2
    claim = store.claim_due_watch(
        lease_owner="activation-collector",
        now=NOW,
        activation_required=True,
    )
    assert claim is not None
    store.create_delivery(
        claim=claim,
        delivery=_delivery(claim),
        status={"success": True, "state": "completed"},
        now=NOW,
        activation_required=True,
    )
    _install_subscription_table(store)
    _insert_subscription(store, claim, effect_kind="feishu_thread_reply")
    with sqlite3.connect(control.db_path) as conn:
        conn.execute(
            "UPDATE rca_delivery_effects SET created_at = ?",
            ((NOW - timedelta(days=2)).isoformat(),),
        )
    _switch_activation_epoch(
        control,
        old_epoch="delivery-epoch-1",
        new_epoch="delivery-epoch-2",
    )

    assert (
        store.materialize_pending_subscriptions(
            now=NOW + timedelta(days=2), activation_required=True
        ).materialized
        == 0
    )
    assert (
        store.claim_due_effect(
            lease_owner="activation-dispatcher",
            now=NOW + timedelta(days=2),
            activation_required=True,
        )
        is None
    )
    assert (
        store.claim_due_watch(
            lease_owner="activation-collector-after-switch",
            now=NOW + timedelta(days=2),
            activation_required=True,
        )
        is None
    )
    assert [row["status"] for row in store.list_rows("rca_delivery_effects")] == [
        "pending"
    ]
    assert store.list_rows("rca_delivery_attempts") == []
    assert sorted(row["state"] for row in store.list_rows("rca_execution_watch")) == [
        "delivery_created",
        "pending",
    ]
    assert store.list_rows("rca_delivery_subscriptions")[-1]["status"] == "pending"
    activation = store.health(now=NOW + timedelta(days=2), activation_required=True)[
        "activation"
    ]
    assert activation["blocked_historical_counts"]["dispatchable_effects"] == 1
    assert activation["blocked_historical_counts"]["pending_subscriptions"] == 1


def test_activation_required_concurrent_backfill_and_claim_are_exactly_once(
    tmp_path,
):
    control, result = _control(tmp_path)
    _bind_activation_execution(control, result, state="steady_active")
    store = RcaDeliveryStore(control.db_path)

    def backfill():
        return RcaDeliveryStore(store.db_path).backfill_completed_submissions(
            now=NOW,
            activation_required=True,
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        assert sum(pool.map(lambda _index: backfill(), range(2))) == 1

    def claim(index):
        return RcaDeliveryStore(store.db_path).claim_due_watch(
            lease_owner=f"activation-collector-{index}",
            now=NOW,
            activation_required=True,
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        claims = list(pool.map(claim, range(2)))
    assert sum(item is not None for item in claims) == 1


def test_capacity_sample_candidates_are_bounded_read_only_snapshots(tmp_path):
    _control(tmp_path)
    store = RcaDeliveryStore(tmp_path / "control.sqlite3")
    assert store.backfill_completed_submissions(now=NOW) == 1
    watch = store.claim_due_watch(lease_owner="collector", now=NOW)
    assert watch is not None
    store.create_delivery(
        claim=watch,
        delivery=_delivery(watch),
        status={
            "success": True,
            "state": "completed",
            "meta": {"rca_prod_attempt_id": "attempt-bootstrap-1"},
        },
        now=NOW,
    )
    effect = store.claim_due_effect(
        lease_owner="dispatcher", now=NOW + timedelta(seconds=1)
    )
    assert effect is not None
    store.complete_effect(
        claim=effect,
        outcome="ack",
        remote_id="comment-1",
        receipt={"remote_id": "comment-1", "request_id": effect.request_id},
        observation=_delivery_observation(
            effect, remote_receipt_id="comment-1"
        ),
        now=NOW + timedelta(seconds=2),
    )

    assert (
        store.capacity_sample_candidates(
            activated_at=NOW - timedelta(seconds=1), limit=1
        )
        == []
    )
    with sqlite3.connect(store.db_path) as conn:
        row = conn.execute(
            "SELECT last_status_json FROM rca_execution_watch WHERE task_id = ?",
            (watch.task_id,),
        ).fetchone()
        assert row is not None
        status = json.loads(row[0])
        status["meta"]["rca_prod_capacity_sample_eligible"] = True
        conn.execute(
            "UPDATE rca_execution_watch SET last_status_json = ? WHERE task_id = ?",
            (
                json.dumps(status, sort_keys=True, separators=(",", ":")),
                watch.task_id,
            ),
        )

    snapshots = store.capacity_sample_candidates(
        activated_at=NOW - timedelta(seconds=1), limit=1
    )
    assert len(snapshots) == 1
    snapshot = snapshots[0]
    assert snapshot.payload["task_id"] == watch.task_id
    assert snapshot.payload["attempt_id"] == "attempt-bootstrap-1"
    assert snapshot.payload["source_kind"] == "kafka_workflow_event"
    assert snapshot.payload["job"]["status"] == "delivered"
    assert snapshot.payload["effects"][0]["status"] == "succeeded"
    assert snapshot.payload["effects"][0]["remote_id"] == "comment-1"
    assert snapshot.payload["effects"][0]["remote_receipt"] == {
        "remote_id": "comment-1",
        "request_id": effect.request_id,
    }
    assert (
        snapshot.snapshot_sha256
        == hashlib.sha256(
            json.dumps(
                snapshot.payload,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
    )
    assert (
        store.capacity_sample_candidates(
            activated_at=NOW - timedelta(seconds=1),
            limit=1,
            excluded_task_attempts={(watch.task_id, "attempt-bootstrap-1")},
        )
        == []
    )


def test_capacity_sample_candidates_ignore_pre_activation_and_failures(tmp_path):
    _control(tmp_path)
    store = RcaDeliveryStore(tmp_path / "control.sqlite3")
    store.backfill_completed_submissions(now=NOW)
    watch = store.claim_due_watch(lease_owner="collector", now=NOW)
    assert watch is not None
    store.create_delivery(
        claim=watch,
        delivery=_delivery(watch),
        status={
            "success": True,
            "state": "completed",
            "meta": {
                "rca_prod_attempt_id": "attempt-bootstrap-1",
                "rca_prod_capacity_sample_eligible": True,
            },
        },
        now=NOW,
    )
    assert (
        store.capacity_sample_candidates(
            activated_at=NOW + timedelta(seconds=1), limit=1
        )
        == []
    )
