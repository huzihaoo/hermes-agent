from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
import threading
import time

import pytest

from gateway.pnc_rca_control_store import (
    ACTIVATION_TRANSITION_BINDING_SCHEMA_V14,
    ACTIVATION_TRANSITION_BINDING_SCHEMA_V15,
    CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION,
    CONTROL_STORE_SCHEMA_VERSION,
    MANUAL_TRIGGER_SCHEMA_VERSION,
    ActivationEpochError,
    KafkaRecord,
    ManualRcaTriggerRequest,
    RcaControlStore,
    build_historical_epoch_rerun_authority,
)
from gateway.pnc_rca_delivery_contract import (
    DELIVERY_EFFECT_KIND,
    DELIVERY_EFFECT_SCHEMA_VERSION,
    RCA_REPORT_FIELD_KEY,
    RCA_RESULT_FIELD_KEY,
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
    DeliveryRecordConflictError,
    RcaDeliveryStore,
    StaleDeliveryEffectLeaseError,
    StaleDeliveryWatchLeaseError,
    _terminal_rerun_payload_identity_matches,
)
from gateway.pnc_rca_kafka_contract import WorkflowEventPolicy, WorkflowTransition
from scripts.pnc_foxglove_delivery import canonical_viz_mcap_path, foxglove_url


NOW = datetime(2026, 7, 10, 8, 0, tzinfo=timezone.utc)
TOPIC = "feishu-project-workflow-event"
REAL_G1Q3_PROJECT_KEY = "68ef617fb371dc80a10641f7"
REAL_G1Q3_PROJECT_SIMPLE_NAME = "t03o4q"


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


def _assert_source_probe_identity(
    before: dict[str, dict[str, int | str] | None],
    after: dict[str, dict[str, int | str] | None],
) -> None:
    assert after["db"] == before["db"]
    assert after["-wal"] == before["-wal"]
    if before["-shm"] is None:
        assert after["-shm"] is None
    else:
        assert after["-shm"] is not None
        assert after["-shm"]["device"] == before["-shm"]["device"]
        assert after["-shm"]["inode"] == before["-shm"]["inode"]


def _activation_audit_payload_bytes(path: Path, epoch_id: str) -> bytes:
    with sqlite3.connect(path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT audit_id, epoch_id, from_state, to_state, operator, "
            "reason, binding_fingerprint, transitioned_at "
            "FROM rca_activation_transition_audit WHERE epoch_id = ? "
            "ORDER BY audit_id DESC LIMIT 1",
            (epoch_id,),
        ).fetchone()
    assert row is not None
    return json.dumps(
        {key: row[key] for key in row.keys()},
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def test_terminal_rerun_payload_identity_accepts_current_issue_effect() -> None:
    payload = {
        "schema_version": DELIVERY_EFFECT_SCHEMA_VERSION,
        "delivery_id": "delivery-1",
        "effect_key": "effect-1",
        "effect_kind": DELIVERY_EFFECT_KIND,
        "target_key": "feishu_project:project:issue:1",
        "project_key": "project",
        "work_item_type_key": "issue",
        "work_item_id": "1",
        "conclusion": "observed conclusion",
    }
    payload_sha256 = compute_delivery_effect_payload_sha256(
        payload, DELIVERY_EFFECT_KIND
    )
    payload["semantic_payload_sha256"] = payload_sha256

    assert _terminal_rerun_payload_identity_matches(
        payload,
        effect_key="effect-1",
        submission_key="submission-1",
        generation=2,
        expected_payload_sha256=payload_sha256,
    )
    assert not _terminal_rerun_payload_identity_matches(
        {**payload, "effect_key": "other-effect"},
        effect_key="effect-1",
        submission_key="submission-1",
        generation=2,
        expected_payload_sha256=payload_sha256,
    )


def _real_g1q3_profile_snapshot(offset: int, option_id: str):
    from tests.gateway.test_pnc_rca_control_store import (
        _profile_snapshot_policy,
        _profile_snapshot_record,
    )

    policy = replace(
        _profile_snapshot_policy(),
        project_keys=frozenset({REAL_G1Q3_PROJECT_KEY}),
        project_simple_names=frozenset({REAL_G1Q3_PROJECT_SIMPLE_NAME}),
    )
    record = _profile_snapshot_record(offset, option_id)
    payload = json.loads(record.value)
    payload["project_key"] = REAL_G1Q3_PROJECT_KEY
    payload["project_simple_name"] = REAL_G1Q3_PROJECT_SIMPLE_NAME
    return policy, replace(
        record,
        value=json.dumps(payload, sort_keys=True).encode(),
    )


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
    db_path=None,
):
    db_path = db_path or (tmp_path / "control.sqlite3")
    db_path.parent.mkdir(parents=True, exist_ok=True)
    control = RcaControlStore(db_path)
    control.activate_direct_steady_epoch(
        epoch_id="delivery-epoch-1",
        release_fingerprint_sha256="a" * 64,
        release_note_sha256="b" * 64,
        config_sha256="c" * 64,
        db_logical_identity={"database": "delivery-test"},
        partition_start_fence={TOPIC: {"2": 0}},
        operator="delivery-test",
        reason="activate steady delivery test runtime",
        now=NOW,
    )
    result = control.ingest_record(
        _record(offset=offset, issue_id=issue_id),
        policy=_policy(),
        submit_enabled=True,
        activation_required=True,
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


def _activate_direct_steady(
    control,
    *,
    epoch_id="delivery-epoch-1",
    start_offset=0,
    now=NOW,
):
    return control.activate_direct_steady_epoch(
        epoch_id=epoch_id,
        release_fingerprint_sha256="a" * 64,
        release_note_sha256="b" * 64,
        config_sha256="c" * 64,
        db_logical_identity={"database": "delivery-test"},
        partition_start_fence={TOPIC: {"2": start_offset}},
        operator="delivery-test",
        reason="activate direct steady delivery test runtime",
        now=now,
    )


def _bind_activation_execution(
    control,
    result,
    *,
    epoch_id="delivery-epoch-1",
    state="steady_active",
):
    current_epoch = control.activation_epoch()
    assert current_epoch is not None
    assert current_epoch["epoch_id"] == epoch_id
    [bound] = [
        row
        for row in control.list_rows("rca_outbox")
        if row["submission_key"] == result.submission_key
    ]
    assert bound["activation_epoch_id"] == epoch_id
    assert bound["activation_ledger_id"] is not None
    if state != "steady_active":
        with sqlite3.connect(control.db_path) as conn:
            updated = conn.execute(
                "UPDATE rca_activation_epochs "
                "SET state = ?, updated_at = ?, production_fingerprint = NULL, "
                "production_gate_receipt_sha256 = NULL "
                "WHERE epoch_id = ? AND is_current = 1",
                (state, NOW.isoformat(), epoch_id),
            )
        assert updated.rowcount == 1
    return int(bound["activation_ledger_id"])


def _switch_activation_epoch(control, *, old_epoch, new_epoch):
    with sqlite3.connect(control.db_path) as conn:
        updated = conn.execute(
            "UPDATE rca_activation_epochs SET state = 'aborted', is_current = 0, "
            "aborted_at = ?, superseded_at = ?, updated_at = ? "
            "WHERE epoch_id = ? AND is_current = 1",
            (NOW.isoformat(), NOW.isoformat(), NOW.isoformat(), old_epoch),
        )
    assert updated.rowcount == 1
    return control.activate_direct_steady_epoch(
        epoch_id=new_epoch,
        release_fingerprint_sha256="d" * 64,
        release_note_sha256="e" * 64,
        config_sha256="f" * 64,
        db_logical_identity={"database": "delivery-test"},
        partition_start_fence={TOPIC: {"2": 11}},
        operator="delivery-test",
        reason="simulate a direct steady delivery epoch switch",
        now=NOW + timedelta(seconds=1),
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
    result_field_value = "归因结论：未发现已知异常模式\n责任模块：待确认"
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
        "result_field_value": result_field_value,
        "field_updates": [
            {
                "field_key": RCA_RESULT_FIELD_KEY,
                "field_value": result_field_value,
            },
            {
                "field_key": RCA_REPORT_FIELD_KEY,
                "field_value": report_url,
            },
        ],
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


def _canonical_canary(
    tmp_path,
    *,
    owner_authorized=True,
    leave_claimed=False,
    successor_write=False,
):
    batch_id = "canary-r15aw-7041712812-20260817"
    issue_id = "7041712812"
    epoch_id = "delivery-test-steady"
    current = NOW
    control = RcaControlStore(tmp_path / "control.sqlite3")
    authority = None
    if owner_authorized:
        prior_epoch_id = "delivery-test-prior-steady"
        control.activate_direct_steady_epoch(
            epoch_id=prior_epoch_id,
            release_fingerprint_sha256="a" * 64,
            release_note_sha256="b" * 64,
            config_sha256="c" * 64,
            db_logical_identity={"database": "canonical-canary-prior-test"},
            partition_start_fence={TOPIC: {"2": 0}},
            operator="delivery-test",
            reason="activate prior canonical canary test runtime",
            now=NOW,
        )
        prior = control.admit_manual_trigger(
            ManualRcaTriggerRequest(
                schema_version=MANUAL_TRIGGER_SCHEMA_VERSION,
                issue_url=(
                    "https://project.feishu.cn/g1q3/issue/detail/7041712812"
                ),
                mode="rerun",
                reason="seed historical canonical canary generation",
                platform="operator",
                chat_id="",
                thread_id="",
                message_id=f"historical-seed-{issue_id}",
                requester_id="automation:rca-batch-rerun",
            ),
            allowed_chat_ids=set(),
            submit_enabled=True,
            operator_authorized=True,
            activation_required=True,
            active_policy=_policy(),
            now=NOW,
        )
        [prior_trigger] = [
            row
            for row in control.list_rows("business_triggers")
            if row["submission_key"] == prior.submission_key
        ]
        _switch_activation_epoch(
            control, old_epoch=prior_epoch_id, new_epoch=epoch_id
        )
        current = NOW + timedelta(seconds=2)
    if successor_write:
        from tests.gateway.test_pnc_rca_control_store import (
            _migrate_v14_fixture_to_v15,
        )

        RcaDeliveryStore(control.db_path)
        migration = _migrate_v14_fixture_to_v15(
            control.db_path,
            successor_epoch_id="delivery-test-v15-steady",
        )
        epoch_id = migration["successor_epoch_id"]
        control = RcaControlStore(
            control.db_path,
            require_current=True,
            allow_successor_write=True,
        )
    if owner_authorized:
        authority = build_historical_epoch_rerun_authority(
            batch_id=batch_id,
            queue_sha256="1" * 64,
            issue_id=issue_id,
            prior_submission_key=prior.submission_key,
            prior_generation=prior.generation,
            prior_activation_epoch_id=prior_epoch_id,
            prior_activation_ledger_id=int(prior_trigger["activation_ledger_id"]),
            target_activation_epoch_id=epoch_id,
            owner_receipt_path=str(tmp_path / "canonical-canary-owner-receipt.json"),
            owner_receipt_sha256="2" * 64,
            requester_id="automation:rca-batch-rerun",
            reason=f"production_gray_batch:{batch_id}",
        )
    else:
        control.activate_direct_steady_epoch(
            epoch_id=epoch_id,
            release_fingerprint_sha256="a" * 64,
            release_note_sha256="b" * 64,
            config_sha256="c" * 64,
            db_logical_identity={"database": "canonical-canary-test"},
            partition_start_fence={TOPIC: {"2": 0}},
            operator="delivery-test",
            reason="activate canonical canary test runtime",
            now=NOW,
        )
    admitted = control.admit_manual_trigger(
        ManualRcaTriggerRequest(
            schema_version=MANUAL_TRIGGER_SCHEMA_VERSION,
            issue_url=(
                "https://project.feishu.cn/g1q3/issue/detail/7041712812"
            ),
            mode="rerun",
            reason=f"production_gray_batch:{batch_id}",
            platform="operator",
            chat_id="",
            thread_id="",
            message_id=f"{batch_id}-{issue_id}-try-1",
            requester_id="automation:rca-batch-rerun",
        ),
        allowed_chat_ids=set(),
        submit_enabled=True,
        operator_authorized=True,
        activation_required=True,
        active_policy=_policy(),
        historical_epoch_rerun_authority=authority,
        now=current,
    )
    outbox = control.claim_outbox(lease_owner="submission-worker", now=current)
    assert outbox is not None
    control.complete_outbox(
        outbox_id=outbox.outbox_id,
        lease_token=outbox.lease_token,
        result={
            "success": True,
            "submission_key": admitted.submission_key,
            "task_id": admitted.submission_key,
            "task_state": "submitted",
            "deduped": False,
        },
        now=current,
    )
    store = (
        RcaDeliveryStore(
            control.db_path,
            require_current=True,
            allow_successor_write=True,
        )
        if successor_write
        else RcaDeliveryStore(control.db_path)
    )
    assert store.backfill_completed_submissions(
        now=current, activation_required=True
    ) == 1
    watch = store.claim_due_watch(
        lease_owner="collector", now=current, activation_required=True
    )
    assert watch is not None
    execution_identity = {
        "schema_version": "pnc_rca_execution_identity_readback_v1",
        "source": "host_collector_canonical_vm_receipts_v1",
        "release_id": "rca-r15aw-20260817",
        "activation_epoch_id": epoch_id,
        "release_fingerprint_sha256": "d" * 64,
        "release_note_sha256": "e" * 64,
        "task_id": admitted.submission_key,
        "submission_key": admitted.submission_key,
        "worker": {"commit": "1" * 40},
        "pipeline": {"commit": "2" * 40},
        "report_service": {"manifest_sha256": "3" * 64},
        "delivery_manifest": {"sha256": "4" * 64},
    }
    store.create_delivery(
        claim=watch,
        delivery=_delivery(watch),
        status={
            "success": True,
            "state": "completed",
            "execution_identity_readback": execution_identity,
        },
        now=current,
        activation_required=True,
    )
    if leave_claimed:
        with sqlite3.connect(control.db_path) as conn:
            updated = conn.execute(
                "UPDATE rca_delivery_jobs SET issue_url = ? "
                "WHERE submission_key = ?",
                (
                    "https://project.feishu.cn/g1q3/issue/detail/7041712812",
                    admitted.submission_key,
                ),
            )
        assert updated.rowcount == 1
    effect = store.claim_due_effect(
        lease_owner="dispatcher", now=current, activation_required=True
    )
    assert effect is not None
    if leave_claimed:
        store.mark_effect_write_started(
            claim=effect,
            now=current,
            activation_required=True,
        )
        return {
            "control": control,
            "store": store,
            "effect": effect,
            "now": current,
        }
    remote_id = "oc_canonical_canary_comment"
    content = str(effect.payload["comment_content"])
    observation = _delivery_observation(
        effect,
        remote_receipt_id=remote_id,
        release_id=execution_identity["release_id"],
        delivered_at=current.isoformat(),
    )
    store.complete_effect(
        claim=effect,
        outcome="ack",
        remote_id=remote_id,
        receipt={
            "remote_id": remote_id,
            "marker": effect.payload["marker"],
            "source": "read_after_write",
            "recovery_write_count": 0,
            "confirmed_content_sha256": hashlib.sha256(
                content.encode("utf-8")
            ).hexdigest(),
            "confirmed_report_url": effect.report_url,
            "confirmed_field_keys": ["field_9193cb", "field_8c912e"],
        },
        observation=observation,
        now=current,
    )
    [intent] = store.list_pending_delivery_observations()
    assert store.mark_delivery_observation_appended(
        observation_id=intent.observation_id,
        payload_sha256=intent.payload_sha256,
        now=current + timedelta(seconds=1),
    )
    return {
        "db_path": control.db_path,
        "batch_id": batch_id,
        "issue_id": issue_id,
        "submission_key": admitted.submission_key,
        "generation": admitted.generation,
        "epoch_id": epoch_id,
        "execution_identity": execution_identity,
        "remote_id": remote_id,
    }


def _canonical_canary_readback(data):
    store = RcaDeliveryStore(
        data["db_path"],
        read_only=True,
        require_current=True,
        ensure_current_rows=False,
    )
    return store.canonical_canary_readback(
        batch_id=data["batch_id"],
        issue_id=data["issue_id"],
        submission_key=data["submission_key"],
        activation_epoch_id=data["epoch_id"],
    )


def test_canonical_canary_readback_projects_settled_db_evidence(tmp_path):
    data = _canonical_canary(tmp_path)

    readback = _canonical_canary_readback(data)
    with sqlite3.connect(data["db_path"]) as conn:
        [job_issue_url] = conn.execute(
            "SELECT issue_url FROM rca_delivery_jobs"
        ).fetchone()

    assert data["generation"] == 2
    assert job_issue_url == (
        "https://project.feishu.cn/t03o4q/issue/detail/7041712812"
    )
    assert readback["batch_id"] == data["batch_id"]
    assert readback["activation_epoch_id"] == data["epoch_id"]
    assert readback["transport"] == {
        "status": "pass",
        "official_comment_id": data["remote_id"],
        "official_field_keys": ["field_8c912e", "field_9193cb"],
        "official_readback_source": "read_after_write",
    }
    assert readback["execution_identity_readback"] == data["execution_identity"]
    assert readback["required_effects"][0]["write_phase"] == "settled"


def test_terminal_rerun_provider_rejects_v15_audit_tamper_before_call(
    tmp_path,
    monkeypatch,
):
    from gateway import pnc_rca_delivery_store as delivery_store_module
    from gateway import pnc_rca_provider_fence as provider_fence
    from gateway.pnc_rca_provider_fence import (
        build_terminal_rerun_provider_claim,
    )
    from gateway.pnc_rca_write_fence import ExternalWriteFenceError
    from scripts import pnc_rca_delivery_dispatcher as dispatcher_module

    data = _canonical_canary(
        tmp_path,
        leave_claimed=True,
        successor_write=True,
    )
    effect = data["effect"]
    binding = data["store"].validate_terminal_rerun_external_write_binding(
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
        now=data["now"],
    )
    claim = build_terminal_rerun_provider_claim(
        authority_sha256=binding["authority_sha256"],
        outbox_id=binding["outbox_id"],
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
        project_key=binding["project_key"],
        project_simple_name=binding["project_simple_name"],
        work_item_type_key=binding["work_item_type_key"],
        work_item_id=binding["work_item_id"],
    )
    monkeypatch.setattr(
        provider_fence,
        "_canonical_store",
        lambda: data["control"],
    )
    monkeypatch.setattr(
        delivery_store_module,
        "_utc_datetime",
        lambda _value=None: data["now"],
    )
    live = provider_fence.revalidate_provider_write_claim(
        claim,
        operation="feishu_issue_comment",
        issue_project_key=binding["project_key"],
        issue_work_item_id=binding["work_item_id"],
    )
    assert live["authority_kind"] == "terminal_rerun"
    provider_calls = []
    adapter = dispatcher_module.MeegleIssueCommentAdapter(
        lambda args: (
            provider_calls.append(args)
            or (0, json.dumps({"comment_id": "unexpected"}), "")
        )
    )
    with sqlite3.connect(data["control"].db_path) as conn:
        conn.execute(
            "UPDATE rca_activation_transition_audit "
            "SET binding_fingerprint = ? WHERE audit_id = ("
            "SELECT MAX(audit.audit_id) "
            "FROM rca_activation_transition_audit AS audit "
            "JOIN rca_activation_epochs AS epoch "
            "ON epoch.epoch_id = audit.epoch_id WHERE epoch.is_current = 1"
            ")",
            ("f" * 64,),
        )

    with dispatcher_module._bound_provider_write_guard(claim):
        with pytest.raises(
            ExternalWriteFenceError,
            match="external_write_fence_epoch_not_current",
        ):
            adapter.add_comment(
                binding["project_key"],
                binding["work_item_id"],
                "must not send",
            )

    assert provider_calls == []


def test_canonical_canary_readback_rejects_generation_one(tmp_path):
    data = _canonical_canary(tmp_path, owner_authorized=False)

    assert data["generation"] == 1
    with pytest.raises(
        DeliveryRecordConflictError,
        match="canonical_canary_readback_invalid:rerun_authority_missing",
    ):
        _canonical_canary_readback(data)


@pytest.mark.parametrize(
    "case",
    [
        "missing_observation",
        "extra_required_effect",
        "wrong_epoch",
        "job_not_delivered",
        "effect_not_succeeded",
        "wrong_source",
        "wrong_fields",
        "wrong_remote_id",
        "effect_target",
        "effect_payload_hash",
        "payload_effect_key",
        "missing_field_update_with_rebound_identity",
        "observation_content_hash",
        "execution_identity_mismatch",
    ],
)
def test_canonical_canary_readback_fails_closed_on_db_drift(tmp_path, case):
    data = _canonical_canary(tmp_path)
    if case == "wrong_epoch":
        data["epoch_id"] = "delivery-other-steady"
    else:
        with sqlite3.connect(data["db_path"]) as conn:
            conn.row_factory = sqlite3.Row
            if case == "missing_observation":
                conn.execute("DELETE FROM rca_delivery_observation_outbox")
            elif case == "extra_required_effect":
                conn.execute(
                    """
                    INSERT INTO rca_delivery_effects(
                        effect_key, delivery_id, effect_kind, required,
                        target_key, payload_json, payload_sha256, status,
                        write_phase, completed_at, created_at, updated_at
                    )
                    SELECT ?, delivery_id, 'feishu_field_update', 1,
                           target_key || ':extra', '{}', ?, 'succeeded',
                           'settled', completed_at, created_at, updated_at
                      FROM rca_delivery_effects LIMIT 1
                    """,
                    ("f" * 64, "f" * 64),
                )
            elif case == "job_not_delivered":
                conn.execute("UPDATE rca_delivery_jobs SET status = 'ready'")
            elif case == "effect_not_succeeded":
                conn.execute(
                    "UPDATE rca_delivery_effects SET status = 'retry_wait'"
                )
            elif case in {"wrong_source", "wrong_fields"}:
                row = conn.execute(
                    "SELECT remote_receipt_json FROM rca_delivery_effects"
                ).fetchone()
                receipt = json.loads(row["remote_receipt_json"])
                if case == "wrong_source":
                    receipt["source"] = "write_response"
                else:
                    receipt["confirmed_field_keys"] = ["field_8c912e"]
                conn.execute(
                    "UPDATE rca_delivery_effects SET remote_receipt_json = ?",
                    (
                        json.dumps(
                            receipt,
                            ensure_ascii=False,
                            sort_keys=True,
                            separators=(",", ":"),
                        ),
                    ),
                )
            elif case == "wrong_remote_id":
                conn.execute(
                    "UPDATE rca_delivery_attempts SET remote_id = 'oc_other' "
                    "WHERE outcome IN ('ack', 'reconciled')"
                )
            elif case == "effect_target":
                conn.execute(
                    "UPDATE rca_delivery_effects SET target_key = 'feishu_project:other'"
                )
            elif case == "effect_payload_hash":
                conn.execute(
                    "UPDATE rca_delivery_effects SET payload_sha256 = ?",
                    ("f" * 64,),
                )
            elif case == "payload_effect_key":
                row = conn.execute(
                    "SELECT payload_json FROM rca_delivery_effects"
                ).fetchone()
                payload = json.loads(row["payload_json"])
                payload["effect_key"] = "g1q3-rca-effect-v1-" + "f" * 64
                conn.execute(
                    "UPDATE rca_delivery_effects SET payload_json = ?",
                    (
                        json.dumps(
                            payload,
                            ensure_ascii=False,
                            sort_keys=True,
                            separators=(",", ":"),
                        ),
                    ),
                )
            elif case == "missing_field_update_with_rebound_identity":
                row = conn.execute(
                    """
                    SELECT e.effect_key, e.payload_json, e.remote_receipt_json,
                           j.delivery_id, j.target_key, j.artifact_set_id
                      FROM rca_delivery_effects AS e
                      JOIN rca_delivery_jobs AS j ON j.delivery_id = e.delivery_id
                    """
                ).fetchone()
                old_effect_key = str(row["effect_key"])
                payload = json.loads(row["payload_json"])
                payload["field_updates"] = [payload["field_updates"][0]]
                semantic_sha = compute_delivery_effect_payload_sha256(
                    payload, DELIVERY_EFFECT_KIND
                )
                effect_key = compute_delivery_effect_key(
                    delivery_id=str(row["delivery_id"]),
                    effect_kind=DELIVERY_EFFECT_KIND,
                    target_key=str(row["target_key"]),
                    semantic_payload_sha256=semantic_sha,
                )
                old_marker = str(payload["marker"])
                marker = delivery_effect_marker(
                    effect_key, str(row["artifact_set_id"])
                )
                payload.update(
                    {
                        "effect_key": effect_key,
                        "semantic_payload_sha256": semantic_sha,
                        "marker": marker,
                        "comment_content": str(payload["comment_content"]).replace(
                            old_marker, marker
                        ),
                    }
                )
                receipt = json.loads(row["remote_receipt_json"])
                receipt["marker"] = marker
                receipt["confirmed_content_sha256"] = hashlib.sha256(
                    payload["comment_content"].encode("utf-8")
                ).hexdigest()
                observation_row = conn.execute(
                    "SELECT payload_json FROM rca_delivery_observation_outbox"
                ).fetchone()
                observation = json.loads(observation_row["payload_json"])
                observation["outcome_content_sha256"] = receipt[
                    "confirmed_content_sha256"
                ]
                observation["observation_id"] = delivery_observation_id(observation)
                observation_raw = json.dumps(
                    observation,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                conn.execute(
                    "UPDATE rca_delivery_attempts SET effect_key = ? "
                    "WHERE effect_key = ?",
                    (effect_key, old_effect_key),
                )
                conn.execute(
                    """
                    UPDATE rca_delivery_observation_outbox
                       SET effect_key = ?, observation_id = ?, payload_json = ?,
                           payload_sha256 = ?
                     WHERE effect_key = ?
                    """,
                    (
                        effect_key,
                        observation["observation_id"],
                        observation_raw,
                        hashlib.sha256(observation_raw.encode("utf-8")).hexdigest(),
                        old_effect_key,
                    ),
                )
                conn.execute(
                    """
                    UPDATE rca_delivery_effects
                       SET effect_key = ?, payload_json = ?, payload_sha256 = ?,
                           remote_receipt_json = ?
                     WHERE effect_key = ?
                    """,
                    (
                        effect_key,
                        json.dumps(
                            payload,
                            ensure_ascii=False,
                            sort_keys=True,
                            separators=(",", ":"),
                        ),
                        semantic_sha,
                        json.dumps(
                            receipt,
                            ensure_ascii=False,
                            sort_keys=True,
                            separators=(",", ":"),
                        ),
                        old_effect_key,
                    ),
                )
            elif case == "observation_content_hash":
                row = conn.execute(
                    "SELECT payload_json FROM rca_delivery_observation_outbox"
                ).fetchone()
                observation = json.loads(row["payload_json"])
                observation["outcome_content_sha256"] = "f" * 64
                observation["observation_id"] = delivery_observation_id(observation)
                observation_raw = json.dumps(
                    observation,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                conn.execute(
                    """
                    UPDATE rca_delivery_observation_outbox
                       SET observation_id = ?, payload_json = ?, payload_sha256 = ?
                    """,
                    (
                        observation["observation_id"],
                        observation_raw,
                        hashlib.sha256(observation_raw.encode("utf-8")).hexdigest(),
                    ),
                )
            else:
                row = conn.execute(
                    "SELECT last_status_json FROM rca_execution_watch"
                ).fetchone()
                status = json.loads(row["last_status_json"])
                status["execution_identity_readback"]["release_id"] = "rca-other"
                conn.execute(
                    "UPDATE rca_execution_watch SET last_status_json = ?",
                    (
                        json.dumps(
                            status,
                            ensure_ascii=False,
                            sort_keys=True,
                            separators=(",", ":"),
                        ),
                    ),
                )

    with pytest.raises(
        (DeliveryRecordConflictError, ValueError),
        match="canonical_canary_readback_(invalid|identity_invalid)",
    ):
        _canonical_canary_readback(data)


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


def test_claim_due_watch_starts_work_at_delayed_outbox_completion(tmp_path):
    control, result = _control(tmp_path, completed=False)
    admitted_at = NOW - timedelta(hours=2)
    submitted_at = NOW
    with sqlite3.connect(control.db_path) as conn:
        conn.execute(
            "UPDATE business_triggers SET created_at = ? WHERE submission_key = ?",
            (admitted_at.isoformat(), result.submission_key),
        )
        conn.execute(
            "UPDATE rca_outbox SET created_at = ?, updated_at = ?, "
            "retry_window_started_at = ? WHERE submission_key = ?",
            (
                admitted_at.isoformat(),
                admitted_at.isoformat(),
                admitted_at.isoformat(),
                result.submission_key,
            ),
        )

    outbox = control.claim_outbox(lease_owner="submission-worker", now=submitted_at)
    assert outbox is not None
    control.complete_outbox(
        outbox_id=outbox.outbox_id,
        lease_token=outbox.lease_token,
        result={
            "success": True,
            "submission_key": result.submission_key,
            "task_id": result.submission_key,
            "task_state": "submitted",
            "deduped": False,
        },
        now=submitted_at,
    )
    store = RcaDeliveryStore(control.db_path)
    assert store.backfill_completed_submissions(now=submitted_at) == 1

    watch = store.claim_due_watch(lease_owner="collector", now=submitted_at)

    assert watch is not None
    assert watch.work_started_at == submitted_at.isoformat()
    assert datetime.fromisoformat(watch.work_started_at) + timedelta(minutes=30) > (
        submitted_at + timedelta(minutes=1)
    )


def test_terminal_watch_failures_do_not_open_delivery_circuit(
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
    assert store.permanent_failure_circuit_state()["consecutive_failures"] == 0

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
    assert store.permanent_failure_circuit_state()["consecutive_failures"] == 0


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


@pytest.mark.parametrize(
    "profile_error_code",
    ("business_profile_unsupported", "business_profile_conflict"),
)
def test_outbox_only_profile_quarantine_does_not_open_pipeline_circuit(
    tmp_path,
    profile_error_code,
):
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
        error_code=profile_error_code,
        error_detail="out-of-scope profile is an outbox terminal",
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
    assert store.permanent_failure_circuit_state()["consecutive_failures"] == 0


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

    if corruption == "missing":
        assert store.claim_due_effect(lease_owner="dispatcher", now=NOW) is None
    else:
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
    store, effect = _claimed_effect(tmp_path)
    conn = store._connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        store._record_permanent_failure_in_transaction(
            conn,
            circuit_name=DELIVERY_EFFECT_KIND,
            subject_key="prior-field-failure",
            failure_state="quarantined",
            error_code="report_http_verification_mismatch",
            error_detail="sealed report mismatch",
            current=NOW.isoformat(),
        )
        conn.commit()
    finally:
        conn.close()

    def fail_circuit_write(*_args, **_kwargs):
        raise RuntimeError("simulated circuit write failure")

    monkeypatch.setattr(
        store,
        "_open_delivery_dispatcher_circuit_in_transaction",
        fail_circuit_write,
    )
    with pytest.raises(RuntimeError, match="simulated circuit write failure"):
        store.quarantine_effect(
            claim=effect,
            error_code="report_http_verification_mismatch",
            error_detail="field report mismatch",
            now=NOW,
        )

    [row] = store.list_rows("rca_delivery_effects")
    assert row["status"] == "claimed"
    assert row["lease_token"] == effect.lease_token
    assert store.permanent_failure_circuit_state()["consecutive_failures"] == 1
    assert store.delivery_dispatcher_circuit().is_open is False


def test_read_existing_backpressure_snapshot_never_creates_missing_database(
    tmp_path,
):
    path = tmp_path / "missing.sqlite3"

    with pytest.raises(RuntimeError, match="file_missing"):
        RcaDeliveryStore.read_existing_backpressure_snapshot(path, now=NOW)

    assert path.exists() is False


def test_read_existing_backpressure_snapshot_stays_live_during_concurrent_wal_writes(
    tmp_path,
    monkeypatch,
):
    path = tmp_path / "control.sqlite3"
    control = RcaControlStore(path)
    _activate_direct_steady(control)
    RcaDeliveryStore(path)

    snapshot_calls = []

    def reject_raw_snapshot(cls, *_args, **_kwargs):
        del cls
        snapshot_calls.append(True)
        raise AssertionError("backpressure hot reads must not create a raw snapshot")

    monkeypatch.setattr(
        RcaControlStore,
        "create_schema_probe_snapshot",
        classmethod(reject_raw_snapshot),
    )

    writer_started = threading.Event()
    stop_writer = threading.Event()

    def write_loop():
        conn = sqlite3.connect(path, timeout=5, isolation_level=None)
        writes = 0
        try:
            assert conn.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal"
            conn.execute("PRAGMA wal_autocheckpoint=0")
            while not stop_writer.is_set():
                conn.execute(
                    "INSERT INTO rca_delivery_meta(key, value) VALUES(?, ?) "
                    "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                    ("active_wal_backpressure_test", str(writes)),
                )
                writes += 1
                writer_started.set()
                time.sleep(0.001)
        finally:
            conn.close()
        return writes

    with ThreadPoolExecutor(max_workers=1) as pool:
        writer = pool.submit(write_loop)
        assert writer_started.wait(timeout=5)
        try:
            snapshots = [
                RcaDeliveryStore.read_existing_backpressure_snapshot(
                    path,
                    now=NOW,
                    activation_required=True,
                )
                for _ in range(30)
            ]
        finally:
            stop_writer.set()
        writes = writer.result(timeout=5)

    assert writes >= 2
    assert snapshot_calls == []
    assert all(snapshot.unresolved_work == 0 for snapshot in snapshots)
    assert all(not snapshot.circuit.is_open for snapshot in snapshots)


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


def _physical_v15_delivery_fixture(
    tmp_path,
) -> tuple[Path, dict[str, str]]:
    from tests.gateway.test_pnc_rca_control_store import (
        _migrate_v14_fixture_to_v15,
    )

    path = tmp_path / "control.sqlite3"
    control = RcaControlStore(path)
    _activate_direct_steady(control)
    RcaDeliveryStore(path)
    migration = _migrate_v14_fixture_to_v15(
        path,
        successor_epoch_id="delivery-epoch-v15",
    )
    return path, migration


def test_nminus1_v15_weakened_activation_audit_ddl_is_rejected(tmp_path):
    path, _migration = _physical_v15_delivery_fixture(tmp_path)
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
        RcaDeliveryStore(
            path,
            require_current=True,
            ensure_current_rows=False,
            allow_successor_read_only=True,
        )

    _assert_source_probe_identity(before, _sqlite_storage_identity(path))


def test_nminus1_v15_control_schema_opens_delivery_store_read_only(
    tmp_path,
    monkeypatch,
):
    path = tmp_path / "control.sqlite3"
    control = RcaControlStore(path)
    _activate_direct_steady(control)
    RcaDeliveryStore(path)

    v14_before = _sqlite_storage_identity(path)
    v14_store = RcaDeliveryStore(
        path,
        require_current=True,
        ensure_current_rows=False,
        allow_successor_read_only=True,
    )
    assert v14_store.schema_runtime_capability() == {
        "observed_control_schema_version": CONTROL_STORE_SCHEMA_VERSION,
        "binary_write_schema_version": CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION,
        "mode": "current_write",
        "read_supported": True,
        "write_enabled": True,
        "work_admission_enabled": True,
        "lease_acquisition_enabled": True,
        "external_effect_enabled": True,
    }
    _assert_source_probe_identity(v14_before, _sqlite_storage_identity(path))

    from tests.gateway.test_pnc_rca_control_store import (
        _migrate_v14_fixture_to_v15,
    )

    predecessor_audit_bytes = _activation_audit_payload_bytes(
        path,
        "delivery-epoch-1",
    )
    migration = _migrate_v14_fixture_to_v15(
        path,
        successor_epoch_id="delivery-epoch-v15",
    )
    assert migration["predecessor_epoch_id"] == "delivery-epoch-1"
    assert migration["successor_epoch_id"] == "delivery-epoch-v15"
    assert (
        _activation_audit_payload_bytes(path, "delivery-epoch-1")
        == predecessor_audit_bytes
    )
    with sqlite3.connect(path) as conn:
        predecessor_audit = conn.execute(
            "SELECT binding_fingerprint, binding_schema_version "
            "FROM rca_activation_transition_audit WHERE epoch_id = ? "
            "ORDER BY audit_id DESC LIMIT 1",
            (migration["predecessor_epoch_id"],),
        ).fetchone()
        successor_audit = conn.execute(
            "SELECT binding_fingerprint, binding_schema_version "
            "FROM rca_activation_transition_audit WHERE epoch_id = ? "
            "ORDER BY audit_id DESC LIMIT 1",
            (migration["successor_epoch_id"],),
        ).fetchone()
    assert predecessor_audit == (
        migration["predecessor_binding_fingerprint"],
        ACTIVATION_TRANSITION_BINDING_SCHEMA_V14,
    )
    assert successor_audit == (
        migration["successor_binding_fingerprint"],
        ACTIVATION_TRANSITION_BINDING_SCHEMA_V15,
    )
    wal_writer = sqlite3.connect(path)
    assert wal_writer.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal"
    wal_writer.execute("PRAGMA wal_autocheckpoint=0")
    wal_writer.execute(
        "INSERT INTO rca_delivery_meta(key, value) VALUES(?, ?)",
        ("nminus1_live_wal_fixture", "present"),
    )
    wal_writer.commit()
    logical_rows_before = {
        "control_schema": wal_writer.execute(
            "SELECT value FROM control_meta WHERE key = 'schema_version'"
        ).fetchone(),
        "delivery_meta": wal_writer.execute(
            "SELECT key, value FROM rca_delivery_meta ORDER BY key"
        ).fetchall(),
        "delivery_jobs": wal_writer.execute(
            "SELECT COUNT(*) FROM rca_delivery_jobs"
        ).fetchone(),
        "delivery_effects": wal_writer.execute(
            "SELECT COUNT(*) FROM rca_delivery_effects"
        ).fetchone(),
    }
    assert Path(f"{path}-wal").is_file()
    assert Path(f"{path}-shm").is_file()
    before = _sqlite_storage_identity(path)
    stale_store_error = "incompatible_control_store_schema:write_marker"
    with pytest.raises(RuntimeError, match=stale_store_error):
        v14_store._connect()
    with pytest.raises(RuntimeError, match=stale_store_error):
        v14_store.backfill_completed_submissions(
            now=NOW,
            activation_required=True,
        )
    with pytest.raises(RuntimeError, match=stale_store_error):
        v14_store.validate_external_write_fence_binding({
            "activation_epoch_id": migration["successor_epoch_id"],
            "activation_ledger_id": 1,
            "admission_key": "stale-v14-provider-fence",
        })
    after_stale_store_checks = _sqlite_storage_identity(path)
    _assert_source_probe_identity(before, after_stale_store_checks)
    before = after_stale_store_checks

    from gateway import pnc_rca_delivery_store as delivery_store_module

    connect_calls = []
    real_connect = sqlite3.connect

    def capture_connect(database, *args, **kwargs):
        connect_calls.append((str(database), dict(kwargs)))
        return real_connect(database, *args, **kwargs)

    monkeypatch.setattr(delivery_store_module.sqlite3, "connect", capture_connect)

    with pytest.raises(
        RuntimeError,
        match="rca_delivery_store_control_schema_not_current",
    ):
        RcaDeliveryStore(path, require_current=True)
    after_default_rejection = _sqlite_storage_identity(path)
    _assert_source_probe_identity(before, after_default_rejection)
    before = after_default_rejection

    store = RcaDeliveryStore(
        path,
        require_current=True,
        ensure_current_rows=False,
        allow_successor_read_only=True,
    )
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
    assert connect_calls
    assert all(
        "?mode=ro" in database and options.get("uri") is True
        for database, options in connect_calls
    )
    assert any(
        database.startswith(path.resolve().as_uri())
        for database, _options in connect_calls
    )
    assert any(
        not database.startswith(path.resolve().as_uri())
        for database, _options in connect_calls
    )
    after_successor_open = _sqlite_storage_identity(path)
    _assert_source_probe_identity(before, after_successor_open)
    before = after_successor_open

    conn = store._connect_read_only()
    try:
        assert int(conn.execute("PRAGMA query_only").fetchone()[0]) == 1
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            conn.execute(
                "UPDATE rca_delivery_meta SET value = value "
                "WHERE key = 'schema_version'"
            )
    finally:
        conn.close()
    _assert_source_probe_identity(before, _sqlite_storage_identity(path))
    assert {
        "control_schema": wal_writer.execute(
            "SELECT value FROM control_meta WHERE key = 'schema_version'"
        ).fetchone(),
        "delivery_meta": wal_writer.execute(
            "SELECT key, value FROM rca_delivery_meta ORDER BY key"
        ).fetchall(),
        "delivery_jobs": wal_writer.execute(
            "SELECT COUNT(*) FROM rca_delivery_jobs"
        ).fetchone(),
        "delivery_effects": wal_writer.execute(
            "SELECT COUNT(*) FROM rca_delivery_effects"
        ).fetchone(),
    } == logical_rows_before
    wal_writer.close()


def test_nminus1_v15_activation_health_is_binding_valid_but_processing_disabled(
    tmp_path,
):
    path, migration = _physical_v15_delivery_fixture(tmp_path)
    store = RcaDeliveryStore(
        path,
        require_current=True,
        allow_successor_read_only=True,
    )

    epoch = store.activation_epoch()
    assert epoch == {
        "epoch_id": migration["successor_epoch_id"],
        "state": "steady_active",
        "config_sha256": migration["successor_config_sha256"],
        "release_fingerprint_sha256": migration[
            "successor_release_fingerprint_sha256"
        ],
        "release_note_sha256": migration["successor_release_note_sha256"],
    }
    health = store.health(now=NOW, activation_required=True)
    assert health["process_healthy"] is True
    assert health["ok"] is False
    assert health["business_ready"] is False
    assert health["schema_runtime_capability"]["mode"] == "successor_read_only"
    assert health["business_blockers"]["schema_successor_read_only"] == 1
    assert health["production_blockers"]["schema_successor_read_only"] == 1
    assert health["activation"]["schema_version"] == (
        CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION
    )
    assert health["activation"]["binding_valid"] is True
    assert health["activation"]["production_ready"] is False
    assert health["activation"]["processing_enabled"] is False


def test_r15ay_v15_delivery_writer_is_explicit_and_audit_bound(tmp_path):
    path, migration = _physical_v15_delivery_fixture(tmp_path)

    store = RcaDeliveryStore(
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
    assert store.activation_epoch()["epoch_id"] == migration["successor_epoch_id"]

    conn = store._connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        epoch = store._validate_current_activation_binding_tx(conn)
        assert str(epoch["epoch_id"]) == migration["successor_epoch_id"]
        conn.execute(
            "UPDATE rca_delivery_meta SET value = value "
            "WHERE key = 'schema_version'"
        )
        conn.rollback()
    finally:
        conn.close()

    with sqlite3.connect(path) as tamper:
        tamper.execute(
            "UPDATE rca_activation_transition_audit "
            "SET binding_fingerprint = ? WHERE epoch_id = ?",
            ("f" * 64, migration["successor_epoch_id"]),
        )
    conn = store._connect_read_only()
    try:
        conn.execute("BEGIN")
        with pytest.raises(
            RuntimeError,
            match="external_write_fence_epoch_not_current",
        ):
            store._validate_current_activation_binding_tx(conn)
        conn.rollback()
    finally:
        conn.close()


def test_nminus1_v15_delivery_writers_and_external_fences_are_denied_without_mutation(
    tmp_path,
):
    path, _migration = _physical_v15_delivery_fixture(tmp_path)
    source_before = _sqlite_storage_identity(path)
    assert source_before["-wal"] is None
    assert source_before["-shm"] is None
    with pytest.raises(
        RuntimeError,
        match="rca_delivery_store_control_schema_not_current",
    ):
        RcaDeliveryStore(path, require_current=True)
    assert _sqlite_storage_identity(path) == source_before

    store = RcaDeliveryStore(
        path,
        require_current=True,
        allow_successor_read_only=True,
    )
    assert _sqlite_storage_identity(path) == source_before
    before = source_before

    denied = "rca_delivery_store_successor_read_only"
    with pytest.raises(RuntimeError, match=denied):
        store._connect()
    with pytest.raises(RuntimeError, match=denied):
        store.backfill_completed_submissions(now=NOW, activation_required=True)
    with pytest.raises(RuntimeError, match=denied):
        store.claim_due_watch(
            lease_owner="successor-collector",
            now=NOW,
            activation_required=True,
        )
    with pytest.raises(RuntimeError, match=denied):
        store.claim_due_effect(
            lease_owner="successor-dispatcher",
            now=NOW,
            activation_required=True,
        )
    with pytest.raises(RuntimeError, match=denied):
        store.validate_external_write_fence_binding({})
    with pytest.raises(RuntimeError, match=denied):
        store.validate_profile_terminal_external_write_binding(
            effect_key="profile-effect",
            delivery_id="profile-delivery",
            lease_token="profile-lease",
            lease_fence=1,
            operation="feishu_issue_comment",
            issue_url="https://project.feishu.cn/test/1",
            target_key="feishu_project:test:issue:1",
            business_key="profile-business",
            submission_key="profile-submission",
            generation=1,
            require_write_started=False,
            now=NOW,
        )
    with pytest.raises(RuntimeError, match=denied):
        store.validate_terminal_rerun_external_write_binding(
            effect_key="rerun-effect",
            delivery_id="rerun-delivery",
            lease_token="rerun-lease",
            lease_fence=1,
            operation="feishu_issue_comment",
            issue_url="https://project.feishu.cn/test/1",
            target_key="feishu_project:test:issue:1",
            business_key="rerun-business",
            submission_key="rerun-submission",
            generation=2,
            require_write_started=False,
            now=NOW,
        )
    with pytest.raises(RuntimeError, match=denied):
        store.validate_learning_lane_external_operation(
            business_key="learning-business",
            generation=1,
            operation="feishu_issue_comment",
        )
    with pytest.raises(
        RuntimeError,
        match="delivery_backpressure_contract_invalid:control_schema_version",
    ):
        RcaDeliveryStore.read_existing_backpressure_snapshot(path, now=NOW)

    assert _sqlite_storage_identity(path) == before


def test_nminus1_v15_tampered_binding_is_health_red(tmp_path):
    path, _migration = _physical_v15_delivery_fixture(tmp_path)
    with sqlite3.connect(path) as conn:
        changed = conn.execute(
            "UPDATE rca_activation_epochs "
            "SET release_fingerprint_sha256 = ? WHERE is_current = 1",
            ("f" * 64,),
        )
    assert changed.rowcount == 1

    store = RcaDeliveryStore(
        path,
        require_current=True,
        allow_successor_read_only=True,
    )
    health = store.health(now=NOW, activation_required=True)
    assert health["process_healthy"] is True
    assert health["ok"] is False
    assert health["business_ready"] is False
    assert health["activation"]["binding_valid"] is False
    assert health["activation"]["production_ready"] is False
    assert health["activation"]["processing_enabled"] is False
    with pytest.raises(ActivationEpochError):
        store.activation_epoch()


def test_current_constructor_rejects_control_marker_drift_before_circuit_insert(
    tmp_path,
    monkeypatch,
):
    from gateway import pnc_rca_delivery_store as delivery_store_module

    path = tmp_path / "control.sqlite3"
    control = RcaControlStore(path)
    _activate_direct_steady(control)
    RcaDeliveryStore(path)

    delivery_tables = (
        "rca_delivery_meta",
        "rca_delivery_dispatcher_circuit",
        "rca_delivery_jobs",
        "rca_delivery_effects",
    )

    def delivery_rows(conn):
        return {
            table: conn.execute(f"SELECT * FROM {table} ORDER BY rowid").fetchall()
            for table in delivery_tables
        }

    with sqlite3.connect(path) as conn:
        conn.execute(
            "DELETE FROM rca_delivery_dispatcher_circuit WHERE circuit_name = ?",
            (delivery_store_module.DELIVERY_CARD_PATCH_EFFECT_KIND,),
        )
        assert int(conn.execute("SELECT changes()").fetchone()[0]) == 1
        business_rows_before = delivery_rows(conn)

    original_ensure = RcaDeliveryStore._ensure_card_patch_circuit_row_at_path
    keepers: list[sqlite3.Connection] = []
    drift_identity: dict[str, dict[str, int | str] | None] = {}

    def drift_control_marker_then_ensure(
        cls,
        db_path,
        *,
        busy_timeout_ms,
        expected_control_schema_version,
    ):
        del cls
        assert expected_control_schema_version == CONTROL_STORE_SCHEMA_VERSION
        writer = sqlite3.connect(db_path, isolation_level=None)
        assert writer.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal"
        writer.execute("PRAGMA wal_autocheckpoint=0")
        writer.execute("BEGIN IMMEDIATE")
        changed = writer.execute(
            "UPDATE control_meta SET value = ? "
            "WHERE key = 'schema_version' AND value = ?",
            (
                CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION,
                CONTROL_STORE_SCHEMA_VERSION,
            ),
        )
        assert changed.rowcount == 1
        writer.commit()
        keepers.append(writer)
        drift_identity.update(_sqlite_storage_identity(Path(db_path)))
        return original_ensure(
            db_path,
            busy_timeout_ms=busy_timeout_ms,
            expected_control_schema_version=expected_control_schema_version,
        )

    monkeypatch.setattr(
        RcaDeliveryStore,
        "_ensure_card_patch_circuit_row_at_path",
        classmethod(drift_control_marker_then_ensure),
    )

    with pytest.raises(
        RuntimeError,
        match="incompatible_control_store_schema:write_marker",
    ):
        RcaDeliveryStore(path, require_current=True)

    assert len(keepers) == 1
    writer = keepers[0]
    try:
        assert writer.execute(
            "SELECT value FROM control_meta WHERE key = 'schema_version'"
        ).fetchone() == (CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION,)
        assert delivery_rows(writer) == business_rows_before
        assert writer.execute(
            "SELECT 1 FROM rca_delivery_dispatcher_circuit "
            "WHERE circuit_name = ?",
            (delivery_store_module.DELIVERY_CARD_PATCH_EFFECT_KIND,),
        ).fetchone() is None
        _assert_source_probe_identity(
            drift_identity,
            _sqlite_storage_identity(path),
        )
    finally:
        writer.close()


def test_external_binding_validators_remain_available_during_active_wal_write(
    tmp_path,
    monkeypatch,
):
    path = tmp_path / "control.sqlite3"
    control = RcaControlStore(path)
    _activate_direct_steady(control)
    RcaDeliveryStore(path)
    store = RcaDeliveryStore(
        path,
        require_current=True,
        ensure_current_rows=False,
        allow_successor_read_only=True,
    )

    snapshot_calls = []

    def reject_raw_snapshot(cls, *_args, **_kwargs):
        del cls
        snapshot_calls.append(True)
        raise AssertionError("external binding validation must not create a snapshot")

    monkeypatch.setattr(
        RcaControlStore,
        "create_schema_probe_snapshot",
        classmethod(reject_raw_snapshot),
    )

    writer = sqlite3.connect(path, isolation_level=None)
    try:
        assert writer.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal"
        writer.execute("PRAGMA wal_autocheckpoint=0")
        writer.execute(
            "INSERT INTO rca_delivery_meta(key, value) VALUES(?, ?)",
            ("active_wal_external_binding_test", "committed"),
        )
        writer.execute("BEGIN IMMEDIATE")
        writer.execute(
            "UPDATE rca_delivery_meta SET value = ? WHERE key = ?",
            ("uncommitted", "active_wal_external_binding_test"),
        )
        assert writer.in_transaction
        before = _sqlite_storage_identity(path)

        with pytest.raises(
            RuntimeError,
            match="external_write_fence_operation_denied",
        ):
            store.validate_profile_terminal_external_write_binding(
                effect_key="profile-effect",
                delivery_id="profile-delivery",
                lease_token="profile-lease",
                lease_fence=1,
                operation="feishu_issue_comment",
                issue_url="https://project.feishu.cn/test/1",
                target_key="feishu_project:test:issue:1",
                business_key="profile-business",
                submission_key="profile-submission",
                generation=1,
                require_write_started=False,
                now=NOW,
            )
        with pytest.raises(
            RuntimeError,
            match="external_write_fence_identity_mismatch",
        ):
            store.validate_terminal_rerun_external_write_binding(
                effect_key="rerun-effect",
                delivery_id="rerun-delivery",
                lease_token="rerun-lease",
                lease_fence=1,
                operation="feishu_issue_comment",
                issue_url="https://project.feishu.cn/test/1",
                target_key="feishu_project:test:issue:1",
                business_key="rerun-business",
                submission_key="rerun-submission",
                generation=2,
                require_write_started=False,
                now=NOW,
            )

        assert snapshot_calls == []
        _assert_source_probe_identity(before, _sqlite_storage_identity(path))
    finally:
        if writer.in_transaction:
            writer.rollback()
        writer.close()

    with sqlite3.connect(path) as conn:
        assert conn.execute(
            "SELECT value FROM rca_delivery_meta WHERE key = ?",
            ("active_wal_external_binding_test",),
        ).fetchone() == ("committed",)


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


def test_current_v14_writable_open_rebuilds_terminal_only_w6_guards(tmp_path):
    path = tmp_path / "control.sqlite3"
    RcaControlStore(path)
    RcaDeliveryStore(path)
    trigger_names = (
        "trg_learning_lane_stock_effect_insert_forbidden",
        "trg_learning_lane_stock_subscription_insert_forbidden",
        "trg_learning_lane_stock_subscription_update_forbidden",
    )
    with sqlite3.connect(path) as conn:
        for name in trigger_names:
            row = conn.execute(
                "SELECT sql FROM sqlite_master WHERE type='trigger' AND name=?",
                (name,),
            ).fetchone()
            assert row is not None
            legacy_sql = str(row[0]).replace(
                "rca_owner_authorized_rerun_delivery_authorities",
                "rca_terminal_rerun_delivery_authorities",
            )
            conn.execute(f"DROP TRIGGER {name}")
            conn.execute(legacy_sql)

    with pytest.raises(
        RuntimeError,
        match="incompatible_delivery_store_schema:w6_trigger:",
    ):
        RcaDeliveryStore(path, require_current=True)

    RcaControlStore(path)
    RcaDeliveryStore(path)
    RcaDeliveryStore(path, require_current=True)
    with sqlite3.connect(path) as conn:
        rebuilt = {
            str(row[0]): str(row[1])
            for row in conn.execute(
                "SELECT name, sql FROM sqlite_master WHERE type='trigger' "
                "AND name IN (?, ?, ?)",
                trigger_names,
            ).fetchall()
        }
    assert set(rebuilt) == set(trigger_names)
    assert all(
        "rca_owner_authorized_rerun_delivery_authorities" in sql
        for sql in rebuilt.values()
    )
    effect_guard = rebuilt["trg_learning_lane_stock_effect_insert_forbidden"]
    assert "pnc_rca_delivery_effect_v4" in effect_guard
    assert "$.semantic_payload_sha256" in effect_guard
    assert "authority_watch.delivery_id = job.delivery_id" not in effect_guard


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


def _current_epoch_valid_w3_quarantined_control(
    tmp_path,
    *,
    error_code="internal_submit_failure",
    error_detail="private submit failure",
):
    from tests.gateway.test_pnc_rca_w3_snapshot import _admit_manual_w3

    control = RcaControlStore(tmp_path / "control.sqlite3")
    epoch = _activate_direct_steady(control)
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
        error_code=error_code,
        error_detail=error_detail,
        now=current + timedelta(seconds=2),
    )
    return control, current, snapshot


def test_quarantined_kafka_outbox_backfills_public_issue_terminal_atomically(
    tmp_path,
):
    control, current, _snapshot = _current_epoch_valid_w3_quarantined_control(
        tmp_path,
        error_code="private_submit_error_with_sensitive_context",
        error_detail="internal bearer SECRET-MUST-NOT-LEAK",
    )
    store = RcaDeliveryStore(control.db_path)

    before = store.backpressure_snapshot(now=current + timedelta(seconds=3))
    assert before.untracked_completed_submissions == 1
    assert store.backfill_completed_submissions(
        now=current + timedelta(seconds=3), activation_required=True
    ) == 1
    assert store.backfill_completed_submissions(
        now=current + timedelta(seconds=4), activation_required=True
    ) == 0

    [watch] = store.list_rows("rca_execution_watch")
    [job] = store.list_rows("rca_delivery_jobs")
    effects = store.list_rows("rca_delivery_effects")
    assert watch["state"] == "delivery_created"
    assert watch["last_error_code"] == "private_submit_error_with_sensitive_context"
    assert "SECRET-MUST-NOT-LEAK" in watch["last_error_detail"]
    assert job["outcome"] == "quarantined"
    assert job["terminal_state"] == OUTBOX_QUARANTINED_TERMINAL_STATE
    assert job["terminal_error_code"] == OUTBOX_QUARANTINED_PUBLIC_ERROR_CODE
    issue_effects = [
        row for row in effects if row["effect_kind"] == "feishu_issue_comment"
    ]
    assert len(issue_effects) == 1
    assert "private_submit_error" not in issue_effects[0]["payload_json"]
    assert "SECRET-MUST-NOT-LEAK" not in issue_effects[0]["payload_json"]


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


@pytest.mark.parametrize(
    "profile_error_code",
    ("business_profile_unsupported", "business_profile_conflict"),
)
def test_current_epoch_unsupported_profile_stays_silent_at_outbox(
    tmp_path,
    profile_error_code,
):
    control = RcaControlStore(tmp_path / "control.sqlite3")
    epoch = _activate_direct_steady(control, start_offset=20)
    with sqlite3.connect(control.db_path) as conn:
        conn.execute(
            "UPDATE rca_activation_epochs SET state='steady_active' "
            "WHERE epoch_id=? AND is_current=1",
            (epoch["epoch_id"],),
        )
    policy, record = _real_g1q3_profile_snapshot(20, "6841983153")
    result = control.ingest_record(
        record,
        policy=policy,
        submit_enabled=True,
        activation_required=True,
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
        error_code=profile_error_code,
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
    assert watch["state"] == "quarantined"
    assert watch["task_id"] is None
    assert watch["delivery_id"] is None
    assert watch["last_error_code"] == profile_error_code
    assert store.list_rows("rca_delivery_jobs") == []
    assert store.list_rows("rca_delivery_effects") == []
    subscriptions = store.list_rows("rca_delivery_subscriptions")
    assert subscriptions
    assert {row["status"] for row in subscriptions} == {"suppressed"}
    assert {row["reason"] for row in subscriptions} == {profile_error_code}
    assert all(row["delivery_id"] is None for row in subscriptions)
    assert all(row["effect_key"] is None for row in subscriptions)


def test_profile_terminal_provider_claim_rechecks_lease_target_and_epoch(tmp_path, monkeypatch):
    from gateway import pnc_rca_provider_fence as provider_fence
    from gateway.pnc_rca_provider_fence import (
        build_profile_terminal_provider_claim,
    )
    from gateway.pnc_rca_write_fence import ExternalWriteFenceError
    from scripts import pnc_rca_delivery_dispatcher as dispatcher_module
    provider_now = datetime.now(timezone.utc)
    control = RcaControlStore(tmp_path / "control.sqlite3")
    epoch = _activate_direct_steady(
        control, start_offset=20, now=provider_now
    )
    assert epoch["state"] == "steady_active"
    RcaDeliveryStore(control.db_path)
    from tests.gateway.test_pnc_rca_control_store import (
        _migrate_v14_fixture_to_v15,
    )

    _migrate_v14_fixture_to_v15(
        control.db_path,
        successor_epoch_id="profile-terminal-v15-steady",
    )
    control = RcaControlStore(
        control.db_path,
        require_current=True,
        allow_successor_write=True,
    )
    policy, record = _real_g1q3_profile_snapshot(20, "7019637554")
    result = control.ingest_record(
        record,
        policy=policy,
        submit_enabled=True,
        activation_required=True,
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
        error_code="business_profile_adapter_not_ready",
        error_detail="profile provider claim fixture",
        now=provider_now + timedelta(seconds=1),
    )
    store = RcaDeliveryStore(
        control.db_path,
        require_current=True,
        allow_successor_write=True,
    )
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
        project_key=binding["project_key"],
        project_simple_name=binding["project_simple_name"],
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
        issue_project_key=REAL_G1Q3_PROJECT_KEY,
        issue_work_item_id="7041712812",
    )
    assert live["authority_kind"] == "profile_terminal"
    assert live["project_key"] == REAL_G1Q3_PROJECT_KEY
    assert live["project_simple_name"] == REAL_G1Q3_PROJECT_SIMPLE_NAME
    provider_calls = []
    adapter = dispatcher_module.MeegleIssueCommentAdapter(
        lambda args: (
            provider_calls.append(args)
            or (0, json.dumps({"comment_id": "unexpected"}), "")
        )
    )
    with sqlite3.connect(control.db_path) as conn:
        original_binding_fingerprint = str(
            conn.execute(
                "SELECT audit.binding_fingerprint "
                "FROM rca_activation_transition_audit AS audit "
                "JOIN rca_activation_epochs AS epoch "
                "ON epoch.epoch_id = audit.epoch_id "
                "WHERE epoch.is_current = 1 "
                "ORDER BY audit.audit_id DESC LIMIT 1"
            ).fetchone()[0]
        )
        conn.execute(
            "UPDATE rca_activation_transition_audit "
            "SET binding_fingerprint = ? WHERE audit_id = ("
            "SELECT MAX(audit.audit_id) "
            "FROM rca_activation_transition_audit AS audit "
            "JOIN rca_activation_epochs AS epoch "
            "ON epoch.epoch_id = audit.epoch_id WHERE epoch.is_current = 1"
            ")",
            ("f" * 64,),
        )
    try:
        with dispatcher_module._bound_provider_write_guard(claim):
            with pytest.raises(
                ExternalWriteFenceError,
                match="external_write_fence_epoch_not_current",
            ):
                adapter.add_comment(
                    REAL_G1Q3_PROJECT_KEY,
                    "7041712812",
                    "must not send",
                )
    finally:
        with sqlite3.connect(control.db_path) as conn:
            conn.execute(
                "UPDATE rca_activation_transition_audit "
                "SET binding_fingerprint = ? WHERE audit_id = ("
                "SELECT MAX(audit.audit_id) "
                "FROM rca_activation_transition_audit AS audit "
                "JOIN rca_activation_epochs AS epoch "
                "ON epoch.epoch_id = audit.epoch_id WHERE epoch.is_current = 1"
                ")",
                (original_binding_fingerprint,),
            )
    assert provider_calls == []
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
            issue_project_key=REAL_G1Q3_PROJECT_KEY,
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
            issue_project_key=REAL_G1Q3_PROJECT_KEY,
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
            issue_project_key=REAL_G1Q3_PROJECT_KEY,
            issue_work_item_id="7041712812",
        )
    authority = claim.payload()["authority"]
    assert authority["operation"] == "feishu_issue_comment"
    assert authority["project_key"] == REAL_G1Q3_PROJECT_KEY
    assert authority["project_simple_name"] == REAL_G1Q3_PROJECT_SIMPLE_NAME
    assert "thread_id" not in authority
    with pytest.raises(
        ExternalWriteFenceError,
        match="external_write_fence_target_mismatch",
    ):
        provider_fence.revalidate_provider_write_claim(
            claim,
            operation="feishu_issue_comment",
            issue_project_key=REAL_G1Q3_PROJECT_SIMPLE_NAME,
            issue_work_item_id="7041712812",
        )
    wrong_simple_name = build_profile_terminal_provider_claim(
        epoch_id=binding["epoch_id"],
        activation_ledger_id=binding["activation_ledger_id"],
        effect_key=binding["effect_key"],
        delivery_id=binding["delivery_id"],
        lease_token=binding["lease_token"],
        lease_fence=binding["lease_fence"],
        issue_target=binding["issue_url"],
        project_key=binding["project_key"],
        project_simple_name="wrong-slug",
        target_key=binding["target_key"],
        business_key=binding["business_key"],
        submission_key=binding["submission_key"],
        generation=binding["generation"],
        source_error_code=binding["source_error_code"],
    )
    with pytest.raises(
        ExternalWriteFenceError,
        match="external_write_fence_identity_mismatch",
    ):
        provider_fence.revalidate_provider_write_claim(
            wrong_simple_name,
            operation="feishu_issue_comment",
            issue_project_key=REAL_G1Q3_PROJECT_KEY,
            issue_work_item_id="7041712812",
        )
    wrong_target = build_profile_terminal_provider_claim(
        epoch_id=binding["epoch_id"],
        activation_ledger_id=binding["activation_ledger_id"],
        effect_key=binding["effect_key"],
        delivery_id=binding["delivery_id"],
        lease_token=binding["lease_token"],
        lease_fence=binding["lease_fence"],
        issue_target="https://project.feishu.cn/t03o4q/issue/detail/9999999999",
        project_key=binding["project_key"],
        project_simple_name=binding["project_simple_name"],
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
            issue_project_key=REAL_G1Q3_PROJECT_KEY,
            issue_work_item_id="7041712812",
        )
    with sqlite3.connect(control.db_path) as conn:
        conn.execute(
            "UPDATE rca_activation_epochs "
            "SET state='retired', is_current=0, "
            "retired_at=COALESCE(retired_at, updated_at) "
            "WHERE epoch_id=? AND is_current=1",
            (binding["epoch_id"],),
        )
    with pytest.raises(
        ExternalWriteFenceError,
        match="external_write_fence_epoch_not_current",
    ):
        provider_fence.revalidate_provider_write_claim(
            claim,
            operation="feishu_issue_comment",
            issue_project_key=REAL_G1Q3_PROJECT_KEY,
            issue_work_item_id="7041712812",
        )


def test_current_epoch_adapter_pending_terminal_without_w3_is_public(tmp_path):
    from tests.gateway.test_pnc_rca_control_store import (
        _profile_snapshot_policy,
        _profile_snapshot_record,
    )

    control = RcaControlStore(tmp_path / "control.sqlite3")
    epoch = _activate_direct_steady(control, start_offset=21)
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
        _profile_snapshot_policy,
        _profile_snapshot_record,
    )

    control = RcaControlStore(tmp_path / "control.sqlite3")
    epoch = _activate_direct_steady(control, start_offset=22)
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
    assert watch["last_error_code"] == "business_profile_unsupported"
    assert store.list_rows("rca_delivery_jobs") == []
    assert store.list_rows("rca_delivery_effects") == []


def test_invalid_profile_terminal_binding_is_silent_and_settled(tmp_path):
    from tests.gateway.test_pnc_rca_control_store import (
        _profile_snapshot_policy,
        _profile_snapshot_record,
    )

    control = RcaControlStore(tmp_path / "control.sqlite3")
    epoch = _activate_direct_steady(control, start_offset=22)
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
    assert "delivery_quarantine" not in health
    assert health["production_blockers"] == {
        "schema_successor_read_only": 0,
        "activation_schema_unavailable": 0,
        "activation_epoch_not_steady": 0,
        "activation_binding_invalid": 0,
        "release_fingerprint_invalid": 0,
        "release_note_invalid": 0,
        "uncertain_effects": 0,
        "pending_delivery_observations": 0,
    }
    assert watch["state"] == "pending"


def test_delivery_health_keeps_historical_quarantine_diagnostic_only(tmp_path):
    store, effect = _claimed_effect(tmp_path)
    store.quarantine_effect(
        claim=effect,
        error_code="feishu_work_item_not_found",
        error_detail="historical delivery quarantine",
        now=NOW,
    )

    health = store.health(now=NOW)

    assert health["business_blockers"]["quarantined_jobs"] == 1
    assert health["business_blockers"]["quarantined_effects"] == 1
    assert health["business_blockers"]["quarantined_subscriptions"] == 0
    assert not {
        "quarantined_jobs",
        "quarantined_effects",
        "quarantined_subscriptions",
    } & health["production_blockers"].keys()
    assert health["business_ready"] is True


def test_activation_health_does_not_block_on_historical_uncertain_effect(tmp_path):
    control, result = _control(tmp_path)
    store = RcaDeliveryStore(control.db_path)
    assert store.backfill_completed_submissions(now=NOW) == 1
    watch = store.claim_due_watch(
        lease_owner="historical-effect-collector",
        now=NOW,
    )
    assert watch is not None
    store.create_delivery(
        claim=watch,
        delivery=_delivery(watch),
        status={"success": True, "state": "completed"},
        now=NOW,
    )
    with sqlite3.connect(control.db_path) as conn:
        conn.row_factory = sqlite3.Row
        delivery_id = str(
            conn.execute("SELECT delivery_id FROM rca_delivery_jobs").fetchone()[0]
        )
        conn.execute("UPDATE rca_delivery_effects SET status = 'uncertain'")

    legacy_health = store.health(now=NOW)
    assert legacy_health["activation"]["required"] is True
    assert legacy_health["business_blockers"]["uncertain_effects"] == 1
    assert legacy_health["business_ready"] is False

    with sqlite3.connect(control.db_path) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute(
            "UPDATE rca_delivery_effects "
            "SET status = 'succeeded', write_phase = 'settled'"
        )
        store._aggregate_job_status(conn, delivery_id, NOW.isoformat())

    _bind_activation_execution(control, result, state="steady_active")
    _switch_activation_epoch(
        control,
        old_epoch="delivery-epoch-1",
        new_epoch="delivery-epoch-2",
    )
    with sqlite3.connect(control.db_path) as conn:
        conn.execute(
            "UPDATE rca_delivery_effects "
            "SET status = 'uncertain', write_phase = 'write_started'"
        )

    health = store.health(
        now=NOW + timedelta(seconds=2), activation_required=True
    )

    assert health["activation"]["current_epoch_id"] == "delivery-epoch-2"
    assert health["activation"]["blocked_historical_counts"][
        "dispatchable_effects"
    ] == 1
    assert health["delivery_effects"]["uncertain"] == 1
    assert health["business_blockers"]["uncertain_effects"] == 0
    assert health["production_blockers"]["uncertain_effects"] == 0
    assert health["business_ready"] is True


def test_activation_health_requires_steady_epoch_and_release_binding(
    tmp_path,
):
    control, result = _control(tmp_path)
    _bind_activation_execution(control, result, state="bounded_active")
    store = RcaDeliveryStore(control.db_path)

    bounded = store.health(now=NOW, activation_required=True)

    assert bounded["activation"]["release_fingerprint_sha256"] == ""
    assert bounded["activation"]["release_note_sha256"] == ""
    assert bounded["activation"]["binding_valid"] is False
    assert bounded["activation"]["production_ready"] is False
    assert bounded["production_blockers"]["activation_epoch_not_steady"] == 1
    assert bounded["production_blockers"]["activation_binding_invalid"] == 1
    assert bounded["production_blockers"]["release_fingerprint_invalid"] == 1
    assert bounded["production_blockers"]["release_note_invalid"] == 1
    assert bounded["business_ready"] is False

    with sqlite3.connect(control.db_path) as conn:
        conn.execute(
            "UPDATE rca_activation_epochs "
            "SET state = 'steady_active', production_fingerprint = ? "
            "WHERE is_current = 1",
            ("0" * 64,),
        )

    zero_fingerprint = store.health(now=NOW, activation_required=True)

    assert zero_fingerprint["activation"]["production_ready"] is False
    assert zero_fingerprint["activation"]["binding_valid"] is False
    assert zero_fingerprint["production_blockers"]["activation_epoch_not_steady"] == 0
    assert zero_fingerprint["production_blockers"]["activation_binding_invalid"] == 1
    assert (
        zero_fingerprint["production_blockers"]["release_fingerprint_invalid"]
        == 1
    )
    assert zero_fingerprint["production_blockers"]["release_note_invalid"] == 1
    assert zero_fingerprint["business_ready"] is False

    ready_control, _ = _control(tmp_path / "ready")
    ready_store = RcaDeliveryStore(ready_control.db_path)
    ready = ready_store.health(now=NOW, activation_required=True)

    assert ready["activation"]["release_fingerprint_sha256"] == "a" * 64
    assert ready["activation"]["release_note_sha256"] == "b" * 64
    assert ready["activation"]["binding_valid"] is True
    assert ready["activation"]["production_ready"] is True
    assert ready["production_blockers"]["activation_epoch_not_steady"] == 0
    assert ready["production_blockers"]["activation_binding_invalid"] == 0
    assert ready["production_blockers"]["release_fingerprint_invalid"] == 0
    assert ready["production_blockers"]["release_note_invalid"] == 0
    assert ready["business_ready"] is True

    epoch = ready_store.activation_epoch()
    assert epoch is not None
    assert epoch["state"] == "steady_active"
    assert epoch["release_fingerprint_sha256"] == "a" * 64
    assert epoch["release_note_sha256"] == "b" * 64
    assert "production_fingerprint" not in epoch
    assert "production_gate_receipt_sha256" not in epoch
    assert isinstance(epoch["config_sha256"], str)


def test_resident_activation_store_and_health_reject_hidden_v14_tamper(tmp_path):
    control, _ = _control(tmp_path)
    store = RcaDeliveryStore(control.db_path)
    with sqlite3.connect(control.db_path) as conn:
        conn.execute(
            "UPDATE rca_activation_epochs "
            "SET preauthorization_fingerprint = ? WHERE is_current = 1",
            ("f" * 64,),
        )

    with pytest.raises(
        ActivationEpochError,
        match="activation_predecessor_binding_invalid",
    ):
        store.activation_epoch()

    health = store.health(now=NOW, activation_required=True)
    assert health["ok"] is False
    assert health["business_ready"] is False
    assert health["activation"]["binding_valid"] is False
    assert health["activation"]["release_fingerprint_sha256"] == ""
    assert health["activation"]["release_note_sha256"] == ""
    assert health["activation"]["processing_enabled"] is False
    assert health["production_blockers"]["activation_binding_invalid"] == 1


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


def test_delivery_writer_is_fenced_when_v15_commits_after_connect(
    tmp_path,
    monkeypatch,
):
    from tests.gateway.test_pnc_rca_control_store import (
        _migrate_v14_fixture_to_v15,
    )

    path = tmp_path / "control.sqlite3"
    control = RcaControlStore(path)
    control.activate_direct_steady_epoch(
        epoch_id="delivery-midflight-v14",
        release_fingerprint_sha256="a" * 64,
        release_note_sha256="b" * 64,
        config_sha256="c" * 64,
        db_logical_identity={"database": "delivery-midflight-test"},
        partition_start_fence={},
        operator="delivery-test",
        reason="activate delivery midflight test runtime",
        now=NOW,
    )
    RcaDeliveryStore(path)
    store = RcaDeliveryStore(path, require_current=True)
    store.open_delivery_dispatcher_circuit(
        reason_code="test_midflight_public_writer",
        now=NOW,
    )
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
        store.close_delivery_dispatcher_circuit(now=NOW + timedelta(seconds=1))

    with sqlite3.connect(path) as conn:
        assert conn.execute(
            "SELECT state FROM rca_delivery_dispatcher_circuit "
            "WHERE circuit_name = ?",
            (DELIVERY_EFFECT_KIND,),
        ).fetchone()[0] == "open"


def test_delivery_require_current_connection_guard_rejects_schema_cookie_drift(
    tmp_path,
):
    path = tmp_path / "control.sqlite3"
    control = RcaControlStore(path)
    control.activate_direct_steady_epoch(
        epoch_id="delivery-schema-cookie-v14",
        release_fingerprint_sha256="a" * 64,
        release_note_sha256="b" * 64,
        config_sha256="c" * 64,
        db_logical_identity={"database": "delivery-schema-cookie-test"},
        partition_start_fence={},
        operator="delivery-test",
        reason="activate delivery schema cookie test runtime",
        now=NOW,
    )
    RcaDeliveryStore(path)
    store = RcaDeliveryStore(path, require_current=True)
    store.open_delivery_dispatcher_circuit(
        reason_code="test_schema_cookie_drift",
        now=NOW,
    )
    guarded = store._connect()
    try:
        with sqlite3.connect(path) as external:
            external.execute("CREATE TABLE unrelated_delivery_schema_drift(id INTEGER)")

        with pytest.raises(
            sqlite3.IntegrityError,
            match="incompatible_control_store_schema:write_marker",
        ):
            guarded.execute(
                "UPDATE rca_delivery_dispatcher_circuit SET state = 'closed' "
                "WHERE circuit_name = ?",
                (DELIVERY_EFFECT_KIND,),
            )
    finally:
        guarded.close()

    with sqlite3.connect(path) as conn:
        assert conn.execute(
            "SELECT value FROM control_meta WHERE key = 'schema_version'"
        ).fetchone()[0] == CONTROL_STORE_SCHEMA_VERSION
        assert conn.execute(
            "SELECT state FROM rca_delivery_dispatcher_circuit "
            "WHERE circuit_name = ?",
            (DELIVERY_EFFECT_KIND,),
        ).fetchone()[0] == "open"


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


def test_forged_activation_deferred_error_does_not_suppress_backfill(tmp_path):
    _control(tmp_path, completed=False)
    store = RcaDeliveryStore(tmp_path / "control.sqlite3")
    with sqlite3.connect(store.db_path) as conn:
        conn.execute(
            """
            UPDATE rca_outbox
               SET status = 'quarantined', quarantined_at = ?,
                   last_error_code = 'activation_epoch_deferred',
                   last_error_detail = 'exact operator-reviewed activation deferral',
                   updated_at = ?
            """,
            (NOW.isoformat(), NOW.isoformat()),
        )
        conn.commit()

    assert store.backfill_completed_submissions(now=NOW) == 1
    assert len(store.list_rows("rca_execution_watch")) == 1
    assert store.list_rows("rca_delivery_jobs") == []
    assert store.list_rows("rca_delivery_effects") == []


def test_audited_activation_deferral_is_never_backfilled_into_delivery(tmp_path):
    control, result = _control(tmp_path, completed=False)
    with sqlite3.connect(control.db_path) as conn:
        row = conn.execute(
            """
            SELECT outbox_id, source_event_id, activation_epoch_id,
                   activation_ledger_id
              FROM rca_outbox
             WHERE submission_key = ?
            """,
            (result.submission_key,),
        ).fetchone()
        assert row is not None
        assert row[2] == "delivery-epoch-1"
        assert row[3] is not None
        conn.execute(
            "UPDATE rca_outbox SET status = 'quarantined', quarantined_at = ?, "
            "last_error_code = 'activation_epoch_deferred', "
            "last_error_detail = 'exact operator-reviewed activation deferral', "
            "updated_at = ? WHERE outbox_id = ?",
            (NOW.isoformat(), NOW.isoformat(), row[0]),
        )
        conn.execute(
            """
            INSERT INTO rca_shadow_promotion_audit(
                event_uid, outbox_id, submission_key, operator, reason,
                outcome, from_status, to_status, detail, created_at
            ) VALUES (?, ?, ?, 'delivery-test',
                      'exact audited deferral must remain local-only',
                      'deferred', 'pending', 'quarantined',
                      'exact activation item deferred for reviewed manual recovery', ?)
            """,
            (str(row[1]), int(row[0]), result.submission_key, NOW.isoformat()),
        )
    store = RcaDeliveryStore(control.db_path)

    assert store.backfill_completed_submissions(now=NOW + timedelta(seconds=2)) == 0
    assert store.list_rows("rca_execution_watch") == []
    assert store.list_rows("rca_delivery_jobs") == []
    assert store.list_rows("rca_delivery_effects") == []


def test_activation_is_required_even_when_caller_uses_legacy_flag(tmp_path):
    _control(tmp_path)
    store = RcaDeliveryStore(tmp_path / "control.sqlite3")

    assert store.backfill_completed_submissions(now=NOW) == 1
    assert store.health(now=NOW)["activation"]["required"] is True


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

    predecessor = control.direct_steady_predecessor()
    assert predecessor is not None
    assert predecessor["epoch_id"] == "delivery-epoch-1"
    assert predecessor["state"] == "steady_active"
    assert predecessor["inflight"]["total"] == 0
    replacement = control.activate_direct_steady_epoch(
        epoch_id="delivery-epoch-issue-only-successor",
        release_fingerprint_sha256="7" * 64,
        release_note_sha256="8" * 64,
        config_sha256="a" * 64,
        db_logical_identity={"database": "delivery-test"},
        partition_start_fence={TOPIC: {"2": 11}},
        operator="delivery-test",
        reason="issue-only delivery settlement permits replacement",
        expected_predecessor_epoch_id=predecessor["epoch_id"],
        expected_predecessor_state=predecessor["state"],
        expected_predecessor_binding_fingerprint=predecessor[
            "binding_fingerprint"
        ],
        now=NOW + timedelta(seconds=4),
    )
    assert replacement["state"] == "steady_active"
    assert control.activation_epoch()["epoch_id"] == replacement["epoch_id"]


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


def test_activation_backpressure_ignores_historical_work_and_counts_current_work(
    tmp_path,
):
    control, historical_effect = _control(tmp_path, issue_id=7041712812)
    control, historical_watch = _control(
        tmp_path,
        offset=11,
        issue_id=7041712813,
    )
    control, historical_untracked = _control(
        tmp_path,
        offset=12,
        issue_id=7041712814,
    )
    for result in (historical_effect, historical_watch, historical_untracked):
        _bind_activation_execution(control, result, state="steady_active")
    store = RcaDeliveryStore(control.db_path)
    assert store.backfill_completed_submissions(now=NOW) == 3
    effect_claim = store.claim_due_watch(
        lease_owner="historical-effect-collector",
        now=NOW,
        activation_required=True,
    )
    assert effect_claim is not None
    store.create_delivery(
        claim=effect_claim,
        delivery=_delivery(effect_claim),
        status={"success": True, "state": "completed"},
        now=NOW,
        activation_required=True,
    )
    with sqlite3.connect(control.db_path) as conn:
        conn.execute(
            "DELETE FROM rca_execution_watch WHERE submission_key = ?",
            (historical_untracked.submission_key,),
        )
    _switch_activation_epoch(
        control,
        old_epoch="delivery-epoch-1",
        new_epoch="delivery-epoch-2",
    )

    activation_snapshot = store.backpressure_snapshot(
        now=NOW + timedelta(seconds=2)
    )
    assert activation_snapshot.unresolved_effects == 0
    assert activation_snapshot.pending_watches == 0
    assert activation_snapshot.untracked_completed_submissions == 0
    assert activation_snapshot.unresolved_work == 0

    with sqlite3.connect(control.db_path) as conn:
        conn.execute("PRAGMA ignore_check_constraints=ON")
        conn.execute(
            "UPDATE rca_delivery_effects SET status = 'invalid_historical_state'"
        )
    with pytest.raises(RuntimeError, match="effect_status"):
        store.backpressure_snapshot(
            now=NOW + timedelta(seconds=2),
            activation_required=True,
        )

    current_path = tmp_path / "current"
    control, current_effect = _control(
        current_path,
        issue_id=7041712815,
    )
    control, current_untracked = _control(
        current_path,
        offset=11,
        issue_id=7041712816,
    )
    _bind_activation_execution(
        control,
        current_effect,
        state="steady_active",
    )
    _bind_activation_execution(
        control,
        current_untracked,
        state="steady_active",
    )
    current_store = RcaDeliveryStore(control.db_path)
    assert current_store.backfill_completed_submissions(
        now=NOW + timedelta(seconds=3)
    ) == 2
    current_claim = current_store.claim_due_watch(
        lease_owner="current-effect-collector",
        now=NOW + timedelta(seconds=3),
        activation_required=True,
    )
    assert current_claim is not None
    current_store.create_delivery(
        claim=current_claim,
        delivery=_delivery(current_claim),
        status={"success": True, "state": "completed"},
        now=NOW + timedelta(seconds=3),
        activation_required=True,
    )
    with sqlite3.connect(control.db_path) as conn:
        conn.execute(
            "DELETE FROM rca_execution_watch WHERE submission_key = ?",
            (current_untracked.submission_key,),
        )

    activation_snapshot = current_store.backpressure_snapshot(
        now=NOW + timedelta(seconds=4),
        activation_required=True,
    )
    assert activation_snapshot.unresolved_effects == 1
    assert activation_snapshot.pending_watches == 0
    assert activation_snapshot.untracked_completed_submissions == 1
    assert activation_snapshot.unresolved_work == 2


def test_activation_backpressure_fails_closed_without_activation_schema(tmp_path):
    control, result = _control(tmp_path)
    _bind_activation_execution(control, result, state="steady_active")
    store = RcaDeliveryStore(control.db_path)
    with sqlite3.connect(control.db_path) as conn:
        conn.execute("DROP TABLE rca_activation_admission_ledger")

    with pytest.raises(RuntimeError, match="delivery_activation_schema_unavailable"):
        store.backpressure_snapshot(now=NOW)


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
