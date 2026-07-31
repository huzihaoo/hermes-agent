from __future__ import annotations

import asyncio
from copy import deepcopy
from datetime import datetime, timedelta
import json
import sqlite3
from types import SimpleNamespace

import pytest

from gateway.pnc_rca_delivery_contract import (
    DELIVERY_CARD_PATCH_EFFECT_KIND,
    DELIVERY_EFFECT_KINDS,
    DELIVERY_TARGET_SCHEMA_VERSION,
    DeliveryContractError,
    build_card_patch_effect,
    validate_card_patch_effect_payload,
)
from gateway.pnc_rca_delivery_store import (
    DELIVERY_STORE_SCHEMA_VERSION,
    DELIVERY_STORE_W6_PREDECESSOR_VERSION,
    DeliveryRecordConflictError,
    RcaDeliveryStore,
)
from gateway.feishu_task_card import stable_render_hash
from gateway.feishu_task_confirm import (
    RCA_CANDIDATE_REVIEW_PRESET,
    RCA_CANDIDATE_REVIEW_SCHEMA_VERSION,
)
from gateway.pnc_rca_provider_fence import build_write_fence_provider_claim
from scripts import pnc_completion_notice_relay
from scripts.pnc_completion_notice_relay import FeishuHotSender, sync_task_card
from scripts.pnc_rca_delivery_dispatcher import DeliveryDispatcher
from scripts.vm_task_state_bridge import _atomic_write_json
from tests.gateway.test_pnc_rca_conclusion_adjudication import (
    NOW,
    _record_retraction,
    _seed_published_conclusion,
)
from tests.gateway.test_pnc_rca_delivery_store import _control


def _card_patch(
    tmp_path,
    *,
    settle_correction: bool,
) -> tuple[RcaDeliveryStore, object, dict[str, object]]:
    store = _seed_published_conclusion(tmp_path)
    adjudication = _record_retraction(store)
    if settle_correction:
        with sqlite3.connect(store.db_path) as conn:
            conn.execute(
                "UPDATE rca_delivery_effects "
                "SET status = 'succeeded', write_phase = 'settled', "
                "completed_at = ?, updated_at = ? WHERE effect_key = ?",
                (
                    NOW.isoformat(),
                    NOW.isoformat(),
                    adjudication.correction_effect_key,
                ),
            )
    [job] = store.list_rows("rca_delivery_jobs")
    target = {
        "schema_version": DELIVERY_TARGET_SCHEMA_VERSION,
        "platform": "feishu",
        "chat_id": "oc_g1q3",
        "thread_id": "topic:om_owner_root",
        "message_id": "om_card123",
        "submission_key": job["submission_key"],
        "output_cap": "L1",
    }
    card_payload = {
        "header": {
            "title": {
                "tag": "plain_text",
                "content": "RCA conclusion review",
            }
        },
        "elements": [
            {
                "tag": "markdown",
                "content": f"追踪号：`{job['submission_key']}`",
            }
        ],
    }
    _effect_key, _semantic_sha, payload = build_card_patch_effect(
        delivery_id=job["delivery_id"],
        project_key=job["project_key"],
        work_item_type_key=job["work_item_type_key"],
        work_item_id=job["work_item_id"],
        business_key=job["business_key"],
        submission_key=job["submission_key"],
        generation=job["generation"],
        adjudication_id=adjudication.adjudication_id,
        action=adjudication.action,
        conclusion_state=adjudication.conclusion_state,
        original_effect_key=adjudication.original_effect_key,
        correction_effect_key=adjudication.correction_effect_key,
        target_key="feishu_card:oc_g1q3:om_card123",
        target=target,
        card_payload=card_payload,
    )
    return store, adjudication, payload


def _card_dispatcher_config(tmp_path):
    return SimpleNamespace(
        enabled=True,
        lease_seconds=90,
        activation_required=True,
        observability_enabled=True,
        observability_path=tmp_path / "card-delivery-observations.jsonl",
        inventory_pin="c" * 64,
        observation_release_id="card-release-test",
        quarantine_release_id="",
    )


def test_card_patch_contract_is_exact_deterministic_and_tamper_evident(tmp_path):
    _store, _adjudication, payload = _card_patch(
        tmp_path,
        settle_correction=False,
    )

    assert DELIVERY_CARD_PATCH_EFFECT_KIND in DELIVERY_EFFECT_KINDS
    assert validate_card_patch_effect_payload(payload) == payload

    tampered = deepcopy(payload)
    tampered["card_payload"]["header"]["title"]["content"] += " changed"
    with pytest.raises(DeliveryContractError) as exc:
        validate_card_patch_effect_payload(tampered)
    assert exc.value.code == "delivery_card_patch_render_hash_invalid"

    prefix_only = deepcopy(payload)
    prefix_only["card_payload"]["elements"][0]["content"] = (
        f"追踪号：`{payload['submission_key']}-extra`"
    )
    with pytest.raises(DeliveryContractError) as exc:
        validate_card_patch_effect_payload(prefix_only)
    assert exc.value.code == "delivery_card_patch_payload_invalid"

    with pytest.raises(DeliveryContractError) as exc:
        validate_card_patch_effect_payload("not-an-object")  # type: ignore[arg-type]
    assert exc.value.code == "delivery_card_patch_effect_shape_invalid"

    mismatched_target = {
        "schema_version": DELIVERY_TARGET_SCHEMA_VERSION,
        "platform": "feishu",
        "chat_id": payload["chat_id"],
        "thread_id": payload["thread_id"],
        "message_id": payload["message_id"],
        "submission_key": "g1q3-rca-s1-" + "9" * 64,
        "output_cap": "L1",
    }
    with pytest.raises(DeliveryContractError) as exc:
        build_card_patch_effect(
            delivery_id=payload["delivery_id"],
            project_key=payload["project_key"],
            work_item_type_key=payload["work_item_type_key"],
            work_item_id=payload["work_item_id"],
            business_key=payload["business_key"],
            submission_key=payload["submission_key"],
            generation=payload["generation"],
            adjudication_id=payload["adjudication_id"],
            action=payload["action"],
            conclusion_state=payload["conclusion_state"],
            original_effect_key=payload["original_effect_key"],
            correction_effect_key=payload["correction_effect_key"],
            target_key=payload["target_key"],
            target=mismatched_target,
            card_payload=payload["card_payload"],
        )
    assert exc.value.code == "delivery_card_patch_effect_identity_invalid"


@pytest.mark.parametrize(
    "error",
    (
        "230006: Forbidden",
        "permission denied while updating message",
    ),
)
def test_card_patch_permission_failure_is_circuit_classified(error):
    result = FeishuHotSender._card_failure_result({"error": error})

    assert result == {
        "success": False,
        "outcome_uncertain": False,
        "error_code": "feishu_permission_denied",
        "error": error,
    }


def test_card_patch_revalidates_after_readback_before_physical_patch(monkeypatch):
    patch_calls = []
    adapter = SimpleNamespace(
        _client=SimpleNamespace(
            im=SimpleNamespace(
                v1=SimpleNamespace(
                    message=SimpleNamespace(
                        patch=lambda request: patch_calls.append(request)
                    )
                )
            )
        )
    )
    provider_claim = build_write_fence_provider_claim({"state": "issued"})

    def revoked(*_args, **_kwargs):
        raise pnc_completion_notice_relay.ExternalWriteFenceError(
            "external_write_fence_epoch_not_current"
        )

    monkeypatch.setattr(
        pnc_completion_notice_relay,
        "revalidate_provider_write_claim",
        revoked,
    )

    with pytest.raises(
        pnc_completion_notice_relay.ExternalWriteFenceError,
        match="external_write_fence_epoch_not_current",
    ):
        asyncio.run(
            FeishuHotSender._patch_verified_task_card(
                adapter,
                object(),
                provider_claim=provider_claim,
                chat_id="oc_g1q3",
                thread_id="om_owner_root",
            )
        )

    assert patch_calls == []


def test_card_patch_readback_rejects_submission_prefix_collision():
    item = SimpleNamespace(
        message_id="om_card123",
        chat_id="oc_g1q3",
        thread_id="om_owner_root",
        root_id="om_owner_root",
        parent_id="",
        body=SimpleNamespace(
            content=json.dumps(
                {
                    "header": {"title": {"tag": "plain_text", "content": "RCA"}},
                    "elements": [
                        {
                            "tag": "markdown",
                            "content": "追踪号：`task-1-extra`",
                        }
                    ],
                }
            )
        ),
    )
    response = SimpleNamespace(data=SimpleNamespace(items=[item]))
    adapter = SimpleNamespace(
        _build_get_message_request=lambda message_id: message_id,
        _response_succeeded=lambda _response: True,
        _client=SimpleNamespace(
            im=SimpleNamespace(
                v1=SimpleNamespace(
                    message=SimpleNamespace(get=lambda _request: response)
                )
            )
        ),
    )

    with pytest.raises(
        pnc_completion_notice_relay.ExternalWriteFenceError,
        match="external_write_fence_target_mismatch",
    ):
        asyncio.run(
            FeishuHotSender._verify_card_patch_target(
                adapter,
                message_id="om_card123",
                chat_id="oc_g1q3",
                thread_id="topic:om_owner_root",
                submission_key="task-1",
            )
        )


def test_store_enqueues_card_patch_only_after_correction_settles(tmp_path):
    store, adjudication, payload = _card_patch(
        tmp_path,
        settle_correction=False,
    )

    with pytest.raises(
        DeliveryRecordConflictError,
        match="delivery_card_patch_correction_not_settled",
    ):
        store.enqueue_card_patch_effect(payload=payload, now=NOW)

    with sqlite3.connect(store.db_path) as conn:
        conn.execute(
            "UPDATE rca_delivery_effects "
            "SET status = 'succeeded', write_phase = 'settled', "
            "completed_at = ?, updated_at = ? WHERE effect_key = ?",
            (
                NOW.isoformat(),
                NOW.isoformat(),
                adjudication.correction_effect_key,
            ),
        )

    first = store.enqueue_card_patch_effect(payload=payload, now=NOW)
    second = store.enqueue_card_patch_effect(payload=payload, now=NOW)

    assert first.created is True
    assert second.created is False
    [card_effect] = [
        row
        for row in store.list_rows("rca_delivery_effects")
        if row["effect_kind"] == DELIVERY_CARD_PATCH_EFFECT_KIND
    ]
    assert card_effect["effect_key"] == payload["effect_key"]
    assert card_effect["status"] == "pending"

    _key, _sha, drifted_target = build_card_patch_effect(
        delivery_id=payload["delivery_id"],
        project_key=payload["project_key"],
        work_item_type_key=payload["work_item_type_key"],
        work_item_id=payload["work_item_id"],
        business_key=payload["business_key"],
        submission_key=payload["submission_key"],
        generation=payload["generation"],
        adjudication_id=payload["adjudication_id"],
        action=payload["action"],
        conclusion_state=payload["conclusion_state"],
        original_effect_key=payload["original_effect_key"],
        correction_effect_key=payload["correction_effect_key"],
        target_key="feishu_card:oc_g1q3:om_card456",
        target={
            "schema_version": DELIVERY_TARGET_SCHEMA_VERSION,
            "platform": "feishu",
            "chat_id": "oc_g1q3",
            "thread_id": "topic:om_owner_root",
            "message_id": "om_card456",
            "submission_key": payload["submission_key"],
            "output_cap": "L1",
        },
        card_payload=payload["card_payload"],
    )
    with pytest.raises(
        DeliveryRecordConflictError,
        match="delivery_card_patch_effect_conflict",
    ):
        store.enqueue_card_patch_effect(payload=drifted_target, now=NOW)


def test_store_rejects_malformed_card_patch_receipts_before_settlement(tmp_path):
    store, _adjudication, payload = _card_patch(
        tmp_path,
        settle_correction=True,
    )
    store.enqueue_card_patch_effect(payload=payload, now=NOW)
    claim = store.claim_due_effect(lease_owner="receipt-test", now=NOW)
    assert claim is not None
    store.mark_effect_write_started(claim=claim, now=NOW)
    success_receipt = {
        "remote_id": payload["message_id"],
        "source": "relay_card_patch",
        "render_hash": payload["render_hash"],
        "adjudication_id": payload["adjudication_id"],
        "conclusion_state": payload["conclusion_state"],
        "correction_effect_key": payload["correction_effect_key"],
    }

    with pytest.raises(
        DeliveryRecordConflictError,
        match="delivery_card_patch_effect_receipt_invalid",
    ):
        store.complete_effect(
            claim=claim,
            outcome="ack",
            remote_id="om_wrong",
            receipt=success_receipt,
            now=NOW,
        )
    with pytest.raises(
        DeliveryRecordConflictError,
        match="delivery_card_patch_effect_receipt_invalid",
    ):
        store.complete_effect(
            claim=claim,
            outcome="ack",
            remote_id=str(payload["message_id"]),
            receipt={**success_receipt, "unexpected": True},
            now=NOW,
        )
    with pytest.raises(
        DeliveryRecordConflictError,
        match="delivery_card_patch_effect_receipt_invalid",
    ):
        store.suppress_expired_card_patch(
            claim=claim,
            error_detail="message expired",
            receipt={"source": "card_message_expired"},
            now=NOW,
        )

    [effect] = [
        row
        for row in store.list_rows("rca_delivery_effects")
        if row["effect_kind"] == DELIVERY_CARD_PATCH_EFFECT_KIND
    ]
    assert effect["status"] == "claimed"
    assert effect["write_phase"] == "write_started"
    assert effect["remote_receipt_json"] is None


def test_card_patch_dependency_exception_opens_card_circuit(tmp_path, monkeypatch):
    store, _adjudication, payload = _card_patch(
        tmp_path,
        settle_correction=True,
    )
    store.enqueue_card_patch_effect(payload=payload, now=NOW)
    provider_claim = build_write_fence_provider_claim({"state": "issued"})

    def missing_dependency(*_args, **_kwargs):
        raise RuntimeError("Feishu dependencies not installed")

    dispatcher = DeliveryDispatcher(
        store=store,
        config=_card_dispatcher_config(tmp_path),
        list_comments=lambda *_args: pytest.fail("card patch must not list comments"),
        add_comment=lambda *_args: pytest.fail("card patch must not add comments"),
        report_verifier=lambda *_args: pytest.fail("card patch has no report read"),
        patch_task_card=missing_dependency,
        now=lambda: NOW,
        lease_owner="card-dependency-test",
    )
    monkeypatch.setattr(
        dispatcher,
        "_validate_external_write",
        lambda *_args, **_kwargs: {},
    )
    monkeypatch.setattr(
        dispatcher,
        "_provider_write_guard",
        lambda _claim: provider_claim,
    )

    outcome = dispatcher.dispatch_one()

    assert outcome.status == "circuit_open"
    assert outcome.error_code == "feishu_card_dependency_unavailable"
    circuit = store.delivery_dispatcher_circuit(DELIVERY_CARD_PATCH_EFFECT_KIND)
    assert circuit.state == "open"
    assert circuit.reason_code == "feishu_card_dependency_unavailable"


def test_stale_prewrite_card_patch_ages_out_without_provider_write(tmp_path):
    store, _adjudication, payload = _card_patch(
        tmp_path,
        settle_correction=True,
    )
    store.enqueue_card_patch_effect(payload=payload, now=NOW)
    with sqlite3.connect(store.db_path) as conn:
        conn.execute("UPDATE rca_activation_epochs SET is_current = 0")

    assert (
        store.claim_due_effect(
            lease_owner="stale-prewrite-test",
            now=NOW + timedelta(seconds=1),
            activation_required=True,
        )
        is None
    )
    [pending] = [
        row
        for row in store.list_rows("rca_delivery_effects")
        if row["effect_kind"] == DELIVERY_CARD_PATCH_EFFECT_KIND
    ]
    assert pending["status"] == "pending"
    assert pending["write_phase"] == "prewrite"

    assert (
        store.claim_due_effect(
            lease_owner="stale-prewrite-test",
            now=NOW + timedelta(seconds=86_401),
            activation_required=True,
        )
        is None
    )
    [expired] = [
        row
        for row in store.list_rows("rca_delivery_effects")
        if row["effect_kind"] == DELIVERY_CARD_PATCH_EFFECT_KIND
    ]
    assert expired["status"] == "quarantined"
    assert expired["write_phase"] == "settled"
    assert expired["last_error_code"] == "delivery_effect_age_exceeded"


def test_require_current_bootstraps_only_missing_card_patch_circuit(tmp_path):
    path = tmp_path / "control.sqlite3"
    _control(tmp_path)
    store = RcaDeliveryStore(path)
    store.open_delivery_dispatcher_circuit(
        effect_kind=DELIVERY_CARD_PATCH_EFFECT_KIND,
        reason_code="preserve-existing-state",
        now=NOW,
    )

    reopened = RcaDeliveryStore(path, require_current=True)
    assert reopened.delivery_dispatcher_circuit(
        DELIVERY_CARD_PATCH_EFFECT_KIND
    ).state == "open"

    with sqlite3.connect(path) as conn:
        conn.execute(
            "DELETE FROM rca_delivery_dispatcher_circuit WHERE circuit_name = ?",
            (DELIVERY_CARD_PATCH_EFFECT_KIND,),
        )

    current_snapshot = RcaDeliveryStore.read_existing_backpressure_snapshot(
        path,
        now=NOW,
    )
    assert current_snapshot.circuits[DELIVERY_CARD_PATCH_EFFECT_KIND].state == "open"
    with sqlite3.connect(path) as conn:
        assert conn.execute(
            "SELECT 1 FROM rca_delivery_dispatcher_circuit "
            "WHERE circuit_name = ?",
            (DELIVERY_CARD_PATCH_EFFECT_KIND,),
        ).fetchone() is None
        conn.execute(
            "UPDATE rca_delivery_meta SET value = ? WHERE key = 'schema_version'",
            (DELIVERY_STORE_W6_PREDECESSOR_VERSION,),
        )

    legacy_snapshot = RcaDeliveryStore.read_existing_backpressure_snapshot(
        path,
        now=NOW,
    )
    assert legacy_snapshot.circuits[DELIVERY_CARD_PATCH_EFFECT_KIND].state == "closed"
    with sqlite3.connect(path) as conn:
        assert conn.execute(
            "SELECT 1 FROM rca_delivery_dispatcher_circuit "
            "WHERE circuit_name = ?",
            (DELIVERY_CARD_PATCH_EFFECT_KIND,),
        ).fetchone() is None
        conn.execute(
            "UPDATE rca_delivery_meta SET value = ? WHERE key = 'schema_version'",
            (DELIVERY_STORE_SCHEMA_VERSION,),
        )

    bootstrapped = RcaDeliveryStore(path, require_current=True)
    circuit = bootstrapped.delivery_dispatcher_circuit(
        DELIVERY_CARD_PATCH_EFFECT_KIND
    )
    assert circuit.state == "closed"
    assert circuit.reason_code == ""


def test_relay_materializes_and_dispatches_exact_card_patch_offline(
    tmp_path,
    monkeypatch,
):
    store, adjudication, payload = _card_patch(
        tmp_path,
        settle_correction=False,
    )
    task_id = str(payload["submission_key"])
    path = tmp_path / "task-state.json"
    body = {
        "task_card": {
            "task_id": task_id,
            "user_state": "done",
            "status_line": "done",
            "chat_id": "oc_g1q3",
            "message_id": "om_owner_root",
            "thread_id": "topic:om_owner_root",
            "card_message_id": "om_card123",
            "last_sent_hash": "previous-render",
            "rca_conclusion_review": {
                "schema_version": RCA_CANDIDATE_REVIEW_SCHEMA_VERSION,
                "kind": RCA_CANDIDATE_REVIEW_PRESET,
                "action": adjudication.action,
                "conclusion_state": adjudication.conclusion_state,
                "adjudication_id": adjudication.adjudication_id,
                "business_key": payload["business_key"],
                "generation": payload["generation"],
                "work_item_id": payload["work_item_id"],
                "original_effect_key": adjudication.original_effect_key,
                "correction_effect_key": adjudication.correction_effect_key,
                "created": True,
                "artifact_repair_pending": False,
            },
            "delivery": {
                "conclusion": "候选结论：感知车道线责任域",
                "conclusion_state": adjudication.conclusion_state,
            },
        },
        "completion_notice": {},
    }
    _atomic_write_json(path, body)
    direct_card_calls: list[object] = []
    def write_target(_task_id):
        return {
            "chat_id": "oc_g1q3",
            "thread_target": "topic:om_owner_root",
        }

    def direct_card_sender(*args, **kwargs):
        direct_card_calls.append((args, kwargs))
        raise AssertionError("semantic correction must not use direct card sync")

    blocked = sync_task_card(
        task_id=task_id,
        path=path,
        body=body,
        send=True,
        send_card_func=direct_card_sender,
        card_patch_store_factory=lambda *_args, **_kwargs: store,
        card_patch_write_fence_loader=lambda _task_id: {
            "chat_id": "oc_wrong",
            "thread_target": "topic:om_wrong",
        },
        throttle_seconds=0,
    )
    assert blocked["reason"] == "durable_card_patch_target_binding_blocked"
    assert blocked["error_code"] == "external_write_fence_target_mismatch"
    assert direct_card_calls == []

    waiting = sync_task_card(
        task_id=task_id,
        path=path,
        body=body,
        send=True,
        send_card_func=direct_card_sender,
        card_patch_store_factory=lambda *_args, **_kwargs: store,
        card_patch_write_fence_loader=write_target,
        throttle_seconds=0,
    )
    assert waiting["reason"] == "awaiting_correction_effect"
    assert waiting["external_write_attempted"] is False
    assert direct_card_calls == []
    assert all(
        row["effect_kind"] != DELIVERY_CARD_PATCH_EFFECT_KIND
        for row in store.list_rows("rca_delivery_effects")
    )

    with sqlite3.connect(store.db_path) as conn:
        conn.execute(
            "UPDATE rca_delivery_effects "
            "SET status = 'succeeded', write_phase = 'settled', "
            "completed_at = ?, updated_at = ? WHERE effect_key = ?",
            (
                NOW.isoformat(),
                NOW.isoformat(),
                adjudication.correction_effect_key,
            ),
        )
    body = json.loads(path.read_text(encoding="utf-8"))
    enqueued = sync_task_card(
        task_id=task_id,
        path=path,
        body=body,
        send=True,
        send_card_func=direct_card_sender,
        card_patch_store_factory=lambda *_args, **_kwargs: store,
        card_patch_write_fence_loader=write_target,
        throttle_seconds=0,
    )
    assert enqueued["reason"] == "durable_card_patch_pending"
    assert enqueued["external_write_attempted"] is False
    [queued_effect] = [
        row
        for row in store.list_rows("rca_delivery_effects")
        if row["effect_kind"] == DELIVERY_CARD_PATCH_EFFECT_KIND
    ]
    queued_payload = json.loads(queued_effect["payload_json"])
    dispatch_now = datetime.fromisoformat(queued_effect["created_at"]) + timedelta(
        seconds=1
    )
    assert queued_payload["adjudication_id"] == adjudication.adjudication_id
    assert queued_payload["message_id"] == "om_card123"
    assert queued_payload["render_hash"] == stable_render_hash(
        queued_payload["card_payload"]
    )

    provider_claim = build_write_fence_provider_claim({"state": "issued"})
    calls: list[dict[str, object]] = []

    def patch_task_card(
        target,
        card_payload,
        message_id=None,
        *,
        provider_claim=None,
    ):
        calls.append(
            {
                "target": target,
                "card_payload": card_payload,
                "message_id": message_id,
                "provider_claim": provider_claim,
            }
        )
        with sqlite3.connect(store.db_path) as conn:
            conn.execute("UPDATE rca_activation_epochs SET is_current = 0")
        return {"success": True, "message_id": message_id, "updated": True}

    dispatcher = DeliveryDispatcher(
        store=store,
        config=_card_dispatcher_config(tmp_path),
        list_comments=lambda *_args: pytest.fail("card patch must not list comments"),
        add_comment=lambda *_args: pytest.fail("card patch must not add comments"),
        report_verifier=lambda *_args: pytest.fail("card patch has no report read"),
        patch_task_card=patch_task_card,
        now=lambda: dispatch_now,
        lease_owner="card-patch-dispatcher-test",
    )
    monkeypatch.setattr(
        dispatcher,
        "_validate_external_write",
        lambda *_args, **_kwargs: {},
    )
    monkeypatch.setattr(
        dispatcher,
        "_provider_write_guard",
        lambda _claim: provider_claim,
    )

    outcome = dispatcher.dispatch_one()

    assert outcome.status == "succeeded"
    assert outcome.remote_id == "om_card123"
    with sqlite3.connect(store.db_path) as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM rca_activation_epochs WHERE is_current = 1"
        ).fetchone()[0] == 0
    assert calls == [
        {
            "target": "feishu:oc_g1q3:om_owner_root",
            "card_payload": queued_payload["card_payload"],
            "message_id": "om_card123",
            "provider_claim": provider_claim,
        }
    ]
    [effect] = [
        row
        for row in store.list_rows("rca_delivery_effects")
        if row["effect_kind"] == DELIVERY_CARD_PATCH_EFFECT_KIND
    ]
    receipt = json.loads(effect["remote_receipt_json"])
    assert effect["status"] == "succeeded"
    assert effect["write_phase"] == "settled"
    assert receipt == {
        "adjudication_id": adjudication.adjudication_id,
        "conclusion_state": "invalidated",
        "correction_effect_key": adjudication.correction_effect_key,
        "remote_id": "om_card123",
        "render_hash": queued_payload["render_hash"],
        "source": "relay_card_patch",
    }

    body = json.loads(path.read_text(encoding="utf-8"))
    observed = sync_task_card(
        task_id=task_id,
        path=path,
        body=body,
        send=True,
        send_card_func=direct_card_sender,
        card_patch_store_factory=lambda *_args, **_kwargs: store,
        card_patch_write_fence_loader=write_target,
        throttle_seconds=0,
    )
    assert observed["success"] is True
    assert observed["disposition"] == "durable_effect_settled"
    assert direct_card_calls == []
    final = json.loads(path.read_text(encoding="utf-8"))["task_card"]
    assert final["last_sent_hash"] == queued_payload["render_hash"]
    assert final["last_render_hash"] == queued_payload["render_hash"]


def test_revoked_ambiguous_card_patch_is_reclaimed_only_for_quarantine(
    tmp_path,
    monkeypatch,
):
    store, adjudication, payload = _card_patch(
        tmp_path,
        settle_correction=True,
    )
    store.enqueue_card_patch_effect(payload=payload, now=NOW)
    provider_claim = build_write_fence_provider_claim({"state": "issued"})
    patch_calls = 0
    clock = {"now": NOW}

    def uncertain_patch(*_args, **_kwargs):
        nonlocal patch_calls
        patch_calls += 1
        raise TimeoutError("provider outcome unknown")

    dispatcher = DeliveryDispatcher(
        store=store,
        config=_card_dispatcher_config(tmp_path),
        list_comments=lambda *_args: pytest.fail("card patch must not list comments"),
        add_comment=lambda *_args: pytest.fail("card patch must not add comments"),
        report_verifier=lambda *_args: pytest.fail("card patch has no report read"),
        patch_task_card=uncertain_patch,
        now=lambda: clock["now"],
        lease_owner="ambiguous-card-patch-test",
    )
    monkeypatch.setattr(
        dispatcher,
        "_validate_external_write",
        lambda *_args, **_kwargs: {},
    )
    monkeypatch.setattr(
        dispatcher,
        "_provider_write_guard",
        lambda _claim: provider_claim,
    )

    first = dispatcher.dispatch_one()
    assert first.status == "uncertain"
    assert first.error_code == "feishu_card_patch_outcome_unknown"
    assert patch_calls == 1

    clock["now"] = NOW + timedelta(seconds=3)
    second = dispatcher.dispatch_one()
    assert second.status == "quarantined"
    assert second.error_code == "feishu_card_patch_outcome_unknown"
    assert patch_calls == 1
    [effect] = [
        row
        for row in store.list_rows("rca_delivery_effects")
        if row["effect_kind"] == DELIVERY_CARD_PATCH_EFFECT_KIND
    ]
    assert effect["status"] == "quarantined"
    assert effect["write_phase"] == "settled"
    with sqlite3.connect(store.db_path) as conn:
        conn.execute("UPDATE rca_activation_epochs SET is_current = 0")

    task_id = str(payload["submission_key"])
    path = tmp_path / "quarantined-task-state.json"
    body = {
        "task_card": {
            "task_id": task_id,
            "user_state": "done",
            "chat_id": "oc_g1q3",
            "message_id": "om_owner_root",
            "thread_id": "topic:om_owner_root",
            "card_message_id": "om_card123",
            "rca_conclusion_review": {
                "schema_version": RCA_CANDIDATE_REVIEW_SCHEMA_VERSION,
                "kind": RCA_CANDIDATE_REVIEW_PRESET,
                "action": adjudication.action,
                "conclusion_state": adjudication.conclusion_state,
                "adjudication_id": adjudication.adjudication_id,
                "business_key": payload["business_key"],
                "generation": payload["generation"],
                "work_item_id": payload["work_item_id"],
                "original_effect_key": adjudication.original_effect_key,
                "correction_effect_key": adjudication.correction_effect_key,
            },
        },
        "completion_notice": {},
    }
    _atomic_write_json(path, body)

    def direct_card_sender(*_args, **_kwargs):
        raise AssertionError("quarantined durable card effect must not direct-retry")

    observed = sync_task_card(
        task_id=task_id,
        path=path,
        body=body,
        send=True,
        send_card_func=direct_card_sender,
        card_patch_store_factory=lambda *_args, **_kwargs: store,
        card_patch_write_fence_loader=lambda _task_id: {
            "chat_id": "oc_g1q3",
            "thread_target": "topic:om_owner_root",
        },
        throttle_seconds=0,
    )
    assert observed["reason"] == "durable_card_patch_terminal_failure"
    assert observed["effect_status"] == "quarantined"
    assert observed["external_write_attempted"] is False


def test_card_patch_success_records_observation_when_settlement_conflicts(
    tmp_path,
    monkeypatch,
):
    store, _adjudication, payload = _card_patch(
        tmp_path,
        settle_correction=True,
    )
    store.enqueue_card_patch_effect(payload=payload, now=NOW)
    provider_claim = build_write_fence_provider_claim({"state": "issued"})
    patch_calls = 0

    def successful_patch(_target, _card, message_id=None, **_kwargs):
        nonlocal patch_calls
        patch_calls += 1
        return {"success": True, "message_id": message_id, "updated": True}

    dispatcher = DeliveryDispatcher(
        store=store,
        config=_card_dispatcher_config(tmp_path),
        list_comments=lambda *_args: pytest.fail("card patch must not list comments"),
        add_comment=lambda *_args: pytest.fail("card patch must not add comments"),
        report_verifier=lambda *_args: pytest.fail("card patch has no report read"),
        patch_task_card=successful_patch,
        now=lambda: NOW,
        lease_owner="card-patch-settlement-conflict-test",
    )
    monkeypatch.setattr(
        dispatcher,
        "_validate_external_write",
        lambda *_args, **_kwargs: {},
    )
    monkeypatch.setattr(
        dispatcher,
        "_provider_write_guard",
        lambda _claim: provider_claim,
    )

    def settlement_conflict(**_kwargs):
        raise DeliveryRecordConflictError("simulated_card_settlement_conflict")

    monkeypatch.setattr(store, "complete_effect", settlement_conflict)

    outcome = dispatcher.dispatch_one()

    assert outcome.status == "activation_stale"
    assert outcome.error_code == "simulated_card_settlement_conflict"
    assert patch_calls == 1
    [intent] = store.list_delivery_observations()
    assert intent.effect_key == payload["effect_key"]
    assert intent.status == "appended"
    receipt = tmp_path / "card-delivery-observations.jsonl"
    assert len(receipt.read_text(encoding="utf-8").splitlines()) == 1


def test_expired_card_patch_is_durably_suppressed_without_direct_retry(
    tmp_path,
    monkeypatch,
):
    store, adjudication, payload = _card_patch(
        tmp_path,
        settle_correction=True,
    )
    store.enqueue_card_patch_effect(payload=payload, now=NOW)
    provider_claim = build_write_fence_provider_claim({"state": "issued"})
    boundary = FeishuHotSender._card_failure_result(
        {"error": "230031: message has expired and cannot be updated"}
    )
    assert boundary == {
        "success": False,
        "outcome_uncertain": False,
        "permanent": True,
        "error_code": "feishu_card_patch_message_expired",
        "error": "230031: message has expired and cannot be updated",
    }
    patch_calls = 0

    def expired_patch(*_args, **_kwargs):
        nonlocal patch_calls
        patch_calls += 1
        return boundary

    dispatcher = DeliveryDispatcher(
        store=store,
        config=_card_dispatcher_config(tmp_path),
        list_comments=lambda *_args: pytest.fail("card patch must not list comments"),
        add_comment=lambda *_args: pytest.fail("card patch must not add comments"),
        report_verifier=lambda *_args: pytest.fail("card patch has no report read"),
        patch_task_card=expired_patch,
        now=lambda: NOW,
        lease_owner="expired-card-patch-test",
    )
    monkeypatch.setattr(
        dispatcher,
        "_validate_external_write",
        lambda *_args, **_kwargs: {},
    )
    monkeypatch.setattr(
        dispatcher,
        "_provider_write_guard",
        lambda _claim: provider_claim,
    )

    outcome = dispatcher.dispatch_one()
    assert outcome.status == "suppressed"
    assert outcome.error_code == "feishu_card_patch_message_expired"
    assert patch_calls == 1
    state = store.card_patch_effect_state(
        delivery_id=payload["delivery_id"],
        target_key=payload["target_key"],
        adjudication_id=payload["adjudication_id"],
    )
    assert state is not None and state["status"] == "suppressed"

    task_id = str(payload["submission_key"])
    path = tmp_path / "expired-task-state.json"
    body = {
        "task_card": {
            "task_id": task_id,
            "user_state": "done",
            "chat_id": "oc_g1q3",
            "message_id": "om_owner_root",
            "thread_id": "topic:om_owner_root",
            "card_message_id": "om_card123",
            "rca_conclusion_review": {
                "schema_version": RCA_CANDIDATE_REVIEW_SCHEMA_VERSION,
                "kind": RCA_CANDIDATE_REVIEW_PRESET,
                "action": adjudication.action,
                "conclusion_state": adjudication.conclusion_state,
                "adjudication_id": adjudication.adjudication_id,
                "business_key": payload["business_key"],
                "generation": payload["generation"],
                "work_item_id": payload["work_item_id"],
                "original_effect_key": adjudication.original_effect_key,
                "correction_effect_key": adjudication.correction_effect_key,
            },
        },
        "completion_notice": {},
    }
    _atomic_write_json(path, body)

    def direct_card_sender(*_args, **_kwargs):
        raise AssertionError("expired durable card effect must not direct-retry")

    observed = sync_task_card(
        task_id=task_id,
        path=path,
        body=body,
        send=True,
        send_card_func=direct_card_sender,
        card_patch_store_factory=lambda *_args, **_kwargs: store,
        card_patch_write_fence_loader=lambda _task_id: {
            "chat_id": "oc_g1q3",
            "thread_target": "topic:om_owner_root",
        },
        throttle_seconds=0,
    )
    assert observed["reason"] == "card_message_expired"
    assert observed["disposition"] == "suppressed_terminal"
    card = json.loads(path.read_text(encoding="utf-8"))["task_card"]
    assert card["card_message_expired_at"]
    assert card["last_sent_hash"] == card["last_render_hash"]
