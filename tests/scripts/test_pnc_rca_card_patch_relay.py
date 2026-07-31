from __future__ import annotations

from datetime import datetime, timedelta
import json
from types import SimpleNamespace

import pytest

from gateway.feishu_task_confirm import (
    RCA_CANDIDATE_REVIEW_PRESET,
    RCA_CANDIDATE_REVIEW_SCHEMA_VERSION,
)
from gateway.pnc_rca_provider_fence import build_write_fence_provider_claim
from scripts import pnc_completion_notice_relay as relay
from scripts.pnc_rca_delivery_dispatcher import DeliveryDispatcher
from tests.gateway.test_pnc_rca_card_patch_delivery import _card_patch


def test_semantic_review_materializes_and_settles_one_durable_card_patch(
    tmp_path,
    monkeypatch,
):
    store, adjudication, _unused_payload = _card_patch(
        tmp_path,
        settle_correction=True,
    )
    [job] = store.list_rows("rca_delivery_jobs")
    task_id = str(job["submission_key"])
    task_card = {
        "task_id": task_id,
        "chat_id": "oc_g1q3",
        "thread_id": "topic:om_owner_root",
        "card_message_id": "om_card123",
        "user_state": "completed",
        "status_line": "RCA conclusion invalidated",
        "delivery": {
            "work_item_id": str(job["work_item_id"]),
            "conclusion": "The earlier conclusion is invalidated.",
        },
        "rca_conclusion_review": {
            "schema_version": RCA_CANDIDATE_REVIEW_SCHEMA_VERSION,
            "kind": RCA_CANDIDATE_REVIEW_PRESET,
            "adjudication_id": adjudication.adjudication_id,
            "action": adjudication.action,
            "conclusion_state": adjudication.conclusion_state,
            "business_key": str(job["business_key"]),
            "generation": int(job["generation"]),
            "work_item_id": str(job["work_item_id"]),
            "original_effect_key": adjudication.original_effect_key,
            "correction_effect_key": adjudication.correction_effect_key,
        },
    }
    path = tmp_path / "task-state" / f"{task_id}.json"
    path.parent.mkdir()
    body = {"task_card": task_card}
    path.write_text(json.dumps(body), encoding="utf-8")
    direct_card_calls = []

    pending = relay.sync_task_card(
        task_id=task_id,
        path=path,
        body=body,
        send=True,
        send_card_func=lambda *_args, **_kwargs: direct_card_calls.append(True),
        card_patch_store_factory=lambda *_args, **_kwargs: store,
        card_patch_write_fence_loader=lambda _task_id: {
            "chat_id": "oc_g1q3",
            "thread_target": "topic:om_owner_root",
        },
        throttle_seconds=0,
    )

    assert pending is not None
    assert pending["reason"] == "durable_card_patch_pending"
    assert pending["external_write_attempted"] is False
    [effect] = [
        row
        for row in store.list_rows("rca_delivery_effects")
        if row["effect_kind"] == "feishu_card_patch"
    ]
    assert effect["status"] == "pending"
    assert direct_card_calls == []
    dispatch_now = datetime.fromisoformat(effect["created_at"]) + timedelta(seconds=1)

    provider_claim = build_write_fence_provider_claim({"state": "issued"})

    def patch_task_card(
        target,
        card_payload,
        message_id=None,
        *,
        provider_claim=None,
    ):
        assert target == "feishu:oc_g1q3:om_owner_root"
        assert card_payload
        assert provider_claim is not None
        return {"success": True, "message_id": message_id, "updated": True}

    dispatcher = DeliveryDispatcher(
        store=store,
        config=SimpleNamespace(
            enabled=True,
            lease_seconds=90,
            activation_required=True,
            observability_enabled=True,
            observability_path=tmp_path / "card-relay-observations.jsonl",
            inventory_pin="c" * 64,
            observation_release_id="card-relay-release-test",
            quarantine_release_id="",
        ),
        list_comments=lambda *_args: pytest.fail("card patch must not list comments"),
        add_comment=lambda *_args: pytest.fail("card patch must not add comments"),
        report_verifier=lambda *_args: pytest.fail("card patch has no report read"),
        patch_task_card=patch_task_card,
        now=lambda: dispatch_now,
        lease_owner="card-patch-relay-test",
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
    latest_body = json.loads(path.read_text(encoding="utf-8"))
    settled = relay.sync_task_card(
        task_id=task_id,
        path=path,
        body=latest_body,
        send=True,
        send_card_func=lambda *_args, **_kwargs: direct_card_calls.append(True),
        card_patch_store_factory=lambda *_args, **_kwargs: store,
        card_patch_write_fence_loader=lambda _task_id: {
            "chat_id": "oc_g1q3",
            "thread_target": "topic:om_owner_root",
        },
        throttle_seconds=0,
    )

    assert settled is not None
    assert settled["success"] is True
    assert settled["disposition"] == "durable_effect_settled"
    persisted = json.loads(path.read_text(encoding="utf-8"))["task_card"]
    assert persisted["last_sent_hash"] == settled["render_hash"]
    assert direct_card_calls == []
