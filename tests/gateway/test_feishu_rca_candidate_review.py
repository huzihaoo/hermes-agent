"""Focused B10 tests for the semantic RCA candidate review card."""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from gateway.config import PlatformConfig
from gateway.feishu_task_card import render_task_card
from gateway.feishu_task_confirm import (
    RCA_CANDIDATE_REVIEW_CARD_RESPONSE_MODE,
    add_rca_candidate_conclusion_confirm,
    rca_candidate_confirm_id,
    resolve_task_confirm,
    resolve_task_confirm_by_text,
)
from gateway.platforms.feishu import FeishuAdapter
from gateway.pnc_group_binding import G1Q3_RCA_GROUP_ID
from gateway.pnc_rca_owner_review import resolve_candidate_conclusion_review
from hermes_constants import reset_hermes_home_override, set_hermes_home_override
from scripts import pnc_completion_notice_relay as relay


def _write_card(home, task_id: str, *, issue_id: str = "7054691974"):
    path = home / "task-state" / f"{task_id}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "task_card": {
                    "task_id": task_id,
                    "chat_id": G1Q3_RCA_GROUP_ID,
                    "thread_id": "topic:om_candidate",
                    "user_state": "awaiting_user",
                    "pending_confirms": [],
                    "delivery": {
                        "work_item_id": issue_id,
                        "conclusion": "候选结论：感知车道线责任域",
                    },
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return path


def _candidate(issue_id: str = "7054691974"):
    return SimpleNamespace(
        business_key="g1q3-business-1",
        generation=7,
        work_item_id=issue_id,
        original_effect_key="g1q3-original-effect-1",
        conclusion="候选结论：感知车道线责任域",
        responsibility_domain="PERCEPTION_LANE",
        completed_at="2026-07-29T00:00:00+00:00",
    )


class _Response:
    def __init__(self):
        self.card = None


class _Card:
    def __init__(self):
        self.type = None
        self.data = None


def test_candidate_confirm_identity_is_stable_and_card_does_not_use_raw_text(tmp_path):
    token = set_hermes_home_override(tmp_path)
    try:
        path = _write_card(tmp_path, "g1q3-rca-review-1")
        candidate = _candidate()
        first = add_rca_candidate_conclusion_confirm(
            task_id="g1q3-rca-review-1", candidate=candidate
        )
        second = add_rca_candidate_conclusion_confirm(
            task_id="g1q3-rca-review-1", candidate=candidate
        )
        body = json.loads(path.read_text(encoding="utf-8"))
    finally:
        reset_hermes_home_override(token)

    expected = rca_candidate_confirm_id(
        business_key=candidate.business_key,
        generation=candidate.generation,
        work_item_id=candidate.work_item_id,
        original_effect_key=candidate.original_effect_key,
    )
    assert first["confirm_id"] == expected
    assert second["duplicate"] is True
    item = body["task_card"]["pending_confirms"][0]
    assert item["preset"] == "rca_candidate_conclusion_review"
    assert item["semantic"]["original_effect_key"] == candidate.original_effect_key
    assert item["semantic"]["candidate_conclusion"] == candidate.conclusion


def test_real_store_button_resolution_is_immutable_and_idempotent(tmp_path, monkeypatch):
    # Reuse the existing canonical fixture, which creates a settled medium
    # publication and the current delivery-store schema.
    from tests.gateway.test_pnc_rca_conclusion_adjudication import (
        _seed_published_conclusion,
    )

    control_dir = tmp_path / "runtime" / "pnc_agent" / "feishu_issue_kafka_rca"
    control_dir.mkdir(parents=True)
    store = _seed_published_conclusion(control_dir)
    [candidate] = store.list_conclusion_review_queue()
    monkeypatch.setenv("HERMES_G1Q3_REVIEW_OWNER_USER_IDS", "ou_owner")
    token = set_hermes_home_override(tmp_path)
    try:
        task_id = "g1q3-rca-review-real"
        path = _write_card(tmp_path, task_id, issue_id=candidate.work_item_id)
        add_rca_candidate_conclusion_confirm(task_id=task_id, candidate=candidate)
        first = resolve_task_confirm(
            task_id=task_id,
            confirm_id=rca_candidate_confirm_id(
                business_key=candidate.business_key,
                generation=candidate.generation,
                work_item_id=candidate.work_item_id,
                original_effect_key=candidate.original_effect_key,
            ),
            choice="追认",
            actor_id="ou_owner",
            actor_name="RCA Owner",
            source="button",
            event_id="om_button",
        )
        duplicate = resolve_task_confirm(
            task_id=task_id,
            confirm_id=first["confirm_id"],
            choice="更正",
            actor_id="ou_owner",
            actor_name="RCA Owner",
            source="button",
            event_id="om_button-retry",
        )
        body = json.loads(path.read_text(encoding="utf-8"))
    finally:
        reset_hermes_home_override(token)

    assert first["changed"] is True
    assert (
        first["card_response_mode"]
        == RCA_CANDIDATE_REVIEW_CARD_RESPONSE_MODE
    )
    assert first["semantic_result"]["conclusion_state"] == "recognized"
    assert duplicate["duplicate"] is True
    assert (
        duplicate["card_response_mode"]
        == RCA_CANDIDATE_REVIEW_CARD_RESPONSE_MODE
    )
    assert len(store.list_rows("rca_conclusion_adjudications")) == 1
    assert len(store.list_rows("rca_delivery_effects")) == 2
    assert body["task_card"]["rca_conclusion_review"]["conclusion_state"] == "recognized"
    assert not any(
        action.get("tag") == "button"
        for element in render_task_card(body["task_card"])["elements"]
        for action in element.get("actions", [])
    )


def test_semantic_button_ack_never_replaces_card_before_durable_patch(
    tmp_path, monkeypatch
):
    from tests.gateway.test_pnc_rca_conclusion_adjudication import (
        _seed_published_conclusion,
    )

    control_dir = tmp_path / "runtime" / "pnc_agent" / "feishu_issue_kafka_rca"
    control_dir.mkdir(parents=True)
    store = _seed_published_conclusion(control_dir)
    [candidate] = store.list_conclusion_review_queue()
    monkeypatch.setenv("HERMES_G1Q3_REVIEW_OWNER_USER_IDS", "ou_owner")
    monkeypatch.setattr("gateway.platforms.feishu.P2CardActionTriggerResponse", _Response)
    monkeypatch.setattr("gateway.platforms.feishu.CallBackCard", _Card)
    token = set_hermes_home_override(tmp_path)
    try:
        task_id = "g1q3-rca-review-adapter"
        _write_card(tmp_path, task_id, issue_id=candidate.work_item_id)
        added = add_rca_candidate_conclusion_confirm(
            task_id=task_id,
            candidate=candidate,
        )
        adapter = FeishuAdapter(PlatformConfig())
        event = SimpleNamespace(
            operator=SimpleNamespace(open_id="ou_owner", name="RCA Owner"),
            token="om_button",
        )
        action = {
            "task_id": task_id,
            "confirm_id": added["confirm_id"],
            "choice": "追认",
        }

        invalid = adapter._handle_task_confirm_card_action(
            event=event,
            action_value={**action, "choice": "确认"},
        )
        assert store.list_rows("rca_conclusion_adjudications") == []
        first = adapter._handle_task_confirm_card_action(
            event=event,
            action_value=action,
        )
        duplicate = adapter._handle_task_confirm_card_action(
            event=event,
            action_value={**action, "choice": "更正"},
        )
    finally:
        reset_hermes_home_override(token)

    assert isinstance(invalid, _Response)
    assert invalid.card is None
    assert isinstance(first, _Response)
    assert first.card is None
    assert isinstance(duplicate, _Response)
    assert duplicate.card is None
    assert len(store.list_rows("rca_conclusion_adjudications")) == 1


def test_real_store_text_resolution_reuses_same_semantic_path(tmp_path, monkeypatch):
    from tests.gateway.test_pnc_rca_conclusion_adjudication import (
        _seed_published_conclusion,
    )

    control_dir = tmp_path / "runtime" / "pnc_agent" / "feishu_issue_kafka_rca"
    control_dir.mkdir(parents=True)
    store = _seed_published_conclusion(control_dir)
    [candidate] = store.list_conclusion_review_queue()
    monkeypatch.setenv("HERMES_G1Q3_REVIEW_OWNER_USER_IDS", "ou_owner")
    token = set_hermes_home_override(tmp_path)
    try:
        task_id = "g1q3-rca-review-text"
        path = _write_card(tmp_path, task_id, issue_id=candidate.work_item_id)
        add_rca_candidate_conclusion_confirm(task_id=task_id, candidate=candidate)
        result = resolve_task_confirm_by_text(
            chat_id=G1Q3_RCA_GROUP_ID,
            thread_id="topic:om_candidate",
            text="撤回",
            actor_id="ou_owner",
            actor_name="RCA Owner",
            event_id="om_text",
        )
        body = json.loads(path.read_text(encoding="utf-8"))
    finally:
        reset_hermes_home_override(token)

    assert result["changed"] is True
    assert result["semantic_result"]["action"] == "retract"
    assert result["semantic_result"]["conclusion_state"] == "invalidated"
    assert body["task_card"]["delivery"]["conclusion_state"] == "invalidated"
    assert "已作废" in body["task_card"]["delivery"]["conclusion"]
    assert len(store.list_rows("rca_conclusion_adjudications")) == 1


def test_stale_candidate_binding_rolls_back_before_any_adjudication(tmp_path, monkeypatch):
    from tests.gateway.test_pnc_rca_conclusion_adjudication import (
        _seed_published_conclusion,
    )

    control_dir = tmp_path / "runtime" / "pnc_agent" / "feishu_issue_kafka_rca"
    control_dir.mkdir(parents=True)
    store = _seed_published_conclusion(control_dir)
    [candidate] = store.list_conclusion_review_queue()
    monkeypatch.setenv("HERMES_G1Q3_REVIEW_OWNER_USER_IDS", "ou_owner")
    event = SimpleNamespace(
        source=SimpleNamespace(
            platform="feishu",
            chat_id=G1Q3_RCA_GROUP_ID,
            thread_id="topic:om_candidate",
        ),
        message_id="om_stale",
    )

    with pytest.raises(ValueError, match="binding changed"):
        resolve_candidate_conclusion_review(
            event=event,
            hermes_home=tmp_path,
            issue_ids=(candidate.work_item_id,),
            action="retract",
            reason="owner_retracted_medium_confidence_candidate",
            owner_id="ou_owner",
            owner_name="RCA Owner",
            candidate_bindings={
                candidate.work_item_id: {
                    "business_key": candidate.business_key,
                    "generation": candidate.generation + 1,
                    "original_effect_key": candidate.original_effect_key,
                }
            },
        )

    assert store.list_rows("rca_conclusion_adjudications") == []
    assert len(store.list_rows("rca_delivery_effects")) == 1


def test_relay_producer_requires_fenced_db_queue_and_ignores_raw_card(tmp_path, monkeypatch):
    token = set_hermes_home_override(tmp_path)
    task_id = "g1q3-rca-issue-intake-7054691974"
    path = _write_card(tmp_path, task_id)
    body = json.loads(path.read_text(encoding="utf-8"))
    body["task_card"]["delivery"]["candidate_cause"] = "raw card text must not qualify"
    monkeypatch.setattr(
        relay,
        "_load_shared_state_meta",
        lambda _task_id: {"work_item_id": "7054691974"},
    )
    monkeypatch.setattr(
        relay,
        "_load_task_write_fence",
        lambda _task_id: {
            "write_fence": {
                "submission_key": task_id,
                "business_key": "g1q3-business-1",
                "generation": 7,
            }
        },
    )

    class Store:
        def __init__(self, _path, *, require_current):
            assert require_current is True

        def list_conclusion_review_queue(self, *, limit):
            assert limit == 100
            return (_candidate(),)

    try:
        updated, result = relay.ensure_rca_candidate_review_confirm(
            task_id=task_id,
            path=path,
            body=body,
            store_factory=Store,
        )
        item = updated["task_card"]["pending_confirms"][0]
        assert result["added"] is True
        assert item["semantic"]["candidate_conclusion"] == _candidate().conclusion
        assert item["semantic"]["candidate_conclusion"] != body["task_card"]["delivery"]["candidate_cause"]

        # No queue row means no pending confirm, even when the card claims a
        # candidate conclusion in plain text.
        body2 = json.loads(path.read_text(encoding="utf-8"))
        body2["task_card"]["pending_confirms"] = []

        class EmptyStore(Store):
            def list_conclusion_review_queue(self, *, limit):
                return ()

        unchanged, skipped = relay.ensure_rca_candidate_review_confirm(
            task_id=task_id,
            path=path,
            body=body2,
            store_factory=EmptyStore,
        )
        assert unchanged["task_card"]["pending_confirms"] == []
        assert skipped["skipped"] == "no_db_proven_medium_candidate"
    finally:
        reset_hermes_home_override(token)
