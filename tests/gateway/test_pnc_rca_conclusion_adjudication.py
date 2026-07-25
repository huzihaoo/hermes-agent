from __future__ import annotations

from datetime import datetime, timezone
import json
import sqlite3
from types import SimpleNamespace

import pytest

from gateway import pnc_rca_owner_review as owner_review_module
from gateway.pnc_rca_conclusion_adjudication import (
    ADJUDICATION_EFFECT_SCHEMA_VERSION,
    ConclusionAdjudicationError,
)
from gateway.config import Platform
from gateway.platforms.base import MessageEvent, MessageType
from gateway.pnc_group_binding import G1Q3_RCA_GROUP_ID
from gateway.pnc_rca_delivery_contract import RCA_RESULT_FIELD_KEY
from gateway.pnc_rca_delivery_store import RcaDeliveryStore
from gateway.pnc_rca_owner_review import handle_owner_review_message
from gateway.session import SessionSource
from scripts.pnc_rca_delivery_dispatcher import DeliveryDispatcher
from scripts.pnc_rca_conclusion_adjudication_audit import (
    audit_conclusion_adjudications,
)


NOW = datetime(2026, 7, 25, 11, 30, tzinfo=timezone.utc)
ISSUE_ID = "7054691974"
ORIGINAL_EFFECT_KEY = "g1q3-rca-effect-v1-" + "1" * 64


def _seed_published_conclusion(tmp_path) -> RcaDeliveryStore:
    store = RcaDeliveryStore(tmp_path / "control.sqlite3")
    current = NOW.isoformat()
    delivery_id = "g1q3-rca-delivery-v1-" + "2" * 64
    submission_key = "g1q3-rca-s1-" + "3" * 64
    business_key = "g1q3-rca-b1-" + "4" * 64
    artifact_set_id = "g1q3-rca-artifact-v1-" + "5" * 64
    target_key = "g1q3-rca-target-v1-" + "6" * 64
    contract = {
        "oracle": {"evaluator_ids": ["ct_evaluator_217"]},
        "responsibility": {"responsibility_domain": "longitudinal_control"},
    }
    manifest = {"submission_key": submission_key}
    payload = {
        "schema_version": "pnc_rca_delivery_effect_v2",
        "conclusion": "候选结论：纵向控制责任域",
    }
    with sqlite3.connect(store.db_path) as conn:
        conn.execute("PRAGMA foreign_keys=OFF")
        conn.execute(
            """
            INSERT INTO rca_delivery_jobs(
                delivery_id, submission_key, business_key, generation,
                artifact_set_id, project_key, work_item_type_key, work_item_id,
                target_key, issue_url, report_url, outcome, status,
                manifest_json, contract_json, artifacts_json, created_at, updated_at
            ) VALUES (?, ?, ?, 7, ?, 't03o4q', 'issue', ?, ?, ?, ?, 'success',
                      'delivered', ?, ?, '[]', ?, ?)
            """,
            (
                delivery_id,
                submission_key,
                business_key,
                artifact_set_id,
                ISSUE_ID,
                target_key,
                f"https://project.feishu.cn/g1q3/issue/detail/{ISSUE_ID}",
                "https://reports.example/G1Q3_RCA/cases/s/a/index.html",
                json.dumps(manifest),
                json.dumps(contract),
                current,
                current,
            ),
        )
        conn.execute(
            """
            INSERT INTO rca_delivery_effects(
                effect_key, delivery_id, effect_kind, required, target_key,
                payload_json, payload_sha256, outcome, write_phase, status,
                completed_at, created_at, updated_at
            ) VALUES (?, ?, 'feishu_issue_comment', 1, ?, ?, ?, 'success',
                      'settled', 'succeeded', ?, ?, ?)
            """,
            (
                ORIGINAL_EFFECT_KEY,
                delivery_id,
                target_key,
                json.dumps(payload),
                "7" * 64,
                current,
                current,
                current,
            ),
        )
    return store


def _record_retraction(store: RcaDeliveryStore, *, reason: str = "证据归属有误"):
    return store.record_conclusion_adjudication(
        work_item_id=ISSUE_ID,
        action="retract",
        reason=reason,
        actor_id="ou_owner",
        actor_name="RCA Owner",
        source={
            "platform": "feishu",
            "chat_id": "oc_g1q3",
            "thread_id": "omt_topic",
            "message_id": "om_command",
        },
        now=NOW,
    )


def _dispatcher(store: RcaDeliveryStore, **boundaries) -> DeliveryDispatcher:
    config = SimpleNamespace(
        enabled=True,
        activation_required=False,
        lease_seconds=120,
        reconciliation_visibility_grace_seconds=30,
        reconciliation_min_missing_reads=2,
        recovery_write_interval_seconds=30,
    )
    return DeliveryDispatcher(
        store=store,
        config=config,
        list_comments=boundaries["list_comments"],
        add_comment=boundaries["add_comment"],
        get_fields=boundaries["get_fields"],
        update_fields=boundaries["update_fields"],
        report_verifier=lambda *_args: (_ for _ in ()).throw(
            AssertionError("adjudication must not verify report artifacts")
        ),
        now=lambda: NOW,
        lease_owner="w16-test",
    )


def test_retract_is_atomic_idempotent_budgeted_and_has_impact_lineage(tmp_path):
    store = _seed_published_conclusion(tmp_path)

    first = _record_retraction(store)
    second = _record_retraction(store)

    assert first.created is True
    assert second.created is False
    assert second.correction_effect_key == first.correction_effect_key
    assert first.conclusion_state == "invalidated"
    assert first.impact_lineage["evaluator_refs"] == ["ct_evaluator_217"]
    assert first.impact_lineage["responsibility_domain"] == "longitudinal_control"
    assert first.impact_lineage["impact_window"] == {
        "start": NOW.isoformat(),
        "end": NOW.isoformat(),
    }
    [adjudication] = store.list_rows("rca_conclusion_adjudications")
    assert adjudication["action"] == "retract"
    assert adjudication["original_effect_key"] == ORIGINAL_EFFECT_KEY
    effects = store.list_rows("rca_delivery_effects")
    assert len(effects) == 2
    correction = next(
        row for row in effects if row["effect_key"] == first.correction_effect_key
    )
    payload = json.loads(correction["payload_json"])
    assert payload["schema_version"] == ADJUDICATION_EFFECT_SCHEMA_VERSION
    assert payload["conclusion_state"] == "invalidated"
    assert correction["status"] == "pending"
    [job] = store.list_rows("rca_delivery_jobs")
    assert job["status"] == "ready"

    with pytest.raises(
        ConclusionAdjudicationError,
        match="conclusion_adjudication_comment_budget_exhausted",
    ):
        _record_retraction(store, reason="第二条更正不得进入队列")
    assert len(store.list_rows("rca_delivery_effects")) == 2


def test_invalid_retraction_fails_before_queue_mutation(tmp_path):
    store = _seed_published_conclusion(tmp_path)

    with pytest.raises(
        ConclusionAdjudicationError,
        match="conclusion_adjudication_reason_invalid",
    ):
        _record_retraction(store, reason="bad\x00reason")

    assert store.list_rows("rca_conclusion_adjudications") == []
    [effect] = store.list_rows("rca_delivery_effects")
    assert effect["effect_key"] == ORIGINAL_EFFECT_KEY
    [job] = store.list_rows("rca_delivery_jobs")
    assert job["status"] == "delivered"


def test_current_delivery_store_fails_closed_without_adjudication_schema(
    tmp_path,
):
    store = _seed_published_conclusion(tmp_path)
    with sqlite3.connect(store.db_path) as conn:
        conn.execute("DROP TABLE rca_conclusion_adjudications")

    with pytest.raises(
        RuntimeError, match="rca_conclusion_adjudication_schema_not_current"
    ):
        RcaDeliveryStore(store.db_path, require_current=True)


def test_tampered_correction_is_quarantined_before_any_external_effect(tmp_path):
    store = _seed_published_conclusion(tmp_path)
    result = _record_retraction(store)
    with sqlite3.connect(store.db_path) as conn:
        row = conn.execute(
            "SELECT payload_json FROM rca_delivery_effects WHERE effect_key = ?",
            (result.correction_effect_key,),
        ).fetchone()
        payload = json.loads(row[0])
        payload["reason"] = "注入后未重新签名"
        conn.execute(
            "UPDATE rca_delivery_effects SET payload_json = ? WHERE effect_key = ?",
            (json.dumps(payload), result.correction_effect_key),
        )
    external_calls: list[str] = []

    def forbidden(name):
        def call(*_args):
            external_calls.append(name)
            raise AssertionError(f"external boundary {name} must not be reached")

        return call

    dispatcher = _dispatcher(
        store,
        list_comments=forbidden("list_comments"),
        add_comment=forbidden("add_comment"),
        get_fields=forbidden("get_fields"),
        update_fields=forbidden("update_fields"),
    )

    outcome = dispatcher.dispatch_one()

    assert outcome.status == "quarantined"
    assert outcome.error_code == "conclusion_adjudication_effect_hash_invalid"
    assert external_calls == []


def test_candidate_retract_correction_dispatches_one_budgeted_comment(tmp_path):
    store = _seed_published_conclusion(tmp_path)
    result = _record_retraction(store)
    comments = [
        {
            "remote_id": "original-comment",
            "content": "[RCA_DELIVERY:original] candidate conclusion",
        }
    ]
    fields = {RCA_RESULT_FIELD_KEY: "候选结论：纵向控制责任域"}

    def list_comments(_project_key, _work_item_id):
        return {"success": True, "comments": list(comments), "pages_read": 1}

    def add_comment(_project_key, _work_item_id, content):
        comments.append({"remote_id": "correction-comment", "content": content})
        return {"success": True, "remote_id": "correction-comment"}

    def get_fields(_project_key, _work_item_id, field_keys):
        return {"success": True, "fields": {key: fields.get(key, "") for key in field_keys}}

    def update_fields(_project_key, _work_item_id, updates):
        fields.update(dict(updates))
        return {"success": True}

    dispatcher = _dispatcher(
        store,
        list_comments=list_comments,
        add_comment=add_comment,
        get_fields=get_fields,
        update_fields=update_fields,
    )

    outcome = dispatcher.dispatch_one()

    assert outcome.status == "succeeded"
    assert outcome.effect_key == result.correction_effect_key
    assert len(comments) == 2
    assert comments[-1]["content"].count("【RCA 更正】") == 1
    assert "不可作为定责依据" in comments[-1]["content"]
    assert fields[RCA_RESULT_FIELD_KEY].startswith("原自动 RCA 结论已撤回")
    [job] = store.list_rows("rca_delivery_jobs")
    assert job["status"] == "delivered"


def test_owner_review_retract_command_uses_shared_adjudication_entrypoint(
    tmp_path, monkeypatch
):
    db_parent = (
        tmp_path / "runtime" / "pnc_agent" / "feishu_issue_kafka_rca"
    )
    db_parent.mkdir(parents=True)
    store = _seed_published_conclusion(db_parent)
    monkeypatch.setenv("HERMES_G1Q3_REVIEW_OWNER_USER_IDS", "ou_owner")
    event = MessageEvent(
        text=f"rca 撤回 {ISSUE_ID} evaluator 归属有误",
        message_type=MessageType.TEXT,
        source=SessionSource(
            platform=Platform.FEISHU,
            user_id="ou_owner",
            user_name="RCA Owner",
            chat_id=G1Q3_RCA_GROUP_ID,
            chat_name="G1Q3 RCA",
            chat_type="group",
        ),
        message_id="om_retract",
    )

    response = handle_owner_review_message(event, hermes_home=tmp_path)

    assert response.handled is True
    assert f"issue {ISSUE_ID} / 撤回" in str(response.response)
    [adjudication] = store.list_rows("rca_conclusion_adjudications")
    assert adjudication["actor_id"] == "ou_owner"
    assert adjudication["conclusion_state"] == "invalidated"
    ledger_path = (
        tmp_path
        / "pnc_agent"
        / "reviews"
        / "g1q3_rca"
        / "ledger.json"
    )
    ledger = json.loads(ledger_path.read_text(encoding="utf-8"))
    current = ledger["issues"][ISSUE_ID]["current"]
    assert current["verdict"] == "retracted"
    assert current["correction_effect_key"] == adjudication["correction_effect_key"]


def test_read_only_audit_recomputes_budget_and_lineage_counts(tmp_path):
    store = _seed_published_conclusion(tmp_path)
    _record_retraction(store)
    with sqlite3.connect(store.db_path) as conn:
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")

    audit = audit_conclusion_adjudications(store.db_path)

    assert audit["external_writes"] is False
    assert audit["sqlite_mode"] == "ro+immutable"
    assert audit["adjudication_schema"]["ready"] is True
    assert audit["counts"]["published_conclusions"] == 1
    assert audit["counts"]["adjudications"] == 1
    assert audit["counts"]["invalidated"] == 1
    assert audit["counts"]["adjudication_effects"] == 1
    assert audit["counts"]["comment_budget_violations"] == 0
    assert audit["invariants"] == {
        "ledger_effect_count_equal": True,
        "comment_budget_clean": True,
        "correction_effects_linked": True,
    }
    assert audit["ga_acceptance_claimed"] is False


def test_owner_command_failure_after_enqueue_is_consumed_not_fallen_through(
    tmp_path, monkeypatch
):
    db_parent = (
        tmp_path / "runtime" / "pnc_agent" / "feishu_issue_kafka_rca"
    )
    db_parent.mkdir(parents=True)
    store = _seed_published_conclusion(db_parent)
    monkeypatch.setenv("HERMES_G1Q3_REVIEW_OWNER_USER_IDS", "ou_owner")

    def injected_ledger_failure(**_kwargs):
        raise OSError("injected audit sidecar failure")

    monkeypatch.setattr(
        owner_review_module, "_write_ledger", injected_ledger_failure
    )
    event = MessageEvent(
        text=f"rca 撤回 {ISSUE_ID} 证据归属有误",
        message_type=MessageType.TEXT,
        source=SessionSource(
            platform=Platform.FEISHU,
            user_id="ou_owner",
            user_name="RCA Owner",
            chat_id=G1Q3_RCA_GROUP_ID,
            chat_name="G1Q3 RCA",
            chat_type="group",
        ),
        message_id="om_retract_failure",
    )

    response = handle_owner_review_message(event, hermes_home=tmp_path)

    assert response.handled is True
    assert "失败并已安全停止" in str(response.response)
    assert len(store.list_rows("rca_conclusion_adjudications")) == 1
    assert len(store.list_rows("rca_delivery_effects")) == 2
