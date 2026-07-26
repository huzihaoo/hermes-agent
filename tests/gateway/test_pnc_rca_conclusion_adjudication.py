from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
import sqlite3
from types import SimpleNamespace

import pytest

from gateway import pnc_rca_delivery_store as delivery_store_module
from gateway import pnc_rca_owner_review as owner_review_module
from gateway.pnc_rca_conclusion_adjudication import (
    ADJUDICATION_EFFECT_SCHEMA_VERSION,
    ADJUDICATION_EFFECT_SCHEMA_VERSION_V1,
    ADJUDICATION_EFFECT_TARGET_PREFIX,
    ConclusionAdjudicationError,
    identifies_adjudication_effect,
    validate_adjudication_effect_claim,
)
from gateway.config import Platform
from gateway.platforms.base import MessageEvent, MessageType
from gateway.pnc_group_binding import G1Q3_RCA_GROUP_ID
from gateway.pnc_rca_delivery_contract import (
    DELIVERY_EFFECT_SCHEMA_VERSION,
    RCA_RESULT_FIELD_KEY,
)
from gateway.pnc_rca_delivery_store import (
    DELIVERY_STORE_SCHEMA_VERSION,
    DeliveryRecordConflictError,
    RcaDeliveryStore,
)
from gateway.pnc_rca_owner_review import handle_owner_review_message
from gateway.pnc_rca_write_fence import (
    build_issued_write_fence,
    canonical_write_fence_sha256,
    snapshot_core_sha256,
)
from gateway.session import SessionSource
from scripts.pnc_rca_delivery_dispatcher import DeliveryDispatcher
from scripts.pnc_rca_conclusion_adjudication_audit import (
    EXPECTED_DELIVERY_STORE_SCHEMA_VERSION,
    audit_conclusion_adjudications,
)


# These hand-built delivery rows model the pre-W5 historical corpus.  New
# effects must carry a live W3/W5 binding; historical fixtures are explicitly
# timestamped before the durable fence cutoff so they exercise the legacy path.
NOW = datetime(2026, 7, 24, 11, 30, tzinfo=timezone.utc)
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
        "consumer_capability": {
            "actual_evaluators": [
                {
                    "evaluator_id": "lane_geometry_quality",
                    "status": "supported",
                }
            ],
            "unused_capabilities": [
                {
                    "evaluator_id": "inventory_only_alias",
                    "status": "not_invoked",
                }
            ],
        },
        "report": {
            "candidate_owner_domain": "PERCEPTION_LANE",
            "is_candidate": True,
        },
        "public_result": {
            "candidate": "PERCEPTION_LANE",
            "responsibility": {"status": "candidate"},
            "summary": {"short_conclusion": "候选结论：感知车道线责任域"},
            "causal_chain": {
                "narrative": [
                    {"role": "现象", "text": "车辆横向控制异常。"},
                    {"role": "证据", "text": "车道线质量判据命中。"},
                    {
                        "role": "因果判断",
                        "text": "候选结论：感知车道线责任域",
                    },
                ]
            },
        },
    }
    manifest = {"submission_key": submission_key}
    payload = {
        "schema_version": DELIVERY_EFFECT_SCHEMA_VERSION,
        "effect_key": ORIGINAL_EFFECT_KEY,
        "delivery_id": delivery_id,
        "target_key": target_key,
        "project_key": "t03o4q",
        "work_item_type_key": "issue",
        "work_item_id": ISSUE_ID,
        "conclusion": "候选结论：感知车道线责任域",
        "terminal_class": "candidate_hypothesis",
        "confidence_tier": "medium",
        "requires_human_review": True,
    }
    with sqlite3.connect(store.db_path) as conn:
        conn.execute("PRAGMA foreign_keys=OFF")
        conn.executescript(
            """
            CREATE TABLE rca_activation_epochs (
                epoch_id TEXT PRIMARY KEY,
                state TEXT NOT NULL,
                is_current INTEGER NOT NULL
            );
            CREATE TABLE rca_activation_budget_slots (
                epoch_id TEXT NOT NULL,
                slot_kind TEXT NOT NULL,
                consumed_ledger_id TEXT
            );
            CREATE TABLE rca_activation_admission_ledger (
                ledger_id TEXT PRIMARY KEY,
                epoch_id TEXT NOT NULL,
                slot_kind TEXT NOT NULL,
                decision TEXT NOT NULL,
                bound_at TEXT,
                business_key TEXT NOT NULL,
                submission_key TEXT NOT NULL,
                generation INTEGER NOT NULL
            );
            CREATE TABLE business_triggers (
                business_key TEXT NOT NULL,
                generation INTEGER NOT NULL,
                activation_epoch_id TEXT,
                activation_ledger_id TEXT
            );
            CREATE TABLE rca_outbox (
                outbox_id INTEGER PRIMARY KEY,
                business_key TEXT NOT NULL,
                submission_key TEXT NOT NULL,
                generation INTEGER NOT NULL,
                activation_epoch_id TEXT,
                activation_ledger_id TEXT
            );
            """
        )
        conn.execute(
            "INSERT INTO rca_activation_epochs(epoch_id, state, is_current) "
            "VALUES('epoch-w16-active', 'bounded_active', 1)"
        )
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


def _add_published_conclusion(store: RcaDeliveryStore, issue_id: str) -> None:
    digest = hashlib.sha256(issue_id.encode("ascii")).hexdigest()
    delivery_id = "g1q3-rca-delivery-v1-" + digest
    submission_key = "g1q3-rca-s1-" + hashlib.sha256(
        f"submission:{issue_id}".encode("ascii")
    ).hexdigest()
    business_key = "g1q3-rca-b1-" + hashlib.sha256(
        f"business:{issue_id}".encode("ascii")
    ).hexdigest()
    artifact_set_id = "g1q3-rca-artifact-v1-" + hashlib.sha256(
        f"artifact:{issue_id}".encode("ascii")
    ).hexdigest()
    target_key = "g1q3-rca-target-v1-" + hashlib.sha256(
        f"target:{issue_id}".encode("ascii")
    ).hexdigest()
    effect_key = "g1q3-rca-effect-v1-" + hashlib.sha256(
        f"effect:{issue_id}".encode("ascii")
    ).hexdigest()
    current = NOW.isoformat()
    with sqlite3.connect(store.db_path) as conn:
        template = conn.execute(
            "SELECT contract_json FROM rca_delivery_jobs LIMIT 1"
        ).fetchone()[0]
        contract = json.loads(template)
        conclusion = f"候选结论：{issue_id} 感知车道线责任域"
        contract["public_result"]["summary"]["short_conclusion"] = conclusion
        contract["public_result"]["causal_chain"]["narrative"][-1][
            "text"
        ] = conclusion
        payload = {
            "schema_version": DELIVERY_EFFECT_SCHEMA_VERSION,
            "effect_key": effect_key,
            "delivery_id": delivery_id,
            "target_key": target_key,
            "project_key": "t03o4q",
            "work_item_type_key": "issue",
            "work_item_id": issue_id,
            "conclusion": conclusion,
            "terminal_class": "candidate_hypothesis",
            "confidence_tier": "medium",
            "requires_human_review": True,
        }
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
                issue_id,
                target_key,
                f"https://project.feishu.cn/g1q3/issue/detail/{issue_id}",
                f"https://reports.example/G1Q3_RCA/cases/{issue_id}/index.html",
                json.dumps({"submission_key": submission_key}),
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
                effect_key,
                delivery_id,
                target_key,
                json.dumps(payload),
                hashlib.sha256(json.dumps(payload).encode()).hexdigest(),
                current,
                current,
                current,
            ),
        )


def _owner_event(text: str, *, message_id: str) -> MessageEvent:
    return MessageEvent(
        text=text,
        message_type=MessageType.TEXT,
        source=SessionSource(
            platform=Platform.FEISHU,
            user_id="ou_owner",
            user_name="RCA Owner",
            chat_id=G1Q3_RCA_GROUP_ID,
            chat_name="G1Q3 RCA",
            chat_type="group",
            thread_id="topic:om_owner_root",
        ),
        message_id=message_id,
    )


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


def _dispatcher(
    store: RcaDeliveryStore,
    *,
    now=None,
    **boundaries,
) -> DeliveryDispatcher:
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
        now=now or (lambda: NOW),
        lease_owner="w16-test",
    )


def _effect_attempt_snapshot(
    store: RcaDeliveryStore,
    *,
    effect_key: str,
) -> dict[str, object]:
    with sqlite3.connect(store.db_path) as conn:
        conn.row_factory = sqlite3.Row
        effect = dict(
            conn.execute(
                "SELECT * FROM rca_delivery_effects WHERE effect_key = ?",
                (effect_key,),
            ).fetchone()
        )
        attempts = [
            dict(row)
            for row in conn.execute(
                "SELECT * FROM rca_delivery_attempts WHERE effect_key = ? "
                "ORDER BY attempt_id",
                (effect_key,),
            )
        ]
    return {"effect": effect, "attempts": attempts}


def _complete_artifact_repair(
    store: RcaDeliveryStore,
    artifact_root,
    result,
    *,
    mark_succeeded: bool = True,
) -> dict:
    [adjudication] = store.list_rows("rca_conclusion_adjudications")
    event = SimpleNamespace(
        message_id="om_command",
        source=SimpleNamespace(
            platform=Platform.FEISHU,
            chat_id="oc_g1q3",
            thread_id="omt_topic",
        ),
    )
    review_dir = artifact_root / "pnc_agent" / "reviews" / "g1q3_rca"
    persisted = owner_review_module._persist_owner_review_artifacts(
        event=event,
        hermes_home=artifact_root,
        review_dir=review_dir,
        issue_id=ISSUE_ID,
        action="撤回",
        reason=adjudication["reason"],
        owner_id="ou_owner",
        owner_name="RCA Owner",
        override=True,
        adjudication_result=result,
        now=NOW,
    )
    if mark_succeeded:
        store.mark_conclusion_adjudication_artifact_repair(
            adjudication_id=result.adjudication_id,
            succeeded=True,
            receipt_binding=persisted["receipt_binding"],
            now=NOW,
        )
    return persisted


def test_retract_is_atomic_idempotent_budgeted_and_has_impact_lineage(tmp_path):
    store = _seed_published_conclusion(tmp_path)

    first = _record_retraction(store)
    second = _record_retraction(store)

    assert first.created is True
    assert second.created is False
    assert second.correction_effect_key == first.correction_effect_key
    assert first.conclusion_state == "invalidated"
    assert first.impact_lineage["evaluator_refs"] == ["lane_geometry_quality"]
    assert "inventory_only_alias" not in first.impact_lineage["evaluator_refs"]
    assert first.impact_lineage["responsibility_domain"] == "PERCEPTION_LANE"
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


@pytest.mark.parametrize(
    ("lineage_case", "error_code"),
    [
        (
            "empty_evaluators",
            "conclusion_adjudication_evaluator_refs_unresolved",
        ),
        (
            "honest_non_attribution",
            "conclusion_adjudication_responsibility_domain_unresolved",
        ),
    ],
)
def test_unresolved_impact_lineage_fails_before_queue_mutation(
    tmp_path, lineage_case, error_code
):
    store = _seed_published_conclusion(tmp_path)
    with sqlite3.connect(store.db_path) as conn:
        contract = json.loads(
            conn.execute("SELECT contract_json FROM rca_delivery_jobs").fetchone()[0]
        )
        if lineage_case == "empty_evaluators":
            contract["consumer_capability"]["actual_evaluators"] = []
        else:
            contract["report"] = {}
            contract["public_result"] = {
                "responsibility": {"status": "not_attributable"},
                "summary": {"short_conclusion": "当前证据不足，无法定责"},
            }
        conn.execute(
            "UPDATE rca_delivery_jobs SET contract_json = ?",
            (json.dumps(contract),),
        )

    with pytest.raises(ConclusionAdjudicationError, match=error_code):
        _record_retraction(store)

    assert store.list_rows("rca_conclusion_adjudications") == []
    with sqlite3.connect(store.db_path) as conn:
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM rca_conclusion_adjudication_repairs"
            ).fetchone()[0]
            == 0
        )
    [effect] = store.list_rows("rca_delivery_effects")
    assert effect["effect_key"] == ORIGINAL_EFFECT_KEY
    [job] = store.list_rows("rca_delivery_jobs")
    assert job["status"] == "delivered"


@pytest.mark.parametrize(
    "reason",
    [
        "请核对问题数据地址",
        "请补齐后重新发起",
        "问题单缺少问题数据地址",
    ],
)
def test_retraction_publication_never_echoes_free_form_owner_reason(
    tmp_path, reason
):
    store = _seed_published_conclusion(tmp_path)

    result = _record_retraction(store, reason=reason)

    correction = next(
        row
        for row in store.list_rows("rca_delivery_effects")
        if row["effect_key"] == result.correction_effect_key
    )
    payload = json.loads(correction["payload_json"])
    publication = payload["comment_content"] + "\n" + "\n".join(
        update["field_value"] for update in payload["field_updates"]
    )
    assert payload["reason"] == reason
    assert reason not in publication
    assert "请核对问题数据地址" not in publication
    assert "请补齐后重新发起" not in publication
    assert "问题单缺少问题数据地址" not in publication
    assert "更正依据已保留在内部不可变审计记录中" in publication


def test_recognize_medium_candidate_enqueues_confirmation_without_echoing_reason(
    tmp_path,
):
    store = _seed_published_conclusion(tmp_path)

    result = store.record_conclusion_adjudication(
        work_item_id=ISSUE_ID,
        action="recognize",
        reason="owner reviewed internal evidence",
        actor_id="ou_owner",
        actor_name="RCA Owner",
        source={
            "platform": "feishu",
            "chat_id": "oc_g1q3",
            "thread_id": "topic:om_root",
            "message_id": "om_recognize",
        },
        now=NOW,
    )

    assert result.conclusion_state == "recognized"
    [adjudication] = store.list_rows("rca_conclusion_adjudications")
    assert adjudication["replacement_conclusion"] == "候选结论：感知车道线责任域"
    effect = next(
        row
        for row in store.list_rows("rca_delivery_effects")
        if row["effect_key"] == result.correction_effect_key
    )
    payload = json.loads(effect["payload_json"])
    publication = payload["comment_content"] + "\n" + "\n".join(
        update["field_value"] for update in payload["field_updates"]
    )
    assert "【RCA 追认】" in publication
    assert "候选结论：感知车道线责任域" in publication
    assert "owner reviewed internal evidence" not in publication
    claim = store.claim_due_effect(
        lease_owner="recognition-contract",
        lease_seconds=120,
        now=NOW,
        activation_required=False,
    )
    assert claim is not None and claim.effect_key == result.correction_effect_key
    marker, content, updates = validate_adjudication_effect_claim(claim)
    assert marker == payload["marker"]
    assert content == payload["comment_content"]
    assert updates == ((RCA_RESULT_FIELD_KEY, "候选结论：感知车道线责任域"),)


def test_review_queue_and_recognition_reject_non_medium_effect(tmp_path):
    store = _seed_published_conclusion(tmp_path)
    [item] = store.list_conclusion_review_queue()
    assert item.work_item_id == ISSUE_ID
    assert item.conclusion == "候选结论：感知车道线责任域"

    with sqlite3.connect(store.db_path) as conn:
        payload = json.loads(
            conn.execute(
                "SELECT payload_json FROM rca_delivery_effects"
            ).fetchone()[0]
        )
        payload["requires_human_review"] = False
        conn.execute(
            "UPDATE rca_delivery_effects SET payload_json = ?",
            (json.dumps(payload),),
        )

    assert store.list_conclusion_review_queue() == ()
    with pytest.raises(
        ConclusionAdjudicationError,
        match="conclusion_adjudication_medium_candidate_required",
    ):
        store.record_conclusion_adjudication(
            work_item_id=ISSUE_ID,
            action="recognize",
            reason="must remain medium only",
            actor_id="ou_owner",
            source={
                "platform": "feishu",
                "chat_id": "oc_g1q3",
                "thread_id": "topic:om_root",
                "message_id": "om_recognize",
            },
            now=NOW,
        )
    assert store.list_rows("rca_conclusion_adjudications") == []


def test_recognition_cannot_replace_or_inject_a_different_conclusion(tmp_path):
    store = _seed_published_conclusion(tmp_path)

    with pytest.raises(
        ConclusionAdjudicationError,
        match="conclusion_adjudication_recognition_replacement_invalid",
    ):
        store.record_conclusion_adjudication(
            work_item_id=ISSUE_ID,
            action="recognize",
            reason="attempted replacement",
            replacement_conclusion="different owner-authored conclusion",
            actor_id="ou_owner",
            source={
                "platform": "feishu",
                "chat_id": "oc_g1q3",
                "thread_id": "topic:om_root",
                "message_id": "om_recognize",
            },
            now=NOW,
        )

    assert store.list_rows("rca_conclusion_adjudications") == []
    assert len(store.list_rows("rca_delivery_effects")) == 1


def test_batch_recognition_of_five_is_atomic_and_drains_queue(tmp_path):
    store = _seed_published_conclusion(tmp_path)
    issue_ids = (ISSUE_ID, "7054691975", "7054691976", "7054691977", "7054691978")
    for issue_id in issue_ids[1:]:
        _add_published_conclusion(store, issue_id)

    assert {item.work_item_id for item in store.list_conclusion_review_queue()} == set(
        issue_ids
    )
    results = store.record_conclusion_adjudications(
        work_item_ids=issue_ids,
        action="recognize",
        reason="batch owner recognition",
        actor_id="ou_owner",
        actor_name="RCA Owner",
        source={
            "platform": "feishu",
            "chat_id": "oc_g1q3",
            "thread_id": "topic:om_batch",
            "message_id": "om_batch",
        },
        now=NOW,
    )

    assert len(results) == 5
    assert all(result.conclusion_state == "recognized" for result in results)
    assert len(store.list_rows("rca_conclusion_adjudications")) == 5
    assert len(store.list_rows("rca_delivery_effects")) == 10
    assert store.list_conclusion_review_queue() == ()


@pytest.mark.parametrize("action", ["recognize", "retract"])
def test_batch_review_rolls_back_all_when_one_item_is_not_medium(
    tmp_path, action
):
    store = _seed_published_conclusion(tmp_path)
    other_issue_id = "7054691975"
    _add_published_conclusion(store, other_issue_id)
    with sqlite3.connect(store.db_path) as conn:
        row = conn.execute(
            "SELECT effect_key, payload_json FROM rca_delivery_effects "
            "WHERE json_extract(payload_json, '$.work_item_id') = ?",
            (other_issue_id,),
        ).fetchone()
        payload = json.loads(row[1])
        payload["requires_human_review"] = False
        conn.execute(
            "UPDATE rca_delivery_effects SET payload_json = ? WHERE effect_key = ?",
            (json.dumps(payload), row[0]),
        )

    with pytest.raises(
        ConclusionAdjudicationError,
        match="conclusion_adjudication_medium_candidate_required",
    ):
        store.record_conclusion_adjudications(
            work_item_ids=(ISSUE_ID, other_issue_id),
            action=action,
            reason="must be all or nothing",
            actor_id="ou_owner",
            source={
                "platform": "feishu",
                "chat_id": "oc_g1q3",
                "thread_id": "topic:om_batch",
                "message_id": "om_batch",
            },
            now=NOW,
        )

    assert store.list_rows("rca_conclusion_adjudications") == []
    assert len(store.list_rows("rca_delivery_effects")) == 2


def test_owner_group_batch_recognition_closes_five_item_queue(
    tmp_path, monkeypatch
):
    control_dir = (
        tmp_path
        / "runtime"
        / "pnc_agent"
        / "feishu_issue_kafka_rca"
    )
    control_dir.mkdir(parents=True)
    store = _seed_published_conclusion(control_dir)
    issue_ids = (ISSUE_ID, "7054691975", "7054691976", "7054691977", "7054691978")
    for issue_id in issue_ids[1:]:
        _add_published_conclusion(store, issue_id)
    monkeypatch.setenv("HERMES_G1Q3_REVIEW_OWNER_USER_IDS", "ou_owner")

    queue = handle_owner_review_message(
        _owner_event("rca 待追认", message_id="om_queue"),
        hermes_home=tmp_path,
    )
    result = handle_owner_review_message(
        _owner_event(
            "rca 追认 " + ",".join(issue_ids) + " owner batch confirmed",
            message_id="om_batch_recognize",
        ),
        hermes_home=tmp_path,
    )

    assert queue.handled is True
    assert all(issue_id in str(queue.response) for issue_id in issue_ids)
    assert result.response == "RCA 追认已完成 5 单。"
    assert len(store.list_rows("rca_conclusion_adjudications")) == 5
    assert {
        row["action"] for row in store.list_rows("rca_conclusion_adjudications")
    } == {"recognize"}
    with sqlite3.connect(store.db_path) as conn:
        repairs = conn.execute(
            "SELECT status FROM rca_conclusion_adjudication_repairs"
        ).fetchall()
    assert {row[0] for row in repairs} == {"succeeded"}
    ledger = json.loads(
        (
            tmp_path
            / "pnc_agent"
            / "reviews"
            / "g1q3_rca"
            / "ledger.json"
        ).read_text(encoding="utf-8")
    )
    assert {
        entry["current"]["action"] for entry in ledger["issues"].values()
    } == {"追认"}
    with sqlite3.connect(store.db_path) as conn:
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    audit = audit_conclusion_adjudications(store.db_path)
    assert audit["ok"] is True
    assert audit["counts"]["recognized"] == 5


def test_owner_group_correction_alias_reuses_medium_only_retraction(
    tmp_path, monkeypatch
):
    control_dir = (
        tmp_path
        / "runtime"
        / "pnc_agent"
        / "feishu_issue_kafka_rca"
    )
    control_dir.mkdir(parents=True)
    store = _seed_published_conclusion(control_dir)
    monkeypatch.setenv("HERMES_G1Q3_REVIEW_OWNER_USER_IDS", "ou_owner")

    result = handle_owner_review_message(
        _owner_event(
            f"rca 更正 {ISSUE_ID} owner evidence disproved candidate",
            message_id="om_correct",
        ),
        hermes_home=tmp_path,
    )

    assert result.response == "RCA 更正已完成 1 单。"
    [adjudication] = store.list_rows("rca_conclusion_adjudications")
    assert adjudication["action"] == "retract"
    repair = store.conclusion_adjudication_artifact_repair(
        adjudication["adjudication_id"]
    )
    assert repair is not None and repair["status"] == "succeeded"


@pytest.mark.parametrize(
    ("epoch_mutation", "error"),
    [
        (
            "DELETE FROM rca_activation_epochs",
            "conclusion_adjudication_activation_unavailable",
        ),
        (
            "UPDATE rca_activation_epochs SET state = 'inactive'",
            "conclusion_adjudication_activation_inactive",
        ),
        (
            "INSERT INTO rca_activation_epochs VALUES "
            "('epoch-w16-second', 'steady_active', 1)",
            "conclusion_adjudication_activation_ambiguous",
        ),
    ],
)
def test_retraction_requires_exactly_one_current_active_epoch(
    tmp_path, epoch_mutation, error
):
    store = _seed_published_conclusion(tmp_path)
    with sqlite3.connect(store.db_path) as conn:
        conn.execute(epoch_mutation)

    with pytest.raises(ConclusionAdjudicationError, match=error):
        _record_retraction(store)

    assert store.list_rows("rca_conclusion_adjudications") == []
    assert len(store.list_rows("rca_delivery_effects")) == 1


def test_epoch_rotation_after_claim_fails_before_remote_boundary(tmp_path):
    store = _seed_published_conclusion(tmp_path)
    _record_retraction(store)
    claim = store.claim_due_effect(
        lease_owner="epoch-rotation",
        lease_seconds=120,
        now=NOW,
        activation_required=False,
    )
    assert claim is not None
    with sqlite3.connect(store.db_path) as conn:
        conn.execute(
            "UPDATE rca_activation_epochs SET is_current = 0 "
            "WHERE epoch_id = 'epoch-w16-active'"
        )
        conn.execute(
            "INSERT INTO rca_activation_epochs VALUES "
            "('epoch-w16-new', 'steady_active', 1)"
        )

    with pytest.raises(
        ConclusionAdjudicationError,
        match="conclusion_adjudication_effect_activation_stale",
    ):
        store.validate_adjudication_effect_binding(claim=claim, now=NOW)


def test_dispatcher_epoch_rotation_after_claim_has_zero_effect_or_attempt_mutation(
    tmp_path,
    monkeypatch,
):
    store = _seed_published_conclusion(tmp_path)
    result = _record_retraction(store)
    frozen: dict[str, object] = {}
    real_validate = store.validate_adjudication_effect_binding

    def rotate_then_validate(*, claim, now):
        with sqlite3.connect(store.db_path) as conn:
            conn.execute(
                "UPDATE rca_activation_epochs SET is_current = 0 "
                "WHERE epoch_id = 'epoch-w16-active'"
            )
            conn.execute(
                "INSERT INTO rca_activation_epochs VALUES "
                "('epoch-w16-rotated', 'steady_active', 1)"
            )
        frozen.update(
            _effect_attempt_snapshot(store, effect_key=result.correction_effect_key)
        )
        return real_validate(claim=claim, now=now)

    monkeypatch.setattr(
        store,
        "validate_adjudication_effect_binding",
        rotate_then_validate,
    )

    def unexpected(*_args, **_kwargs):
        raise AssertionError("stale adjudication must stop before remote boundaries")

    dispatcher = _dispatcher(
        store,
        list_comments=unexpected,
        add_comment=unexpected,
        get_fields=unexpected,
        update_fields=unexpected,
    )
    outcome = dispatcher.dispatch_one()

    assert outcome.status == "activation_stale"
    assert outcome.error_code == "conclusion_adjudication_effect_activation_stale"
    assert _effect_attempt_snapshot(
        store, effect_key=result.correction_effect_key
    ) == frozen


def test_epoch_rotation_after_successful_remote_gate_fences_next_local_mutation(
    tmp_path,
    monkeypatch,
):
    store = _seed_published_conclusion(tmp_path)
    result = _record_retraction(store)
    frozen: dict[str, object] = {}
    calls = {"list": 0, "fields": 0}

    def list_comments(_project_key, _work_item_id):
        calls["list"] += 1
        return {
            "success": True,
            "comments": [
                {
                    "remote_id": "original-comment",
                    "content": "[RCA_DELIVERY:original] candidate conclusion",
                }
            ],
            "pages_read": 1,
        }

    def get_fields(*_args):
        calls["fields"] += 1
        raise AssertionError("stale epoch must fence the next outward boundary")

    dispatcher = _dispatcher(
        store,
        list_comments=list_comments,
        add_comment=lambda *_args: (_ for _ in ()).throw(
            AssertionError("stale epoch must prevent remote writes")
        ),
        get_fields=get_fields,
        update_fields=lambda *_args: (_ for _ in ()).throw(
            AssertionError("stale epoch must prevent remote writes")
        ),
    )
    real_gate = dispatcher._adjudication_binding_gate

    def rotate_after_successful_gate(claim, *, after_outward_boundary):
        outcome = real_gate(
            claim,
            after_outward_boundary=after_outward_boundary,
        )
        if after_outward_boundary and not frozen:
            assert outcome is None
            with sqlite3.connect(store.db_path) as conn:
                conn.execute(
                    "UPDATE rca_activation_epochs SET is_current = 0 "
                    "WHERE epoch_id = 'epoch-w16-active'"
                )
                conn.execute(
                    "INSERT INTO rca_activation_epochs VALUES "
                    "('epoch-w16-after-gate', 'steady_active', 1)"
                )
            frozen.update(
                _effect_attempt_snapshot(
                    store,
                    effect_key=result.correction_effect_key,
                )
            )
        return outcome

    monkeypatch.setattr(
        dispatcher,
        "_adjudication_binding_gate",
        rotate_after_successful_gate,
    )

    outcome = dispatcher.dispatch_one()

    assert outcome.status == "activation_stale"
    assert outcome.error_code == "conclusion_adjudication_effect_activation_stale"
    assert calls == {"list": 1, "fields": 0}
    assert _effect_attempt_snapshot(
        store,
        effect_key=result.correction_effect_key,
    ) == frozen


def test_historical_correction_is_visible_to_activation_preview(tmp_path):
    store = _seed_published_conclusion(tmp_path)
    result = _record_retraction(store)

    preview = store.preview_dispatchable_effects(activation_required=True)

    assert [row["effect_key"] for row in preview] == [
        result.correction_effect_key
    ]


def test_correction_identity_covers_legacy_schema_and_reserved_target():
    assert identifies_adjudication_effect(
        {"schema_version": ADJUDICATION_EFFECT_SCHEMA_VERSION_V1},
        target_key="unrelated",
    )
    assert identifies_adjudication_effect(
        {"schema_version": DELIVERY_EFFECT_SCHEMA_VERSION},
        target_key=f"{ADJUDICATION_EFFECT_TARGET_PREFIX}-{'a' * 64}",
    )


@pytest.mark.parametrize(
    "identity_case",
    ["v1_json_whitespace", "reserved_target_schema_laundering"],
)
def test_correction_identity_blocks_duplicate_and_fails_orphan_audit(
    tmp_path,
    identity_case,
):
    store = _seed_published_conclusion(tmp_path)
    if identity_case == "v1_json_whitespace":
        payload_json = json.dumps(
            {"schema_version": ADJUDICATION_EFFECT_SCHEMA_VERSION_V1}
        )
        assert '": "' in payload_json
        target_key = "legacy-correction-target"
    else:
        payload_json = json.dumps(
            {"schema_version": DELIVERY_EFFECT_SCHEMA_VERSION},
            separators=(",", ":"),
        )
        target_key = f"{ADJUDICATION_EFFECT_TARGET_PREFIX}-{'b' * 64}"
    effect_key = "g1q3-rca-effect-v1-" + (
        "8" * 64 if identity_case == "v1_json_whitespace" else "9" * 64
    )
    current = NOW.isoformat()
    with sqlite3.connect(store.db_path) as conn:
        [delivery_id] = conn.execute(
            "SELECT delivery_id FROM rca_delivery_jobs"
        ).fetchone()
        conn.execute(
            """
            INSERT INTO rca_delivery_effects(
                effect_key, delivery_id, effect_kind, required, target_key,
                payload_json, payload_sha256, outcome, write_phase, status,
                adjudication_comment_attempt_count,
                adjudication_comment_attempted_at,
                completed_at, created_at, updated_at
            ) VALUES (?, ?, 'feishu_issue_comment', 1, ?, ?, ?, 'success',
                      'settled', 'succeeded', 1, ?, ?, ?, ?)
            """,
            (
                effect_key,
                delivery_id,
                target_key,
                payload_json,
                hashlib.sha256(payload_json.encode("utf-8")).hexdigest(),
                current,
                current,
                current,
                current,
            ),
        )

    with pytest.raises(
        ConclusionAdjudicationError,
        match="conclusion_adjudication_comment_budget_exhausted",
    ):
        _record_retraction(store)

    assert store.list_rows("rca_conclusion_adjudications") == []
    assert len(store.list_rows("rca_delivery_effects")) == 2
    with sqlite3.connect(store.db_path) as conn:
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    audit = audit_conclusion_adjudications(store.db_path)
    assert audit["ok"] is False
    assert audit["counts"]["adjudication_effects"] == 1
    assert audit["counts"]["orphan_correction_effects"] == 1
    assert audit["invariants"]["ledger_effect_count_equal"] is False
    assert audit["invariants"]["correction_effects_linked"] is False


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


def test_v7_marker_requires_explicit_v9_migration(tmp_path):
    assert EXPECTED_DELIVERY_STORE_SCHEMA_VERSION == DELIVERY_STORE_SCHEMA_VERSION
    store = RcaDeliveryStore(tmp_path / "control.sqlite3")
    with sqlite3.connect(store.db_path) as conn:
        conn.execute(
            "UPDATE rca_delivery_meta SET value = 'pnc_rca_delivery_store_v7' "
            "WHERE key = 'schema_version'"
        )
        conn.executescript(
            """
            DROP TABLE rca_conclusion_adjudication_repairs;
            DROP TABLE rca_conclusion_adjudications;
            DROP TABLE rca_failure_routes;
            ALTER TABLE rca_delivery_effects
                DROP COLUMN adjudication_comment_attempted_at;
            ALTER TABLE rca_delivery_effects
                DROP COLUMN adjudication_comment_attempt_count;
            """
        )

    with pytest.raises(RuntimeError, match="rca_delivery_store_schema_not_current"):
        RcaDeliveryStore(store.db_path, require_current=True)

    RcaDeliveryStore(store.db_path)
    with sqlite3.connect(store.db_path) as conn:
        marker = conn.execute(
            "SELECT value FROM rca_delivery_meta WHERE key = 'schema_version'"
        ).fetchone()[0]
        repair_table = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' "
            "AND name = 'rca_conclusion_adjudication_repairs'"
        ).fetchone()
    assert marker == DELIVERY_STORE_SCHEMA_VERSION
    assert repair_table is not None


def test_v7_migrates_to_combined_w2_w16_v9_schema(tmp_path):
    path = tmp_path / "combined-control.sqlite3"
    RcaDeliveryStore(path)
    with sqlite3.connect(path) as conn:
        conn.executescript(
            """
            DROP TABLE rca_conclusion_adjudication_repairs;
            DROP TABLE rca_conclusion_adjudications;
            DROP TABLE rca_failure_routes;
            ALTER TABLE rca_delivery_effects
                DROP COLUMN adjudication_comment_attempted_at;
            ALTER TABLE rca_delivery_effects
                DROP COLUMN adjudication_comment_attempt_count;
            UPDATE rca_delivery_meta
               SET value = 'pnc_rca_delivery_store_v7'
             WHERE key = 'schema_version';
            """
        )

    with pytest.raises(RuntimeError, match="rca_delivery_store_schema_not_current"):
        RcaDeliveryStore(path, require_current=True)

    RcaDeliveryStore(path)
    RcaDeliveryStore(path, require_current=True)
    with sqlite3.connect(path) as conn:
        marker = conn.execute(
            "SELECT value FROM rca_delivery_meta WHERE key = 'schema_version'"
        ).fetchone()[0]
        effect_columns = {
            row[1] for row in conn.execute("PRAGMA table_info(rca_delivery_effects)")
        }
        failure_route_columns = {
            row[1] for row in conn.execute("PRAGMA table_info(rca_failure_routes)")
        }
        repair_columns = {
            row[1]
            for row in conn.execute(
                "PRAGMA table_info(rca_conclusion_adjudication_repairs)"
            )
        }
    assert marker == DELIVERY_STORE_SCHEMA_VERSION
    assert {
        "adjudication_comment_attempt_count",
        "adjudication_comment_attempted_at",
    } <= effect_columns
    assert {"route_key", "dedupe_key", "remediation_attempt_count"} <= (
        failure_route_columns
    )
    assert {
        "adjudication_id",
        "status",
        "attempt_count",
        "receipt_path",
        "receipt_offset",
        "receipt_length",
        "receipt_sha256",
        "receipt_device",
        "receipt_inode",
        "receipt_event_id",
    } <= repair_columns


def test_w2_v8_migrates_explicitly_to_combined_v9_schema(tmp_path):
    path = tmp_path / "w2-v8-control.sqlite3"
    RcaDeliveryStore(path)
    with sqlite3.connect(path) as conn:
        conn.executescript(
            """
            DROP TABLE rca_conclusion_adjudication_repairs;
            ALTER TABLE rca_delivery_effects
                DROP COLUMN adjudication_comment_attempted_at;
            ALTER TABLE rca_delivery_effects
                DROP COLUMN adjudication_comment_attempt_count;
            UPDATE rca_delivery_meta
               SET value = 'pnc_rca_delivery_store_v8'
             WHERE key = 'schema_version';
            """
        )

    with pytest.raises(RuntimeError, match="rca_delivery_store_schema_not_current"):
        RcaDeliveryStore(path, require_current=True)

    RcaDeliveryStore(path)
    RcaDeliveryStore(path, require_current=True)
    with sqlite3.connect(path) as conn:
        marker = conn.execute(
            "SELECT value FROM rca_delivery_meta WHERE key = 'schema_version'"
        ).fetchone()[0]
        failure_routes = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' "
            "AND name = 'rca_failure_routes'"
        ).fetchone()
        repairs = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' "
            "AND name = 'rca_conclusion_adjudication_repairs'"
        ).fetchone()
    assert marker == "pnc_rca_delivery_store_v9"
    assert failure_routes is not None
    assert repairs is not None


@pytest.mark.parametrize(
    "source_version",
    ["pnc_rca_delivery_store_v7", "pnc_rca_delivery_store_v8"],
)
def test_predecessor_schema_and_marker_rollback_together_on_w16_fault(
    tmp_path, monkeypatch, source_version
):
    path = tmp_path / f"{source_version}-rollback.sqlite3"
    RcaDeliveryStore(path)
    with sqlite3.connect(path) as conn:
        conn.executescript(
            """
            DROP TABLE rca_conclusion_adjudication_repairs;
            ALTER TABLE rca_delivery_effects
                DROP COLUMN adjudication_comment_attempted_at;
            ALTER TABLE rca_delivery_effects
                DROP COLUMN adjudication_comment_attempt_count;
            """
        )
        if source_version == "pnc_rca_delivery_store_v7":
            conn.execute("DROP TABLE rca_conclusion_adjudications")
            conn.execute("DROP TABLE rca_failure_routes")
        conn.execute(
            "UPDATE rca_delivery_meta SET value = ? WHERE key = 'schema_version'",
            (source_version,),
        )
    real_ensure = delivery_store_module.ensure_conclusion_adjudication_schema

    def fail_after_w16_schema(conn):
        real_ensure(conn)
        raise RuntimeError("injected failure before v9 marker")

    monkeypatch.setattr(
        delivery_store_module,
        "ensure_conclusion_adjudication_schema",
        fail_after_w16_schema,
    )

    with pytest.raises(RuntimeError, match="injected failure before v9 marker"):
        RcaDeliveryStore(path)

    with sqlite3.connect(path) as conn:
        marker = conn.execute(
            "SELECT value FROM rca_delivery_meta WHERE key = 'schema_version'"
        ).fetchone()[0]
        effect_columns = {
            row[1] for row in conn.execute("PRAGMA table_info(rca_delivery_effects)")
        }
        repairs = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' "
            "AND name = 'rca_conclusion_adjudication_repairs'"
        ).fetchone()
        failure_routes = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' "
            "AND name = 'rca_failure_routes'"
        ).fetchone()
    assert marker == source_version
    assert "adjudication_comment_attempt_count" not in effect_columns
    assert "adjudication_comment_attempted_at" not in effect_columns
    assert repairs is None
    assert (failure_routes is not None) is (
        source_version == "pnc_rca_delivery_store_v8"
    )


def test_current_schema_rejects_missing_w16_attempt_token_column(tmp_path):
    path = tmp_path / "missing-token.sqlite3"
    RcaDeliveryStore(path)
    with sqlite3.connect(path) as conn:
        conn.execute(
            "ALTER TABLE rca_delivery_effects "
            "DROP COLUMN adjudication_comment_attempted_at"
        )

    with pytest.raises(
        RuntimeError, match="rca_conclusion_adjudication_schema_not_current"
    ):
        RcaDeliveryStore(path, require_current=True)


def test_audit_fails_when_w16_schema_is_absent(tmp_path):
    store = RcaDeliveryStore(tmp_path / "control.sqlite3")
    with sqlite3.connect(store.db_path) as conn:
        conn.execute("DROP TABLE rca_conclusion_adjudication_repairs")
        conn.execute("DROP TABLE rca_conclusion_adjudications")
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")

    audit = audit_conclusion_adjudications(store.db_path)

    assert audit["ok"] is False
    assert audit["adjudication_schema"]["ready"] is False
    assert "adjudication_schema_missing" in audit["adjudication_schema"]["errors"]


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


def test_self_consistent_orphan_correction_is_quarantined_before_boundary(
    tmp_path,
):
    store = _seed_published_conclusion(tmp_path)
    result = _record_retraction(store)
    claim = store.claim_due_effect(
        lease_owner="orphan-probe",
        lease_seconds=120,
        now=NOW,
        activation_required=False,
    )
    assert claim is not None
    assert claim.effect_key == result.correction_effect_key
    with sqlite3.connect(store.db_path) as conn:
        conn.executescript(
            """
            DROP TRIGGER trg_rca_conclusion_adjudication_no_delete;
            DELETE FROM rca_conclusion_adjudication_repairs;
            DELETE FROM rca_conclusion_adjudications;
            CREATE TRIGGER trg_rca_conclusion_adjudication_no_delete
            BEFORE DELETE ON rca_conclusion_adjudications
            BEGIN
                SELECT RAISE(
                    ABORT, 'rca_conclusion_adjudication_immutable'
                );
            END;
            """
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

    keeper = dispatcher._start_effect_lease_keeper(claim)
    try:
        outcome = dispatcher._dispatch_claim(claim)
    finally:
        dispatcher._stop_effect_lease_keeper(keeper)

    assert outcome.status == "quarantined"
    assert outcome.error_code == "conclusion_adjudication_effect_ledger_missing"
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
    fields = {RCA_RESULT_FIELD_KEY: "候选结论：感知车道线责任域"}

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


def test_post_cutoff_retract_correction_uses_current_w3_w5_fence(tmp_path):
    fenced_now = datetime(2026, 7, 26, 12, 0, tzinfo=timezone.utc)
    candidate_at = fenced_now - timedelta(minutes=30)
    fence_issued_at = candidate_at - timedelta(minutes=5)
    store = _seed_published_conclusion(tmp_path)
    [job] = store.list_rows("rca_delivery_jobs")
    snapshot = {
        "schema_version": "pnc_rca_admission_snapshot_v1",
        "request_sha256": "a" * 64,
        "canonical_request": {
            "schema_version": "pnc_rca_canonical_request_v1",
            "ticket": {
                "project_key": job["project_key"],
                "issue_url": job["issue_url"],
            },
            "business_profile": {"value": {"profile_id": "g1q3"}},
        },
        "resolved_admission": {
            "business_key": job["business_key"],
            "submission_key": job["submission_key"],
            "generation": job["generation"],
        },
        "execution_admission": {
            "activation_epoch_id": "epoch-w16-active",
            "activation_ledger_id": 7,
            "decision": "admit",
            "legacy_unconfigured": False,
        },
    }
    fence = build_issued_write_fence(
        snapshot=snapshot,
        activation_epoch_id="epoch-w16-active",
        activation_ledger_id=7,
        admission_key="admission-w16-current",
        target_set={"issue_target": job["issue_url"], "thread_target": None},
        now=fence_issued_at,
        expires_at=fenced_now + timedelta(hours=2),
    )
    snapshot_identity = {**snapshot, "write_fence": fence}
    snapshot_sha256 = canonical_write_fence_sha256(snapshot_identity)
    snapshot = {
        **snapshot_identity,
        "snapshot_id": f"pnc-rca-snapshot-v1-{snapshot_sha256}",
        "snapshot_sha256": snapshot_sha256,
    }
    assert fence["issued_at"] < candidate_at.isoformat().replace("+00:00", "Z")
    assert not store.is_historical_external_write_effect(candidate_at.isoformat())
    contract = json.loads(job["contract_json"])
    contract["w3_execution_snapshot"] = {
        "write_fence": fence,
        "snapshot_core_sha256": snapshot_core_sha256(snapshot),
    }
    with sqlite3.connect(store.db_path) as conn:
        conn.execute(
            "ALTER TABLE rca_activation_admission_ledger "
            "ADD COLUMN admission_key TEXT"
        )
        conn.execute(
            """
            INSERT INTO rca_activation_admission_ledger(
                ledger_id, epoch_id, admission_key, slot_kind, decision,
                bound_at, business_key, submission_key, generation
            ) VALUES(7, 'epoch-w16-active', 'admission-w16-current',
                     'manual_success', 'admit', ?, ?, ?, ?)
            """,
            (
                fence_issued_at.isoformat(),
                job["business_key"],
                job["submission_key"],
                job["generation"],
            ),
        )
        # This test uses a small, exact projection of the immutable W3 rows.
        # The production store joins these rows before accepting a post-cutoff
        # effect; hand-built historical fixtures do not contain them.
        conn.executescript(
            """
            CREATE TABLE rca_admission_snapshots(
                snapshot_sha256 TEXT,
                snapshot_id TEXT,
                schema_version TEXT,
                request_sha256 TEXT,
                business_key TEXT,
                submission_key TEXT,
                generation INTEGER,
                activation_epoch_id TEXT,
                activation_ledger_id INTEGER,
                execution_decision TEXT,
                execution_reason TEXT,
                execution_state TEXT,
                legacy_unconfigured INTEGER,
                creator_source_envelope_sha256 TEXT,
                creator_authority_sha256 TEXT,
                creator_source_id TEXT,
                admission_snapshot_json TEXT
            );
            CREATE TABLE rca_snapshot_source_envelopes(
                source_envelope_sha256 TEXT,
                source_envelope_id TEXT,
                schema_version TEXT,
                snapshot_sha256 TEXT,
                snapshot_id TEXT,
                submission_key TEXT,
                source_authority_sha256 TEXT,
                source_id TEXT,
                source_kind TEXT,
                payload_sha256 TEXT,
                authorization_evidence_sha256 TEXT,
                binding_action TEXT,
                decision TEXT,
                source_metadata_json TEXT,
                anchor_json TEXT,
                ingress_decision_json TEXT,
                source_envelope_json TEXT
            );
            """
        )
        source_envelope_identity = {
            "schema_version": "pnc_rca_snapshot_source_envelope_v1",
            "source_authority_sha256": "b" * 64,
            "snapshot_id": snapshot["snapshot_id"],
            "snapshot_sha256": snapshot["snapshot_sha256"],
            "submission_key": job["submission_key"],
            "source_id": "w16-test-source",
            "source_kind": "kafka_workflow_event",
            "ingress_decision": {
                "requested_mode": "pending",
                "binding_action": "create",
                "decision": "admit",
                "authorization_evidence_sha256": "d" * 64,
            },
            "source_metadata": {
                "source_kind": "kafka_workflow_event",
                "event_uid": "w16-test-topic:0:1",
                "topic": "w16-test-topic",
                "partition": 0,
                "offset": 1,
                "payload_sha256": "c" * 64,
                "observed_at": fence_issued_at.isoformat(),
            },
            "anchor": {
                "issue_target": job["issue_url"],
                "thread_target": None,
            },
        }
        source_envelope_sha256 = canonical_write_fence_sha256(
            source_envelope_identity
        )
        source_envelope = {
            **source_envelope_identity,
            "source_envelope_id": (
                f"pnc-rca-source-envelope-v1-{source_envelope_sha256}"
            ),
            "source_envelope_sha256": source_envelope_sha256,
        }
        conn.execute(
            """
            INSERT INTO rca_admission_snapshots(
                snapshot_sha256, snapshot_id, schema_version, request_sha256,
                business_key, submission_key, generation,
                activation_epoch_id, activation_ledger_id,
                execution_decision, execution_reason, execution_state,
                legacy_unconfigured, creator_source_envelope_sha256,
                creator_authority_sha256, creator_source_id,
                admission_snapshot_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                snapshot["snapshot_sha256"],
                snapshot["snapshot_id"],
                snapshot["schema_version"],
                snapshot["request_sha256"],
                job["business_key"],
                job["submission_key"],
                job["generation"],
                "epoch-w16-active",
                7,
                "admit",
                "activation_bounded_slot_consumed",
                "bounded_active",
                0,
                source_envelope["source_envelope_sha256"],
                source_envelope["source_authority_sha256"],
                source_envelope["source_id"],
                json.dumps(snapshot),
            ),
        )
        conn.execute(
            """
            INSERT INTO rca_snapshot_source_envelopes(
                source_envelope_sha256, source_envelope_id, schema_version,
                snapshot_sha256, snapshot_id, submission_key,
                source_authority_sha256, source_id, source_kind,
                payload_sha256, authorization_evidence_sha256,
                binding_action, decision, source_metadata_json, anchor_json,
                ingress_decision_json, source_envelope_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                source_envelope["source_envelope_sha256"],
                source_envelope["source_envelope_id"],
                source_envelope["schema_version"],
                source_envelope["snapshot_sha256"],
                source_envelope["snapshot_id"],
                source_envelope["submission_key"],
                source_envelope["source_authority_sha256"],
                source_envelope["source_id"],
                source_envelope["source_kind"],
                source_envelope["source_metadata"]["payload_sha256"],
                source_envelope["ingress_decision"][
                    "authorization_evidence_sha256"
                ],
                source_envelope["ingress_decision"]["binding_action"],
                source_envelope["ingress_decision"]["decision"],
                json.dumps(source_envelope["source_metadata"]),
                json.dumps(source_envelope["anchor"]),
                json.dumps(source_envelope["ingress_decision"]),
                json.dumps(source_envelope),
            ),
        )
        conn.execute(
            "UPDATE rca_delivery_jobs "
            "SET contract_json = ?, created_at = ?, updated_at = ? "
            "WHERE delivery_id = ?",
            (
                json.dumps(contract),
                candidate_at.isoformat(),
                candidate_at.isoformat(),
                job["delivery_id"],
            ),
        )
        conn.execute(
            "UPDATE rca_delivery_effects "
            "SET created_at = ?, completed_at = ?, updated_at = ? "
            "WHERE effect_key = ?",
            (
                candidate_at.isoformat(),
                candidate_at.isoformat(),
                candidate_at.isoformat(),
                ORIGINAL_EFFECT_KEY,
            ),
        )

    result = store.record_conclusion_adjudication(
        work_item_id=ISSUE_ID,
        action="retract",
        reason="current candidate attribution is invalid",
        actor_id="ou_owner",
        actor_name="RCA Owner",
        source={
            "platform": "feishu",
            "chat_id": "oc_g1q3",
            "thread_id": "omt_topic",
            "message_id": "om_current_command",
        },
        now=fenced_now,
    )
    comments = [
        {
            "remote_id": "original-comment",
            "content": "[RCA_DELIVERY:original] candidate conclusion",
        }
    ]
    fields = {RCA_RESULT_FIELD_KEY: "候选结论：感知车道线责任域"}
    boundary_calls: list[str] = []

    def list_comments(_project_key, _work_item_id):
        boundary_calls.append("list_comments")
        return {"success": True, "comments": list(comments), "pages_read": 1}

    def add_comment(_project_key, _work_item_id, content):
        boundary_calls.append("add_comment")
        comments.append({"remote_id": "correction-comment", "content": content})
        return {"success": True, "remote_id": "correction-comment"}

    def get_fields(_project_key, _work_item_id, field_keys):
        boundary_calls.append("get_fields")
        return {
            "success": True,
            "fields": {key: fields.get(key, "") for key in field_keys},
        }

    def update_fields(_project_key, _work_item_id, updates):
        boundary_calls.append("update_fields")
        fields.update(dict(updates))
        return {"success": True}

    dispatcher = _dispatcher(
        store,
        now=lambda: fenced_now,
        list_comments=list_comments,
        add_comment=add_comment,
        get_fields=get_fields,
        update_fields=update_fields,
    )

    outcome = dispatcher.dispatch_one()

    assert outcome.status == "succeeded"
    assert outcome.effect_key == result.correction_effect_key
    [correction] = [
        row
        for row in store.list_rows("rca_delivery_effects")
        if row["effect_key"] == result.correction_effect_key
    ]
    assert not store.is_historical_external_write_effect(correction["created_at"])
    assert boundary_calls.count("update_fields") == 1
    assert boundary_calls.count("add_comment") == 1
    assert len(comments) == 2
    assert fields[RCA_RESULT_FIELD_KEY].startswith("原自动 RCA 结论已撤回")


def test_medium_recognition_dispatches_one_confirmation_comment(tmp_path):
    store = _seed_published_conclusion(tmp_path)
    result = store.record_conclusion_adjudication(
        work_item_id=ISSUE_ID,
        action="recognize",
        reason="owner confirmed internal evidence",
        actor_id="ou_owner",
        actor_name="RCA Owner",
        source={
            "platform": "feishu",
            "chat_id": "oc_g1q3",
            "thread_id": "topic:om_root",
            "message_id": "om_recognize",
        },
        now=NOW,
    )
    comments = [
        {
            "remote_id": "original-comment",
            "content": "[RCA_DELIVERY:original] candidate conclusion",
        }
    ]
    fields = {RCA_RESULT_FIELD_KEY: "候选结论：感知车道线责任域"}

    def list_comments(_project_key, _work_item_id):
        return {"success": True, "comments": list(comments), "pages_read": 1}

    def add_comment(_project_key, _work_item_id, content):
        comments.append({"remote_id": "recognition-comment", "content": content})
        return {"success": True, "remote_id": "recognition-comment"}

    def get_fields(_project_key, _work_item_id, field_keys):
        return {
            "success": True,
            "fields": {key: fields.get(key, "") for key in field_keys},
        }

    def update_fields(_project_key, _work_item_id, updates):
        fields.update(dict(updates))
        return {"success": True}

    outcome = _dispatcher(
        store,
        list_comments=list_comments,
        add_comment=add_comment,
        get_fields=get_fields,
        update_fields=update_fields,
    ).dispatch_one()

    assert outcome.status == "succeeded"
    assert outcome.effect_key == result.correction_effect_key
    assert len(comments) == 2
    assert comments[-1]["content"].count("【RCA 追认】") == 1
    assert "上一条自动 RCA 候选评论为准" in comments[-1]["content"]
    assert fields[RCA_RESULT_FIELD_KEY] == "候选结论：感知车道线责任域"


def test_invisible_correction_success_never_repeats_remote_comment_write(
    tmp_path,
):
    store = _seed_published_conclusion(tmp_path)
    result = _record_retraction(store)
    fields = {RCA_RESULT_FIELD_KEY: "候选结论：感知车道线责任域"}
    add_calls = 0

    class Clock:
        current = NOW

        def __call__(self):
            return self.current

    clock = Clock()

    def list_comments(_project_key, _work_item_id):
        return {
            "success": True,
            "comments": [
                {
                    "remote_id": "original-comment",
                    "content": "[RCA_DELIVERY:original] candidate conclusion",
                }
            ],
            "pages_read": 1,
        }

    def add_comment(_project_key, _work_item_id, _content):
        nonlocal add_calls
        add_calls += 1
        return {"success": True, "remote_id": f"invisible-{add_calls}"}

    def get_fields(_project_key, _work_item_id, field_keys):
        return {
            "success": True,
            "fields": {key: fields.get(key, "") for key in field_keys},
        }

    def update_fields(_project_key, _work_item_id, updates):
        fields.update(dict(updates))
        return {"success": True}

    dispatcher = _dispatcher(
        store,
        now=clock,
        list_comments=list_comments,
        add_comment=add_comment,
        get_fields=get_fields,
        update_fields=update_fields,
    )
    outcomes = []
    for _ in range(12):
        outcome = dispatcher.dispatch_one()
        outcomes.append(outcome)
        assert outcome.status == "uncertain"
        assert outcome.next_attempt_at is not None
        clock.current = datetime.fromisoformat(outcome.next_attempt_at)

    assert add_calls == 1
    assert any(
        outcome.error_code
        == "conclusion_adjudication_reconciliation_read_only"
        for outcome in outcomes
    )
    effect = next(
        row
        for row in store.list_rows("rca_delivery_effects")
        if row["effect_key"] == result.correction_effect_key
    )
    assert effect["adjudication_comment_attempt_count"] == 1
    assert effect["recovery_write_count"] == 0


def test_consumed_comment_token_blocks_field_rewrite_after_epoch_rotates_on_read(
    tmp_path,
):
    store = _seed_published_conclusion(tmp_path)
    result = _record_retraction(store)
    fields = {RCA_RESULT_FIELD_KEY: "候选结论：感知车道线责任域"}
    calls = {"get": 0, "update": 0, "add": 0}
    frozen: dict[str, object] = {}

    class Clock:
        current = NOW

        def __call__(self):
            return self.current

    clock = Clock()

    def list_comments(_project_key, _work_item_id):
        return {
            "success": True,
            "comments": [
                {
                    "remote_id": "original-comment",
                    "content": "[RCA_DELIVERY:original] candidate conclusion",
                }
            ],
            "pages_read": 1,
        }

    def get_fields(_project_key, _work_item_id, field_keys):
        calls["get"] += 1
        if calls["get"] == 3:
            with sqlite3.connect(store.db_path) as conn:
                conn.execute(
                    "UPDATE rca_activation_epochs SET is_current = 0 "
                    "WHERE epoch_id = 'epoch-w16-active'"
                )
                conn.execute(
                    "INSERT INTO rca_activation_epochs VALUES "
                    "('epoch-w16-rotated', 'steady_active', 1)"
                )
            frozen.update(
                _effect_attempt_snapshot(
                    store,
                    effect_key=result.correction_effect_key,
                )
            )
        return {
            "success": True,
            "fields": {key: fields.get(key, "") for key in field_keys},
        }

    def update_fields(_project_key, _work_item_id, updates):
        calls["update"] += 1
        fields.update(dict(updates))
        return {"success": True}

    def add_comment(_project_key, _work_item_id, _content):
        calls["add"] += 1
        return {"success": True, "remote_id": "invisible-correction"}

    dispatcher = _dispatcher(
        store,
        now=clock,
        list_comments=list_comments,
        add_comment=add_comment,
        get_fields=get_fields,
        update_fields=update_fields,
    )
    first = dispatcher.dispatch_one()
    assert first.status == "uncertain"
    assert calls == {"get": 2, "update": 1, "add": 1}
    fields[RCA_RESULT_FIELD_KEY] = "字段被外部漂移"
    clock.current = datetime.fromisoformat(first.next_attempt_at)

    second = dispatcher.dispatch_one()

    assert second.status == "activation_stale"
    assert second.error_code == "conclusion_adjudication_effect_activation_stale"
    assert calls == {"get": 3, "update": 1, "add": 1}
    assert _effect_attempt_snapshot(
        store,
        effect_key=result.correction_effect_key,
    ) == frozen
    effect = frozen["effect"]
    assert isinstance(effect, dict)
    assert effect["status"] == "claimed"
    assert effect["lease_token"]
    assert effect["lease_owner"]
    assert effect["write_phase"] == "write_started"
    assert effect["adjudication_comment_attempt_count"] == 1
    with sqlite3.connect(store.db_path) as conn:
        epochs = conn.execute(
            "SELECT epoch_id, is_current FROM rca_activation_epochs ORDER BY epoch_id"
        ).fetchall()
    assert epochs == [("epoch-w16-active", 0), ("epoch-w16-rotated", 1)]


@pytest.mark.parametrize("field_update_applied", [False, True])
def test_unknown_field_update_is_never_repeated_on_adjudication_retry(
    tmp_path, field_update_applied
):
    store = _seed_published_conclusion(tmp_path)
    result = _record_retraction(store)
    fields = {RCA_RESULT_FIELD_KEY: "候选结论：感知车道线责任域"}
    comments = [
        {
            "remote_id": "original-comment",
            "content": "[RCA_DELIVERY:original] candidate conclusion",
        }
    ]
    calls = {"update": 0, "add": 0}

    class Clock:
        current = NOW

        def __call__(self):
            return self.current

    clock = Clock()

    def list_comments(_project_key, _work_item_id):
        return {"success": True, "comments": list(comments), "pages_read": 1}

    def get_fields(_project_key, _work_item_id, field_keys):
        return {
            "success": True,
            "fields": {key: fields.get(key, "") for key in field_keys},
        }

    def update_fields(_project_key, _work_item_id, updates):
        calls["update"] += 1
        if field_update_applied:
            fields.update(dict(updates))
        raise TimeoutError("field update outcome is unknown")

    def add_comment(_project_key, _work_item_id, content):
        calls["add"] += 1
        comments.append({"remote_id": "correction-comment", "content": content})
        return {"success": True, "remote_id": "correction-comment"}

    dispatcher = _dispatcher(
        store,
        now=clock,
        list_comments=list_comments,
        add_comment=add_comment,
        get_fields=get_fields,
        update_fields=update_fields,
    )
    first = dispatcher.dispatch_one()
    assert first.status == "uncertain"
    assert first.error_code == "feishu_field_update_outcome_unknown"
    clock.current = datetime.fromisoformat(first.next_attempt_at)

    second = dispatcher.dispatch_one()

    assert calls["update"] == 1
    effect = next(
        row
        for row in store.list_rows("rca_delivery_effects")
        if row["effect_key"] == result.correction_effect_key
    )
    if field_update_applied:
        assert second.status == "succeeded"
        assert calls["add"] == 1
        assert effect["write_phase"] == "settled"
        assert effect["adjudication_comment_attempt_count"] == 1
    else:
        assert second.status == "uncertain"
        assert (
            second.error_code
            == "conclusion_adjudication_field_reconciliation_read_only"
        )
        assert calls["add"] == 0
        assert effect["write_phase"] == "write_started"
        assert effect["adjudication_comment_attempt_count"] == 0
    with sqlite3.connect(store.db_path) as conn:
        [epoch] = conn.execute(
            "SELECT epoch_id, state, is_current FROM rca_activation_epochs"
        ).fetchall()
    assert epoch == ("epoch-w16-active", "bounded_active", 1)


def test_definite_field_prewrite_failure_can_retry_normally(tmp_path):
    store = _seed_published_conclusion(tmp_path)
    _record_retraction(store)
    fields = {RCA_RESULT_FIELD_KEY: "候选结论：感知车道线责任域"}
    comments = [
        {
            "remote_id": "original-comment",
            "content": "[RCA_DELIVERY:original] candidate conclusion",
        }
    ]
    calls = {"update": 0, "add": 0}

    class Clock:
        current = NOW

        def __call__(self):
            return self.current

    clock = Clock()

    def update_fields(_project_key, _work_item_id, updates):
        calls["update"] += 1
        if calls["update"] == 1:
            return {
                "success": False,
                "outcome_uncertain": False,
                "error_code": "meegle_field_update_rejected_prewrite",
            }
        fields.update(dict(updates))
        return {"success": True}

    def add_comment(_project_key, _work_item_id, content):
        calls["add"] += 1
        comments.append({"remote_id": "correction-comment", "content": content})
        return {"success": True, "remote_id": "correction-comment"}

    dispatcher = _dispatcher(
        store,
        now=clock,
        list_comments=lambda *_args: {
            "success": True,
            "comments": list(comments),
            "pages_read": 1,
        },
        add_comment=add_comment,
        get_fields=lambda _project, _issue, keys: {
            "success": True,
            "fields": {key: fields.get(key, "") for key in keys},
        },
        update_fields=update_fields,
    )
    first = dispatcher.dispatch_one()
    assert first.status == "retry_wait"
    assert first.error_code == "meegle_field_update_rejected_prewrite"
    correction = [
        row
        for row in store.list_rows("rca_delivery_effects")
        if row["effect_key"] != ORIGINAL_EFFECT_KEY
    ][0]
    assert correction["write_phase"] == "prewrite"
    clock.current = datetime.fromisoformat(first.next_attempt_at)

    second = dispatcher.dispatch_one()

    assert second.status == "succeeded"
    assert calls == {"update": 2, "add": 1}


def test_existing_correction_marker_with_field_drift_is_read_only(tmp_path):
    store = _seed_published_conclusion(tmp_path)
    result = _record_retraction(store)
    correction = next(
        row
        for row in store.list_rows("rca_delivery_effects")
        if row["effect_key"] == result.correction_effect_key
    )
    payload = json.loads(correction["payload_json"])
    calls = {"update": 0, "add": 0}

    dispatcher = _dispatcher(
        store,
        list_comments=lambda *_args: {
            "success": True,
            "comments": [
                {"remote_id": "existing-correction", "content": payload["comment_content"]}
            ],
            "pages_read": 1,
        },
        add_comment=lambda *_args: calls.__setitem__("add", calls["add"] + 1),
        get_fields=lambda _project, _issue, keys: {
            "success": True,
            "fields": {key: "field drift" for key in keys},
        },
        update_fields=lambda *_args: calls.__setitem__(
            "update", calls["update"] + 1
        ),
    )

    outcome = dispatcher.dispatch_one()

    assert outcome.status == "uncertain"
    assert (
        outcome.error_code
        == "conclusion_adjudication_field_reconciliation_read_only"
    )
    assert calls == {"update": 0, "add": 0}


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
    result = _record_retraction(store)
    _complete_artifact_repair(store, tmp_path, result)
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
    assert audit["ok"] is True
    assert all(audit["invariants"].values())
    assert audit["ga_acceptance_claimed"] is False


def test_artifact_repair_cannot_be_marked_succeeded_without_exact_receipt(tmp_path):
    store = _seed_published_conclusion(tmp_path)
    result = _record_retraction(store)

    with pytest.raises(
        DeliveryRecordConflictError,
        match="conclusion adjudication artifact receipt is required",
    ):
        store.mark_conclusion_adjudication_artifact_repair(
            adjudication_id=result.adjudication_id,
            succeeded=True,
            now=NOW,
        )

    repair = store.conclusion_adjudication_artifact_repair(result.adjudication_id)
    assert repair is not None
    assert repair["status"] == "pending"
    assert repair["receipt_path"] == ""
    assert repair["receipt_length"] == 0


def test_read_only_audit_rejects_tampered_exact_owner_review_receipt_line(
    tmp_path,
):
    store = _seed_published_conclusion(tmp_path)
    result = _record_retraction(store)
    persisted = _complete_artifact_repair(store, tmp_path, result)
    binding = persisted["receipt_binding"]
    receipt_path = binding["path"]
    with open(receipt_path, "r+b") as handle:
        handle.seek(binding["offset"])
        raw = handle.read(binding["length"])
        assert b"ou_owner" in raw
        tampered = raw.replace(b"ou_owner", b"ou_ownez", 1)
        assert len(tampered) == len(raw)
        handle.seek(binding["offset"])
        handle.write(tampered)
        handle.flush()
    with sqlite3.connect(store.db_path) as conn:
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")

    audit = audit_conclusion_adjudications(store.db_path)

    assert audit["ok"] is False
    assert audit["counts"]["ledger_payload_binding_mismatches"] == 1
    assert any(
        "conclusion_adjudication_artifact_receipt_hash_invalid" in item["error"]
        for item in audit["binding_validation_errors"]
    )


@pytest.mark.parametrize(
    "mutation",
    ["reason", "owner_name", "reviewed_at", "verdict", "action", "event_id"],
)
def test_read_only_audit_rejects_semantically_substituted_owner_receipt(
    tmp_path, mutation
):
    store = _seed_published_conclusion(tmp_path)
    result = _record_retraction(store)
    persisted = _complete_artifact_repair(store, tmp_path, result)
    binding = persisted["receipt_binding"]
    path = binding["path"]
    with open(path, "rb") as handle:
        handle.seek(binding["offset"])
        receipt = json.loads(handle.read(binding["length"]))
    if mutation == "reason":
        receipt["reason"] = "替换后的内部理由"
    elif mutation == "owner_name":
        receipt["owner_name"] = "Different Owner"
    elif mutation == "reviewed_at":
        receipt["reviewed_at"] = "2026-07-25T11:31:00+00:00"
    elif mutation == "verdict":
        receipt["verdict"] = "approved"
    elif mutation == "action":
        receipt["action"] = "通过"
    else:
        receipt["review_event_id"] = "g1q3-rca-owner-review-v1-" + "f" * 64
    if mutation != "event_id":
        material = {
            key: receipt.get(key)
            for key in (
                "issue_id",
                "action",
                "reason",
                "owner_id",
                "adjudication_id",
                "source",
            )
        }
        receipt["review_event_id"] = "g1q3-rca-owner-review-v1-" + hashlib.sha256(
            json.dumps(
                material,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
        ).hexdigest()
    replacement = (json.dumps(receipt, ensure_ascii=False, sort_keys=True) + "\n").encode(
        "utf-8"
    )
    with open(path, "ab") as handle:
        replacement_offset = handle.tell()
        handle.write(replacement)
        handle.flush()
    observed = os.stat(path)
    with sqlite3.connect(store.db_path) as conn:
        conn.execute(
            """
            UPDATE rca_conclusion_adjudication_repairs
               SET receipt_offset = ?, receipt_length = ?, receipt_sha256 = ?,
                   receipt_device = ?, receipt_inode = ?, receipt_event_id = ?
             WHERE adjudication_id = ?
            """,
            (
                replacement_offset,
                len(replacement),
                hashlib.sha256(replacement).hexdigest(),
                observed.st_dev,
                observed.st_ino,
                receipt["review_event_id"],
                result.adjudication_id,
            ),
        )
    with sqlite3.connect(store.db_path) as conn:
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")

    audit = audit_conclusion_adjudications(store.db_path)

    assert audit["ok"] is False
    assert audit["counts"]["ledger_payload_binding_mismatches"] == 1
    assert any(
        "conclusion_adjudication_artifact_receipt_ledger_mismatch" in item["error"]
        for item in audit["binding_validation_errors"]
    )


@pytest.mark.parametrize("link_kind", ["hardlink", "symlink"])
def test_artifact_repair_rejects_linked_receipt_paths(tmp_path, link_kind):
    store = _seed_published_conclusion(tmp_path)
    result = _record_retraction(store)
    persisted = _complete_artifact_repair(
        store,
        tmp_path,
        result,
        mark_succeeded=False,
    )
    binding = dict(persisted["receipt_binding"])
    linked = tmp_path / f"linked-{link_kind}.jsonl"
    if link_kind == "hardlink":
        os.link(binding["path"], linked)
    else:
        os.symlink(binding["path"], linked)
    binding["path"] = str(linked)

    with pytest.raises(
        ConclusionAdjudicationError,
        match="conclusion_adjudication_artifact_receipt_",
    ):
        store.mark_conclusion_adjudication_artifact_repair(
            adjudication_id=result.adjudication_id,
            succeeded=True,
            receipt_binding=binding,
            now=NOW,
        )

    repair = store.conclusion_adjudication_artifact_repair(result.adjudication_id)
    assert repair is not None
    assert repair["status"] == "pending"


@pytest.mark.parametrize(
    ("tamper", "error_fragment"),
    [
        ("content", "effect_content_invalid"),
        ("hash", "effect_hash_invalid"),
        ("source", "effect_lineage_invalid"),
        ("lineage", "effect_lineage_invalid"),
        ("original", "effect_ledger_mismatch"),
        ("correction_required", "correction_storage_binding_invalid"),
    ],
)
def test_read_only_audit_recomputes_every_binding_and_rejects_tampering(
    tmp_path, tamper, error_fragment
):
    store = _seed_published_conclusion(tmp_path)
    result = _record_retraction(store)
    _complete_artifact_repair(store, tmp_path, result)
    with sqlite3.connect(store.db_path) as conn:
        if tamper == "content":
            row = conn.execute(
                "SELECT payload_json FROM rca_delivery_effects "
                "WHERE effect_key = ?",
                (result.correction_effect_key,),
            ).fetchone()
            payload = json.loads(row[0])
            payload["comment_content"] = "tampered public correction"
            conn.execute(
                "UPDATE rca_delivery_effects SET payload_json = ? "
                "WHERE effect_key = ?",
                (json.dumps(payload), result.correction_effect_key),
            )
        elif tamper == "hash":
            conn.execute(
                "UPDATE rca_delivery_effects SET payload_sha256 = ? "
                "WHERE effect_key = ?",
                ("f" * 64, result.correction_effect_key),
            )
        elif tamper in {"source", "lineage"}:
            row = conn.execute(
                "SELECT source_json, lineage_json "
                "FROM rca_conclusion_adjudications WHERE adjudication_id = ?",
                (result.adjudication_id,),
            ).fetchone()
            if tamper == "source":
                value = json.loads(row[0])
                value["chat_id"] = "oc_tampered"
                column = "source_json"
            else:
                value = json.loads(row[1])
                value["responsibility_domain"] = "CONTROL_TAMPERED"
                column = "lineage_json"
            conn.execute("DROP TRIGGER trg_rca_conclusion_adjudication_no_update")
            conn.execute(
                f"UPDATE rca_conclusion_adjudications SET {column} = ? "
                "WHERE adjudication_id = ?",
                (
                    json.dumps(
                        value,
                        ensure_ascii=False,
                        separators=(",", ":"),
                        sort_keys=True,
                    ),
                    result.adjudication_id,
                ),
            )
            conn.executescript(
                """
                CREATE TRIGGER trg_rca_conclusion_adjudication_no_update
                BEFORE UPDATE ON rca_conclusion_adjudications
                BEGIN
                    SELECT RAISE(
                        ABORT, 'rca_conclusion_adjudication_immutable'
                    );
                END;
                """
            )
        elif tamper == "original":
            conn.execute(
                "UPDATE rca_delivery_effects SET status = 'quarantined' "
                "WHERE effect_key = ?",
                (ORIGINAL_EFFECT_KEY,),
            )
        else:
            conn.execute(
                "UPDATE rca_delivery_effects SET required = 0 "
                "WHERE effect_key = ?",
                (result.correction_effect_key,),
            )
    with sqlite3.connect(store.db_path) as conn:
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")

    audit = audit_conclusion_adjudications(store.db_path)

    assert audit["ok"] is False
    assert audit["counts"]["ledger_payload_binding_mismatches"] == 1
    assert any(
        error_fragment in item["error"]
        for item in audit["binding_validation_errors"]
    )


def test_audit_keeps_succeeded_correction_valid_after_epoch_rotation(tmp_path):
    store = _seed_published_conclusion(tmp_path)
    result = _record_retraction(store)
    _complete_artifact_repair(store, tmp_path, result)
    with sqlite3.connect(store.db_path) as conn:
        conn.execute(
            "UPDATE rca_delivery_effects "
            "SET status = 'succeeded', write_phase = 'settled', "
            "adjudication_comment_attempt_count = 1, completed_at = ? "
            "WHERE effect_key = ?",
            (NOW.isoformat(), result.correction_effect_key),
        )
        conn.execute(
            "UPDATE rca_activation_epochs SET is_current = 0 "
            "WHERE epoch_id = 'epoch-w16-active'"
        )
        conn.execute(
            "INSERT INTO rca_activation_epochs VALUES "
            "('epoch-w16-new', 'steady_active', 1)"
        )
    with sqlite3.connect(store.db_path) as conn:
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")

    audit = audit_conclusion_adjudications(store.db_path)

    assert audit["ok"] is True
    assert audit["counts"]["ledger_payload_binding_mismatches"] == 0
    assert audit["counts"]["activation_binding_violations"] == 0


def test_audit_rejects_unresolved_correction_after_epoch_rotation(tmp_path):
    store = _seed_published_conclusion(tmp_path)
    result = _record_retraction(store)
    _complete_artifact_repair(store, tmp_path, result)
    with sqlite3.connect(store.db_path) as conn:
        conn.execute(
            "UPDATE rca_activation_epochs SET is_current = 0 "
            "WHERE epoch_id = 'epoch-w16-active'"
        )
        conn.execute(
            "INSERT INTO rca_activation_epochs VALUES "
            "('epoch-w16-new', 'steady_active', 1)"
        )
    with sqlite3.connect(store.db_path) as conn:
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")

    audit = audit_conclusion_adjudications(store.db_path)

    assert audit["ok"] is False
    assert audit["counts"]["activation_binding_violations"] == 1
    assert audit["counts"]["ledger_payload_binding_mismatches"] == 1


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
    assert "撤回已提交" in str(response.response)
    assert "审计材料待修复" in str(response.response)
    assert len(store.list_rows("rca_conclusion_adjudications")) == 1
    assert len(store.list_rows("rca_delivery_effects")) == 2
    [adjudication] = store.list_rows("rca_conclusion_adjudications")
    repair = store.conclusion_adjudication_artifact_repair(
        adjudication["adjudication_id"]
    )
    assert repair is not None
    assert repair["status"] == "pending"
    assert repair["last_error_code"] == "OSError"


def test_owner_command_retry_repairs_postcommit_artifacts_once(
    tmp_path, monkeypatch
):
    db_parent = (
        tmp_path / "runtime" / "pnc_agent" / "feishu_issue_kafka_rca"
    )
    db_parent.mkdir(parents=True)
    store = _seed_published_conclusion(db_parent)
    monkeypatch.setenv("HERMES_G1Q3_REVIEW_OWNER_USER_IDS", "ou_owner")
    real_write_sidecar = owner_review_module._write_business_state_sidecar
    calls = 0

    def fail_once(**kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise OSError("injected sidecar failure")
        return real_write_sidecar(**kwargs)

    monkeypatch.setattr(
        owner_review_module,
        "_write_business_state_sidecar",
        fail_once,
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
        message_id="om_retract_repair",
    )

    first = handle_owner_review_message(event, hermes_home=tmp_path)
    second = handle_owner_review_message(event, hermes_home=tmp_path)

    assert "撤回已提交" in str(first.response)
    assert "审计材料待修复" in str(first.response)
    assert f"issue {ISSUE_ID} / 撤回" in str(second.response)
    [adjudication] = store.list_rows("rca_conclusion_adjudications")
    repair = store.conclusion_adjudication_artifact_repair(
        adjudication["adjudication_id"]
    )
    assert repair is not None
    assert repair["status"] == "succeeded"
    review_dir = tmp_path / "pnc_agent" / "reviews" / "g1q3_rca"
    receipt_lines = [
        line
        for path in review_dir.glob("owner_review-*.jsonl")
        for line in path.read_text(encoding="utf-8").splitlines()
    ]
    assert len(receipt_lines) == 1
    assert (
        review_dir
        / "business-states"
        / f"G1Q3-{ISSUE_ID}.business-state.yaml"
    ).is_file()
