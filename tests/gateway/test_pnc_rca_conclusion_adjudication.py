from __future__ import annotations

from datetime import datetime, timezone
import json
import sqlite3
from types import SimpleNamespace

import pytest

from gateway import pnc_rca_owner_review as owner_review_module
from gateway.pnc_rca_conclusion_adjudication import (
    ADJUDICATION_EFFECT_SCHEMA_VERSION,
    ADJUDICATION_EFFECT_SCHEMA_VERSION_V1,
    ADJUDICATION_EFFECT_TARGET_PREFIX,
    ConclusionAdjudicationError,
    identifies_adjudication_effect,
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
    RcaDeliveryStore,
)
from gateway.pnc_rca_owner_review import handle_owner_review_message
from gateway.session import SessionSource
from scripts.pnc_rca_delivery_dispatcher import DeliveryDispatcher
from scripts.pnc_rca_conclusion_adjudication_audit import (
    EXPECTED_DELIVERY_STORE_SCHEMA_VERSION,
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
        },
    }
    manifest = {"submission_key": submission_key}
    payload = {
        "schema_version": DELIVERY_EFFECT_SCHEMA_VERSION,
        "conclusion": "候选结论：感知车道线责任域",
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


def test_recognize_fails_closed_without_validated_publication_contract(tmp_path):
    store = _seed_published_conclusion(tmp_path)

    with pytest.raises(
        ConclusionAdjudicationError,
        match=(
            "conclusion_adjudication_recognize_"
            "publication_contract_unavailable"
        ),
    ):
        store.record_conclusion_adjudication(
            work_item_id=ISSUE_ID,
            action="recognize",
            reason="问题单缺少问题数据地址",
            replacement_conclusion="请核对问题数据地址",
            actor_id="ou_owner",
            actor_name="RCA Owner",
            source={"platform": "feishu", "chat_id": "oc_g1q3"},
            now=NOW,
        )

    assert store.list_rows("rca_conclusion_adjudications") == []
    assert len(store.list_rows("rca_delivery_effects")) == 1


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


def test_v7_marker_requires_explicit_v8_migration(tmp_path):
    assert EXPECTED_DELIVERY_STORE_SCHEMA_VERSION == DELIVERY_STORE_SCHEMA_VERSION
    store = RcaDeliveryStore(tmp_path / "control.sqlite3")
    with sqlite3.connect(store.db_path) as conn:
        conn.execute(
            "UPDATE rca_delivery_meta SET value = 'pnc_rca_delivery_store_v7' "
            "WHERE key = 'schema_version'"
        )
        conn.execute("DROP TABLE rca_conclusion_adjudication_repairs")

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


def test_v7_migrates_to_combined_w2_w16_v8_schema(tmp_path):
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
    assert {"adjudication_id", "status", "attempt_count"} <= repair_columns


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
    store.mark_conclusion_adjudication_artifact_repair(
        adjudication_id=result.adjudication_id,
        succeeded=True,
        now=NOW,
    )
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
    store.mark_conclusion_adjudication_artifact_repair(
        adjudication_id=result.adjudication_id,
        succeeded=True,
        now=NOW,
    )
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
    store.mark_conclusion_adjudication_artifact_repair(
        adjudication_id=result.adjudication_id,
        succeeded=True,
        now=NOW,
    )
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
    store.mark_conclusion_adjudication_artifact_repair(
        adjudication_id=result.adjudication_id,
        succeeded=True,
        now=NOW,
    )
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
