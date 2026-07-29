from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from gateway.pnc_rca_admission import build_rca_admission
from gateway.pnc_rca_control_store import (
    CONTROL_STORE_SCHEMA_VERSION,
    MANUAL_TRIGGER_SCHEMA_VERSION,
    ManualRcaTriggerRequest,
    RcaControlStore,
)
from gateway.pnc_rca_delivery_contract import DeliveryContractError
from gateway.pnc_rca_delivery_store import (
    LEARNING_LANE_ADMISSION_MISSING_ERROR,
    LEARNING_LANE_EXTERNAL_EFFECT_ERROR,
    RcaDeliveryStore,
)
from gateway.pnc_rca_write_fence import ExternalWriteFenceError
from scripts.pnc_rca_delivery_dispatcher import DeliveryDispatcher


BEFORE_STOCK_CUTOFF = "2026-07-24T00:00:00+00:00"
AFTER_STOCK_CUTOFF = datetime(2026, 7, 26, tzinfo=timezone.utc)


def _insert_job(
    conn: sqlite3.Connection,
    *,
    delivery_id: str,
    submission_key: str,
    business_key: str,
    generation: int,
    work_item_id: str,
    target_key: str,
    created_at: str,
    status: str = "ready",
) -> None:
    conn.execute(
        """
        INSERT INTO rca_delivery_jobs(
            delivery_id, submission_key, business_key, generation,
            artifact_set_id, project_key, work_item_type_key, work_item_id,
            target_key, issue_url, report_url, status, manifest_json,
            contract_json, artifacts_json, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, 'g1q3', 'issue', ?, ?, '', '', ?, '{}', '{}',
                  '[]', ?, ?)
        """,
        (
            delivery_id,
            submission_key,
            business_key,
            generation,
            f"artifact-{delivery_id}",
            work_item_id,
            target_key,
            status,
            created_at,
            created_at,
        ),
    )


def _insert_issue_effect(
    conn: sqlite3.Connection,
    *,
    effect_key: str,
    delivery_id: str,
    target_key: str,
    created_at: str,
    status: str = "succeeded",
    payload: dict[str, object] | None = None,
    comment_slot: dict[str, object] | None = None,
) -> None:
    slot = comment_slot or {
        "comment_slot_budget_exempt": 0,
        "comment_slot_generation": None,
        "comment_slot_key": "",
        "comment_slot_kind": "",
        "comment_slot_revision": None,
        "comment_slot_schema_version": "",
    }
    conn.execute(
        """
        INSERT INTO rca_delivery_effects(
            effect_key, delivery_id, effect_kind, required, target_key,
            payload_json, payload_sha256, status, created_at, updated_at,
            completed_at, comment_slot_schema_version, comment_slot_key,
            comment_slot_kind, comment_slot_generation,
            comment_slot_revision, comment_slot_budget_exempt
        ) VALUES (?, ?, 'feishu_issue_comment', 1, ?, ?, ?, ?, ?, ?, ?,
                  ?, ?, ?, ?, ?, ?)
        """,
        (
            effect_key,
            delivery_id,
            target_key,
            json.dumps(
                payload or {"schema_version": "pnc_rca_delivery_effect_v1"},
                separators=(",", ":"),
            ),
            "a" * 64,
            status,
            created_at,
            created_at,
            created_at if status == "succeeded" else None,
            slot["comment_slot_schema_version"],
            slot["comment_slot_key"],
            slot["comment_slot_kind"],
            slot["comment_slot_generation"],
            slot["comment_slot_revision"],
            slot["comment_slot_budget_exempt"],
        ),
    )


def _insert_business_trigger(
    conn: sqlite3.Connection,
    *,
    business_key: str,
    generation: int,
    submission_key: str,
    work_item_id: str,
    created_at: str,
    origin_source_id: str | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO business_triggers(
            business_key, generation, submission_key, creation_rule_version,
            work_item_id, project_key, work_item_type_key, normalized_json,
            state, created_at
        ) VALUES (?, ?, ?, 'w6-test-rule', ?, 'g1q3', 'issue', '{}',
                  'accepted', ?)
        """,
        (
            business_key,
            generation,
            submission_key,
            work_item_id,
            created_at,
        ),
    )
    if origin_source_id is not None:
        conn.execute(
            "UPDATE business_triggers SET origin_source_id = ? "
            "WHERE business_key = ? AND generation = ?",
            (origin_source_id, business_key, generation),
        )


def _seed_learning_lane(tmp_path: Path):
    db_path = tmp_path / "w6.sqlite3"
    control = RcaControlStore(db_path)
    delivery = RcaDeliveryStore(db_path)
    stock_target = "feishu_project:g1q3:issue:1001"

    # The stock cohort is derived only from settled primary effects at the
    # immutable cutoff, just as the production seal job does.
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA foreign_keys=OFF")
        _insert_job(
            conn,
            delivery_id="stock-delivery",
            submission_key="stock-submission",
            business_key="stock-business",
            generation=1,
            work_item_id="1001",
            target_key=stock_target,
            created_at=BEFORE_STOCK_CUTOFF,
            status="delivered",
        )
        _insert_issue_effect(
            conn,
            effect_key="stock-effect",
            delivery_id="stock-delivery",
            target_key=stock_target,
            created_at=BEFORE_STOCK_CUTOFF,
        )

    cohort = control.seal_learning_lane_cohort(now=AFTER_STOCK_CUTOFF)
    admission = build_rca_admission(
        project_key="g1q3",
        project_simple_name="g1q3",
        work_item_type_key="issue",
        work_item_id="1001",
        rule_version="w6-test-rule",
    )
    with sqlite3.connect(db_path) as conn:
        _insert_business_trigger(
            conn,
            business_key=admission.business_key,
            generation=admission.generation,
            submission_key=admission.submission_key,
            work_item_id="1001",
            created_at=AFTER_STOCK_CUTOFF.isoformat(),
        )
    conn = control._connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        assert control._ensure_learning_lane_admission_tx(
            conn,
            admission=admission,
            current=AFTER_STOCK_CUTOFF.isoformat(),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return control, delivery, admission, cohort, db_path


def test_stock_member_without_admission_fails_closed_at_all_write_boundaries(tmp_path):
    _control, delivery, _admission, _cohort, db_path = _seed_learning_lane(tmp_path)
    business_key = "missing-admission-business"
    submission_key = "missing-admission-submission"
    delivery_id = "missing-admission-delivery"
    target = "feishu_project:g1q3:issue:1001"
    current = AFTER_STOCK_CUTOFF.isoformat()
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA foreign_keys=OFF")
        _insert_business_trigger(
            conn,
            business_key=business_key,
            generation=1,
            submission_key=submission_key,
            work_item_id="1001",
            created_at=current,
        )
        _insert_job(
            conn,
            delivery_id=delivery_id,
            submission_key=submission_key,
            business_key=business_key,
            generation=1,
            work_item_id="1001",
            target_key=target,
            created_at=current,
        )

        with pytest.raises(
            sqlite3.IntegrityError, match=LEARNING_LANE_ADMISSION_MISSING_ERROR
        ):
            conn.execute(
                """
                INSERT INTO rca_delivery_subscriptions(
                    subscription_key, business_key, generation, source_id,
                    effect_kind, target_key, target_json, required, status,
                    created_at, updated_at
                ) VALUES (?, ?, 1, NULL, 'feishu_issue_comment', ?, '{}', 1,
                          'pending', ?, ?)
                """,
                (
                    "missing-admission-subscription",
                    business_key,
                    target,
                    current,
                    current,
                ),
            )

        with pytest.raises(
            sqlite3.IntegrityError, match=LEARNING_LANE_ADMISSION_MISSING_ERROR
        ):
            _insert_issue_effect(
                conn,
                effect_key="missing-admission-effect",
                delivery_id=delivery_id,
                target_key=target,
                created_at=current,
                status="pending",
            )

    with pytest.raises(RuntimeError, match=LEARNING_LANE_ADMISSION_MISSING_ERROR):
        delivery.validate_learning_lane_external_operation(
            business_key=business_key,
            generation=1,
            operation="feishu_issue_comment",
        )

    conn = delivery._connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        with pytest.raises(
            DeliveryContractError, match=LEARNING_LANE_ADMISSION_MISSING_ERROR
        ):
            delivery.enforce_issue_comment_budget_tx(
                conn,
                delivery_id=delivery_id,
                business_key=business_key,
                generation=1,
                target_key=target,
                payload={"schema_version": "pnc_rca_delivery_effect_v1"},
            )
        conn.rollback()
    finally:
        conn.close()

    dispatcher = object.__new__(DeliveryDispatcher)
    dispatcher.store = delivery
    claim = SimpleNamespace(business_key=business_key, generation=1)
    with pytest.raises(
        ExternalWriteFenceError, match=LEARNING_LANE_ADMISSION_MISSING_ERROR
    ):
        dispatcher._validate_external_write(
            claim,
            operation="feishu_issue_comment",
            target=target,
        )


def test_comment_budget_cannot_borrow_rerun_authority_from_another_business_key(tmp_path):
    _control, delivery, _admission, _cohort, db_path = _seed_learning_lane(tmp_path)
    current = AFTER_STOCK_CUTOFF.isoformat()
    target = "feishu_project:g1q3:issue:3001"
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA foreign_keys=OFF")
        for business_key in ("borrow-authority", "borrow-target"):
            _insert_business_trigger(
                conn,
                business_key=business_key,
                generation=2,
                submission_key=f"{business_key}-submission",
                work_item_id="3001",
                created_at=current,
            )
            _insert_job(
                conn,
                delivery_id=f"{business_key}-delivery",
                submission_key=f"{business_key}-submission",
                business_key=business_key,
                generation=2,
                work_item_id="3001",
                target_key=target,
                created_at=current,
            )
        conn.execute(
            """
            INSERT INTO rca_trigger_sources(
                source_id, source_kind, source_dedupe_key, payload_sha256,
                platform, chat_id, thread_id, message_id, requester_id, mode,
                created_at
            ) VALUES ('borrow-source', 'feishu_group_manual', 'borrow-dedupe',
                      ?, 'feishu', 'oc_test', 'topic:root', 'om_test', ?,
                      'rerun', ?)
            """,
            ("c" * 64, "ou_" + "b" * 32, current),
        )
        conn.execute(
            """
            INSERT INTO rca_trigger_bindings(
                source_id, business_key, generation, role, bound_at
            ) VALUES ('borrow-source', 'borrow-authority', 2, 'origin', ?)
            """,
            (current,),
        )
        conn.execute(
            "UPDATE business_triggers SET origin_source_id = 'borrow-source' "
            "WHERE business_key = 'borrow-authority' AND generation = 2"
        )

    conn = delivery._connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        with pytest.raises(
            DeliveryContractError,
            match="delivery_comment_budget_generation_not_user_rerun",
        ):
            delivery.enforce_issue_comment_budget_tx(
                conn,
                delivery_id="borrow-target-delivery",
                business_key="borrow-target",
                generation=2,
                target_key=target,
                payload={"schema_version": "pnc_rca_delivery_effect_v1"},
            )
        conn.rollback()
    finally:
        conn.close()
def test_v11_marker_migrates_the_v12_learning_schema(tmp_path):
    db_path = tmp_path / "migration.sqlite3"
    store = RcaControlStore(db_path)
    conn = store._connect()
    try:
        conn.execute("PRAGMA foreign_keys=OFF")
        for trigger in (
            "trg_learning_lane_admission_cohort_binding",
            "trg_learning_lane_admission_no_delete",
            "trg_learning_lane_admission_no_update",
            "trg_learning_lane_stock_item_no_delete",
            "trg_learning_lane_stock_item_no_update",
            "trg_learning_lane_stock_item_no_append",
            "trg_learning_lane_cohort_no_replace",
            "trg_learning_lane_cohort_no_delete",
            "trg_learning_lane_cohort_no_update",
        ):
            conn.execute(f"DROP TRIGGER {trigger}")
        conn.execute("DROP TABLE rca_learning_lane_admissions")
        conn.execute("DROP TABLE rca_learning_lane_stock_items")
        conn.execute("DROP TABLE rca_learning_lane_cohorts")
        conn.execute(
            "UPDATE control_meta SET value = 'pnc_rca_control_store_v11' "
            "WHERE key = 'schema_version'"
        )
    finally:
        conn.close()

    migrated = RcaControlStore(db_path)
    assert migrated.health()["schema_version"] == CONTROL_STORE_SCHEMA_VERSION
    assert migrated.initialization_observation()["mode"] == "migration"
    assert migrated.list_rows("rca_learning_lane_cohorts") == []
    assert migrated.list_rows("rca_learning_lane_admissions") == []


def test_learning_cohort_and_admission_are_immutable(tmp_path):
    control, _delivery, admission, cohort, db_path = _seed_learning_lane(tmp_path)

    assert cohort["stock_count"] == 1
    assert cohort["stock_cutoff"] == "2026-07-25T10:15:43.473251+00:00"
    assert control.learning_lane_admission(admission.business_key, 1) == {
        "business_key": admission.business_key,
        "generation": 1,
        "work_item_id": "1001",
        "schema_version": "g1q3_rca_learning_lane_admission_v1",
        "lane": "learning",
        "reason": "stock",
        "external_write_allowed": 0,
        "cohort_id": cohort["cohort_id"],
        "stock_cutoff": cohort["stock_cutoff"],
        "stock_ids_sha256": cohort["stock_ids_sha256"],
        "admitted_at": AFTER_STOCK_CUTOFF.isoformat(),
    }
    assert control.seal_learning_lane_cohort(now=AFTER_STOCK_CUTOFF) == cohort

    with sqlite3.connect(db_path) as conn:
        with pytest.raises(sqlite3.IntegrityError, match="learning_lane_cohort_immutable"):
            conn.execute(
                "UPDATE rca_learning_lane_cohorts SET sealed_at = 'changed'"
            )
        with pytest.raises(sqlite3.IntegrityError, match="learning_lane_cohort_immutable"):
            conn.execute(
                "INSERT INTO rca_learning_lane_stock_items(cohort_id, work_item_id) "
                "VALUES (?, 'new-item')",
                (cohort["cohort_id"],),
            )
        with pytest.raises(sqlite3.IntegrityError, match="learning_lane_admission_immutable"):
            conn.execute(
                "UPDATE rca_learning_lane_admissions SET lane = 'learning'"
            )


def test_learning_admission_has_no_issue_or_thread_subscriptions(tmp_path):
    control, delivery, admission, _cohort, db_path = _seed_learning_lane(tmp_path)
    request = ManualRcaTriggerRequest(
        schema_version=MANUAL_TRIGGER_SCHEMA_VERSION,
        issue_url="https://project.feishu.cn/g1q3/issue/detail/1001",
        mode="rerun",
        reason="w6_test",
        platform="feishu",
        chat_id="oc_test",
        thread_id="topic:root",
        message_id="om_test",
        requester_id="ou_" + "a" * 32,
    )
    conn = control._connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        assert control._insert_issue_subscription_tx(
            conn, admission=admission, current=AFTER_STOCK_CUTOFF.isoformat()
        ) == ""
        thread_key, created = control._insert_thread_subscription_tx(
            conn,
            admission=admission,
            source_id="unused-source",
            request=request,
            current=AFTER_STOCK_CUTOFF.isoformat(),
        )
        assert thread_key == ""
        assert created is False
        conn.commit()
    finally:
        conn.close()
    assert delivery.list_rows("rca_delivery_subscriptions") == []
    assert control.learning_lane_admission(admission.business_key, 1) is not None


def test_learning_lane_quarantines_materialization_and_blocks_provider(tmp_path):
    control, delivery, admission, _cohort, db_path = _seed_learning_lane(tmp_path)
    target = "feishu_project:g1q3:issue:1001"
    current = AFTER_STOCK_CUTOFF.isoformat()
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA foreign_keys=OFF")
        _insert_job(
            conn,
            delivery_id="learning-delivery",
            submission_key="learning-submission",
            business_key=admission.business_key,
            generation=1,
            work_item_id="1001",
            target_key=target,
            created_at=current,
        )
        conn.execute(
            """
            INSERT INTO rca_delivery_subscriptions(
                subscription_key, business_key, generation, source_id,
                effect_kind, target_key, target_json, required, status,
                created_at, updated_at
            ) VALUES (?, ?, 1, NULL, 'feishu_issue_comment', ?, ?, 1,
                      'pending', ?, ?)
            """,
            (
                "learning-subscription",
                admission.business_key,
                target,
                json.dumps(
                    {
                        "schema_version": "pnc_rca_delivery_target_v1",
                        "platform": "feishu_project",
                        "project_key": "g1q3",
                        "work_item_type_key": "issue",
                        "work_item_id": "1001",
                        "output_cap": "L1",
                    },
                    separators=(",", ":"),
                ),
                current,
                current,
            ),
        )

    result = delivery.materialize_pending_subscriptions(now=AFTER_STOCK_CUTOFF)
    assert result.materialized == 0
    assert result.quarantined == 1
    with sqlite3.connect(db_path) as conn:
        assert conn.execute(
            "SELECT status FROM rca_delivery_subscriptions "
            "WHERE subscription_key = 'learning-subscription'"
        ).fetchone()[0] == "quarantined"
        assert conn.execute(
            "SELECT status FROM rca_delivery_jobs WHERE delivery_id = 'learning-delivery'"
        ).fetchone()[0] == "quarantined"
        with pytest.raises(sqlite3.IntegrityError, match=LEARNING_LANE_EXTERNAL_EFFECT_ERROR):
            _insert_issue_effect(
                conn,
                effect_key="forbidden-effect",
                delivery_id="learning-delivery",
                target_key=target,
                created_at=current,
                status="pending",
            )

    delivery.validate_learning_lane_external_operation(
        business_key=admission.business_key,
        generation=1,
        operation="internal_alert",
    )
    with pytest.raises(RuntimeError, match=LEARNING_LANE_EXTERNAL_EFFECT_ERROR):
        delivery.validate_learning_lane_external_operation(
            business_key=admission.business_key,
            generation=1,
            operation="feishu_issue_comment",
        )

    dispatcher = object.__new__(DeliveryDispatcher)
    dispatcher.store = delivery
    claim = SimpleNamespace(
        business_key=admission.business_key,
        generation=1,
    )
    with pytest.raises(ExternalWriteFenceError, match=LEARNING_LANE_EXTERNAL_EFFECT_ERROR):
        dispatcher._validate_external_write(
            claim,
            operation="feishu_issue_comment",
            target=target,
        )


def test_issue_comment_budget_allows_initial_adjudication_and_explicit_rerun_only(
    tmp_path,
):
    _control, delivery, _admission, _cohort, db_path = _seed_learning_lane(tmp_path)
    target = "feishu_project:g1q3:issue:2001"
    current = AFTER_STOCK_CUTOFF.isoformat()
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA foreign_keys=OFF")
        for generation in (1, 2, 3):
            business_key = f"normal-business-{generation}"
            submission_key = f"normal-submission-{generation}"
            _insert_business_trigger(
                conn,
                business_key=business_key,
                generation=generation,
                submission_key=submission_key,
                work_item_id="2001",
                created_at=current,
            )
            _insert_job(
                conn,
                delivery_id=f"normal-delivery-{generation}",
                submission_key=submission_key,
                business_key=business_key,
                generation=generation,
                work_item_id="2001",
                target_key=target,
                created_at=current,
            )

    def enforce(
        conn: sqlite3.Connection,
        *,
        delivery_id: str,
        business_key: str,
        generation: int,
        payload: dict[str, object] | None = None,
        target_key: str = target,
    ) -> dict[str, object]:
        return delivery.enforce_issue_comment_budget_tx(
            conn,
            delivery_id=delivery_id,
            business_key=business_key,
            generation=generation,
            target_key=target_key,
            payload=payload or {"schema_version": "pnc_rca_delivery_effect_v1"},
        )

    conn = delivery._connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        slot = enforce(
            conn,
            delivery_id="normal-delivery-1",
            business_key="normal-business-1",
            generation=1,
        )
        _insert_issue_effect(
            conn,
            effect_key="normal-primary-1",
            delivery_id="normal-delivery-1",
            target_key=target,
            created_at=current,
            comment_slot=slot,
        )
        conn.commit()
    finally:
        conn.close()

    conn = delivery._connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        with pytest.raises(DeliveryContractError, match="delivery_comment_budget_exhausted"):
            enforce(
                conn,
                delivery_id="normal-delivery-1",
                business_key="normal-business-1",
                generation=1,
            )
        conn.rollback()
    finally:
        conn.close()

    conn = delivery._connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        with pytest.raises(
            DeliveryContractError,
            match="delivery_comment_budget_generation_not_user_rerun",
        ):
            enforce(
                conn,
                delivery_id="normal-delivery-2",
                business_key="normal-business-2",
                generation=2,
            )
        conn.rollback()
    finally:
        conn.close()

    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA foreign_keys=OFF")
        conn.execute(
            """
            INSERT INTO rca_trigger_sources(
                source_id, source_kind, source_dedupe_key, payload_sha256,
                platform, chat_id, thread_id, message_id, requester_id, mode,
                created_at
            ) VALUES ('rerun-source-2', 'feishu_group_manual', 'rerun-dedupe-2',
                      ?, 'feishu', 'oc_test', 'topic:root', 'om_test', ?,
                      'rerun', ?)
            """,
            ("b" * 64, "ou_" + "a" * 32, current),
        )
        conn.execute(
            """
            INSERT INTO rca_trigger_bindings(
                source_id, business_key, generation, role, bound_at
            ) VALUES ('rerun-source-2', 'normal-business-2', 2, 'origin', ?)
            """,
            (current,),
        )
        conn.execute(
            "UPDATE business_triggers SET origin_source_id = ? "
            "WHERE business_key = ? AND generation = 2",
            ("rerun-source-2", "normal-business-2"),
        )

    conn = delivery._connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        slot = enforce(
            conn,
            delivery_id="normal-delivery-2",
            business_key="normal-business-2",
            generation=2,
        )
        _insert_issue_effect(
            conn,
            effect_key="normal-primary-2",
            delivery_id="normal-delivery-2",
            target_key=target,
            created_at=current,
            comment_slot=slot,
        )
        conn.commit()
    finally:
        conn.close()

    conn = delivery._connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        with pytest.raises(DeliveryContractError, match="delivery_comment_budget_exhausted"):
            enforce(
                conn,
                delivery_id="normal-delivery-2",
                business_key="normal-business-2",
                generation=2,
            )
        conn.rollback()
    finally:
        conn.close()

    adjudication_target = "g1q3-rca-adjudication-target-v1-2001"
    adjudication_payload = {
        "schema_version": "pnc_rca_conclusion_adjudication_effect_v2"
    }
    conn = delivery._connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        slot = enforce(
            conn,
            delivery_id="normal-delivery-1",
            business_key="normal-business-1",
            generation=1,
            payload=adjudication_payload,
            target_key=adjudication_target,
        )
        _insert_issue_effect(
            conn,
            effect_key="adjudication-effect-1",
            delivery_id="normal-delivery-1",
            target_key=adjudication_target,
            created_at=current,
            payload=adjudication_payload,
            comment_slot=slot,
        )
        conn.commit()
    finally:
        conn.close()

    conn = delivery._connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        with pytest.raises(
            DeliveryContractError,
            match="conclusion_adjudication_comment_budget_exhausted",
        ):
            enforce(
                conn,
                delivery_id="normal-delivery-1",
                business_key="normal-business-1",
                generation=1,
                payload=adjudication_payload,
                target_key=adjudication_target,
            )
        conn.rollback()
    finally:
        conn.close()
