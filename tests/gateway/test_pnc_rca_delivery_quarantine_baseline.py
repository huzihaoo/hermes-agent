from __future__ import annotations

from datetime import timedelta
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3

import pytest

from gateway import pnc_rca_delivery_quarantine_baseline as baseline_module
from gateway import pnc_rca_delivery_store as delivery_store_module
from gateway.pnc_rca_delivery_contract import (
    DELIVERY_EFFECT_KIND,
    TERMINAL_DELIVERY_EFFECT_SCHEMA_VERSION_COMMENT_ONLY,
    build_terminal_delivery,
    compute_terminal_delivery_effect_key,
    compute_terminal_delivery_effect_payload_sha256,
    terminal_delivery_effect_marker,
)
from gateway.pnc_rca_delivery_quarantine_baseline import (
    APPROVAL_SCHEMA_VERSION,
    CORE_SCHEMA_VERSION,
    DEFERRED_CORE_SCHEMA_VERSION,
    DeliveryQuarantineBaselineError,
    _activation_deferred_materialized_projection,
    _db_identity_matches_baseline,
    _quarantined_subscription_projection,
    build_quarantine_core,
    build_quarantine_core_from_offline_clone,
    build_successor_preactivation_quarantine_core,
    canonical_quarantine_baseline_bytes,
    issue_quarantine_baseline,
)
from gateway.pnc_rca_delivery_quarantine_migration import (
    QuarantineMigrationError,
    assert_combined_live_post_migration_matches,
    assert_combined_live_pre_migration_matches,
    assert_live_post_migration_matches,
    assert_live_pre_migration_matches,
    build_combined_offline_migration_receipt,
    build_offline_migration_receipt,
    canonical_migration_receipt_bytes,
    logical_database_projection,
    validate_combined_migration_receipt,
)
from gateway.pnc_rca_delivery_store import RcaDeliveryStore
from gateway.pnc_rca_control_store import RcaControlStore
from tests.gateway.test_pnc_rca_delivery_store import (
    NOW,
    _control,
    _delivery,
    _delivery_observation,
    _insert_subscription,
)


MIGRATION_RUNTIME_SHA256 = "9" * 64


def _json_bytes(value) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode()


def test_baseline_identity_allows_only_safe_off_epoch_binding() -> None:
    def identity(activation_sha256: str) -> dict[str, str]:
        body = {
            "path": "/clone/control.sqlite3",
            "control_schema_version": "pnc_rca_control_store_v13",
            "delivery_schema_version": "pnc_rca_delivery_store_v11",
            "activation_db_logical_identity_sha256": activation_sha256,
        }
        return {
            **body,
            "logical_identity_sha256": hashlib.sha256(
                _json_bytes(body).removesuffix(b"\n")
            ).hexdigest(),
        }

    baseline = identity("")
    current = identity("a" * 64)

    assert _db_identity_matches_baseline(baseline, baseline) is True
    assert _db_identity_matches_baseline(baseline, current) is True
    assert _db_identity_matches_baseline(current, baseline) is False
    assert _db_identity_matches_baseline(current, identity("b" * 64)) is False
    assert _db_identity_matches_baseline(
        baseline,
        {**current, "logical_identity_sha256": baseline["logical_identity_sha256"]},
    ) is False
    assert _db_identity_matches_baseline(
        baseline,
        {**current, "path": "/other/control.sqlite3"},
    ) is False


def test_terminal_quarantine_without_external_write_uses_io_facts(tmp_path) -> None:
    conn = sqlite3.connect(tmp_path / "attempts.sqlite3")
    conn.row_factory = sqlite3.Row
    conn.execute(
        "CREATE TABLE rca_delivery_attempts("
        "effect_key TEXT, outcome TEXT, remote_id TEXT, error_code TEXT, "
        "attempt_no INTEGER, event_seq INTEGER)"
    )
    conn.executemany(
        "INSERT INTO rca_delivery_attempts VALUES(?, ?, ?, ?, ?, ?)",
        [
            ("effect-1", "started", "", "", 1, 1),
            ("effect-1", "nack", "", "transient_prewrite_block", 1, 2),
            ("effect-1", "quarantined", "", "arbitrary_prewrite_block", 2, 1),
        ],
    )
    row = {
        "effect_key": "effect-1",
        "last_error_code": "arbitrary_prewrite_block",
        "job_status": "quarantined",
        "effect_status": "quarantined",
        "write_phase": "settled",
        "quarantined_at": NOW.isoformat(),
        "remote_receipt_json": None,
        "write_started_at": None,
        "completed_at": None,
        "attempt": 1,
        "fence": 1,
    }

    assert baseline_module._prewrite_quarantine_without_external_write(conn, row)
    assert not baseline_module._prewrite_quarantine_without_external_write(
        conn, {**row, "write_started_at": NOW.isoformat()}
    )
    conn.execute(
        "UPDATE rca_delivery_attempts SET remote_id='remote-write' "
        "WHERE outcome='quarantined'"
    )
    assert not baseline_module._prewrite_quarantine_without_external_write(conn, row)
    conn.close()


def _write(path: Path, value, *, mode: int = 0o600) -> str:
    path.write_bytes(_json_bytes(value))
    path.chmod(mode)
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _rows(store: RcaDeliveryStore) -> dict[str, list[dict]]:
    return {
        table: [dict(row) for row in store.list_rows(table)]
        for table in (
            "rca_delivery_jobs",
            "rca_delivery_effects",
            "rca_delivery_subscriptions",
        )
    }


_V11_W6_TRIGGER_SQL = (
    """
    CREATE TRIGGER trg_learning_lane_stock_effect_insert_forbidden
    BEFORE INSERT ON rca_delivery_effects
    WHEN NEW.effect_kind LIKE 'feishu_%'
     AND EXISTS (
         SELECT 1
           FROM rca_delivery_jobs AS job
           JOIN business_triggers AS bt
             ON bt.business_key = job.business_key
            AND bt.generation = job.generation
           JOIN rca_learning_lane_cohorts AS cohort
           JOIN rca_learning_lane_stock_items AS item
             ON item.cohort_id = cohort.cohort_id
            AND item.work_item_id = job.work_item_id
          WHERE job.delivery_id = NEW.delivery_id
            AND (
                julianday(bt.created_at) IS NULL
                OR julianday(cohort.stock_cutoff) IS NULL
                OR julianday(bt.created_at) > julianday(cohort.stock_cutoff)
            )
            AND NOT EXISTS (
                SELECT 1
                  FROM rca_learning_lane_admissions AS admission
                 WHERE admission.business_key = job.business_key
                   AND admission.generation = job.generation
                   AND admission.lane = 'learning'
                   AND admission.external_write_allowed = 0
            )
     )
    BEGIN
        SELECT RAISE(ABORT, 'learning_lane_admission_missing');
    END
    """,
    """
    CREATE TRIGGER trg_learning_lane_stock_subscription_insert_forbidden
    BEFORE INSERT ON rca_delivery_subscriptions
    WHEN NEW.effect_kind LIKE 'feishu_%'
     AND EXISTS (
         SELECT 1
           FROM business_triggers AS bt
           JOIN rca_learning_lane_cohorts AS cohort
           JOIN rca_learning_lane_stock_items AS item
             ON item.cohort_id = cohort.cohort_id
            AND item.work_item_id = bt.work_item_id
          WHERE bt.business_key = NEW.business_key
            AND bt.generation = NEW.generation
            AND (
                julianday(bt.created_at) IS NULL
                OR julianday(cohort.stock_cutoff) IS NULL
                OR julianday(bt.created_at) > julianday(cohort.stock_cutoff)
            )
            AND NOT EXISTS (
                SELECT 1
                  FROM rca_learning_lane_admissions AS admission
                 WHERE admission.business_key = NEW.business_key
                   AND admission.generation = NEW.generation
                   AND admission.lane = 'learning'
                   AND admission.external_write_allowed = 0
            )
     )
    BEGIN
        SELECT RAISE(ABORT, 'learning_lane_admission_missing');
    END
    """,
    """
    CREATE TRIGGER trg_learning_lane_stock_subscription_update_forbidden
    BEFORE UPDATE OF business_key, generation, effect_kind
        ON rca_delivery_subscriptions
    WHEN NEW.effect_kind LIKE 'feishu_%'
     AND EXISTS (
         SELECT 1
           FROM business_triggers AS bt
           JOIN rca_learning_lane_cohorts AS cohort
           JOIN rca_learning_lane_stock_items AS item
             ON item.cohort_id = cohort.cohort_id
            AND item.work_item_id = bt.work_item_id
          WHERE bt.business_key = NEW.business_key
            AND bt.generation = NEW.generation
            AND (
                julianday(bt.created_at) IS NULL
                OR julianday(cohort.stock_cutoff) IS NULL
                OR julianday(bt.created_at) > julianday(cohort.stock_cutoff)
            )
            AND NOT EXISTS (
                SELECT 1
                  FROM rca_learning_lane_admissions AS admission
                 WHERE admission.business_key = NEW.business_key
                   AND admission.generation = NEW.generation
                   AND admission.lane = 'learning'
                   AND admission.external_write_allowed = 0
            )
     )
    BEGIN
        SELECT RAISE(ABORT, 'learning_lane_admission_missing');
    END
    """,
)


def _restore_v11_w6_triggers(path: Path, *, update_marker: bool = False) -> None:
    with sqlite3.connect(path) as conn:
        for name in (
            "trg_learning_lane_stock_effect_insert_forbidden",
            "trg_learning_lane_stock_subscription_insert_forbidden",
            "trg_learning_lane_stock_subscription_update_forbidden",
        ):
            conn.execute(f"DROP TRIGGER IF EXISTS {name}")
        for statement in _V11_W6_TRIGGER_SQL:
            conn.execute(statement)
        if update_marker:
            conn.execute(
                "UPDATE rca_delivery_meta SET value = 'pnc_rca_delivery_store_v11' "
                "WHERE key = 'schema_version'"
            )
        conn.commit()
    path.chmod(0o600)


def _normalize_sqlite_copy(path: Path) -> None:
    """Materialize a standalone DELETE-journal copy after WAL writes finish."""
    normalized = path.with_name(f"{path.name}.standalone")
    if normalized.exists():
        normalized.unlink()
    source = sqlite3.connect(path)
    destination = sqlite3.connect(normalized)
    try:
        source.backup(destination)
        destination.execute("PRAGMA journal_mode=DELETE")
        destination.commit()
    finally:
        destination.close()
        source.close()
    os.replace(normalized, path)
    for suffix in ("-wal", "-shm", "-journal"):
        sidecar = Path(f"{path}{suffix}")
        if sidecar.exists():
            sidecar.unlink()
    path.chmod(0o600)


def _downgrade_delivery_clone_to_v11(path: Path) -> None:
    with sqlite3.connect(path) as conn:
        tables = {
            str(row[0])
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
    if {
        "business_triggers",
        "rca_learning_lane_admissions",
        "rca_learning_lane_cohorts",
        "rca_learning_lane_stock_items",
    }.issubset(tables):
        _restore_v11_w6_triggers(path, update_marker=True)
    else:
        with sqlite3.connect(path) as conn:
            conn.execute(
                "UPDATE rca_delivery_meta SET value = 'pnc_rca_delivery_store_v11' "
                "WHERE key = 'schema_version'"
            )
            conn.commit()
    path.chmod(0o600)


def _seed_quarantine(root: Path) -> tuple[RcaDeliveryStore, str]:
    root.mkdir(parents=True, exist_ok=True)
    _control(root)
    store = RcaDeliveryStore(root / "control.sqlite3")
    assert store.backfill_completed_submissions(now=NOW) == 1
    claim = store.claim_due_watch(lease_owner="collector", now=NOW)
    assert claim is not None
    _insert_subscription(
        store,
        claim,
        effect_kind="feishu_thread_reply",
        invalid_thread=True,
    )
    with sqlite3.connect(store.db_path) as conn:
        conn.execute(
            """
            INSERT INTO rca_trigger_sources(
                source_id, source_kind, source_dedupe_key, payload_sha256,
                platform, chat_id, thread_id, message_id, requester_id,
                mode, outcome, created_at
            ) VALUES (
                'source-manual-baseline', 'feishu_group_manual',
                'dedupe-manual-baseline', ?, 'feishu', 'oc_group123',
                'topic:om_root123', 'om_trigger456', 'ou_requester789',
                'rerun', 'joined', ?
            )
            """,
            ("a" * 64, NOW.isoformat()),
        )
        conn.execute(
            "UPDATE rca_delivery_subscriptions "
            "SET source_id = 'source-manual-baseline' "
            "WHERE effect_kind = 'feishu_thread_reply'"
        )
    store.create_delivery(
        claim=claim,
        delivery=_delivery(claim),
        status={"success": True, "state": "completed"},
        now=NOW,
    )
    effect = store.claim_due_effect(lease_owner="dispatcher", now=NOW)
    assert effect is not None
    store.complete_effect(
        claim=effect,
        outcome="ack",
        remote_id="comment-baseline",
        receipt={"remote_id": "comment-baseline"},
        observation=_delivery_observation(
            effect, remote_receipt_id="comment-baseline"
        ),
        now=NOW,
    )
    [observation_intent] = store.list_pending_delivery_observations()
    store.mark_delivery_observation_appended(
        observation_id=observation_intent.observation_id,
        payload_sha256=observation_intent.payload_sha256,
        now=NOW,
    )
    with sqlite3.connect(store.db_path) as conn:
        conn.execute(
            "UPDATE rca_delivery_effects SET status = 'quarantined', "
            "last_error_code = 'operator_superseded_test', updated_at = ? "
            "WHERE effect_key = ?",
            ((NOW + timedelta(seconds=1)).isoformat(), effect.effect_key),
        )
    return store, effect.effect_key


def _mark_issue_comment_activation_deferred(
    store: RcaDeliveryStore, *, reason: str = "activation_epoch_deferred"
) -> None:
    with sqlite3.connect(store.db_path) as conn:
        cursor = conn.execute(
            """
            UPDATE rca_delivery_subscriptions
               SET status = 'quarantined', reason = ?, source_id = NULL,
                   delivery_id = NULL, effect_key = NULL, updated_at = ?
             WHERE effect_kind = 'feishu_issue_comment'
            """,
            (reason, (NOW + timedelta(seconds=1)).isoformat()),
        )
        assert cursor.rowcount == 1


def _seed_manual_dual_effect_quarantine(
    root: Path, *, source_kind: str = "feishu_group_manual"
) -> tuple[RcaDeliveryStore, list[str]]:
    root.mkdir(parents=True, exist_ok=True)
    _control(root)
    store = RcaDeliveryStore(root / "control.sqlite3")
    assert store.backfill_completed_submissions(now=NOW) == 1
    claim = store.claim_due_watch(lease_owner="collector", now=NOW)
    assert claim is not None
    _insert_subscription(
        store,
        claim,
        effect_kind="feishu_thread_reply",
        invalid_thread=False,
    )
    with sqlite3.connect(store.db_path) as conn:
        conn.execute(
            """
            INSERT INTO rca_trigger_sources(
                source_id, source_kind, source_dedupe_key, payload_sha256,
                platform, chat_id, thread_id, message_id, requester_id,
                mode, outcome, created_at
            ) VALUES (
                'source-manual-dual-baseline', ?,
                'dedupe-manual-dual-baseline', ?, 'feishu', 'oc_group123',
                'topic:om_root123', 'om_trigger456', 'ou_requester789',
                'run_or_join', 'joined', ?
            )
            """,
            (source_kind, "a" * 64, NOW.isoformat()),
        )
        conn.execute(
            "UPDATE rca_delivery_subscriptions "
            "SET source_id = 'source-manual-dual-baseline' "
            "WHERE effect_kind = 'feishu_thread_reply'"
        )
    store.create_delivery(
        claim=claim,
        delivery=_delivery(claim),
        status={"success": True, "state": "completed"},
        now=NOW,
    )
    quarantined_at = (NOW + timedelta(seconds=1)).isoformat()
    with sqlite3.connect(store.db_path) as conn:
        effect_keys = [
            str(row[0])
            for row in conn.execute(
                "SELECT effect_key FROM rca_delivery_effects ORDER BY effect_key"
            )
        ]
        assert len(effect_keys) == 2
        conn.execute(
            "UPDATE rca_delivery_effects SET status='quarantined', "
            "outcome='quarantined', write_phase='settled', "
            "last_error_code='external_write_fence_missing', "
            "quarantined_at=?, updated_at=?",
            (quarantined_at, quarantined_at),
        )
    return store, effect_keys


def _seed_materialized_activation_deferral_race(root: Path) -> RcaDeliveryStore:
    store, _effect_keys = _seed_manual_dual_effect_quarantine(root)
    now = (NOW + timedelta(seconds=2)).isoformat()
    with sqlite3.connect(store.db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT outbox_id, business_key, submission_key FROM rca_outbox"
        ).fetchone()
        assert row is not None
        job = conn.execute(
            """
            SELECT delivery_id, business_key, submission_key, generation,
                   project_key, work_item_type_key, work_item_id, target_key
              FROM rca_delivery_jobs
             LIMIT 1
            """
        ).fetchone()
        assert job is not None
        epoch_id = "rca-materialized-deferral-test"
        conn.execute(
            """
            INSERT INTO rca_activation_epochs(
                epoch_id, state, is_current,
                preauthorization_fingerprint,
                preauthorization_gate_receipt_sha256,
                preauthorization_capsule_sha256, config_sha256,
                db_logical_identity_json, db_logical_identity_sha256,
                partition_start_fence_json, partition_start_fence_sha256,
                created_at, updated_at, aborted_at
            ) VALUES (?, 'aborted', 1, ?, ?, ?, ?, '{}', ?, '{}', ?, ?, ?, ?)
            """,
            (
                epoch_id,
                "1" * 64,
                "2" * 64,
                "3" * 64,
                "4" * 64,
                "5" * 64,
                "6" * 64,
                now,
                now,
                now,
            ),
        )
        ledger = conn.execute(
            """
            INSERT INTO rca_activation_admission_ledger(
                epoch_id, admission_key, entrypoint, source_kind,
                source_identity_sha256, slot_kind, decision, reason,
                business_key, submission_key, generation,
                first_adjudicated_at, last_adjudicated_at,
                admitted_at, bound_at
            ) VALUES (?, 'manual-materialized-race', 'manual_admit', 'manual',
                      ?, 'manual_success', 'admit',
                      'activation_bounded_slot_consumed', ?, ?, 1,
                      ?, ?, ?, ?)
            """,
            (
                epoch_id,
                "7" * 64,
                row["business_key"],
                row["submission_key"],
                NOW.isoformat(),
                NOW.isoformat(),
                NOW.isoformat(),
                NOW.isoformat(),
            ),
        )
        assert ledger.lastrowid is not None
        conn.execute(
            "UPDATE rca_trigger_sources "
            "SET source_dedupe_key='feishu:' || message_id "
            "WHERE source_id='source-manual-dual-baseline'"
        )
        conn.execute(
            """
            INSERT INTO rca_trigger_bindings(
                source_id, business_key, generation, role, bound_at
            ) VALUES ('source-manual-dual-baseline', ?, 1, 'origin', ?)
            """,
            (row["business_key"], NOW.isoformat()),
        )
        conn.execute(
            """
            UPDATE rca_outbox
               SET status='quarantined', quarantined_at=?,
                   last_error_code='activation_epoch_deferred',
                   last_error_detail='exact operator-reviewed activation deferral',
                   attempt=0, fence=0, next_attempt_at=NULL,
                   lease_token=NULL, lease_owner=NULL, lease_expires_at=NULL,
                   origin_source_id='source-manual-dual-baseline',
                   activation_epoch_id=?, activation_ledger_id=?, updated_at=?
             WHERE outbox_id=?
            """,
            (now, epoch_id, ledger.lastrowid, now, row["outbox_id"]),
        )
        conn.execute(
            """
            INSERT INTO rca_shadow_promotion_audit(
                event_uid, outbox_id, submission_key, operator, reason,
                outcome, from_status, to_status, detail, created_at
            ) VALUES (
                'om_trigger456', ?, ?, 'test-operator',
                'exact test activation deferral', 'deferred', 'pending',
                'quarantined',
                'exact activation item deferred for reviewed manual recovery', ?
            )
            """,
            (row["outbox_id"], row["submission_key"], now),
        )
        conn.execute(
            "UPDATE business_triggers SET state='quarantined' "
            "WHERE business_key=? AND generation=1",
            (row["business_key"],),
        )
        conn.execute(
            """
            UPDATE rca_execution_watch
               SET state='delivery_created', delivery_id=?,
                   poll_attempt=0, fence=0, lease_token=NULL,
                   lease_owner=NULL, lease_expires_at=NULL,
                   last_error_code='activation_epoch_deferred', updated_at=?
             WHERE submission_key=?
            """,
            (job["delivery_id"], now, row["submission_key"]),
        )
        conn.execute(
            """
            UPDATE rca_delivery_jobs
               SET status='ready', outcome='quarantined',
                   terminal_state='submission_quarantined',
                   terminal_error_code='outbox_submission_quarantined',
                   created_at=?, updated_at=?
            """,
            (now, now),
        )
        conn.execute(
            """
            UPDATE rca_delivery_subscriptions
               SET status='quarantined', reason='activation_epoch_deferred',
                   delivery_id=NULL, effect_key=NULL, updated_at=?
            """,
            (now,),
        )
        conn.execute(
            """
            DELETE FROM rca_delivery_effects
             WHERE effect_kind='feishu_thread_reply'
            """
        )
        effect = conn.execute(
            """
            SELECT e.effect_key, e.delivery_id, e.effect_kind, e.target_key,
                   j.submission_key, j.generation, j.project_key,
                   j.work_item_type_key, j.work_item_id, j.outcome,
                   j.terminal_state, j.terminal_error_code
              FROM rca_delivery_effects AS e
              JOIN rca_delivery_jobs AS j ON j.delivery_id=e.delivery_id
             WHERE e.effect_kind='feishu_issue_comment'
            """
        ).fetchone()
        assert effect is not None
        terminal = build_terminal_delivery(
            business_key=row["business_key"],
            submission_key=effect["submission_key"],
            generation=effect["generation"],
            project_key=effect["project_key"],
            work_item_type_key=effect["work_item_type_key"],
            work_item_id=effect["work_item_id"],
            outcome=effect["outcome"],
            terminal_state=effect["terminal_state"],
            error_code=effect["terminal_error_code"],
            schema_version=TERMINAL_DELIVERY_EFFECT_SCHEMA_VERSION_COMMENT_ONLY,
        )
        payload = dict(terminal.effect_payload)
        conn.execute(
            "UPDATE rca_execution_watch SET delivery_id=? WHERE delivery_id=?",
            (terminal.delivery_id, effect["delivery_id"]),
        )
        conn.execute(
            """
            UPDATE rca_delivery_effects
               SET effect_key=?, delivery_id=?, status='pending',
                   outcome='quarantined',
                   write_phase='prewrite',
                   attempt=0, fence=0, remote_receipt_json=NULL,
                   lease_token=NULL, lease_owner=NULL, lease_expires_at=NULL,
                   next_attempt_at=NULL, write_started_at=NULL,
                   payload_json=?, payload_sha256=?, created_at=?, updated_at=?
            """,
            (
                terminal.effect_key,
                terminal.delivery_id,
                json.dumps(
                    payload,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                terminal.semantic_payload_sha256,
                now,
                now,
            ),
        )
        conn.execute(
            """
            UPDATE rca_delivery_jobs
               SET delivery_id=?, artifact_set_id=?, outcome_key=?,
                   report_url='', manifest_json='{}', contract_json=?,
                   artifacts_json='[]'
             WHERE delivery_id=?
            """,
            (
                terminal.delivery_id,
                terminal.outcome_key,
                terminal.outcome_key,
                json.dumps(
                    terminal.contract,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                effect["delivery_id"],
            ),
        )
        conn.commit()
    return store


def _seed_unmaterialized_activation_deferral_companion(
    root: Path,
) -> RcaDeliveryStore:
    store = _seed_materialized_activation_deferral_race(root)
    claimed_at = (NOW + timedelta(seconds=1)).isoformat()
    with sqlite3.connect(store.db_path) as conn:
        conn.execute("DELETE FROM rca_delivery_effects")
        conn.execute("DELETE FROM rca_execution_watch")
        conn.execute("DELETE FROM rca_delivery_jobs")
        conn.execute(
            "UPDATE business_triggers SET "
            "activation_epoch_id=(SELECT activation_epoch_id FROM rca_outbox), "
            "activation_ledger_id=(SELECT activation_ledger_id FROM rca_outbox)"
        )
        conn.execute(
            "UPDATE rca_outbox SET attempt=9, fence=9, claimed_at=?, "
            "completed_at=NULL, result_json=NULL",
            (claimed_at,),
        )
        conn.commit()
    return store


def test_unmaterialized_activation_deferral_companion_is_exactly_classified(
    tmp_path,
):
    store = _seed_unmaterialized_activation_deferral_companion(tmp_path)
    with sqlite3.connect(store.db_path) as conn:
        conn.row_factory = sqlite3.Row
        projection = _quarantined_subscription_projection(conn)

    assert projection == {
        "total": 2,
        "invalid_manual_thread": 0,
        "activation_deferred_issue_comment": 1,
        "activation_deferred_materialized_delivery": 0,
        "activation_deferred_unmaterialized_thread": 1,
        "unrecognized": 0,
    }


@pytest.mark.parametrize(
    "mutation",
    (
        "UPDATE rca_activation_epochs SET state='safe_off'",
        "UPDATE rca_delivery_subscriptions SET delivery_id='unexpected' "
        "WHERE effect_kind='feishu_thread_reply'",
        "UPDATE rca_delivery_subscriptions SET effect_key='unexpected' "
        "WHERE effect_kind='feishu_thread_reply'",
        "UPDATE rca_delivery_subscriptions SET reason='unexpected' "
        "WHERE effect_kind='feishu_issue_comment'",
        "UPDATE rca_trigger_sources SET mode='rerun'",
        "UPDATE rca_outbox SET last_error_code='unexpected'",
        "UPDATE business_triggers SET state='completed'",
        "UPDATE rca_activation_admission_ledger SET decision='join'",
    ),
)
def test_unmaterialized_activation_deferral_companion_fails_closed_on_drift(
    tmp_path,
    mutation,
):
    store = _seed_unmaterialized_activation_deferral_companion(tmp_path)
    with sqlite3.connect(store.db_path) as conn:
        conn.execute(mutation)
        conn.commit()
        conn.row_factory = sqlite3.Row
        projection = _quarantined_subscription_projection(conn)

    assert projection["activation_deferred_unmaterialized_thread"] == 0
    assert projection["unrecognized"] >= 1


def test_materialized_activation_deferral_race_is_exactly_classified(tmp_path):
    store = _seed_materialized_activation_deferral_race(tmp_path)
    with sqlite3.connect(store.db_path) as conn:
        conn.row_factory = sqlite3.Row
        payload = json.loads(
            conn.execute(
                "SELECT payload_json FROM rca_delivery_effects"
            ).fetchone()["payload_json"]
        )
        projection = _quarantined_subscription_projection(conn)
        materialized = _activation_deferred_materialized_projection(conn)

    assert payload["schema_version"] == "pnc_rca_terminal_delivery_effect_v4"
    assert payload["comment_content"]
    assert payload["diagnostic_code"]
    assert payload["diagnostic_result"]
    assert payload["field_updates"] == []
    assert projection == {
        "total": 2,
        "invalid_manual_thread": 0,
        "activation_deferred_issue_comment": 1,
        "activation_deferred_materialized_delivery": 1,
        "activation_deferred_unmaterialized_thread": 0,
        "unrecognized": 0,
    }
    assert materialized["count"] == 1
    assert materialized["entries"][0]["effect_status"] == "pending"
    assert materialized["entries"][0]["write_phase"] == "prewrite"
    assert materialized["entries"][0]["attempt"] == 0
    assert materialized["entries"][0]["fence"] == 0
    assert materialized["entries_sha256"]


def _assert_materialized_activation_deferral_rejected(store: RcaDeliveryStore) -> None:
    with sqlite3.connect(store.db_path) as conn:
        conn.row_factory = sqlite3.Row
        projection = _quarantined_subscription_projection(conn)
        materialized = _activation_deferred_materialized_projection(conn)

    assert materialized["count"] == 0
    assert projection["activation_deferred_materialized_delivery"] == 0
    assert projection["unrecognized"] == 1


def test_materialized_activation_deferral_rejects_coordinated_sha_and_extra_key(
    tmp_path,
):
    store = _seed_materialized_activation_deferral_race(tmp_path)
    with sqlite3.connect(store.db_path) as conn:
        payload = json.loads(
            conn.execute(
                "SELECT payload_json FROM rca_delivery_effects"
            ).fetchone()[0]
        )
        forged_sha256 = "a" * 64
        payload["semantic_payload_sha256"] = forged_sha256
        payload["reviewer_extra"] = "ignored-by-semantic-hash"
        conn.execute(
            "UPDATE rca_delivery_effects SET payload_json=?, payload_sha256=?",
            (
                json.dumps(
                    payload,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                forged_sha256,
            ),
        )
        conn.commit()

    _assert_materialized_activation_deferral_rejected(store)


def test_materialized_activation_deferral_rejects_duplicate_trailing_outcome(
    tmp_path,
):
    store = _seed_materialized_activation_deferral_race(tmp_path)
    with sqlite3.connect(store.db_path) as conn:
        payload_json = str(
            conn.execute(
                "SELECT payload_json FROM rca_delivery_effects"
            ).fetchone()[0]
        )
        assert payload_json.endswith("}")
        conn.execute(
            "UPDATE rca_delivery_effects SET payload_json=?",
            (f'{payload_json[:-1]},"outcome":"success"}}',),
        )
        conn.commit()

    _assert_materialized_activation_deferral_rejected(store)


def test_materialized_activation_deferral_rejects_marker_substring_match(tmp_path):
    store = _seed_materialized_activation_deferral_race(tmp_path)
    with sqlite3.connect(store.db_path) as conn:
        payload = json.loads(
            conn.execute(
                "SELECT payload_json FROM rca_delivery_effects"
            ).fetchone()[0]
        )
        payload["marker"] = f"forged-prefix:{payload['effect_key']}:suffix"
        conn.execute(
            "UPDATE rca_delivery_effects SET payload_json=?",
            (
                json.dumps(
                    payload,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            ),
        )
        conn.commit()

    _assert_materialized_activation_deferral_rejected(store)


def test_materialized_activation_deferral_rejects_coordinated_effect_identity(
    tmp_path,
):
    store = _seed_materialized_activation_deferral_race(tmp_path)
    with sqlite3.connect(store.db_path) as conn:
        payload = json.loads(
            conn.execute(
                "SELECT payload_json FROM rca_delivery_effects"
            ).fetchone()[0]
        )
        original_marker = payload["marker"]
        forged_effect_key = f"g1q3-rca-terminal-effect-v1-{'f' * 64}"
        forged_marker = terminal_delivery_effect_marker(
            forged_effect_key, payload["outcome"], payload["generation"]
        )
        payload.update({
            "effect_key": forged_effect_key,
            "marker": forged_marker,
            "comment_content": payload["comment_content"].replace(
                original_marker, forged_marker
            ),
        })
        conn.execute(
            "UPDATE rca_delivery_effects SET effect_key=?, payload_json=?",
            (
                forged_effect_key,
                json.dumps(
                    payload,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            ),
        )
        conn.commit()

    _assert_materialized_activation_deferral_rejected(store)


def test_materialized_activation_deferral_rejects_coordinated_diagnostic_payload(
    tmp_path,
):
    store = _seed_materialized_activation_deferral_race(tmp_path)
    with sqlite3.connect(store.db_path) as conn:
        payload = json.loads(
            conn.execute(
                "SELECT payload_json FROM rca_delivery_effects"
            ).fetchone()[0]
        )
        original_result = payload["diagnostic_result"]
        original_marker = payload["marker"]
        payload["diagnostic_result"] = f"{original_result}\nreviewer-forged-result"
        payload["comment_content"] = payload["comment_content"].replace(
            original_result, payload["diagnostic_result"]
        )
        semantic_sha256 = compute_terminal_delivery_effect_payload_sha256(
            payload, DELIVERY_EFFECT_KIND
        )
        effect_key = compute_terminal_delivery_effect_key(
            delivery_id=payload["delivery_id"],
            effect_kind=DELIVERY_EFFECT_KIND,
            target_key=payload["target_key"],
            semantic_payload_sha256=semantic_sha256,
        )
        marker = terminal_delivery_effect_marker(
            effect_key, payload["outcome"], payload["generation"]
        )
        payload.update({
            "effect_key": effect_key,
            "semantic_payload_sha256": semantic_sha256,
            "marker": marker,
            "comment_content": payload["comment_content"].replace(
                original_marker, marker
            ),
        })
        conn.execute(
            """
            UPDATE rca_delivery_effects
               SET effect_key=?, payload_json=?, payload_sha256=?
            """,
            (
                effect_key,
                json.dumps(
                    payload,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                semantic_sha256,
            ),
        )
        conn.commit()

    _assert_materialized_activation_deferral_rejected(store)


def test_materialized_activation_deferral_rejects_comment_content_only_tamper(
    tmp_path,
):
    store = _seed_materialized_activation_deferral_race(tmp_path)
    with sqlite3.connect(store.db_path) as conn:
        payload = json.loads(
            conn.execute(
                "SELECT payload_json FROM rca_delivery_effects"
            ).fetchone()[0]
        )
        payload["comment_content"] = (
            f"reviewer-forged-comment\n{payload['marker']}"
        )
        conn.execute(
            "UPDATE rca_delivery_effects SET payload_json=?",
            (
                json.dumps(
                    payload,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            ),
        )
        conn.commit()

    _assert_materialized_activation_deferral_rejected(store)


@pytest.mark.parametrize(
    ("mutation", "parameters"),
    (
        ("UPDATE rca_trigger_bindings SET role='joined'", ()),
        ("UPDATE rca_outbox SET origin_source_id=NULL", ()),
        ("UPDATE business_triggers SET state='completed'", ()),
        ("UPDATE rca_execution_watch SET state='quarantined'", ()),
        ("UPDATE rca_delivery_jobs SET submission_key='wrong-submission'", ()),
        ("UPDATE rca_delivery_effects SET required=0", ()),
        ("UPDATE rca_delivery_effects SET outcome='success'", ()),
        ("UPDATE rca_delivery_effects SET target_key='wrong-target'", ()),
        ("UPDATE rca_delivery_effects SET payload_json='{}'", ()),
        (
            """
            INSERT INTO rca_delivery_attempts(
                effect_key, attempt_no, event_seq, fence, request_id,
                outcome, started_at
            ) SELECT effect_key, 1, 1, 0, 'attempt-present', 'started', ?
                FROM rca_delivery_effects
            """,
            (NOW.isoformat(),),
        ),
    ),
)
def test_materialized_activation_deferral_shape_fails_closed_on_drift(
    tmp_path, mutation, parameters
):
    store = _seed_materialized_activation_deferral_race(tmp_path)
    with sqlite3.connect(store.db_path) as conn:
        conn.execute(mutation, parameters)
        conn.commit()
        conn.row_factory = sqlite3.Row
        projection = _quarantined_subscription_projection(conn)
        materialized = _activation_deferred_materialized_projection(conn)

    assert materialized["count"] == 0
    assert projection["activation_deferred_materialized_delivery"] == 0
    assert projection["unrecognized"] == 1


def _settlement_receipt(effect_key: str, root: Path) -> Path:
    path = root / "functional-settlement.json"
    backup = root / "control.pre-settlement.sqlite3"
    with sqlite3.connect(backup) as conn:
        conn.execute("CREATE TABLE settlement_backup(identity TEXT PRIMARY KEY)")
    backup.chmod(0o600)
    backup_sha256 = hashlib.sha256(backup.read_bytes()).hexdigest()
    _write(
        path,
        {
            "schema_version": "rca_functional_backlog_settlement_v1",
            "external_writes_performed": False,
            "backup": {
                "path": str(backup),
                "sha256": backup_sha256,
            },
            "postconditions": {"unresolved_issue_effects": 0},
            "cumulative_settled": [
                {
                    "effect_key": effect_key,
                    "status": "quarantined",
                    "write_phase": "settled",
                }
            ],
        },
    )
    return path


def _downgrade_live_to_v6(path: Path) -> None:
    with sqlite3.connect(path) as conn:
        triggers = conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'trigger' "
            "AND (name LIKE 'trg_rca_delivery_%_quarantine_%' "
            "OR name LIKE 'trg_rca_quarantine_audit_%')"
        ).fetchall()
        for (name,) in triggers:
            conn.execute(f'DROP TRIGGER "{name}"')
        conn.execute("DROP TABLE rca_delivery_quarantine_mutation_audit")
        conn.execute(
            "UPDATE rca_delivery_meta SET value = 'pnc_rca_delivery_store_v6' "
            "WHERE key = 'schema_version'"
        )
    path.chmod(0o600)


def _prepare_offline_migration(store: RcaDeliveryStore, root: Path) -> dict:
    root.mkdir(parents=True, exist_ok=True)
    live_path = store.db_path
    _downgrade_live_to_v6(live_path)
    source_backup = root / "control.v6.backup.sqlite3"
    source = sqlite3.connect(live_path)
    destination = sqlite3.connect(source_backup)
    try:
        source.backup(destination)
        destination.execute("PRAGMA journal_mode=DELETE")
        destination.commit()
    finally:
        destination.close()
        source.close()
    source_backup.chmod(0o600)
    clone_path = root / "control.v7.offline-clone.sqlite3"
    shutil.copyfile(source_backup, clone_path)
    clone_path.chmod(0o600)
    RcaDeliveryStore(clone_path)
    with sqlite3.connect(clone_path) as conn:
        conn.execute(
            "UPDATE rca_delivery_meta SET value = 'pnc_rca_delivery_store_v8' "
            "WHERE key = 'schema_version'"
        )
        conn.commit()
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        conn.execute("PRAGMA journal_mode=DELETE")
    clone_path.chmod(0o600)
    receipt = build_offline_migration_receipt(
        source_backup_path=source_backup,
        migrated_clone_path=clone_path,
        target_live_db_path=live_path,
        migration_runtime_sha256=MIGRATION_RUNTIME_SHA256,
    )
    receipt_path = root / "offline-migration-receipt.json"
    receipt_path.write_bytes(canonical_migration_receipt_bytes(receipt))
    receipt_path.chmod(0o600)
    receipt_sha256 = hashlib.sha256(receipt_path.read_bytes()).hexdigest()
    return {
        "source_backup": source_backup,
        "clone_path": clone_path,
        "receipt_path": receipt_path,
        "receipt_sha256": receipt_sha256,
        "runtime_sha256": MIGRATION_RUNTIME_SHA256,
        "live_path": live_path,
    }


def _prepare_combined_schema_migration(
    root: Path,
    source_version: str,
    *,
    seed_w2_adjudication: bool = False,
    shared_control_plane: bool = True,
) -> dict:
    root.mkdir(parents=True, exist_ok=True)
    live_path = root / "live-control.sqlite3"
    if shared_control_plane:
        RcaControlStore(live_path)
    RcaDeliveryStore(live_path)
    with sqlite3.connect(live_path) as conn:
        if source_version in {
            "pnc_rca_delivery_store_v7",
            "pnc_rca_delivery_store_v8",
        }:
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
            conn.executescript(
                """
                DROP TRIGGER IF EXISTS trg_rca_delivery_subscription_reason_required;
                DROP TRIGGER IF EXISTS trg_rca_delivery_subscription_event_insert;
                DROP TRIGGER IF EXISTS trg_rca_delivery_subscription_event_update;
                DROP TRIGGER IF EXISTS trg_learning_lane_effect_insert_forbidden;
                DROP TRIGGER IF EXISTS trg_learning_lane_stock_effect_insert_forbidden;
                DROP TRIGGER IF EXISTS trg_learning_lane_stock_subscription_insert_forbidden;
                DROP TRIGGER IF EXISTS trg_learning_lane_stock_subscription_update_forbidden;
                DROP INDEX IF EXISTS idx_rca_delivery_subscription_events;
                DROP TABLE IF EXISTS rca_delivery_subscription_events;
                DROP INDEX idx_delivery_effects_comment_slot;
                DROP INDEX IF EXISTS idx_delivery_observation_outbox_status;
                DROP TABLE IF EXISTS rca_delivery_observation_outbox;
                DROP TABLE rca_conclusion_adjudications;
                DROP TABLE rca_failure_routes;
                DELETE FROM rca_delivery_dispatcher_circuit
                 WHERE circuit_name = 'feishu_card_patch';
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
                """
            )
            subscription_columns = {
                str(row[1])
                for row in conn.execute(
                    "PRAGMA table_info(rca_delivery_subscriptions)"
                )
            }
            if "reason" in subscription_columns:
                conn.execute(
                    "ALTER TABLE rca_delivery_subscriptions DROP COLUMN reason"
                )
        conn.executescript(
            """
            DROP INDEX IF EXISTS idx_delivery_observation_outbox_status;
            DROP TABLE IF EXISTS rca_delivery_observation_outbox;
            """
        )
        conn.execute(
            "UPDATE rca_delivery_meta SET value = ? WHERE key = 'schema_version'",
            (source_version,),
        )
        if source_version == "pnc_rca_delivery_store_v10":
            conn.execute(
                "INSERT INTO rca_delivery_meta(key, value) VALUES(?, ?)",
                ("v10_migration_preservation_sentinel", "preserve-exactly"),
            )
        if source_version == "pnc_rca_delivery_store_v7" and shared_control_plane:
            legacy_at = "2026-07-25T10:00:00+00:00"
            conn.execute(
                """
                INSERT INTO business_triggers(
                    business_key, generation, submission_key,
                    creation_rule_version, work_item_id, project_key,
                    work_item_type_key, normalized_json, state, created_at
                ) VALUES ('legacy-migration-business', 1,
                          'legacy-migration-submission', 'v1', 'legacy-item',
                          'project', 'issue', '{}', 'completed', ?)
                """,
                (legacy_at,),
            )
            conn.executemany(
                """
                INSERT INTO rca_delivery_subscriptions(
                    subscription_key, business_key, generation, effect_kind,
                    target_key, target_json, required, status,
                    created_at, updated_at
                ) VALUES (?, 'legacy-migration-business', 1,
                          'feishu_issue_comment', ?, '{}', 1, ?, ?, ?)
                """,
                [
                    ("legacy-pending", "target-pending", "pending", legacy_at, legacy_at),
                    ("legacy-materialized", "target-materialized", "materialized", legacy_at, legacy_at),
                    ("legacy-suppressed", "target-suppressed", "suppressed", legacy_at, legacy_at),
                    ("legacy-quarantined", "target-quarantined", "quarantined", legacy_at, legacy_at),
                ],
            )
        if seed_w2_adjudication:
            assert source_version == "pnc_rca_delivery_store_v8"
            assert shared_control_plane
            created_at = "2026-07-25T11:30:00+00:00"
            conn.execute(
                """
                INSERT INTO business_triggers(
                    business_key, generation, submission_key,
                    creation_rule_version, work_item_id, project_key,
                    work_item_type_key, normalized_json, state, created_at
                ) VALUES ('w2-business', 1, 'w2-submission', 'v1', '123',
                          'project', 'issue', '{}', 'completed', ?)
                """,
                (created_at,),
            )
            conn.execute(
                """
                INSERT INTO rca_outbox(
                    outbox_id, action, business_key, submission_key,
                    creation_rule_version, generation, payload_json, status,
                    created_at, updated_at
                ) VALUES (1, 'submit', 'w2-business', 'w2-submission',
                          'v1', 1, '{}', 'succeeded', ?, ?)
                """,
                (created_at, created_at),
            )
            conn.execute(
                """
                INSERT INTO rca_execution_watch(
                    submission_key, submission_outbox_id, business_key,
                    generation, project_key, work_item_type_key, work_item_id,
                    task_id, state, next_poll_at, created_at, updated_at
                ) VALUES (
                    'w2-submission', 1, 'w2-business', 1, 'project', 'issue',
                    '123', 'w2-task', 'delivery_created', ?, ?, ?
                )
                """,
                (created_at, created_at, created_at),
            )
            conn.execute(
                """
                INSERT INTO rca_delivery_jobs(
                    delivery_id, submission_key, business_key, generation,
                    artifact_set_id, project_key, work_item_type_key,
                    work_item_id, target_key, issue_url, report_url, status,
                    manifest_json, contract_json, artifacts_json,
                    created_at, updated_at
                ) VALUES (
                    'w2-delivery', 'w2-submission', 'w2-business', 1,
                    'w2-artifacts', 'project', 'issue', '123', 'w2-target',
                    'https://issue/123', 'https://report/123', 'delivered',
                    '{}', '{}', '[]', ?, ?
                )
                """,
                (created_at, created_at),
            )
            conn.executemany(
                """
                INSERT INTO rca_delivery_effects(
                    effect_key, delivery_id, effect_kind, required, target_key,
                    payload_json, payload_sha256, status, created_at, updated_at
                ) VALUES (?, 'w2-delivery', 'feishu_issue_comment', 1, ?,
                          '{}', ?, 'succeeded', ?, ?)
                """,
                (
                    ("w2-original", "w2-original-target", "b" * 64, created_at, created_at),
                    ("w2-correction", "w2-correction-target", "c" * 64, created_at, created_at),
                ),
            )
            conn.execute(
                """
                INSERT INTO rca_conclusion_adjudications(
                    adjudication_id, schema_version, business_key, generation,
                    project_key, work_item_type_key, work_item_id, action,
                    conclusion_state, reason, replacement_conclusion, actor_id,
                    actor_name, source_json, original_delivery_id,
                    original_effect_key, correction_effect_key,
                    activation_epoch_id, evaluator_refs_json,
                    responsibility_domain, impact_window_start,
                    impact_window_end, lineage_json, lineage_sha256, created_at
                ) VALUES (
                    'w2-adjudication', 'pnc_rca_conclusion_adjudication_v1',
                    'w2-business', 1, 'project', 'issue', '123', 'retract',
                    'invalidated', 'verified legacy row', '', 'owner', '', '{}',
                    'w2-delivery', 'w2-original', 'w2-correction',
                    'epoch-safe', '["evaluator"]', 'unknown', ?, ?, '{}', ?, ?
                )
                """,
                (created_at, created_at, "d" * 64, created_at),
            )
    # The v3 receipt is delivery-only and targets the historical v11 marker.
    # Restore the predecessor trigger shape after the mutation connection has
    # closed; opening a second SQLite writer while that connection is live can
    # produce a platform-specific disk I/O error.
    if shared_control_plane and source_version != "pnc_rca_delivery_store_v7":
        _restore_v11_w6_triggers(live_path)
    _normalize_sqlite_copy(live_path)
    live_path.chmod(0o600)
    source_backup = root / f"control.{source_version.rsplit('_', 1)[-1]}.backup.sqlite3"
    source = sqlite3.connect(live_path)
    destination = sqlite3.connect(source_backup)
    try:
        source.backup(destination)
        destination.execute("PRAGMA journal_mode=DELETE")
        destination.commit()
    finally:
        destination.close()
        source.close()
    source_backup.chmod(0o600)
    with sqlite3.connect(source_backup) as conn:
        conn.row_factory = sqlite3.Row
        source_schema_sha256 = logical_database_projection(conn)["schema_sha256"]
    clone_path = root / "control.v11.offline-clone.sqlite3"
    shutil.copyfile(source_backup, clone_path)
    clone_path.chmod(0o600)
    RcaDeliveryStore(clone_path)
    _downgrade_delivery_clone_to_v11(clone_path)
    if not shared_control_plane:
        # Isolated delivery fixtures predate W6 authority tables/triggers.
        with sqlite3.connect(clone_path) as conn:
            for name in (
                "trg_learning_lane_stock_effect_insert_forbidden",
                "trg_learning_lane_stock_subscription_insert_forbidden",
                "trg_learning_lane_stock_subscription_update_forbidden",
            ):
                conn.execute(f"DROP TRIGGER IF EXISTS {name}")
            conn.commit()
    _normalize_sqlite_copy(clone_path)
    with sqlite3.connect(clone_path) as conn:
        assert conn.execute("PRAGMA quick_check").fetchone()[0] == "ok"
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        conn.execute("PRAGMA journal_mode=DELETE")
    clone_path.chmod(0o600)
    receipt = build_combined_offline_migration_receipt(
        source_backup_path=source_backup,
        migrated_clone_path=clone_path,
        target_live_db_path=live_path,
        migration_runtime_sha256=MIGRATION_RUNTIME_SHA256,
        expected_source_schema_sha256=source_schema_sha256,
    )
    receipt_path = root / "combined-offline-migration-receipt.json"
    receipt_path.write_bytes(canonical_migration_receipt_bytes(receipt))
    receipt_path.chmod(0o600)
    return {
        "live_path": live_path,
        "source_backup": source_backup,
        "clone_path": clone_path,
        "receipt": receipt,
        "receipt_path": receipt_path,
        "receipt_sha256": hashlib.sha256(receipt_path.read_bytes()).hexdigest(),
        "source_schema_sha256": source_schema_sha256,
        "shared_control_plane": shared_control_plane,
    }


def _prepare_combined_quarantine_migration(
    store: RcaDeliveryStore,
    root: Path,
) -> dict:
    root.mkdir(parents=True, exist_ok=True)
    source_backup = root / "control.v7.backup.sqlite3"
    with sqlite3.connect(store.db_path) as source, sqlite3.connect(
        source_backup
    ) as destination:
        source.backup(destination)
    with sqlite3.connect(source_backup) as conn:
        conn.executescript(
            """
            DROP TRIGGER IF EXISTS trg_rca_delivery_subscription_reason_required;
            DROP TRIGGER IF EXISTS trg_rca_delivery_subscription_event_insert;
            DROP TRIGGER IF EXISTS trg_rca_delivery_subscription_event_update;
            DROP TRIGGER IF EXISTS trg_learning_lane_effect_insert_forbidden;
            DROP TRIGGER IF EXISTS trg_learning_lane_stock_effect_insert_forbidden;
            DROP TRIGGER IF EXISTS trg_learning_lane_stock_subscription_insert_forbidden;
            DROP TRIGGER IF EXISTS trg_learning_lane_stock_subscription_update_forbidden;
            DROP INDEX IF EXISTS idx_rca_delivery_subscription_events;
            DROP TABLE IF EXISTS rca_delivery_subscription_events;
            DROP TABLE rca_conclusion_adjudication_repairs;
            DROP TABLE rca_conclusion_adjudications;
            DROP TABLE rca_failure_routes;
            DELETE FROM rca_delivery_dispatcher_circuit
             WHERE circuit_name = 'feishu_card_patch';
            DROP INDEX IF EXISTS idx_delivery_effects_comment_slot;
            DROP INDEX IF EXISTS idx_delivery_observation_outbox_status;
            DROP TABLE IF EXISTS rca_delivery_observation_outbox;
            ALTER TABLE rca_delivery_subscriptions DROP COLUMN reason;
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
            ALTER TABLE rca_delivery_effects
                DROP COLUMN adjudication_comment_attempted_at;
            ALTER TABLE rca_delivery_effects
                DROP COLUMN adjudication_comment_attempt_count;
            UPDATE rca_delivery_meta SET value = 'pnc_rca_delivery_store_v7'
             WHERE key = 'schema_version';
            """
        )
        conn.execute("PRAGMA journal_mode=DELETE")
    source_backup.chmod(0o600)
    clone = root / "control.v9.offline-clone.sqlite3"
    shutil.copyfile(source_backup, clone)
    clone.chmod(0o600)
    RcaDeliveryStore(clone)
    _downgrade_delivery_clone_to_v11(clone)
    _normalize_sqlite_copy(clone)
    with sqlite3.connect(source_backup) as conn:
        conn.row_factory = sqlite3.Row
        source_schema_sha256 = logical_database_projection(conn)["schema_sha256"]
    receipt = build_combined_offline_migration_receipt(
        source_backup_path=source_backup,
        migrated_clone_path=clone,
        target_live_db_path=store.db_path,
        migration_runtime_sha256=MIGRATION_RUNTIME_SHA256,
        expected_source_schema_sha256=source_schema_sha256,
    )
    receipt_path = root / "combined-offline-migration-receipt.json"
    receipt_path.write_bytes(canonical_migration_receipt_bytes(receipt))
    receipt_path.chmod(0o600)
    return {
        "clone_path": clone,
        "receipt_path": receipt_path,
        "receipt_sha256": hashlib.sha256(receipt_path.read_bytes()).hexdigest(),
        "source_schema_sha256": source_schema_sha256,
    }


@pytest.mark.parametrize(
    "source_version",
    [
        "pnc_rca_delivery_store_v7",
        "pnc_rca_delivery_store_v8",
        "pnc_rca_delivery_store_v10",
    ],
)
def test_combined_v2_offline_receipt_requires_quick_checked_copy_and_rollback(
    tmp_path, source_version
):
    migration = _prepare_combined_schema_migration(tmp_path, source_version)

    binding = validate_combined_migration_receipt(
        receipt_path=migration["receipt_path"],
        expected_sha256=migration["receipt_sha256"],
        target_live_db_path=migration["live_path"],
        migrated_db_path=migration["clone_path"],
        expected_migration_runtime_sha256=MIGRATION_RUNTIME_SHA256,
        expected_source_schema_sha256=migration["source_schema_sha256"],
    )

    assert binding["source_schema_version"] == source_version
    assert migration["receipt"]["schema_version"] == (
        "pnc_rca_delivery_store_offline_migration_v3"
    )
    assert (
        binding["target_schema_version"]
        == "pnc_rca_delivery_store_v11"
    )
    assert binding["source_quick_check"] == "ok"
    assert binding["target_quick_check"] == "ok"
    assert binding["rollback_path"] == str(migration["source_backup"])
    assert binding["rollback_sha256"] == hashlib.sha256(
        migration["source_backup"].read_bytes()
    ).hexdigest()
    assert binding["source_schema_sha256"] == migration["source_schema_sha256"]
    assert migration["receipt"]["source_schema_contract"]["schema_sha256"] == (
        migration["source_schema_sha256"]
    )
    assert binding["no_live_database_writes"] is True
    preservation = migration["receipt"]["cross_projection_preservation"]
    assert preservation["policy"] == "all_source_owned_rows_exact_v1"
    conditional_shape = preservation["source_owned_schema"][
        "conditional_schema_shape"
    ]
    assert conditional_shape["schema_version"] == (
        "pnc_rca_combined_conditional_schema_shape_v1"
    )
    assert all(
        condition["active"] is True
        for condition in conditional_shape["conditions"].values()
    )
    assert preservation["deterministic_transforms"][0] == {
        "rule": "replace_exact_schema_version_marker_v1",
        "table": "rca_delivery_meta",
        "selector": {"key": "schema_version"},
        "column": "value",
        "source_value": source_version,
        "target_value": "pnc_rca_delivery_store_v11",
    }
    expected_variant = {
        "pnc_rca_delivery_store_v7": "active_prod_v7_no_adjudication_v1",
        "pnc_rca_delivery_store_v8": "w2_v8_failure_routes_adjudication_v1",
        "pnc_rca_delivery_store_v10": "v10_without_observation_outbox_v1",
    }[source_version]
    assert migration["receipt"]["source_schema_variant"] == expected_variant
    if source_version == "pnc_rca_delivery_store_v8":
        assert preservation["deterministic_transforms"][1]["rule"] == (
            "backfill_pending_repairs_from_adjudication_created_at_v1"
        )
    expected_effect_columns = (
        []
        if source_version == "pnc_rca_delivery_store_v10"
        else [
            {
                "name": "adjudication_comment_attempt_count",
                "existing_row_value": 0,
            },
            {
                "name": "adjudication_comment_attempted_at",
                "existing_row_value": None,
            },
        ]
    )
    if source_version == "pnc_rca_delivery_store_v7":
        expected_effect_columns.extend(
            [
                {
                    "name": "comment_slot_schema_version",
                    "existing_row_value": "",
                },
                {"name": "comment_slot_key", "existing_row_value": ""},
                {"name": "comment_slot_kind", "existing_row_value": ""},
                {
                    "name": "comment_slot_generation",
                    "existing_row_value": None,
                },
                {
                    "name": "comment_slot_revision",
                    "existing_row_value": None,
                },
                {
                    "name": "comment_slot_budget_exempt",
                    "existing_row_value": 0,
                },
            ]
        )
        assert "idx_delivery_effects_comment_slot" in preservation[
            "source_owned_schema"
        ]["added_target_objects"]
        assert "rca_delivery_subscription_events" in preservation[
            "source_owned_schema"
        ]["added_target_objects"]
        assert preservation["source_owned_tables"]["rca_delivery_subscriptions"][
            "added_target_columns"
        ] == [{"name": "reason", "existing_row_value": "status_derived_v1"}]
        assert any(
            item["rule"]
            == "backfill_card_patch_circuit_from_latest_source_timestamp_v1"
            for item in preservation["deterministic_transforms"]
        )
        with sqlite3.connect(migration["source_backup"]) as source_conn:
            latest_source_circuit_at = source_conn.execute(
                "SELECT MAX(updated_at) FROM rca_delivery_dispatcher_circuit"
            ).fetchone()[0]
        with sqlite3.connect(migration["clone_path"]) as clone_conn:
            card_patch = clone_conn.execute(
                "SELECT state, updated_at FROM rca_delivery_dispatcher_circuit "
                "WHERE circuit_name = 'feishu_card_patch'"
            ).fetchone()
        assert card_patch == ("closed", latest_source_circuit_at)
        assert preservation["source_owned_tables"]["sqlite_sequence"][
            "allowed_added_target_rows"
        ] == [
            {
                "name": "rca_delivery_subscription_events",
                "seq": 4,
            }
        ]
    else:
        assert preservation["source_owned_tables"]["sqlite_sequence"][
            "allowed_added_target_rows"
        ] == []
    assert preservation["source_owned_tables"]["rca_delivery_effects"][
        "added_target_columns"
    ] == expected_effect_columns
    if source_version == "pnc_rca_delivery_store_v10":
        assert set(preservation["added_target_tables"]) == {
            "rca_delivery_observation_outbox"
        }
        assert preservation["added_target_tables"][
            "rca_delivery_observation_outbox"
        ]["observed_row_count"] == 0
        with sqlite3.connect(migration["clone_path"]) as clone_conn:
            marker = clone_conn.execute(
                "SELECT value FROM rca_delivery_meta WHERE key = 'schema_version'"
            ).fetchone()[0]
            sentinel = clone_conn.execute(
                "SELECT value FROM rca_delivery_meta "
                "WHERE key = 'v10_migration_preservation_sentinel'"
            ).fetchone()[0]
            outbox_count = clone_conn.execute(
                "SELECT COUNT(*) FROM rca_delivery_observation_outbox"
            ).fetchone()[0]
        assert marker == "pnc_rca_delivery_store_v11"
        assert sentinel == "preserve-exactly"
        assert outbox_count == 0


def test_combined_v7_standalone_delivery_selects_only_applicable_schema(
    tmp_path,
):
    migration = _prepare_combined_schema_migration(
        tmp_path,
        "pnc_rca_delivery_store_v7",
        shared_control_plane=False,
    )

    binding = validate_combined_migration_receipt(
        receipt_path=migration["receipt_path"],
        expected_sha256=migration["receipt_sha256"],
        target_live_db_path=migration["live_path"],
        migrated_db_path=migration["clone_path"],
        expected_migration_runtime_sha256=MIGRATION_RUNTIME_SHA256,
        expected_source_schema_sha256=migration["source_schema_sha256"],
    )

    assert binding["target_quick_check"] == "ok"
    schema = migration["receipt"]["cross_projection_preservation"][
        "source_owned_schema"
    ]
    conditions = schema["conditional_schema_shape"]["conditions"]
    for name in (
        "rca_delivery_subscription_events",
        "idx_rca_delivery_subscription_events",
        "trg_rca_delivery_subscription_event_insert",
        "trg_rca_delivery_subscription_event_update",
        "trg_rca_delivery_subscription_reason_required",
        "trg_learning_lane_effect_insert_forbidden",
        "trg_learning_lane_stock_effect_insert_forbidden",
        "trg_learning_lane_stock_subscription_insert_forbidden",
        "trg_learning_lane_stock_subscription_update_forbidden",
    ):
        assert conditions[name]["active"] is False
        assert name not in schema["added_target_objects"]


def test_combined_v7_standalone_rejects_inapplicable_guard_object(tmp_path):
    migration = _prepare_combined_schema_migration(
        tmp_path,
        "pnc_rca_delivery_store_v7",
        shared_control_plane=False,
    )
    with sqlite3.connect(migration["clone_path"]) as conn:
        conn.executescript(
            """
            CREATE TRIGGER trg_learning_lane_effect_insert_forbidden
            BEFORE INSERT ON rca_delivery_effects
            BEGIN
                SELECT RAISE(ABORT, 'forged_inapplicable_guard');
            END;
            """
        )

    with pytest.raises(
        QuarantineMigrationError,
        match="delivery_store_combined_migration_cross_schema_mismatch",
    ):
        build_combined_offline_migration_receipt(
            source_backup_path=migration["source_backup"],
            migrated_clone_path=migration["clone_path"],
            target_live_db_path=migration["live_path"],
            migration_runtime_sha256=MIGRATION_RUNTIME_SHA256,
            expected_source_schema_sha256=migration["source_schema_sha256"],
        )


def test_combined_v2_receipt_binds_w5_cutoff_backfill_for_legacy_v7_source(
    tmp_path,
):
    migration = _prepare_combined_schema_migration(
        tmp_path / "seed",
        "pnc_rca_delivery_store_v7",
    )
    source_backup = tmp_path / "legacy-v7.sqlite3"
    shutil.copyfile(migration["source_backup"], source_backup)
    source_backup.chmod(0o600)
    with sqlite3.connect(source_backup) as conn:
        conn.execute(
            "DELETE FROM rca_delivery_meta "
            "WHERE key = 'w5_external_write_fence_cutoff'"
        )
        conn.commit()
        conn.execute("PRAGMA journal_mode=DELETE")
    clone = tmp_path / "legacy-v7-migrated-v9.sqlite3"
    shutil.copyfile(source_backup, clone)
    clone.chmod(0o600)
    RcaDeliveryStore(clone)
    _downgrade_delivery_clone_to_v11(clone)
    _normalize_sqlite_copy(clone)
    with sqlite3.connect(source_backup) as conn:
        conn.row_factory = sqlite3.Row
        source_schema_sha256 = logical_database_projection(conn)["schema_sha256"]
    receipt = build_combined_offline_migration_receipt(
        source_backup_path=source_backup,
        migrated_clone_path=clone,
        target_live_db_path=migration["live_path"],
        migration_runtime_sha256=MIGRATION_RUNTIME_SHA256,
        expected_source_schema_sha256=source_schema_sha256,
    )
    assert receipt["cross_projection_preservation"]["deterministic_transforms"][1] == {
        "rule": "backfill_w5_external_write_fence_cutoff_v1",
        "table": "rca_delivery_meta",
        "selector": {"key": "w5_external_write_fence_cutoff"},
        "column": "value",
        "source_value": None,
        "target_value": "2026-07-25T00:00:00+00:00",
    }


def test_combined_v3_rejects_old_staged_v10_clone_and_unsupported_sources(
    tmp_path,
):
    migration = _prepare_combined_schema_migration(
        tmp_path / "valid", "pnc_rca_delivery_store_v10"
    )
    old_staged_clone = tmp_path / "old-staged-v10-clone.sqlite3"
    shutil.copyfile(migration["clone_path"], old_staged_clone)
    old_staged_clone.chmod(0o600)
    with sqlite3.connect(old_staged_clone) as conn:
        conn.execute(
            "UPDATE rca_delivery_meta SET value = 'pnc_rca_delivery_store_v10' "
            "WHERE key = 'schema_version'"
        )
        conn.commit()
        conn.execute("PRAGMA journal_mode=DELETE")

    with pytest.raises(
        QuarantineMigrationError,
        match="delivery_store_combined_migration_target_schema_invalid",
    ):
        build_combined_offline_migration_receipt(
            source_backup_path=migration["source_backup"],
            migrated_clone_path=old_staged_clone,
            target_live_db_path=migration["live_path"],
            migration_runtime_sha256=MIGRATION_RUNTIME_SHA256,
            expected_source_schema_sha256=migration["source_schema_sha256"],
        )

    old_same_version_source = tmp_path / "v10-with-outbox-source.sqlite3"
    shutil.copyfile(old_staged_clone, old_same_version_source)
    old_same_version_source.chmod(0o600)
    with sqlite3.connect(old_same_version_source) as conn:
        conn.row_factory = sqlite3.Row
        source_schema_sha256 = logical_database_projection(conn)["schema_sha256"]

    with pytest.raises(
        QuarantineMigrationError,
        match=(
            "delivery_store_combined_migration_"
            "v10_observation_outbox_operator_rebuild_required"
        ),
    ):
        build_combined_offline_migration_receipt(
            source_backup_path=old_same_version_source,
            migrated_clone_path=migration["clone_path"],
            target_live_db_path=migration["live_path"],
            migration_runtime_sha256=MIGRATION_RUNTIME_SHA256,
            expected_source_schema_sha256=source_schema_sha256,
        )

    unsupported_v9_source = tmp_path / "unsupported-v9-source.sqlite3"
    shutil.copyfile(old_staged_clone, unsupported_v9_source)
    unsupported_v9_source.chmod(0o600)
    with sqlite3.connect(unsupported_v9_source) as conn:
        conn.execute(
            "UPDATE rca_delivery_meta SET value = 'pnc_rca_delivery_store_v9' "
            "WHERE key = 'schema_version'"
        )
        conn.commit()
        conn.execute("PRAGMA journal_mode=DELETE")
        conn.row_factory = sqlite3.Row
        unsupported_schema_sha256 = logical_database_projection(conn)[
            "schema_sha256"
        ]

    with pytest.raises(
        QuarantineMigrationError,
        match="delivery_store_combined_migration_source_schema_invalid",
    ):
        build_combined_offline_migration_receipt(
            source_backup_path=unsupported_v9_source,
            migrated_clone_path=migration["clone_path"],
            target_live_db_path=migration["live_path"],
            migration_runtime_sha256=MIGRATION_RUNTIME_SHA256,
            expected_source_schema_sha256=unsupported_schema_sha256,
        )


def test_combined_v2_v7_subscription_reason_and_event_backfill_are_status_derived(
    tmp_path,
):
    migration = _prepare_combined_schema_migration(
        tmp_path / "seed", "pnc_rca_delivery_store_v7"
    )
    with sqlite3.connect(migration["clone_path"]) as conn:
        rows = conn.execute(
            "SELECT subscription_key, status, reason "
            "FROM rca_delivery_subscriptions ORDER BY subscription_key"
        ).fetchall()
        assert rows == [
            ("legacy-materialized", "materialized", "delivery_effect_materialized"),
            ("legacy-pending", "pending", "awaiting_delivery_materialization"),
            ("legacy-quarantined", "quarantined", "legacy_quarantine_reason_unknown"),
            ("legacy-suppressed", "suppressed", "legacy_suppression_reason_unknown"),
        ]
        events = conn.execute(
            "SELECT event_id, subscription_key, old_status, new_status, reason "
            "FROM rca_delivery_subscription_events ORDER BY event_id"
        ).fetchall()
        assert events == [
            (1, "legacy-materialized", "", "materialized", "delivery_effect_materialized"),
            (2, "legacy-pending", "", "pending", "awaiting_delivery_materialization"),
            (3, "legacy-quarantined", "", "quarantined", "legacy_quarantine_reason_unknown"),
            (4, "legacy-suppressed", "", "suppressed", "legacy_suppression_reason_unknown"),
        ]


def test_combined_v2_receipt_builds_quarantine_core_from_offline_v9_clone(
    tmp_path,
):
    store, effect_key = _seed_quarantine(tmp_path / "live")
    settlement = _settlement_receipt(effect_key, tmp_path / "live")
    migration = _prepare_combined_quarantine_migration(
        store,
        tmp_path / "migration",
    )

    core = build_quarantine_core_from_offline_clone(
        migration["clone_path"],
        target_live_db_path=store.db_path,
        migration_receipt_path=migration["receipt_path"],
        expected_migration_receipt_sha256=migration["receipt_sha256"],
        migration_runtime_sha256=MIGRATION_RUNTIME_SHA256,
        release_id="release-combined-v9-001",
        snapshot_at=NOW + timedelta(seconds=2),
        settlement_receipt_paths=[settlement],
        analyzed_by="forensic-operator",
        reason="exact combined-v9 quarantine evidence",
    )

    assert core["migration_binding"]["receipt_sha256"] == migration[
        "receipt_sha256"
    ]
    assert core["migration_binding"]["target_live_db_path"] == str(
        store.db_path.absolute()
    )
    assert core["quarantine_snapshot"]["counts"] == {
        "jobs": 1,
        "effects": 1,
        "subscriptions": 1,
    }


def test_combined_v2_w2_repair_backfill_is_deterministic_and_receipted(tmp_path):
    migration = _prepare_combined_schema_migration(
        tmp_path,
        "pnc_rca_delivery_store_v8",
        seed_w2_adjudication=True,
    )

    preservation = migration["receipt"]["cross_projection_preservation"]
    repairs = preservation["added_target_tables"][
        "rca_conclusion_adjudication_repairs"
    ]
    assert repairs["expected_row_count"] == 1
    assert repairs["observed_row_count"] == 1
    assert repairs["expected_rows_sha256"] == repairs["observed_rows_sha256"]
    with sqlite3.connect(migration["clone_path"]) as conn:
        repair = conn.execute(
            "SELECT adjudication_id, status, attempt_count, created_at, "
            "updated_at, receipt_path FROM rca_conclusion_adjudication_repairs"
        ).fetchone()
    assert repair == (
        "w2-adjudication",
        "pending",
        0,
        "2026-07-25T11:30:00+00:00",
        "2026-07-25T11:30:00+00:00",
        "",
    )


def _unrelated_v9_clone(path: Path) -> Path:
    RcaDeliveryStore(path)
    with sqlite3.connect(path) as conn:
        conn.execute(
            "INSERT INTO rca_delivery_meta(key, value) "
            "VALUES('unrelated_business_state', 'B')"
        )
        conn.commit()
    _downgrade_delivery_clone_to_v11(path)
    _normalize_sqlite_copy(path)
    return path


def test_combined_v2_offline_receipt_rejects_unrelated_healthy_v9_clone(tmp_path):
    migration = _prepare_combined_schema_migration(
        tmp_path / "source-a", "pnc_rca_delivery_store_v7"
    )
    unrelated = _unrelated_v9_clone(tmp_path / "unrelated-b.sqlite3")

    with pytest.raises(
        QuarantineMigrationError,
        match="delivery_store_combined_migration_cross_schema_mismatch",
    ):
        build_combined_offline_migration_receipt(
            source_backup_path=migration["source_backup"],
            migrated_clone_path=unrelated,
            target_live_db_path=migration["live_path"],
            migration_runtime_sha256=MIGRATION_RUNTIME_SHA256,
            expected_source_schema_sha256=migration["source_schema_sha256"],
        )


def test_combined_v2_validator_rejects_forged_unrelated_clone_binding(tmp_path):
    migration = _prepare_combined_schema_migration(
        tmp_path / "source-a", "pnc_rca_delivery_store_v8"
    )
    unrelated = _unrelated_v9_clone(tmp_path / "unrelated-b.sqlite3")
    with sqlite3.connect(unrelated) as conn:
        conn.row_factory = sqlite3.Row
        unrelated_projection = logical_database_projection(conn)
    forged = dict(migration["receipt"])
    unrelated_raw = unrelated.read_bytes()
    forged["migrated_clone"] = {
        "path": str(unrelated),
        "sha256": hashlib.sha256(unrelated_raw).hexdigest(),
        "size_bytes": len(unrelated_raw),
    }
    forged["post_migration_logical_projection"] = unrelated_projection
    forged_path = tmp_path / "forged-unrelated-receipt.json"
    forged_sha256 = _write(forged_path, forged)

    with pytest.raises(
        QuarantineMigrationError,
        match="delivery_store_combined_migration_cross_schema_mismatch",
    ):
        validate_combined_migration_receipt(
            receipt_path=forged_path,
            expected_sha256=forged_sha256,
            target_live_db_path=migration["live_path"],
            migrated_db_path=unrelated,
            expected_migration_runtime_sha256=MIGRATION_RUNTIME_SHA256,
            expected_source_schema_sha256=migration["source_schema_sha256"],
        )


@pytest.mark.parametrize(
    "source_version",
    [
        "pnc_rca_delivery_store_v7",
        "pnc_rca_delivery_store_v8",
        "pnc_rca_delivery_store_v10",
    ],
)
def test_combined_v2_live_pre_and_post_gates_bind_exact_source_clone_and_rollback(
    tmp_path,
    source_version,
):
    migration = _prepare_combined_schema_migration(tmp_path, source_version)

    pre = assert_combined_live_pre_migration_matches(
        receipt_path=migration["receipt_path"],
        expected_sha256=migration["receipt_sha256"],
        live_db_path=migration["live_path"],
        expected_migration_runtime_sha256=MIGRATION_RUNTIME_SHA256,
        expected_source_schema_sha256=migration["source_schema_sha256"],
    )

    assert pre["live_validation"]["schema_version"] == source_version
    assert pre["live_validation"]["quick_check"] == "ok"
    assert pre["live_validation"]["sidecars"] == []
    assert pre["rollback_path"] == str(migration["source_backup"])
    assert pre["rollback_sha256"] == hashlib.sha256(
        migration["source_backup"].read_bytes()
    ).hexdigest()

    # v3 is the historical delivery-only v11 receipt.  Simulate the
    # post-migration replacement with its already-validated offline clone;
    # v11->v12 is covered by the coupled v4 contract tests.
    shutil.copyfile(migration["clone_path"], migration["live_path"])
    migration["live_path"].chmod(0o600)
    post = assert_combined_live_post_migration_matches(
        receipt_path=migration["receipt_path"],
        expected_sha256=migration["receipt_sha256"],
        live_db_path=migration["live_path"],
        expected_migration_runtime_sha256=MIGRATION_RUNTIME_SHA256,
        expected_source_schema_sha256=migration["source_schema_sha256"],
    )

    assert (
        post["live_validation"]["schema_version"]
        == "pnc_rca_delivery_store_v11"
    )
    assert post["live_validation"]["quick_check"] == "ok"
    assert post["live_validation"]["sidecars"] == []
    assert post["rollback_path"] == str(migration["source_backup"])


def _combined_receipt_for_target(
    migration: dict,
    *,
    target: Path,
    receipt_path: Path,
) -> tuple[Path, str]:
    receipt = build_combined_offline_migration_receipt(
        source_backup_path=migration["source_backup"],
        migrated_clone_path=migration["clone_path"],
        target_live_db_path=target,
        migration_runtime_sha256=MIGRATION_RUNTIME_SHA256,
        expected_source_schema_sha256=migration["source_schema_sha256"],
    )
    return receipt_path, _write(receipt_path, receipt)


def test_combined_v2_live_pre_gate_rejects_nonexistent_target(tmp_path):
    migration = _prepare_combined_schema_migration(
        tmp_path / "migration", "pnc_rca_delivery_store_v7"
    )
    missing = tmp_path / "missing-live.sqlite3"
    receipt_path, receipt_sha256 = _combined_receipt_for_target(
        migration,
        target=missing,
        receipt_path=tmp_path / "missing-live-receipt.json",
    )

    with pytest.raises(
        QuarantineMigrationError,
        match="delivery_quarantine_live_database_unavailable",
    ):
        assert_combined_live_pre_migration_matches(
            receipt_path=receipt_path,
            expected_sha256=receipt_sha256,
            live_db_path=missing,
            expected_migration_runtime_sha256=MIGRATION_RUNTIME_SHA256,
            expected_source_schema_sha256=migration["source_schema_sha256"],
        )


def test_combined_v2_live_gates_reject_wrong_pre_and_unmigrated_post(tmp_path):
    migration = _prepare_combined_schema_migration(
        tmp_path / "migration", "pnc_rca_delivery_store_v7"
    )
    wrong_live = tmp_path / "wrong-live.sqlite3"
    shutil.copyfile(migration["source_backup"], wrong_live)
    with sqlite3.connect(wrong_live) as conn:
        conn.execute(
            "INSERT INTO rca_delivery_meta(key, value) "
            "VALUES('unrelated_business_state', 'wrong')"
        )
        conn.execute("PRAGMA journal_mode=DELETE")
    wrong_live.chmod(0o600)
    receipt_path, receipt_sha256 = _combined_receipt_for_target(
        migration,
        target=wrong_live,
        receipt_path=tmp_path / "wrong-live-receipt.json",
    )

    with pytest.raises(
        QuarantineMigrationError,
        match="delivery_store_combined_live_pre_migration_drift",
    ):
        assert_combined_live_pre_migration_matches(
            receipt_path=receipt_path,
            expected_sha256=receipt_sha256,
            live_db_path=wrong_live,
            expected_migration_runtime_sha256=MIGRATION_RUNTIME_SHA256,
            expected_source_schema_sha256=migration["source_schema_sha256"],
        )
    with pytest.raises(
        QuarantineMigrationError,
        match="delivery_store_combined_live_post_migration_drift",
    ):
        assert_combined_live_post_migration_matches(
            receipt_path=migration["receipt_path"],
            expected_sha256=migration["receipt_sha256"],
            live_db_path=migration["live_path"],
            expected_migration_runtime_sha256=MIGRATION_RUNTIME_SHA256,
            expected_source_schema_sha256=migration["source_schema_sha256"],
        )


@pytest.mark.parametrize("mutation", ["delete_trigger", "weaken_check"])
def test_combined_v2_cross_schema_rejects_source_owned_schema_drift(
    tmp_path,
    mutation,
):
    migration = _prepare_combined_schema_migration(
        tmp_path, "pnc_rca_delivery_store_v7"
    )
    with sqlite3.connect(migration["clone_path"]) as conn:
        if mutation == "delete_trigger":
            conn.execute("DROP TRIGGER trg_rca_quarantine_audit_no_update")
        else:
            before = conn.execute(
                "SELECT sql FROM sqlite_master WHERE type = 'table' "
                "AND name = 'rca_delivery_effects'"
            ).fetchone()[0]
            conn.execute("PRAGMA writable_schema=ON")
            conn.execute(
                "UPDATE sqlite_master SET sql = REPLACE(sql, ?, ?) "
                "WHERE type = 'table' AND name = 'rca_delivery_effects'",
                ("CHECK (attempt >= 0)", "CHECK (attempt >= -1)"),
            )
            schema_version = conn.execute("PRAGMA schema_version").fetchone()[0]
            conn.execute(f"PRAGMA schema_version = {schema_version + 1}")
            conn.execute("PRAGMA writable_schema=OFF")
            after = conn.execute(
                "SELECT sql FROM sqlite_master WHERE type = 'table' "
                "AND name = 'rca_delivery_effects'"
            ).fetchone()[0]
            assert after != before
        conn.execute("PRAGMA journal_mode=DELETE")
    migration["clone_path"].chmod(0o600)

    with pytest.raises(
        QuarantineMigrationError,
        match="delivery_store_combined_migration_cross_schema_mismatch",
    ):
        build_combined_offline_migration_receipt(
            source_backup_path=migration["source_backup"],
            migrated_clone_path=migration["clone_path"],
            target_live_db_path=migration["live_path"],
            migration_runtime_sha256=MIGRATION_RUNTIME_SHA256,
            expected_source_schema_sha256=migration["source_schema_sha256"],
        )


def test_combined_v2_cross_projection_rejects_existing_sequence_drift(tmp_path):
    migration = _prepare_combined_schema_migration(
        tmp_path, "pnc_rca_delivery_store_v7"
    )
    with sqlite3.connect(migration["source_backup"]) as source_conn:
        source_sequence = source_conn.execute(
            "SELECT name, seq FROM sqlite_sequence "
            "WHERE name != 'rca_delivery_subscription_events' "
            "ORDER BY name LIMIT 1"
        ).fetchone()
    assert source_sequence is not None
    with sqlite3.connect(migration["clone_path"]) as clone_conn:
        clone_conn.execute(
            "UPDATE sqlite_sequence SET seq = seq + 1000000 WHERE name = ?",
            (source_sequence[0],),
        )
        clone_conn.execute("PRAGMA journal_mode=DELETE")
    migration["clone_path"].chmod(0o600)

    with pytest.raises(
        QuarantineMigrationError,
        match="delivery_store_combined_migration_cross_projection_mismatch",
    ):
        build_combined_offline_migration_receipt(
            source_backup_path=migration["source_backup"],
            migrated_clone_path=migration["clone_path"],
            target_live_db_path=migration["live_path"],
            migration_runtime_sha256=MIGRATION_RUNTIME_SHA256,
            expected_source_schema_sha256=migration["source_schema_sha256"],
        )


def test_combined_v2_cross_projection_rejects_unallowlisted_sequence_row(tmp_path):
    migration = _prepare_combined_schema_migration(
        tmp_path, "pnc_rca_delivery_store_v7"
    )
    with sqlite3.connect(migration["clone_path"]) as clone_conn:
        clone_conn.execute(
            "INSERT INTO sqlite_sequence(name, seq) VALUES('forged_sequence', 1)"
        )
        clone_conn.execute("PRAGMA journal_mode=DELETE")
    migration["clone_path"].chmod(0o600)

    with pytest.raises(
        QuarantineMigrationError,
        match="delivery_store_combined_migration_cross_projection_mismatch",
    ):
        build_combined_offline_migration_receipt(
            source_backup_path=migration["source_backup"],
            migrated_clone_path=migration["clone_path"],
            target_live_db_path=migration["live_path"],
            migration_runtime_sha256=MIGRATION_RUNTIME_SHA256,
            expected_source_schema_sha256=migration["source_schema_sha256"],
        )


def test_combined_v2_cross_projection_rejects_added_sequence_drift(tmp_path):
    migration = _prepare_combined_schema_migration(
        tmp_path, "pnc_rca_delivery_store_v7"
    )
    with sqlite3.connect(migration["clone_path"]) as clone_conn:
        clone_conn.execute(
            "UPDATE sqlite_sequence SET seq = seq + 1 "
            "WHERE name = 'rca_delivery_subscription_events'"
        )
        clone_conn.execute("PRAGMA journal_mode=DELETE")
    migration["clone_path"].chmod(0o600)

    with pytest.raises(
        QuarantineMigrationError,
        match="delivery_store_combined_migration_cross_projection_mismatch",
    ):
        build_combined_offline_migration_receipt(
            source_backup_path=migration["source_backup"],
            migrated_clone_path=migration["clone_path"],
            target_live_db_path=migration["live_path"],
            migration_runtime_sha256=MIGRATION_RUNTIME_SHA256,
            expected_source_schema_sha256=migration["source_schema_sha256"],
        )


@pytest.mark.parametrize("mutation", ["extra_trigger", "weaken_new_table_check"])
def test_combined_v2_rejects_noncanonical_added_v9_objects(tmp_path, mutation):
    migration = _prepare_combined_schema_migration(
        tmp_path, "pnc_rca_delivery_store_v7"
    )
    with sqlite3.connect(migration["clone_path"]) as conn:
        if mutation == "extra_trigger":
            conn.execute(
                "CREATE TRIGGER forged_repair_trigger "
                "AFTER INSERT ON rca_conclusion_adjudication_repairs "
                "BEGIN SELECT 1; END"
            )
        else:
            before = conn.execute(
                "SELECT sql FROM sqlite_master WHERE type = 'table' "
                "AND name = 'rca_conclusion_adjudication_repairs'"
            ).fetchone()[0]
            conn.execute("PRAGMA writable_schema=ON")
            conn.execute(
                "UPDATE sqlite_master SET sql = REPLACE(sql, ?, ?) "
                "WHERE type = 'table' "
                "AND name = 'rca_conclusion_adjudication_repairs'",
                (
                    "status IN ('pending', 'succeeded')",
                    "status IN ('pending', 'succeeded', 'forged')",
                ),
            )
            schema_version = conn.execute("PRAGMA schema_version").fetchone()[0]
            conn.execute(f"PRAGMA schema_version = {schema_version + 1}")
            conn.execute("PRAGMA writable_schema=OFF")
            after = conn.execute(
                "SELECT sql FROM sqlite_master WHERE type = 'table' "
                "AND name = 'rca_conclusion_adjudication_repairs'"
            ).fetchone()[0]
            assert after != before
        conn.execute("PRAGMA journal_mode=DELETE")
    migration["clone_path"].chmod(0o600)

    with pytest.raises(
        QuarantineMigrationError,
        match="delivery_store_combined_migration_target_schema_contract_invalid",
    ):
        build_combined_offline_migration_receipt(
            source_backup_path=migration["source_backup"],
            migrated_clone_path=migration["clone_path"],
            target_live_db_path=migration["live_path"],
            migration_runtime_sha256=MIGRATION_RUNTIME_SHA256,
            expected_source_schema_sha256=migration["source_schema_sha256"],
        )


@pytest.mark.parametrize("mutation", ["drop_index", "weaken_check"])
def test_combined_v2_rejects_noncanonical_observation_outbox_schema(
    tmp_path,
    mutation,
):
    migration = _prepare_combined_schema_migration(
        tmp_path, "pnc_rca_delivery_store_v7"
    )
    with sqlite3.connect(migration["clone_path"]) as conn:
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
        conn.execute("PRAGMA journal_mode=DELETE")
    migration["clone_path"].chmod(0o600)

    with sqlite3.connect(migration["clone_path"]) as conn:
        after_columns = [
            row[1]
            for row in conn.execute(
                "PRAGMA table_info(rca_delivery_observation_outbox)"
            )
        ]
    assert after_columns == before_columns

    with pytest.raises(
        QuarantineMigrationError,
        match="delivery_store_combined_migration_target_schema_contract_invalid",
    ):
        build_combined_offline_migration_receipt(
            source_backup_path=migration["source_backup"],
            migrated_clone_path=migration["clone_path"],
            target_live_db_path=migration["live_path"],
            migration_runtime_sha256=MIGRATION_RUNTIME_SHA256,
            expected_source_schema_sha256=migration["source_schema_sha256"],
        )


def test_combined_v2_rejects_malformed_w2_source_schema_as_self_proof(tmp_path):
    migration = _prepare_combined_schema_migration(
        tmp_path, "pnc_rca_delivery_store_v8"
    )
    with sqlite3.connect(migration["source_backup"]) as conn:
        before = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'table' "
            "AND name = 'rca_failure_routes'"
        ).fetchone()[0]
        conn.execute("PRAGMA writable_schema=ON")
        conn.execute(
            "UPDATE sqlite_master SET sql = REPLACE(sql, ?, ?) "
            "WHERE type = 'table' AND name = 'rca_failure_routes'",
            ("CHECK (generation >= 1)", "CHECK (generation >= 0)"),
        )
        schema_version = conn.execute("PRAGMA schema_version").fetchone()[0]
        conn.execute(f"PRAGMA schema_version = {schema_version + 1}")
        conn.execute("PRAGMA writable_schema=OFF")
        after = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'table' "
            "AND name = 'rca_failure_routes'"
        ).fetchone()[0]
        assert after != before
        conn.execute("PRAGMA journal_mode=DELETE")
    migration["source_backup"].chmod(0o600)

    with pytest.raises(
        QuarantineMigrationError,
        match="delivery_store_combined_migration_source_schema_contract_invalid",
    ):
        build_combined_offline_migration_receipt(
            source_backup_path=migration["source_backup"],
            migrated_clone_path=migration["clone_path"],
            target_live_db_path=migration["live_path"],
            migration_runtime_sha256=MIGRATION_RUNTIME_SHA256,
            expected_source_schema_sha256=migration["source_schema_sha256"],
        )


def _mutate_inherited_schema(path: Path, mutation: str) -> None:
    with sqlite3.connect(path) as conn:
        if mutation == "delete_trigger":
            conn.execute("DROP TRIGGER trg_rca_quarantine_audit_no_update")
        else:
            conn.execute("PRAGMA writable_schema=ON")
            conn.execute(
                "UPDATE sqlite_master SET sql = REPLACE(sql, ?, ?) "
                "WHERE type = 'table' AND name = 'rca_delivery_effects'",
                ("CHECK (attempt >= 0)", "CHECK (attempt >= -1)"),
            )
            schema_version = conn.execute("PRAGMA schema_version").fetchone()[0]
            conn.execute(f"PRAGMA schema_version = {schema_version + 1}")
            conn.execute("PRAGMA writable_schema=OFF")
        conn.execute("PRAGMA journal_mode=DELETE")
    path.chmod(0o600)


@pytest.mark.parametrize(
    "source_version",
    ["pnc_rca_delivery_store_v7", "pnc_rca_delivery_store_v8"],
)
@pytest.mark.parametrize("mutation", ["delete_trigger", "weaken_check"])
def test_combined_v2_external_schema_anchor_rejects_synchronized_drift(
    tmp_path,
    source_version,
    mutation,
):
    migration = _prepare_combined_schema_migration(tmp_path, source_version)
    _mutate_inherited_schema(migration["source_backup"], mutation)
    _mutate_inherited_schema(migration["clone_path"], mutation)

    with pytest.raises(
        QuarantineMigrationError,
        match="delivery_store_combined_migration_source_schema_contract_invalid",
    ):
        build_combined_offline_migration_receipt(
            source_backup_path=migration["source_backup"],
            migrated_clone_path=migration["clone_path"],
            target_live_db_path=migration["live_path"],
            migration_runtime_sha256=MIGRATION_RUNTIME_SHA256,
            expected_source_schema_sha256=migration["source_schema_sha256"],
        )


def test_combined_v2_validator_requires_external_not_receipt_schema_anchor(tmp_path):
    migration = _prepare_combined_schema_migration(
        tmp_path, "pnc_rca_delivery_store_v8"
    )
    _mutate_inherited_schema(migration["source_backup"], "weaken_check")
    _mutate_inherited_schema(migration["clone_path"], "weaken_check")
    with sqlite3.connect(migration["source_backup"]) as conn:
        conn.row_factory = sqlite3.Row
        self_attested_sha256 = logical_database_projection(conn)["schema_sha256"]
    forged = build_combined_offline_migration_receipt(
        source_backup_path=migration["source_backup"],
        migrated_clone_path=migration["clone_path"],
        target_live_db_path=migration["live_path"],
        migration_runtime_sha256=MIGRATION_RUNTIME_SHA256,
        expected_source_schema_sha256=self_attested_sha256,
    )
    forged_path = tmp_path / "self-attested-schema-receipt.json"
    forged_sha256 = _write(forged_path, forged)

    with pytest.raises(
        QuarantineMigrationError,
        match="delivery_store_combined_migration_source_schema_contract_invalid",
    ):
        validate_combined_migration_receipt(
            receipt_path=forged_path,
            expected_sha256=forged_sha256,
            target_live_db_path=migration["live_path"],
            expected_migration_runtime_sha256=MIGRATION_RUNTIME_SHA256,
            expected_source_schema_sha256=migration["source_schema_sha256"],
        )


def _migration_kwargs(migration: dict) -> dict:
    return {
        "migration_receipt_path": migration["receipt_path"],
        "expected_migration_receipt_sha256": migration["receipt_sha256"],
        "migration_runtime_sha256": migration["runtime_sha256"],
    }


def _live_file_state(path: Path) -> tuple[int, int, tuple[str, ...]]:
    current = path.stat()
    sidecars = tuple(
        suffix
        for suffix in ("-wal", "-shm", "-journal")
        if Path(str(path) + suffix).exists()
    )
    return current.st_mtime_ns, current.st_ctime_ns, sidecars


def _migrate_live(migration: dict) -> None:
    before_precheck = _live_file_state(migration["live_path"])
    assert before_precheck[2] == ()
    precheck = assert_live_pre_migration_matches(
        receipt_path=migration["receipt_path"],
        expected_sha256=migration["receipt_sha256"],
        live_db_path=migration["live_path"],
        expected_migration_runtime_sha256=migration["runtime_sha256"],
    )
    assert _live_file_state(migration["live_path"]) == before_precheck
    assert precheck["live_validation"]["sidecars"] == []
    assert precheck["live_validation"]["journal_mode"] in {"delete", "wal"}
    RcaDeliveryStore(migration["live_path"])
    with sqlite3.connect(migration["live_path"]) as conn:
        conn.execute(
            "UPDATE rca_delivery_meta SET value = 'pnc_rca_delivery_store_v8' "
            "WHERE key = 'schema_version'"
        )
        conn.commit()
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        conn.execute("PRAGMA journal_mode=DELETE")
    migration["live_path"].chmod(0o600)
    before_postcheck = _live_file_state(migration["live_path"])
    postcheck = assert_live_post_migration_matches(
        receipt_path=migration["receipt_path"],
        expected_sha256=migration["receipt_sha256"],
        live_db_path=migration["live_path"],
        expected_migration_runtime_sha256=migration["runtime_sha256"],
    )
    assert _live_file_state(migration["live_path"]) == before_postcheck
    assert postcheck["live_validation"]["sidecars"] == []
    assert postcheck["live_validation"]["journal_mode"] in {"delete", "wal"}


def _build_core(store: RcaDeliveryStore, effect_key: str, root: Path):
    receipt = _settlement_receipt(effect_key, root)
    migration = _prepare_offline_migration(store, root / "migration")
    kwargs = {
        **_migration_kwargs(migration),
        "release_id": "release-baseline-001",
        "snapshot_at": NOW + timedelta(seconds=2),
        "settlement_receipt_paths": [receipt],
        "analyzed_by": "forensic-operator",
        "reason": "exact historical terminal rows and settlement evidence",
    }
    core = build_quarantine_core_from_offline_clone(
        migration["clone_path"],
        target_live_db_path=store.db_path,
        **kwargs,
    )
    assert (
        build_quarantine_core_from_offline_clone(
            migration["clone_path"],
            target_live_db_path=store.db_path,
            **kwargs,
        )
        == core
    )
    _migrate_live(migration)
    assert build_quarantine_core(store.db_path, **kwargs) == core
    return core, receipt, migration


def _offline_case(root: Path) -> tuple[RcaDeliveryStore, Path, dict, dict]:
    store, effect_key = _seed_quarantine(root)
    settlement = _settlement_receipt(effect_key, root)
    migration = _prepare_offline_migration(store, root / "migration")
    kwargs = {
        **_migration_kwargs(migration),
        "release_id": "release-baseline-001",
        "snapshot_at": NOW + timedelta(seconds=2),
        "settlement_receipt_paths": [settlement],
        "analyzed_by": "forensic-operator",
        "reason": "exact historical terminal rows and settlement evidence",
    }
    return store, settlement, migration, kwargs


def _abort_predecessor(store: RcaDeliveryStore, *, epoch_id: str):
    control = RcaControlStore(store.db_path)
    created = control.create_activation_epoch(
        epoch_id=epoch_id,
        preauthorization_fingerprint="1" * 64,
        preauthorization_gate_receipt_sha256="2" * 64,
        preauthorization_capsule_sha256="3" * 64,
        config_sha256="4" * 64,
        db_logical_identity={
            "device": 7,
            "inode": 11,
            "logical_store_id": "successor-predecessor",
        },
        partition_start_fence={"successor-test-topic": {"0": 0}},
        operator="successor-test",
        reason="create exact predecessor fixture",
    )
    return control, control.transition_activation_epoch(
        epoch_id=created["epoch_id"],
        expected_state="safe_off",
        target_state="aborted",
        operator="successor-test",
        reason="abort exact predecessor fixture",
    )


def test_successor_preactivation_core_projects_only_activation_binding_read_only(
    tmp_path,
):
    store, _settlement, migration, kwargs = _offline_case(tmp_path)
    _migrate_live(migration)
    control, predecessor = _abort_predecessor(
        store, epoch_id="rca-predecessor-successor-core"
    )
    before = store.db_path.read_bytes()

    predecessor_core = build_quarantine_core(store.db_path, **kwargs)
    successor_core = build_successor_preactivation_quarantine_core(
        store.db_path,
        expected_predecessor_epoch_id=predecessor["epoch_id"],
        expected_predecessor_state="aborted",
        **kwargs,
    )

    assert store.db_path.read_bytes() == before
    assert successor_core["control_db"][
        "activation_db_logical_identity_sha256"
    ] == ""
    assert successor_core["control_db"]["path"] == predecessor_core["control_db"][
        "path"
    ]
    assert successor_core["control_db"]["control_schema_version"] == (
        predecessor_core["control_db"]["control_schema_version"]
    )
    assert successor_core["control_db"]["delivery_schema_version"] == (
        predecessor_core["control_db"]["delivery_schema_version"]
    )
    projected_body = {
        key: value
        for key, value in successor_core["control_db"].items()
        if key != "logical_identity_sha256"
    }
    assert successor_core["control_db"]["logical_identity_sha256"] == hashlib.sha256(
        _json_bytes(projected_body).removesuffix(b"\n")
    ).hexdigest()
    for field in (
        "schema_version",
        "release_id",
        "snapshot_at",
        "migration_binding",
        "quarantine_snapshot",
        "quarantine_event_projection",
        "settlement_receipts",
        "effect_settlement",
        "invalid_manual_thread_adjudication",
        "issuance_policy",
    ):
        assert successor_core[field] == predecessor_core[field]
    assert successor_core["core_sha256"] != predecessor_core["core_sha256"]

    with pytest.raises(
        DeliveryQuarantineBaselineError,
        match="delivery_quarantine_successor_predecessor_state_invalid",
    ):
        build_successor_preactivation_quarantine_core(
            store.db_path,
            expected_predecessor_epoch_id=predecessor["epoch_id"],
            expected_predecessor_state="steady_active",
            **kwargs,
        )
    with pytest.raises(
        DeliveryQuarantineBaselineError,
        match="delivery_quarantine_successor_predecessor_not_current_aborted",
    ):
        build_successor_preactivation_quarantine_core(
            store.db_path,
            expected_predecessor_epoch_id="rca-other-predecessor",
            expected_predecessor_state="aborted",
            **kwargs,
        )
    assert control.activation_epoch()["epoch_id"] == predecessor["epoch_id"]


def test_successor_preactivation_baseline_validates_before_and_after_successor_create(
    tmp_path,
):
    store, _settlement, migration, kwargs = _offline_case(tmp_path)
    _migrate_live(migration)
    control, predecessor = _abort_predecessor(
        store, epoch_id="rca-predecessor-baseline-compat"
    )
    core = build_successor_preactivation_quarantine_core(
        store.db_path,
        expected_predecessor_epoch_id=predecessor["epoch_id"],
        expected_predecessor_state="aborted",
        **kwargs,
    )
    release_bom_sha256 = "f" * 64
    manifest_path = tmp_path / "successor-release-manifest.json"
    manifest_sha256 = _write(
        manifest_path,
        {
            "schema_version": "pnc_rca_release_prepare_manifest_v1",
            "release_id": core["release_id"],
            "release_bom_sha256": release_bom_sha256,
            "quarantine_core_sha256": core["core_sha256"],
            "created_at": (NOW + timedelta(seconds=3)).isoformat(),
            "complete": True,
            "side_effect_contract": {
                "live_files_written": False,
                "launchctl_invoked": False,
            },
        },
    )
    approval_path = tmp_path / "successor-approval.json"
    approval_sha256 = _write(
        approval_path,
        {
            "schema_version": APPROVAL_SCHEMA_VERSION,
            "release_id": core["release_id"],
            "quarantine_core_sha256": core["core_sha256"],
            "release_bom_sha256": release_bom_sha256,
            "decision": "authorize_rca_delivery_quarantine_baseline",
            "identity": {"uid": os.geteuid(), "username": "owner-user"},
            "created_at": (NOW + timedelta(seconds=4)).isoformat(),
        },
    )

    before = issue_quarantine_baseline(
        store.db_path,
        quarantine_core=core,
        release_manifest_path=manifest_path,
        expected_release_manifest_sha256=manifest_sha256,
        expected_release_bom_sha256=release_bom_sha256,
        approval_evidence_path=approval_path,
        expected_approval_evidence_sha256=approval_sha256,
        baseline_id="successor-baseline-before",
        issued_at=NOW + timedelta(seconds=5),
    )

    successor = control.create_activation_epoch(
        epoch_id="rca-successor-baseline-compat",
        preauthorization_fingerprint="5" * 64,
        preauthorization_gate_receipt_sha256="6" * 64,
        preauthorization_capsule_sha256="7" * 64,
        config_sha256="8" * 64,
        db_logical_identity={
            "device": 7,
            "inode": 11,
            "logical_store_id": "successor-baseline",
        },
        partition_start_fence={"successor-test-topic": {"0": 1}},
        operator="successor-test",
        reason="create successor after predecessor abort",
    )
    assert successor["state"] == "safe_off"

    after = issue_quarantine_baseline(
        store.db_path,
        quarantine_core=core,
        release_manifest_path=manifest_path,
        expected_release_manifest_sha256=manifest_sha256,
        expected_release_bom_sha256=release_bom_sha256,
        approval_evidence_path=approval_path,
        expected_approval_evidence_sha256=approval_sha256,
        baseline_id="successor-baseline-after",
        issued_at=NOW + timedelta(seconds=6),
    )

    assert before["quarantine_core"]["control_db"][
        "activation_db_logical_identity_sha256"
    ] == ""
    assert after["quarantine_core"]["control_db"][
        "activation_db_logical_identity_sha256"
    ] == ""
    assert before["quarantine_core_sha256"] == core["core_sha256"]
    assert after["quarantine_core_sha256"] == core["core_sha256"]


def _build_bundle(
    root: Path, *, with_activation_deferred_issue_comment: bool = False
) -> dict:
    store, effect_key = _seed_quarantine(root)
    if with_activation_deferred_issue_comment:
        _mark_issue_comment_activation_deferred(store)
    rows_before = _rows(store)
    core, receipt, migration = _build_core(store, effect_key, root)
    assert "owner_attestation" not in core
    release_bom_sha256 = "c" * 64
    release_manifest_path = root / "release_prepare_manifest.json"
    release_manifest_sha256 = _write(
        release_manifest_path,
        {
            "schema_version": "pnc_rca_release_prepare_manifest_v1",
            "release_id": core["release_id"],
            "release_bom_sha256": release_bom_sha256,
            "quarantine_core_sha256": core["core_sha256"],
            "created_at": (NOW + timedelta(seconds=2, milliseconds=500)).isoformat(),
            "complete": True,
            "side_effect_contract": {
                "live_files_written": False,
                "launchctl_invoked": False,
            },
        },
    )
    approval_path = root / "approval.json"
    approval_sha256 = _write(
        approval_path,
        {
            "schema_version": APPROVAL_SCHEMA_VERSION,
            "release_id": core["release_id"],
            "quarantine_core_sha256": core["core_sha256"],
            "release_bom_sha256": release_bom_sha256,
            "decision": "authorize_rca_delivery_quarantine_baseline",
            "identity": {"uid": os.geteuid(), "username": "owner-user"},
            "created_at": (NOW + timedelta(seconds=3)).isoformat(),
        },
    )
    baseline = issue_quarantine_baseline(
        store.db_path,
        quarantine_core=core,
        release_manifest_path=release_manifest_path,
        expected_release_manifest_sha256=release_manifest_sha256,
        expected_release_bom_sha256=release_bom_sha256,
        approval_evidence_path=approval_path,
        expected_approval_evidence_sha256=approval_sha256,
        baseline_id="baseline-release-001",
        issued_at=NOW + timedelta(seconds=4),
    )
    assert _rows(store) == rows_before
    baseline_path = root / "delivery-quarantine-baseline.json"
    baseline_path.write_bytes(canonical_quarantine_baseline_bytes(baseline))
    baseline_path.chmod(0o600)
    baseline_sha256 = hashlib.sha256(baseline_path.read_bytes()).hexdigest()
    live_env_path = root / ".env"
    live_env_path.write_text(
        f"HERMES_RCA_DELIVERY_QUARANTINE_BASELINE_SHA256={baseline_sha256}\n"
    )
    live_env_path.chmod(0o600)
    live_env_sha256 = hashlib.sha256(live_env_path.read_bytes()).hexdigest()
    bootstrap_epoch_id = "rca-bootstrap-baseline-test"
    active_release_binding_path = root / "active-release-binding.json"
    _write(
        active_release_binding_path,
        {
            "schema_version": "pnc_rca_production_env_stage_receipt_v2",
            "release_id": core["release_id"],
            "authority_sha256": "9" * 64,
            "authority_epoch_id": "rca-authority-baseline-test",
            "complete": True,
            "live_write_performed": False,
            "bindings": {
                "release_bom_sha256": release_bom_sha256,
                "release_approval": {"sha256": approval_sha256},
                "candidate_env": {"sha256": live_env_sha256},
                "bootstrap_authorization": {
                    "sha256": "d" * 64,
                    "receipt_fingerprint": "e" * 64,
                },
            },
            "policy": {
                "capacity_admission": {
                    "capacity_mode": "bootstrap",
                    "bootstrap_epoch_id": bootstrap_epoch_id,
                    "release_approval_id": core["release_id"],
                    "release_bom_sha256": release_bom_sha256,
                    "approval_evidence_sha256": approval_sha256,
                    "bootstrap_authorization_sha256": "d" * 64,
                    "bootstrap_authorization_fingerprint": "e" * 64,
                }
            },
            "side_effect_contract": {
                "canonical_active_release_binding": str(active_release_binding_path),
                "canonical_live_env": str(live_env_path),
            },
        },
    )
    return {
        "store": store,
        "core": core,
        "receipt": receipt,
        "migration": migration,
        "approval_path": approval_path,
        "release_manifest_path": release_manifest_path,
        "release_bom_sha256": release_bom_sha256,
        "baseline_path": baseline_path,
        "baseline_sha256": baseline_sha256,
        "bootstrap_epoch_id": bootstrap_epoch_id,
        "active_release_binding_path": active_release_binding_path,
        "live_env_path": live_env_path,
    }


def _health(bundle: dict, *, path: Path | None = None, sha256: str | None = None):
    return bundle["store"].health(
        now=NOW + timedelta(seconds=5),
        quarantine_baseline_path=path or bundle["baseline_path"],
        expected_quarantine_baseline_sha256=(
            bundle["baseline_sha256"] if sha256 is None else sha256
        ),
        quarantine_release_id=bundle["core"]["release_id"],
        quarantine_bootstrap_epoch_id=bundle["bootstrap_epoch_id"],
        quarantine_active_release_binding_path=bundle["active_release_binding_path"],
        quarantine_live_env_path=bundle["live_env_path"],
    )


def test_exact_approved_baseline_acknowledges_lifetime_without_db_writes(tmp_path):
    bundle = _build_bundle(tmp_path)
    store = bundle["store"]
    assert bundle["core"]["schema_version"] == CORE_SCHEMA_VERSION
    missing = store.health(now=NOW + timedelta(seconds=5))
    assert missing["business_ready"] is False
    assert missing["business_blockers"]["quarantined_jobs"] == 1
    assert missing["production_blockers"] == {
        "activation_schema_unavailable": 0,
        "uncertain_effects": 0,
        "quarantined_jobs": 1,
        "quarantined_effects": 1,
            "quarantined_subscriptions": 1,
            "quarantine_baseline_invalid": 1,
            "pending_delivery_observations": 0,
        }
    before_backpressure = store.backpressure_snapshot(
        now=NOW + timedelta(seconds=5)
    ).public_dict()

    health = _health(bundle)

    assert health["business_ready"] is True
    assert not any(health["production_blockers"].values())
    assert health["delivery_jobs"]["quarantined"] == 1
    assert health["delivery_effects"]["quarantined"] == 1
    assert health["delivery_subscriptions"]["quarantined"] == 1
    baseline = health["delivery_quarantine"]
    assert baseline["lifetime"] == {"jobs": 1, "effects": 1, "subscriptions": 1}
    assert baseline["acknowledged"] == baseline["lifetime"]
    assert baseline["unacknowledged"] == {
        "jobs": 0,
        "effects": 0,
        "subscriptions": 0,
    }
    assert baseline["baseline_identity"]["release_id"] == "release-baseline-001"
    assert (
        baseline["baseline_identity"]["quarantine_core_sha256"]
        == bundle["core"]["core_sha256"]
    )
    assert bundle["core"]["issuance_policy"]["bom_binding"] == (
        "quarantine_core_sha256"
    )
    assert (
        store.backpressure_snapshot(now=NOW + timedelta(seconds=5)).public_dict()
        == before_backpressure
    )


def test_static_migration_cache_does_not_hide_live_quarantine_drift(
    tmp_path, monkeypatch
):
    bundle = _build_bundle(tmp_path)
    baseline_module._clear_migration_validation_cache()
    calls = []
    original = baseline_module.validate_migration_receipt

    def counted(**kwargs):
        calls.append(kwargs)
        return original(**kwargs)

    monkeypatch.setattr(baseline_module, "validate_migration_receipt", counted)

    assert _health(bundle)["business_ready"] is True
    assert _health(bundle)["business_ready"] is True
    assert len(calls) == 1
    with sqlite3.connect(bundle["store"].db_path) as conn:
        conn.execute(
            "UPDATE rca_delivery_jobs SET updated_at = ?",
            ((NOW + timedelta(seconds=9)).isoformat(),),
        )

    drifted = _health(bundle)

    assert len(calls) == 1
    assert drifted["business_ready"] is False
    assert drifted["business_blockers"]["quarantine_baseline_invalid"] == 1
    assert drifted["delivery_quarantine"]["error_code"] == (
        "delivery_quarantine_baseline_snapshot_mismatch"
    )


def test_activation_deferred_issue_comment_is_explicitly_adjudicated(tmp_path):
    bundle = _build_bundle(
        tmp_path, with_activation_deferred_issue_comment=True
    )

    core = bundle["core"]
    assert core["schema_version"] == DEFERRED_CORE_SCHEMA_VERSION
    assert core["invalid_manual_thread_adjudication"]["count"] == 1
    assert core["activation_deferred_issue_comment_adjudication"] == {
        "effect_kind": "feishu_issue_comment",
        "reason_code": "activation_epoch_deferred",
        "technical_finding": "deferred_before_delivery_materialization",
        "proposed_disposition": "retain_terminal_for_fresh_rerun",
        "required": True,
        "source_id": None,
        "delivery_id": None,
        "effect_key": None,
        "delivery_job_present": False,
        "delivery_effect_present": False,
        "count": 1,
        "analyzed_by": "forensic-operator",
        "analyzed_at": (NOW + timedelta(seconds=2)).isoformat(),
        "reason": "exact historical terminal rows and settlement evidence",
    }
    health = _health(bundle)
    assert health["business_ready"] is True
    assert health["delivery_quarantine"]["acknowledged"]["subscriptions"] == 2


def test_unrecognized_quarantined_issue_comment_still_fails_closed(tmp_path):
    store, effect_key = _seed_quarantine(tmp_path)
    _mark_issue_comment_activation_deferred(
        store, reason="activation_epoch_deferred_unproven"
    )

    with pytest.raises(
        DeliveryQuarantineBaselineError,
        match="delivery_quarantine_baseline_manual_thread_adjudication_invalid",
    ):
        _build_core(store, effect_key, tmp_path)


def test_w3_pre_delivery_terminal_is_covered_by_quarantine_snapshot(tmp_path):
    store, effect_key = _seed_quarantine(tmp_path)
    _mark_issue_comment_activation_deferred(
        store, reason="w3_execution_snapshot_missing"
    )

    core, _receipt, _migration = _build_core(store, effect_key, tmp_path)

    assert core["quarantine_snapshot"]["counts"]["subscriptions"] == 2
    assert "activation_deferred_issue_comment_adjudication" not in core


@pytest.mark.parametrize(
    ("column", "value_sql"),
    [
        ("required", "0"),
        ("effect_kind", "'feishu_thread_reply'"),
        ("source_id", "'source-manual-baseline'"),
        (
            "delivery_id",
            "(SELECT delivery_id FROM rca_delivery_jobs ORDER BY delivery_id LIMIT 1)",
        ),
        (
            "effect_key",
            "(SELECT effect_key FROM rca_delivery_effects ORDER BY effect_key LIMIT 1)",
        ),
    ],
)
def test_activation_deferred_issue_comment_requires_exact_unmaterialized_shape(
    tmp_path, column, value_sql
):
    store, effect_key = _seed_quarantine(tmp_path)
    _mark_issue_comment_activation_deferred(store)
    with sqlite3.connect(store.db_path) as conn:
        cursor = conn.execute(
            f"UPDATE rca_delivery_subscriptions SET {column} = {value_sql} "
            "WHERE effect_kind = 'feishu_issue_comment' AND status = 'quarantined'"
        )
        assert cursor.rowcount == 1

    with pytest.raises(
        DeliveryQuarantineBaselineError,
        match="delivery_quarantine_baseline_manual_thread_adjudication_invalid",
    ):
        _build_core(store, effect_key, tmp_path)


def test_activation_deferred_core_cannot_be_downgraded_to_v1(tmp_path):
    bundle = _build_bundle(
        tmp_path, with_activation_deferred_issue_comment=True
    )
    body = {
        key: value
        for key, value in bundle["core"].items()
        if key
        not in {
            "activation_deferred_issue_comment_adjudication",
            "core_sha256",
        }
    }
    body["schema_version"] = CORE_SCHEMA_VERSION
    downgraded = {
        **body,
        "core_sha256": hashlib.sha256(
            _json_bytes(body).removesuffix(b"\n")
        ).hexdigest(),
    }

    with pytest.raises(
        DeliveryQuarantineBaselineError,
        match="delivery_quarantine_baseline_deferred_issue_adjudication_invalid",
    ):
        issue_quarantine_baseline(
            bundle["store"].db_path,
            quarantine_core=downgraded,
            release_manifest_path=bundle["release_manifest_path"],
            expected_release_manifest_sha256=hashlib.sha256(
                bundle["release_manifest_path"].read_bytes()
            ).hexdigest(),
            expected_release_bom_sha256=bundle["release_bom_sha256"],
            approval_evidence_path=bundle["approval_path"],
            expected_approval_evidence_sha256=hashlib.sha256(
                bundle["approval_path"].read_bytes()
            ).hexdigest(),
            baseline_id="baseline-release-downgrade",
            issued_at=NOW + timedelta(seconds=4),
        )


def test_offline_preapproval_core_exactly_matches_post_migration_live_core(tmp_path):
    store, _settlement, migration, kwargs = _offline_case(tmp_path)
    offline_core = build_quarantine_core_from_offline_clone(
        migration["clone_path"],
        target_live_db_path=store.db_path,
        **kwargs,
    )

    _migrate_live(migration)
    live_core = build_quarantine_core(store.db_path, **kwargs)

    assert live_core == offline_core
    assert offline_core["control_db"]["path"] == str(store.db_path.absolute())
    assert offline_core["migration_binding"]["target_live_db_path"] == str(
        store.db_path.absolute()
    )
    migration_receipt = json.loads(migration["receipt_path"].read_text())
    assert migration_receipt["source_backup_normalization"] == {
        "method": "sqlite_backup_api_then_delete_journal_v1",
        "journal_mode": "delete",
        "integrity_check": "ok",
        "foreign_key_violation_count": 0,
        "byte_identical_live_copy": False,
    }
    assert migration_receipt["post_migration_health"] == {
        "integrity_check": "ok",
        "foreign_key_violation_count": 0,
    }


def test_baseline_can_be_reissued_after_monotonic_quarantine_audit_events(tmp_path):
    bundle = _build_bundle(tmp_path)
    with sqlite3.connect(bundle["store"].db_path) as conn:
        conn.execute(
            "UPDATE rca_delivery_subscriptions SET status = 'quarantined', "
            "reason = 'test_quarantine_for_reissue', updated_at = ? "
            "WHERE effect_kind = 'feishu_issue_comment'",
            ((NOW + timedelta(seconds=6)).isoformat(),),
        )
        conn.execute(
            "UPDATE rca_delivery_subscriptions SET status = 'materialized', "
            "reason = 'test_materialized_after_quarantine', updated_at = ? "
            "WHERE effect_kind = 'feishu_issue_comment'",
            ((NOW + timedelta(seconds=7)).isoformat(),),
        )

    assert _health(bundle)["business_ready"] is False
    core = build_quarantine_core(
        bundle["store"].db_path,
        **_migration_kwargs(bundle["migration"]),
        release_id=bundle["core"]["release_id"],
        snapshot_at=NOW + timedelta(seconds=8),
        settlement_receipt_paths=[bundle["receipt"]],
        analyzed_by="forensic-operator",
        reason="reissue after legitimate monotonic quarantine audit events",
    )
    release_manifest_sha256 = _write(
        tmp_path / "release_prepare_manifest-002.json",
        {
            "schema_version": "pnc_rca_release_prepare_manifest_v1",
            "release_id": core["release_id"],
            "release_bom_sha256": bundle["release_bom_sha256"],
            "quarantine_core_sha256": core["core_sha256"],
            "created_at": (NOW + timedelta(seconds=9)).isoformat(),
            "complete": True,
            "side_effect_contract": {
                "live_files_written": False,
                "launchctl_invoked": False,
            },
        },
    )
    approval_sha256 = _write(
        tmp_path / "approval-002.json",
        {
            "schema_version": APPROVAL_SCHEMA_VERSION,
            "release_id": core["release_id"],
            "quarantine_core_sha256": core["core_sha256"],
            "release_bom_sha256": bundle["release_bom_sha256"],
            "decision": "authorize_rca_delivery_quarantine_baseline",
            "identity": {"uid": os.geteuid(), "username": "owner-user"},
            "created_at": (NOW + timedelta(seconds=10)).isoformat(),
        },
    )

    baseline = issue_quarantine_baseline(
        bundle["store"].db_path,
        quarantine_core=core,
        release_manifest_path=tmp_path / "release_prepare_manifest-002.json",
        expected_release_manifest_sha256=release_manifest_sha256,
        expected_release_bom_sha256=bundle["release_bom_sha256"],
        approval_evidence_path=tmp_path / "approval-002.json",
        expected_approval_evidence_sha256=approval_sha256,
        baseline_id="baseline-release-002",
        issued_at=NOW + timedelta(seconds=11),
    )

    reissued_path = tmp_path / "delivery-quarantine-baseline-002.json"
    reissued_sha256 = _write(reissued_path, baseline)
    health = _health(bundle, path=reissued_path, sha256=reissued_sha256)

    assert baseline["quarantine_core"]["quarantine_event_projection"] == (
        core["quarantine_event_projection"]
    )
    assert health["business_ready"] is True, health
    assert core["quarantine_event_projection"]["event_count"] == 5
    identity = health["delivery_quarantine"]["baseline_identity"]
    assert identity["quarantine_core_sha256"] == core["core_sha256"]
    assert identity["approval_evidence_sha256"] == approval_sha256
    assert identity["active_release_binding_sha256"] == hashlib.sha256(
        bundle["active_release_binding_path"].read_bytes()
    ).hexdigest()


@pytest.mark.parametrize(
    "drift",
    ["clone_replaced", "missing_table", "trigger_drift", "index_drift"],
)
def test_offline_clone_schema_or_identity_drift_fails_closed(tmp_path, drift):
    store, _settlement, migration, kwargs = _offline_case(tmp_path)
    clone = migration["clone_path"]
    if drift == "clone_replaced":
        replacement = clone.with_suffix(".replacement")
        shutil.copyfile(migration["source_backup"], replacement)
        replacement.chmod(0o600)
        os.replace(replacement, clone)
    else:
        with sqlite3.connect(clone) as conn:
            if drift == "missing_table":
                conn.execute("DROP TABLE rca_delivery_dispatcher_circuit")
            elif drift == "trigger_drift":
                conn.execute("DROP TRIGGER trg_rca_delivery_job_quarantine_insert")
            else:
                conn.execute("DROP INDEX idx_delivery_jobs_status")

    with pytest.raises(
        DeliveryQuarantineBaselineError,
        match="delivery_quarantine_post_migration_drift",
    ):
        build_quarantine_core_from_offline_clone(
            clone,
            target_live_db_path=store.db_path,
            **kwargs,
        )


def test_offline_migration_rejects_nondeterministic_audit_seed(tmp_path):
    _store, _settlement, migration, _kwargs = _offline_case(tmp_path)
    with sqlite3.connect(migration["clone_path"]) as conn:
        conn.execute("DROP TRIGGER trg_rca_quarantine_audit_no_update")
        conn.execute(
            "UPDATE rca_delivery_quarantine_mutation_audit "
            "SET observed_at = '2099-01-01T00:00:00+00:00' WHERE audit_id = 1"
        )

    with pytest.raises(
        QuarantineMigrationError,
        match="delivery_quarantine_migration_seed_nondeterministic",
    ):
        build_offline_migration_receipt(
            source_backup_path=migration["source_backup"],
            migrated_clone_path=migration["clone_path"],
            target_live_db_path=migration["live_path"],
            migration_runtime_sha256=migration["runtime_sha256"],
        )


def test_live_pre_migration_drift_stops_before_env_or_binding_writes(tmp_path):
    store, _settlement, migration, _kwargs = _offline_case(tmp_path)
    env_path = tmp_path / ".env"
    binding_path = tmp_path / "active-release-binding.json"
    conn = sqlite3.connect(store.db_path)
    try:
        conn.execute(
            "UPDATE rca_delivery_jobs SET updated_at = ?",
            ((NOW + timedelta(seconds=30)).isoformat(),),
        )
        conn.commit()
    finally:
        conn.close()
    before_validation = _live_file_state(store.db_path)

    with pytest.raises(
        QuarantineMigrationError,
        match="delivery_quarantine_live_pre_migration_drift",
    ):
        assert_live_pre_migration_matches(
            receipt_path=migration["receipt_path"],
            expected_sha256=migration["receipt_sha256"],
            live_db_path=migration["live_path"],
            expected_migration_runtime_sha256=migration["runtime_sha256"],
        )

    assert _live_file_state(store.db_path) == before_validation
    assert not env_path.exists()
    assert not binding_path.exists()


def test_v6_to_v7_audit_schema_failure_rolls_back_atomically(tmp_path, monkeypatch):
    store, _effect_key = _seed_quarantine(tmp_path)
    _downgrade_live_to_v6(store.db_path)

    def fail_after_one_statement(conn, _script):
        conn.execute("CREATE TABLE rca_delivery_quarantine_partial_probe(value TEXT)")
        raise RuntimeError("injected_quarantine_schema_failure")

    monkeypatch.setattr(
        delivery_store_module,
        "_execute_schema_script_in_transaction",
        fail_after_one_statement,
    )
    with pytest.raises(RuntimeError, match="injected_quarantine_schema_failure"):
        RcaDeliveryStore(store.db_path)

    with sqlite3.connect(store.db_path) as conn:
        schema_version = conn.execute(
            "SELECT value FROM rca_delivery_meta WHERE key = 'schema_version'"
        ).fetchone()[0]
        partial = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' "
            "AND name = 'rca_delivery_quarantine_partial_probe'"
        ).fetchone()
        audit = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' "
            "AND name = 'rca_delivery_quarantine_mutation_audit'"
        ).fetchone()
    assert schema_version == "pnc_rca_delivery_store_v6"
    assert partial is None
    assert audit is None


def test_w2_v8_malformed_failure_route_contract_rolls_back_before_v9_marker(
    tmp_path,
):
    migration = _prepare_combined_schema_migration(
        tmp_path, "pnc_rca_delivery_store_v8"
    )
    live_path = migration["live_path"]
    with sqlite3.connect(live_path) as conn:
        conn.executescript(
            """
            ALTER TABLE rca_failure_routes RENAME TO rca_failure_routes_valid;
            CREATE TABLE rca_failure_routes AS
                SELECT * FROM rca_failure_routes_valid;
            CREATE UNIQUE INDEX malformed_failure_routes_dedupe
                ON rca_failure_routes(dedupe_key);
            DROP TABLE rca_failure_routes_valid;
            """
        )
        malformed_sql = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'table' "
            "AND name = 'rca_failure_routes'"
        ).fetchone()[0]

    with pytest.raises(
        RuntimeError,
        match="incompatible_delivery_store_schema:failure_routes_contract",
    ):
        RcaDeliveryStore(live_path)

    with sqlite3.connect(live_path) as conn:
        marker = conn.execute(
            "SELECT value FROM rca_delivery_meta WHERE key = 'schema_version'"
        ).fetchone()[0]
        observed_sql = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'table' "
            "AND name = 'rca_failure_routes'"
        ).fetchone()[0]
        repair_table = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' "
            "AND name = 'rca_conclusion_adjudication_repairs'"
        ).fetchone()
        effect_columns = {
            str(row[1])
            for row in conn.execute("PRAGMA table_info(rca_delivery_effects)")
        }
    assert marker == "pnc_rca_delivery_store_v8"
    assert observed_sql == malformed_sql
    assert repair_table is None
    assert "adjudication_comment_attempt_count" not in effect_columns
    assert "adjudication_comment_attempted_at" not in effect_columns


def test_candidate_v7_schema_variant_requires_operator_remediation(tmp_path):
    migration = _prepare_combined_schema_migration(
        tmp_path, "pnc_rca_delivery_store_v7"
    )
    live_path = migration["live_path"]
    with sqlite3.connect(live_path) as conn:
        conn.execute(
            "CREATE TABLE rca_conclusion_adjudications("
            "adjudication_id TEXT PRIMARY KEY)"
        )

    with pytest.raises(
        RuntimeError,
        match="pre_v9_source_variant_operator_remediation",
    ):
        RcaDeliveryStore(live_path)

    with sqlite3.connect(live_path) as conn:
        marker = conn.execute(
            "SELECT value FROM rca_delivery_meta WHERE key = 'schema_version'"
        ).fetchone()[0]
        repair_table = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' "
            "AND name = 'rca_conclusion_adjudication_repairs'"
        ).fetchone()
    assert marker == "pnc_rca_delivery_store_v7"
    assert repair_table is None


def test_w2_v8_blank_legacy_activation_requires_operator_remediation(tmp_path):
    migration = _prepare_combined_schema_migration(
        tmp_path, "pnc_rca_delivery_store_v8"
    )
    live_path = migration["live_path"]
    with sqlite3.connect(live_path) as conn:
        conn.execute(
            """
            INSERT INTO rca_conclusion_adjudications(
                adjudication_id, schema_version, business_key, generation,
                project_key, work_item_type_key, work_item_id, action,
                conclusion_state, reason, replacement_conclusion, actor_id,
                actor_name, source_json, original_delivery_id,
                original_effect_key, correction_effect_key, activation_epoch_id,
                evaluator_refs_json, responsibility_domain,
                impact_window_start, impact_window_end, lineage_json,
                lineage_sha256, created_at
            ) VALUES (
                'legacy-adjudication', 'pnc_rca_conclusion_adjudication_v1',
                'legacy-business', 1, 'project', 'issue', '123', 'retract',
                'invalidated', 'legacy', '', 'owner', '', '{}',
                'missing-delivery', 'missing-original', 'missing-correction', '',
                '["evaluator"]', 'unknown', '2026-01-01T00:00:00+00:00',
                '2026-01-01T01:00:00+00:00', '{}', ?,
                '2026-01-01T01:00:00+00:00'
            )
            """,
            ("a" * 64,),
        )

    with pytest.raises(
        RuntimeError,
        match="legacy_adjudication_activation_operator_remediation",
    ):
        RcaDeliveryStore(live_path)

    with sqlite3.connect(live_path) as conn:
        marker = conn.execute(
            "SELECT value FROM rca_delivery_meta WHERE key = 'schema_version'"
        ).fetchone()[0]
        repair_table = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' "
            "AND name = 'rca_conclusion_adjudication_repairs'"
        ).fetchone()
    assert marker == "pnc_rca_delivery_store_v8"
    assert repair_table is None


def test_w16_v8_succeeded_repair_without_receipt_requires_operator_remediation(
    tmp_path,
):
    migration = _prepare_combined_schema_migration(
        tmp_path, "pnc_rca_delivery_store_v8"
    )
    live_path = migration["live_path"]
    with sqlite3.connect(live_path) as conn:
        conn.executescript(
            """
            CREATE TABLE rca_conclusion_adjudication_repairs(
                adjudication_id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            INSERT INTO rca_conclusion_adjudication_repairs(
                adjudication_id, status, created_at, updated_at
            ) VALUES (
                'legacy-adjudication', 'succeeded',
                '2026-01-01T00:00:00+00:00',
                '2026-01-01T00:00:00+00:00'
            );
            """
        )

    with pytest.raises(
        RuntimeError,
        match="legacy_adjudication_receipt_operator_remediation",
    ):
        RcaDeliveryStore(live_path)

    with sqlite3.connect(live_path) as conn:
        marker = conn.execute(
            "SELECT value FROM rca_delivery_meta WHERE key = 'schema_version'"
        ).fetchone()[0]
        columns = {
            str(row[1])
            for row in conn.execute(
                "PRAGMA table_info(rca_conclusion_adjudication_repairs)"
            )
        }
    assert marker == "pnc_rca_delivery_store_v8"
    assert "receipt_sha256" not in columns


def test_require_current_rejects_v6_without_mutating_it(tmp_path):
    store, _effect_key = _seed_quarantine(tmp_path)
    _downgrade_live_to_v6(store.db_path)
    with sqlite3.connect(store.db_path) as conn:
        before = conn.execute(
            "SELECT type, name, sql FROM sqlite_master ORDER BY type, name"
        ).fetchall()

    with pytest.raises(RuntimeError, match="rca_delivery_store_schema_not_current"):
        RcaDeliveryStore(store.db_path, require_current=True)

    with sqlite3.connect(store.db_path) as conn:
        after = conn.execute(
            "SELECT type, name, sql FROM sqlite_master ORDER BY type, name"
        ).fetchall()
        version = conn.execute(
            "SELECT value FROM rca_delivery_meta WHERE key = 'schema_version'"
        ).fetchone()[0]
    assert after == before
    assert version == "pnc_rca_delivery_store_v6"


def test_historical_enter_delete_requires_baseline_without_current_rows(tmp_path):
    _control(tmp_path)
    store = RcaDeliveryStore(tmp_path / "control.sqlite3")
    with sqlite3.connect(store.db_path) as conn:
        conn.executemany(
            "INSERT INTO rca_delivery_quarantine_mutation_audit("
            "entity_kind, entity_key, operation, old_status, new_status, observed_at"
            ") VALUES ('job', 'historical-job', ?, ?, ?, ?)",
            (
                ("entered", "", "quarantined", NOW.isoformat()),
                ("deleted", "quarantined", "", NOW.isoformat()),
            ),
        )

    health = store.health(now=NOW + timedelta(seconds=1))

    baseline = health["delivery_quarantine"]
    assert baseline["required"] is True
    assert baseline["state"] == "unavailable"
    assert baseline["error_code"] == "delivery_quarantine_baseline_not_configured"
    assert baseline["lifetime"] == {
        "jobs": 1,
        "effects": 0,
        "subscriptions": 0,
    }
    assert health["business_blockers"]["quarantined_jobs"] == 1


@pytest.mark.parametrize(
    "case",
    [
        "baseline_tamper",
        "baseline_mode",
        "baseline_symlink",
        "wrong_sha",
        "new_quarantine",
        "row_mutation",
        "approval_tamper",
        "settlement_tamper",
        "migration_receipt_tamper",
        "migration_source_backup_tamper",
        "migration_source_backup_sidecar",
        "active_binding_bom_tamper",
        "live_env_tamper",
    ],
)
def test_baseline_or_snapshot_drift_fails_closed(tmp_path, case):
    bundle = _build_bundle(tmp_path)
    path = bundle["baseline_path"]
    sha256 = bundle["baseline_sha256"]
    if case == "baseline_tamper":
        path.write_bytes(path.read_bytes() + b" ")
    elif case == "baseline_mode":
        path.chmod(0o640)
    elif case == "baseline_symlink":
        link = tmp_path / "baseline-link.json"
        link.symlink_to(path)
        path = link
    elif case == "wrong_sha":
        sha256 = "0" * 64
    elif case == "new_quarantine":
        with sqlite3.connect(bundle["store"].db_path) as conn:
            conn.execute(
                "UPDATE rca_delivery_subscriptions SET status = 'quarantined', "
                "reason = 'test_new_quarantine' "
                "WHERE effect_kind = 'feishu_issue_comment'"
            )
    elif case == "row_mutation":
        with sqlite3.connect(bundle["store"].db_path) as conn:
            conn.execute(
                "UPDATE rca_delivery_jobs SET updated_at = ?",
                ((NOW + timedelta(seconds=9)).isoformat(),),
            )
    elif case == "approval_tamper":
        bundle["approval_path"].write_bytes(bundle["approval_path"].read_bytes() + b" ")
    elif case == "settlement_tamper":
        bundle["receipt"].write_bytes(bundle["receipt"].read_bytes() + b" ")
    elif case == "migration_receipt_tamper":
        receipt = bundle["migration"]["receipt_path"]
        receipt.write_bytes(receipt.read_bytes() + b" ")
    elif case == "migration_source_backup_tamper":
        backup = bundle["migration"]["source_backup"]
        backup.write_bytes(backup.read_bytes() + b" ")
    elif case == "migration_source_backup_sidecar":
        backup = bundle["migration"]["source_backup"]
        Path(str(backup) + "-wal").write_bytes(b"unexpected-sidecar")
    elif case == "active_binding_bom_tamper":
        binding = json.loads(bundle["active_release_binding_path"].read_text())
        binding["bindings"]["release_bom_sha256"] = "f" * 64
        binding["policy"]["capacity_admission"]["release_bom_sha256"] = "f" * 64
        _write(bundle["active_release_binding_path"], binding)
    elif case == "live_env_tamper":
        bundle["live_env_path"].write_text("tampered=true\n")
        bundle["live_env_path"].chmod(0o600)

    health = _health(bundle, path=path, sha256=sha256)

    assert health["business_ready"] is False
    assert health["business_blockers"]["quarantine_baseline_invalid"] == 1
    assert health["delivery_quarantine"]["ready"] is False
    assert health["delivery_quarantine"]["unacknowledged"]["jobs"] == 1


@pytest.mark.parametrize("mutation", ["status_roundtrip", "insert_delete"])
def test_monotonic_quarantine_events_prevent_old_baseline_revival(tmp_path, mutation):
    bundle = _build_bundle(tmp_path)
    assert _health(bundle)["business_ready"] is True
    with sqlite3.connect(bundle["store"].db_path) as conn:
        if mutation == "status_roundtrip":
            conn.execute(
                "UPDATE rca_delivery_subscriptions SET status = 'quarantined', "
                "reason = 'test_roundtrip_quarantine' "
                "WHERE effect_kind = 'feishu_issue_comment'"
            )
            conn.execute(
                "UPDATE rca_delivery_subscriptions SET status = 'materialized', "
                "reason = 'test_roundtrip_materialized' "
                "WHERE effect_kind = 'feishu_issue_comment'"
            )
        else:
            job = conn.execute(
                "SELECT business_key, generation FROM rca_delivery_jobs LIMIT 1"
            ).fetchone()
            conn.execute(
                """
                INSERT INTO rca_delivery_subscriptions(
                    subscription_key, business_key, generation, effect_kind,
                    target_key, target_json, required, status,
                    created_at, updated_at
                ) VALUES (
                    'transient-quarantine-subscription', ?, ?,
                    'feishu_issue_comment', 'transient-target', '{}', 0,
                    'quarantined', ?, ?
                )
                """,
                (job[0], job[1], NOW.isoformat(), NOW.isoformat()),
            )
            conn.execute(
                "DELETE FROM rca_delivery_subscriptions "
                "WHERE subscription_key = 'transient-quarantine-subscription'"
            )

    health = _health(bundle)

    assert health["business_ready"] is False
    assert health["delivery_quarantine"]["error_code"] == (
        "delivery_quarantine_event_projection_mismatch"
    )


@pytest.mark.parametrize("coverage", ["empty", "unknown"])
def test_settlement_receipts_must_cover_or_explicitly_supersede_every_effect(
    tmp_path, coverage
):
    store, effect_key = _seed_quarantine(tmp_path)
    receipt = _settlement_receipt(effect_key, tmp_path)
    body = json.loads(receipt.read_text())
    body["cumulative_settled"] = (
        []
        if coverage == "empty"
        else [
            {
                "effect_key": "g1q3-rca-effect-v1-" + "f" * 64,
                "status": "quarantined",
                "write_phase": "settled",
            }
        ]
    )
    _write(receipt, body)
    migration = _prepare_offline_migration(store, tmp_path / "migration")
    _migrate_live(migration)

    if coverage == "unknown":
        with pytest.raises(
            DeliveryQuarantineBaselineError,
            match="delivery_quarantine_settlement_receipt_scope_invalid",
        ):
            build_quarantine_core(
                store.db_path,
                **_migration_kwargs(migration),
                release_id="release-baseline-001",
                snapshot_at=NOW + timedelta(seconds=2),
                settlement_receipt_paths=[receipt],
                analyzed_by="forensic-operator",
                reason="exact historical terminal rows and settlement evidence",
            )
        return

    core = build_quarantine_core(
        store.db_path,
        **_migration_kwargs(migration),
        release_id="release-baseline-001",
        snapshot_at=NOW + timedelta(seconds=2),
        settlement_receipt_paths=[receipt],
        analyzed_by="forensic-operator",
        reason="exact historical terminal rows and settlement evidence",
    )
    settlement = core["effect_settlement"]
    assert settlement["retained_terminal_count"] == 1
    assert settlement["entries"][0]["disposition"] == "retained_terminal"


@pytest.mark.parametrize(
    ("source_kind", "expected_error"),
    [
        ("feishu_group_manual", None),
        (
            "kafka_workflow_event",
            "delivery_quarantine_settlement_receipt_scope_invalid",
        ),
    ],
)
def test_settlement_accepts_only_exact_manual_thread_effects(
    tmp_path, source_kind, expected_error
):
    store, effect_keys = _seed_manual_dual_effect_quarantine(
        tmp_path, source_kind=source_kind
    )
    receipt = _settlement_receipt(effect_keys[0], tmp_path)
    body = json.loads(receipt.read_text())
    body["cumulative_settled"] = [
        {
            "effect_key": effect_key,
            "status": "quarantined",
            "write_phase": "settled",
        }
        for effect_key in effect_keys
    ]
    _write(receipt, body)
    migration = _prepare_offline_migration(store, tmp_path / "migration")
    _migrate_live(migration)
    kwargs = {
        **_migration_kwargs(migration),
        "release_id": "release-baseline-thread-001",
        "snapshot_at": NOW + timedelta(seconds=2),
        "settlement_receipt_paths": [receipt],
        "analyzed_by": "forensic-operator",
        "reason": "exact aborted manual canary effects remain terminal with no rearm",
    }

    if expected_error is not None:
        with pytest.raises(DeliveryQuarantineBaselineError, match=expected_error):
            build_quarantine_core(store.db_path, **kwargs)
        return

    core = build_quarantine_core(store.db_path, **kwargs)
    settlement = core["effect_settlement"]
    assert settlement["quarantined_effect_count"] == 2
    assert settlement["receipt_settled_count"] == 2
    assert {entry["effect_kind"] for entry in settlement["entries"]} == {
        "feishu_issue_comment",
        "feishu_thread_reply",
    }


@pytest.mark.parametrize("failure", ["missing", "sha"])
def test_settlement_backup_bytes_and_sha_are_required(tmp_path, failure):
    store, effect_key = _seed_quarantine(tmp_path)
    receipt = _settlement_receipt(effect_key, tmp_path)
    body = json.loads(receipt.read_text())
    backup = Path(body["backup"]["path"])
    if failure == "missing":
        backup.unlink()
    else:
        body["backup"]["sha256"] = "0" * 64
        _write(receipt, body)
    migration = _prepare_offline_migration(store, tmp_path / "migration")
    _migrate_live(migration)

    with pytest.raises(DeliveryQuarantineBaselineError):
        build_quarantine_core(
            store.db_path,
            **_migration_kwargs(migration),
            release_id="release-baseline-001",
            snapshot_at=NOW + timedelta(seconds=2),
            settlement_receipt_paths=[receipt],
            analyzed_by="forensic-operator",
            reason="exact historical terminal rows and settlement evidence",
        )


@pytest.mark.parametrize("payload", ["duplicate", "nan"])
def test_strict_baseline_json_rejects_duplicate_keys_and_nan(tmp_path, payload):
    bundle = _build_bundle(tmp_path)
    path = bundle["baseline_path"]
    raw = path.read_text()
    if payload == "duplicate":
        raw = '{"baseline_id":"duplicate",' + raw[1:]
    else:
        raw = '{"unexpected":NaN,' + raw[1:]
    path.write_text(raw)
    path.chmod(0o600)
    sha256 = hashlib.sha256(path.read_bytes()).hexdigest()

    health = _health(bundle, sha256=sha256)

    assert health["business_ready"] is False
    assert health["delivery_quarantine"]["error_code"].endswith(
        "duplicate_key" if payload == "duplicate" else "number_invalid"
    )


def test_database_path_identity_mismatch_fails_closed(tmp_path):
    first = _build_bundle(tmp_path / "first")
    second = _build_bundle(tmp_path / "second")

    health = second["store"].health(
        now=NOW + timedelta(seconds=5),
        quarantine_baseline_path=first["baseline_path"],
        expected_quarantine_baseline_sha256=first["baseline_sha256"],
        quarantine_release_id=first["core"]["release_id"],
        quarantine_bootstrap_epoch_id=first["bootstrap_epoch_id"],
        quarantine_active_release_binding_path=first["active_release_binding_path"],
        quarantine_live_env_path=first["live_env_path"],
    )

    assert health["business_ready"] is False
    assert health["delivery_quarantine"]["error_code"] == (
        "delivery_quarantine_migration_receipt_scope_invalid"
    )


def test_unrelated_json_cannot_be_settlement_or_approval_evidence(tmp_path):
    store, _effect_key = _seed_quarantine(tmp_path / "settlement")
    unrelated = tmp_path / "unrelated.json"
    _write(unrelated, {"ok": True})
    migration = _prepare_offline_migration(store, tmp_path / "settlement" / "migration")
    _migrate_live(migration)
    with pytest.raises(
        DeliveryQuarantineBaselineError,
        match="delivery_quarantine_settlement_receipt_schema_invalid",
    ):
        build_quarantine_core(
            store.db_path,
            **_migration_kwargs(migration),
            release_id="release-baseline-001",
            snapshot_at=NOW + timedelta(seconds=2),
            settlement_receipt_paths=[unrelated],
            analyzed_by="forensic-operator",
            reason="exact historical terminal rows and settlement evidence",
        )

    bundle = _build_bundle(tmp_path / "approval")
    unrelated_sha256 = _write(unrelated, {"decision": "approved"})
    with pytest.raises(
        DeliveryQuarantineBaselineError,
        match="delivery_quarantine_approval_scope_invalid",
    ):
        issue_quarantine_baseline(
            bundle["store"].db_path,
            quarantine_core=bundle["core"],
            release_manifest_path=bundle["release_manifest_path"],
            expected_release_manifest_sha256=hashlib.sha256(
                bundle["release_manifest_path"].read_bytes()
            ).hexdigest(),
            expected_release_bom_sha256=bundle["release_bom_sha256"],
            approval_evidence_path=unrelated,
            expected_approval_evidence_sha256=unrelated_sha256,
            baseline_id="baseline-release-001",
            issued_at=NOW + timedelta(seconds=4),
        )


def test_approval_evidence_must_be_owner_only(tmp_path):
    bundle = _build_bundle(tmp_path)
    core = bundle["core"]
    approval = tmp_path / "approval.json"
    approval_sha256 = _write(
        approval,
        {
            "schema_version": APPROVAL_SCHEMA_VERSION,
            "release_id": core["release_id"],
            "quarantine_core_sha256": core["core_sha256"],
            "release_bom_sha256": bundle["release_bom_sha256"],
            "decision": "authorize_rca_delivery_quarantine_baseline",
            "identity": {"uid": os.geteuid(), "username": "owner-user"},
            "created_at": (NOW + timedelta(seconds=3)).isoformat(),
        },
        mode=0o644,
    )

    with pytest.raises(
        DeliveryQuarantineBaselineError,
        match="delivery_quarantine_approval_evidence_permissions_invalid",
    ):
        issue_quarantine_baseline(
            bundle["store"].db_path,
            quarantine_core=core,
            release_manifest_path=bundle["release_manifest_path"],
            expected_release_manifest_sha256=hashlib.sha256(
                bundle["release_manifest_path"].read_bytes()
            ).hexdigest(),
            expected_release_bom_sha256=bundle["release_bom_sha256"],
            approval_evidence_path=approval,
            expected_approval_evidence_sha256=approval_sha256,
            baseline_id="baseline-release-001",
            issued_at=NOW + timedelta(seconds=4),
        )
