from __future__ import annotations

from datetime import timedelta
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3

import pytest

from gateway import pnc_rca_delivery_store as delivery_store_module
from gateway.pnc_rca_delivery_quarantine_baseline import (
    APPROVAL_SCHEMA_VERSION,
    DeliveryQuarantineBaselineError,
    build_quarantine_core,
    build_quarantine_core_from_offline_clone,
    canonical_quarantine_baseline_bytes,
    issue_quarantine_baseline,
)
from gateway.pnc_rca_delivery_quarantine_migration import (
    QuarantineMigrationError,
    assert_live_post_migration_matches,
    assert_live_pre_migration_matches,
    build_offline_migration_receipt,
    canonical_migration_receipt_bytes,
)
from gateway.pnc_rca_delivery_store import RcaDeliveryStore
from tests.gateway.test_pnc_rca_delivery_store import (
    NOW,
    _control,
    _delivery,
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


def _build_bundle(root: Path) -> dict:
    store, effect_key = _seed_quarantine(root)
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
            "schema_version": "pnc_rca_production_env_stage_receipt_v1",
            "release_id": core["release_id"],
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
    missing = store.health(now=NOW + timedelta(seconds=5))
    assert missing["business_ready"] is False
    assert missing["business_blockers"]["quarantined_jobs"] == 1
    before_backpressure = store.backpressure_snapshot(
        now=NOW + timedelta(seconds=5)
    ).public_dict()

    health = _health(bundle)

    assert health["business_ready"] is True
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
            "updated_at = ? WHERE effect_kind = 'feishu_issue_comment'",
            ((NOW + timedelta(seconds=6)).isoformat(),),
        )
        conn.execute(
            "UPDATE rca_delivery_subscriptions SET status = 'materialized', "
            "updated_at = ? WHERE effect_kind = 'feishu_issue_comment'",
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
                "UPDATE rca_delivery_subscriptions SET status = 'quarantined' "
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
                "UPDATE rca_delivery_subscriptions SET status = 'quarantined' "
                "WHERE effect_kind = 'feishu_issue_comment'"
            )
            conn.execute(
                "UPDATE rca_delivery_subscriptions SET status = 'materialized' "
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

    with pytest.raises(
        DeliveryQuarantineBaselineError,
        match=(
            "delivery_quarantine_effect_settlement_incomplete"
            if coverage == "empty"
            else "delivery_quarantine_settlement_receipt_scope_invalid"
        ),
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
