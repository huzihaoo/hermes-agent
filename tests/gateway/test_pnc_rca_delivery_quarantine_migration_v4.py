from __future__ import annotations

import hashlib
import json
from pathlib import Path
import shutil
import sqlite3

import pytest

from gateway import pnc_rca_delivery_quarantine_baseline as baseline
from gateway import pnc_rca_delivery_quarantine_migration as migration
from gateway.pnc_rca_control_store import RcaControlStore
from gateway.pnc_rca_delivery_store import RcaDeliveryStore


RUNTIME_SHA256 = "9" * 64


def _remove_sidecars(path: Path) -> None:
    for suffix in ("-wal", "-shm", "-journal"):
        sidecar = Path(f"{path}{suffix}")
        if sidecar.exists():
            sidecar.unlink()


def _standalone(path: Path) -> None:
    with sqlite3.connect(path) as conn:
        conn.execute("PRAGMA journal_mode=DELETE")
        conn.commit()
    _remove_sidecars(path)
    path.chmod(0o600)


def _downgrade_source(path: Path) -> None:
    with sqlite3.connect(path) as conn:
        for name in sorted(migration._COUPLED_AUTHORITY_OBJECT_NAMES):
            row = conn.execute(
                "SELECT type FROM sqlite_master WHERE name = ?", (name,)
            ).fetchone()
            if row is None:
                continue
            if row[0] == "trigger":
                conn.execute(f'DROP TRIGGER "{name}"')
            elif row[0] == "index":
                conn.execute(f'DROP INDEX "{name}"')
        conn.execute(
            f'DROP TABLE IF EXISTS "{migration._COUPLED_AUTHORITY_TABLE}"'
        )
        conn.executescript(
            """
            DROP TRIGGER IF EXISTS trg_learning_lane_stock_effect_insert_forbidden;
            DROP TRIGGER IF EXISTS trg_learning_lane_stock_subscription_insert_forbidden;
            DROP TRIGGER IF EXISTS trg_learning_lane_stock_subscription_update_forbidden;
            CREATE TRIGGER trg_learning_lane_stock_effect_insert_forbidden
            BEFORE INSERT ON rca_delivery_effects
            WHEN 0
            BEGIN
                SELECT RAISE(ABORT, 'learning_lane_admission_missing');
            END;
            CREATE TRIGGER trg_learning_lane_stock_subscription_insert_forbidden
            BEFORE INSERT ON rca_delivery_subscriptions
            WHEN 0
            BEGIN
                SELECT RAISE(ABORT, 'learning_lane_admission_missing');
            END;
            CREATE TRIGGER trg_learning_lane_stock_subscription_update_forbidden
            BEFORE UPDATE OF business_key, generation, effect_kind
                ON rca_delivery_subscriptions
            WHEN 0
            BEGIN
                SELECT RAISE(ABORT, 'learning_lane_admission_missing');
            END;
            """
        )
        conn.execute(
            "UPDATE control_meta SET value = ? WHERE key = 'schema_version'",
            (migration.COUPLED_SOURCE_CONTROL_SCHEMA_VERSION,),
        )
        conn.execute(
            "UPDATE rca_delivery_meta SET value = ? WHERE key = 'schema_version'",
            (migration.COUPLED_SOURCE_DELIVERY_SCHEMA_VERSION,),
        )
        conn.execute("PRAGMA journal_mode=DELETE")
        conn.commit()
    _remove_sidecars(path)
    path.chmod(0o600)


@pytest.fixture()
def coupled_bundle(tmp_path: Path) -> dict[str, Path | str | dict]:
    seed = tmp_path / "seed.sqlite3"
    RcaControlStore(seed)
    RcaDeliveryStore(seed)
    _standalone(seed)

    source = tmp_path / "source-v13-v11.sqlite3"
    clone = tmp_path / "clone-v14-v12.sqlite3"
    shutil.copyfile(seed, source)
    shutil.copyfile(seed, clone)
    _downgrade_source(source)
    _standalone(clone)
    source_projection = migration.logical_database_projection_path(
        source, require_standalone=True
    )
    receipt = migration.build_coupled_offline_migration_receipt(
        source_backup_path=source,
        migrated_clone_path=clone,
        target_live_db_path=tmp_path / "live.sqlite3",
        migration_runtime_sha256=RUNTIME_SHA256,
        expected_source_schema_sha256=source_projection["schema_sha256"],
    )
    receipt_path = tmp_path / "coupled-receipt.json"
    receipt_path.write_bytes(migration.canonical_migration_receipt_bytes(receipt))
    receipt_path.chmod(0o600)
    return {
        "source": source,
        "clone": clone,
        "receipt": receipt_path,
        "live": tmp_path / "live.sqlite3",
        "source_schema_sha256": source_projection["schema_sha256"],
    }


def _receipt_args(bundle: dict[str, Path | str | dict]) -> dict[str, object]:
    receipt = Path(bundle["receipt"])
    body = json.loads(receipt.read_text())
    return {
        "receipt_path": receipt,
        "expected_sha256": hashlib.sha256(receipt.read_bytes()).hexdigest(),
        "target_live_db_path": bundle["live"],
        "expected_migration_runtime_sha256": RUNTIME_SHA256,
        "expected_source_schema_sha256": bundle["source_schema_sha256"],
        "migrated_db_path": bundle["clone"],
        "body": body,
    }


def test_v4_coupled_receipt_binds_both_markers_and_preserves_clone(coupled_bundle):
    args = _receipt_args(coupled_bundle)
    binding = migration.validate_combined_migration_receipt(**{
        key: value
        for key, value in args.items()
        if key != "body"
    })
    assert args["body"]["schema_version"] == migration.COUPLED_SCHEMA_VERSION
    assert binding["source_control_schema_version"] == "pnc_rca_control_store_v13"
    assert binding["source_delivery_schema_version"] == "pnc_rca_delivery_store_v11"
    assert binding["target_control_schema_version"] == "pnc_rca_control_store_v14"
    assert binding["target_delivery_schema_version"] == "pnc_rca_delivery_store_v12"
    preservation = args["body"]["cross_projection_preservation"]
    assert preservation["authority_table"]["target_row_count"] == 0
    assert set(preservation["w6_trigger_replacement"]["triggers"]) == set(
        migration._COUPLED_W6_TRIGGER_NAMES
    )


def test_v4_receipt_is_dispatched_by_quarantine_baseline(coupled_bundle):
    args = _receipt_args(coupled_bundle)
    binding = baseline._validate_migration_artifact(
        receipt_path=str(args["receipt_path"]),
        expected_sha256=str(args["expected_sha256"]),
        target_live_db_path=args["target_live_db_path"],
        migrated_db_path=args["migrated_db_path"],
        migrated_db_is_live=False,
        expected_migration_runtime_sha256=RUNTIME_SHA256,
    )
    assert binding["target_live_db_path"] == str(Path(args["target_live_db_path"]).absolute())
    assert binding["post_migration_logical_sha256"] == args["body"][
        "post_migration_logical_projection"
    ]["logical_sha256"]


@pytest.mark.parametrize(
    ("path", "value", "error"),
    [
        (
            ("target_delivery_schema_version",),
            "pnc_rca_delivery_store_v11",
            "delivery_store_coupled_migration_receipt_scope_invalid",
        ),
        (
            ("cross_projection_preservation", "authority_table", "target_row_count"),
            1,
            "delivery_store_coupled_migration_artifact_drift",
        ),
        (
            (
                "cross_projection_preservation",
                "w6_trigger_replacement",
                "triggers",
                "trg_learning_lane_stock_effect_insert_forbidden",
                "target_contract_sha256",
            ),
            "0" * 64,
            "delivery_store_coupled_migration_artifact_drift",
        ),
    ],
)
def test_v4_receipt_rejects_tampered_contract(coupled_bundle, path, value, error):
    args = _receipt_args(coupled_bundle)
    body = args.pop("body")
    cursor = body
    for key in path[:-1]:
        cursor = cursor[key]
    cursor[path[-1]] = value
    receipt = Path(args["receipt_path"])
    receipt.write_bytes(migration.canonical_migration_receipt_bytes(body))
    receipt.chmod(0o600)
    args["expected_sha256"] = hashlib.sha256(receipt.read_bytes()).hexdigest()
    with pytest.raises(migration.QuarantineMigrationError, match=error):
        migration.validate_coupled_migration_receipt(**args)


def test_v3_target_marker_remains_frozen_for_legacy_receipts():
    assert migration.COMBINED_SCHEMA_VERSION.endswith("_v3")
    assert migration.COMBINED_TARGET_SCHEMA_VERSION == "pnc_rca_delivery_store_v11"

