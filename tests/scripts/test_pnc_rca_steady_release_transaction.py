from __future__ import annotations

import hashlib
import json
from pathlib import Path
import plistlib
import sqlite3
import subprocess

import pytest

from gateway import pnc_rca_delivery_quarantine_baseline as quarantine_baseline
from tests.scripts.test_pnc_rca_release_transaction import (
    _fixture,
    _json_bytes,
    _seed_old_targets,
)
from scripts import pnc_rca_steady_release_transaction as transaction
from tests.scripts.test_pnc_rca_release_transaction import RELEASE_ID


SUCCESSOR_EPOCH = "rca-activation-r15l-successor"
INVENTORY_PIN = "9" * 64


def _install_activation_fixture(args: dict) -> None:
    connection = sqlite3.connect(args["control_db"])
    connection.executescript(
        """
        CREATE TABLE rca_activation_epochs (
            epoch_id TEXT PRIMARY KEY,
            state TEXT NOT NULL,
            is_current INTEGER NOT NULL,
            preauthorization_fingerprint TEXT NOT NULL,
            preauthorization_gate_receipt_sha256 TEXT NOT NULL,
            preauthorization_capsule_sha256 TEXT NOT NULL,
            preproduction_fingerprint TEXT,
            preproduction_gate_receipt_sha256 TEXT,
            preproduction_capsule_sha256 TEXT,
            config_sha256 TEXT NOT NULL,
            db_logical_identity_json TEXT NOT NULL,
            db_logical_identity_sha256 TEXT NOT NULL,
            partition_start_fence_json TEXT NOT NULL,
            partition_start_fence_sha256 TEXT NOT NULL,
            partition_end_fence_json TEXT,
            partition_end_fence_sha256 TEXT,
            production_fingerprint TEXT,
            production_gate_receipt_sha256 TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            bounded_activated_at TEXT,
            confirmed_at TEXT,
            steady_activated_at TEXT,
            aborted_at TEXT,
            superseded_at TEXT
        );
        CREATE TABLE rca_activation_budget_slots (
            epoch_id TEXT NOT NULL,
            slot_kind TEXT NOT NULL,
            authorized_source_kind TEXT,
            authorized_identity_sha256 TEXT,
            authorized_operator TEXT,
            authorized_reason TEXT,
            consumed_ledger_id INTEGER,
            consumed_at TEXT,
            authorized_at TEXT
        );
        CREATE TABLE rca_activation_transition_audit (
            audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
            epoch_id TEXT NOT NULL,
            from_state TEXT NOT NULL,
            to_state TEXT NOT NULL,
            operator TEXT NOT NULL,
            reason TEXT NOT NULL,
            binding_fingerprint TEXT NOT NULL,
            transitioned_at TEXT NOT NULL
        );
        """
    )
    epoch = {
        "epoch_id": "rca-old-epoch",
        "state": "aborted",
        "is_current": 1,
        "preauthorization_fingerprint": "1" * 64,
        "preauthorization_gate_receipt_sha256": "2" * 64,
        "preauthorization_capsule_sha256": "3" * 64,
        "preproduction_fingerprint": "4" * 64,
        "preproduction_gate_receipt_sha256": "5" * 64,
        "preproduction_capsule_sha256": "6" * 64,
        "config_sha256": "7" * 64,
        "db_logical_identity_json": "{}",
        "db_logical_identity_sha256": "8" * 64,
        "partition_start_fence_json": "{}",
        "partition_start_fence_sha256": "9" * 64,
        "partition_end_fence_json": None,
        "partition_end_fence_sha256": None,
        "production_fingerprint": "",
        "production_gate_receipt_sha256": "",
        "created_at": "2026-08-07T00:00:00+00:00",
        "updated_at": "2026-08-07T00:00:00+00:00",
        "bounded_activated_at": "",
        "confirmed_at": "",
        "steady_activated_at": "",
        "aborted_at": "2026-08-07T00:00:00+00:00",
        "superseded_at": None,
    }
    slots = [
        {
            "authorized_identity_sha256": "a" * 64,
            "authorized_operator": "owner",
            "authorized_reason": "test",
            "authorized_source_kind": "kafka",
            "consumed_ledger_id": 1,
            "slot_kind": "kafka_success",
        }
    ]
    material = transaction._activation_fingerprint_material(
        epoch, slot_bindings=slots, from_state="steady_active", to_state="aborted"
    )
    fingerprint = transaction._canonical_sha256(material)
    columns = ",".join(epoch)
    placeholders = ",".join("?" for _ in epoch)
    connection.execute(
        f"INSERT INTO rca_activation_epochs({columns}) VALUES ({placeholders})",
        tuple(epoch.values()),
    )
    connection.execute(
        """
        INSERT INTO rca_activation_budget_slots(
            epoch_id, slot_kind, authorized_source_kind,
            authorized_identity_sha256, authorized_operator,
            authorized_reason, consumed_ledger_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            epoch["epoch_id"],
            slots[0]["slot_kind"],
            slots[0]["authorized_source_kind"],
            slots[0]["authorized_identity_sha256"],
            slots[0]["authorized_operator"],
            slots[0]["authorized_reason"],
            slots[0]["consumed_ledger_id"],
        ),
    )
    connection.execute(
        """
        INSERT INTO rca_activation_transition_audit(
            epoch_id, from_state, to_state, operator, reason,
            binding_fingerprint, transitioned_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            epoch["epoch_id"],
            "steady_active",
            "aborted",
            "owner",
            "test abort",
            fingerprint,
            "2026-08-07T00:00:01+00:00",
        ),
    )
    connection.commit()
    connection.close()


def _prepare_candidate(args: dict, monkeypatch: pytest.MonkeyPatch) -> None:
    _install_activation_fixture(args)
    # The candidate's baseline file is intentionally a test stub; the focused
    # transaction tests replace the expensive DB projection with a read-only receipt.
    baseline_path = args["candidate_root"] / "baseline.json"
    baseline_raw = b'{"test":true}\n'
    baseline_path.write_bytes(baseline_raw)
    baseline_path.chmod(0o600)
    env_path = args["candidate_root"] / "candidate.env"
    env = env_path.read_text(encoding="utf-8")
    env = env.replace(
        "HERMES_RCA_DELIVERY_DISPATCHER_ENABLED=false",
        "HERMES_RCA_DELIVERY_DISPATCHER_ENABLED=true",
    )
    env += "HERMES_OUTBOUND_MODE=live\n"
    env += f"HERMES_RCA_DELIVERY_QUARANTINE_BASELINE_PATH={baseline_path}\n"
    env_path.write_text(env, encoding="utf-8")
    env_path.chmod(0o600)
    env_sha = hashlib.sha256(env.encode()).hexdigest()
    manifest_path = args["candidate_root"] / "LIVE_MANIFEST.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["env_sha256"] = env_sha
    manifest_path.write_bytes(_json_bytes(manifest))
    manifest_path.chmod(0o600)
    binding_path = args["candidate_root"] / "active-release-binding.json"
    binding = json.loads(binding_path.read_text(encoding="utf-8"))
    binding["bindings"]["candidate_env"]["sha256"] = env_sha
    binding_path.write_bytes(_json_bytes(binding))
    binding_path.chmod(0o600)

    relay_path = args["candidate_root"] / "local.pnc.completion-notice-relay.plist"
    relay = plistlib.loads(relay_path.read_bytes())
    relay["EnvironmentVariables"]["HERMES_OUTBOUND_MODE"] = "live"
    relay_path.write_bytes(plistlib.dumps(relay))
    relay_path.chmod(0o600)
    dispatcher_path = args["candidate_root"] / "local.pnc.rca-delivery-dispatcher.plist"
    dispatcher = plistlib.loads(dispatcher_path.read_bytes())
    dispatcher["EnvironmentVariables"].update(
        {
            "HERMES_OUTBOUND_MODE": "live",
            "HERMES_RCA_DELIVERY_DISPATCHER_ENABLED": "true",
            "HERMES_RCA_DELIVERY_DISPATCHER_INVENTORY_PIN": INVENTORY_PIN,
            "HERMES_RCA_DELIVERY_DISPATCHER_OBSERVABILITY_ENABLED": "true",
            "HERMES_RCA_DELIVERY_DISPATCHER_OBSERVATION_RELEASE_ID": RELEASE_ID,
        }
    )
    dispatcher_path.write_bytes(plistlib.dumps(dispatcher))
    dispatcher_path.chmod(0o600)

    anchor_values = {}
    for name in transaction.READ_ONLY_PLIST_NAMES:
        path = args["home"] / "Library/LaunchAgents" / name
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        path.write_bytes(b"read-only-anchor\n")
        path.chmod(0o600)
        anchor_values[name] = {
            "path": str(path),
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        }
    profile = {
        "schema_version": transaction.PROFILE_SCHEMA_VERSION,
        "release_id": RELEASE_ID,
        "authority_sha256": transaction.authority.canonical_json_sha256(
            json.loads((args["candidate_root"] / "authority.json").read_text())
        ),
        "source": {
            "commit": subprocess.check_output(
                ("git", "-C", str(args["source_root"]), "rev-parse", "HEAD"),
                text=True,
            ).strip(),
            "tree": subprocess.check_output(
                ("git", "-C", str(args["source_root"]), "rev-parse", "HEAD^{tree}"),
                text=True,
            ).strip(),
        },
        "activation": {
            "predecessor_epoch_id": "rca-old-epoch",
            "predecessor_state": "aborted",
            "predecessor_binding_fingerprint": transaction._read_activation_binding(
                args["control_db"]
            )["binding_fingerprint"],
            "successor_epoch_id": SUCCESSOR_EPOCH,
        },
        "read_only_plist_anchors": anchor_values,
    }
    profile_path = args["candidate_root"] / transaction.PROFILE_NAME
    profile_path.write_bytes(transaction._canonical_bytes(profile))
    profile_path.chmod(0o600)

    def fake_baseline(**kwargs):
        path = Path(kwargs["env"]["HERMES_RCA_DELIVERY_QUARANTINE_BASELINE_PATH"])
        return {
            "path": str(path),
            "observation": transaction.base._observe(path, required=True),
            "baseline_id": "test-baseline",
            "baseline_fingerprint": "b" * 64,
        }

    monkeypatch.setattr(transaction, "_validate_quarantine_baseline", fake_baseline)


def _build(args: dict):
    return transaction.build_plan(
        candidate_root=args["candidate_root"],
        source_root=args["source_root"],
        home=args["home"],
        hermes_home=args["hermes_home"],
        control_db=args["control_db"],
        evidence_root=args["evidence"],
        transaction_id="steady-test-transaction",
    )


def test_steady_transaction_accepts_aborted_predecessor_and_rolls_back(
    tmp_path, monkeypatch
):
    args = _fixture(tmp_path, monkeypatch)
    _prepare_candidate(args, monkeypatch)
    _seed_old_targets(args)
    plan, plan_path = _build(args)
    assert plan["activation_binding"]["state"] == "aborted"
    assert plan["read_only_plist_anchors"]["ai.hermes.gateway.plist"]["sha256"]
    result = transaction.apply_plan(plan, plan_path=plan_path)
    assert result["production_effects"] == {
        "database_mutation": False,
        "task_submission": False,
        "kafka_consume": False,
        "feishu_write": False,
        "resident_restart": False,
    }
    rollback = transaction.rollback_transaction(
        Path(result["receipt_path"]),
        output_path=args["evidence"] / "steady-rollback.json",
    )
    assert rollback["restored_to_pre_transaction"] is True


def test_steady_transaction_rejects_steady_predecessor(tmp_path, monkeypatch):
    args = _fixture(tmp_path, monkeypatch)
    _prepare_candidate(args, monkeypatch)
    connection = sqlite3.connect(args["control_db"])
    connection.execute(
        "UPDATE rca_activation_epochs SET state='steady_active' WHERE is_current=1"
    )
    connection.commit()
    connection.close()
    with pytest.raises(
        transaction.SteadyReleaseTransactionError,
        match="activation_not_aborted",
    ):
        _build(args)


def test_steady_transaction_rechecks_read_only_anchor_and_auto_rolls_back(
    tmp_path, monkeypatch
):
    args = _fixture(tmp_path, monkeypatch)
    _prepare_candidate(args, monkeypatch)
    _seed_old_targets(args)
    plan, plan_path = _build(args)
    anchor = args["home"] / "Library/LaunchAgents/ai.hermes.gateway.plist"
    anchor.write_bytes(b"drifted\n")
    anchor.chmod(0o600)
    with pytest.raises(
        transaction.SteadyReleaseTransactionError,
        match="profile_anchor_changed",
    ):
        transaction.apply_plan(plan, plan_path=plan_path)
    assert not (args["state_root"] / f"{RELEASE_ID}.authority.json").exists()


def test_empty_activation_baseline_is_compatible_with_current_binding():
    baseline = {
        "path": "/db",
        "control_schema_version": "schema",
        "delivery_schema_version": "delivery",
        "activation_db_logical_identity_sha256": "",
    }
    current = {
        **baseline,
        "activation_db_logical_identity_sha256": "a" * 64,
    }
    baseline["logical_identity_sha256"] = hashlib.sha256(
        transaction.quarantine_baseline._canonical_bytes(baseline)
    ).hexdigest()
    current["logical_identity_sha256"] = hashlib.sha256(
        transaction.quarantine_baseline._canonical_bytes(current)
    ).hexdigest()
    assert quarantine_baseline._db_identity_matches_baseline(baseline, current)
