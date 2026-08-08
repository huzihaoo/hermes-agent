from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import plistlib
import sqlite3
import subprocess

import pytest

from gateway import pnc_rca_delivery_quarantine_baseline as quarantine_baseline
from gateway.pnc_rca_control_store import RcaControlStore
from tests.scripts.test_pnc_rca_release_transaction import (
    _fixture,
    _json_bytes,
    _seed_old_targets,
)
from scripts import pnc_rca_steady_release_transaction as transaction
from tests.scripts.test_pnc_rca_release_transaction import RELEASE_ID


SUCCESSOR_EPOCH = "rca-activation-r15l-successor"
INVENTORY_PIN = "9" * 64


def _install_activation_fixture(
    args: dict,
    *,
    predecessor_from_state: str = "steady_active",
    current_state: str = "aborted",
) -> None:
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
        "state": current_state,
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
        "aborted_at": (
            "2026-08-07T00:00:00+00:00" if current_state == "aborted" else None
        ),
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
    if predecessor_from_state == "safe_off":
        slots = []
        for field in (
            "preproduction_fingerprint",
            "preproduction_gate_receipt_sha256",
            "preproduction_capsule_sha256",
        ):
            epoch[field] = None
    audit_from_state = predecessor_from_state
    audit_to_state = "aborted"
    if current_state == "steady_active":
        slots = []
        audit_from_state = "direct_release"
        audit_to_state = "steady_active"
        epoch["partition_end_fence_json"] = epoch["partition_start_fence_json"]
        epoch["partition_end_fence_sha256"] = epoch[
            "partition_start_fence_sha256"
        ]
        epoch["production_fingerprint"] = epoch[
            "preauthorization_fingerprint"
        ]
        epoch["production_gate_receipt_sha256"] = epoch[
            "preauthorization_gate_receipt_sha256"
        ]
        epoch["steady_activated_at"] = "2026-08-07T00:00:00+00:00"
    material = transaction._activation_fingerprint_material(
        epoch,
        slot_bindings=slots,
        from_state=audit_from_state,
        to_state=audit_to_state,
    )
    fingerprint = transaction._canonical_sha256(material)
    columns = ",".join(epoch)
    placeholders = ",".join("?" for _ in epoch)
    connection.execute(
        f"INSERT INTO rca_activation_epochs({columns}) VALUES ({placeholders})",
        tuple(epoch.values()),
    )
    if slots:
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
            audit_from_state,
            audit_to_state,
            "owner",
            "test abort",
            fingerprint,
            "2026-08-07T00:00:01+00:00",
        ),
    )
    connection.commit()
    connection.close()


def _prepare_candidate(
    args: dict,
    monkeypatch: pytest.MonkeyPatch,
    *,
    current_state: str = "aborted",
) -> None:
    _install_activation_fixture(args, current_state=current_state)
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
        "HERMES_RCA_DELIVERY_DISPATCHER_ENABLED=false",
    )
    env += "HERMES_OUTBOUND_MODE=record-only\n"
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
    relay["EnvironmentVariables"]["HERMES_OUTBOUND_MODE"] = "record-only"
    relay_path.write_bytes(plistlib.dumps(relay))
    relay_path.chmod(0o600)
    dispatcher_path = args["candidate_root"] / "local.pnc.rca-delivery-dispatcher.plist"
    dispatcher = plistlib.loads(dispatcher_path.read_bytes())
    dispatcher["EnvironmentVariables"].update(
        {
            "HERMES_OUTBOUND_MODE": "record-only",
            "HERMES_RCA_DELIVERY_DISPATCHER_ENABLED": "false",
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
    activation_binding = transaction._read_activation_binding(args["control_db"])
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
            "predecessor_state": activation_binding["state"],
            "predecessor_binding_fingerprint": activation_binding[
                "binding_fingerprint"
            ],
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
            "status_sha256": "c" * 64,
            "db_logical_identity_sha256": "d" * 64,
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


def test_steady_transaction_accepts_zero_binding_safe_off_predecessor(
    tmp_path, monkeypatch
):
    args = _fixture(tmp_path, monkeypatch)
    _install_activation_fixture(args, predecessor_from_state="safe_off")
    binding = transaction._read_activation_binding(args["control_db"])
    assert binding["state"] == "aborted"


def test_steady_transaction_rejects_bound_safe_off_predecessor(
    tmp_path, monkeypatch
):
    args = _fixture(tmp_path, monkeypatch)
    _install_activation_fixture(args, predecessor_from_state="safe_off")
    connection = sqlite3.connect(args["control_db"])
    connection.execute(
        "UPDATE rca_activation_epochs SET preproduction_fingerprint=? WHERE is_current=1",
        ("4" * 64,),
    )
    connection.commit()
    connection.close()
    with pytest.raises(
        transaction.SteadyReleaseTransactionError,
        match="activation_audit_invalid",
    ):
        transaction._read_activation_binding(args["control_db"])


def test_steady_transaction_accepts_exact_steady_predecessor(
    tmp_path,
    monkeypatch,
):
    args = _fixture(tmp_path, monkeypatch)
    _prepare_candidate(args, monkeypatch, current_state="steady_active")
    _seed_old_targets(args)

    plan, plan_path = _build(args)
    assert plan["activation_binding"]["state"] == "steady_active"
    result = transaction.apply_plan(plan, plan_path=plan_path)

    assert result["verification"] == "pass"
    assert result["activation_binding"] == plan["activation_binding"]
    assert result["production_effects"] == {
        "database_mutation": False,
        "task_submission": False,
        "kafka_consume": False,
        "feishu_write": False,
        "resident_restart": False,
    }


def test_steady_transaction_reads_live_direct_steady_fingerprint(
    tmp_path,
    monkeypatch,
):
    args = _fixture(tmp_path, monkeypatch)
    args["control_db"].unlink()
    store = RcaControlStore(args["control_db"])
    epoch = store.activate_direct_steady_epoch(
        epoch_id="rca-live-direct-predecessor",
        release_fingerprint="1" * 64,
        release_binding_sha256="2" * 64,
        config_sha256="3" * 64,
        db_logical_identity={"logical_store_id": "test-control"},
        partition_start_fence={"feishu-project-workflow-event": {"0": 10}},
        operator="release-test",
        reason="bind live direct predecessor",
    )

    transaction_binding = transaction._read_activation_binding(args["control_db"])
    store_binding = store.direct_steady_predecessor()

    assert store_binding is not None
    assert transaction_binding == {
        "epoch_id": epoch["epoch_id"],
        "state": "steady_active",
        "binding_fingerprint": store_binding["binding_fingerprint"],
        "transition_audit_id": store_binding["audit_id"],
        "transitioned_at": store_binding["transitioned_at"],
    }


def test_steady_transaction_rejects_steady_predecessor_fingerprint_drift(
    tmp_path,
    monkeypatch,
):
    args = _fixture(tmp_path, monkeypatch)
    _prepare_candidate(args, monkeypatch, current_state="steady_active")
    connection = sqlite3.connect(args["control_db"])
    connection.execute(
        "UPDATE rca_activation_transition_audit "
        "SET binding_fingerprint = ? WHERE epoch_id = 'rca-old-epoch'",
        ("f" * 64,),
    )
    connection.commit()
    connection.close()

    with pytest.raises(
        transaction.SteadyReleaseTransactionError,
        match="activation_fingerprint_invalid",
    ):
        _build(args)


def test_steady_apply_rejects_steady_predecessor_drift_before_file_mutation(
    tmp_path,
    monkeypatch,
):
    args = _fixture(tmp_path, monkeypatch)
    _prepare_candidate(args, monkeypatch, current_state="steady_active")
    _seed_old_targets(args)
    plan, plan_path = _build(args)
    prior_env = (args["hermes_home"] / ".env").read_bytes()
    connection = sqlite3.connect(args["control_db"])
    connection.execute(
        "UPDATE rca_activation_transition_audit "
        "SET binding_fingerprint = ? WHERE epoch_id = 'rca-old-epoch'",
        ("f" * 64,),
    )
    connection.commit()
    connection.close()

    with pytest.raises(
        transaction.SteadyReleaseTransactionError,
        match="activation_fingerprint_invalid",
    ):
        transaction.apply_plan(plan, plan_path=plan_path)
    assert (args["hermes_home"] / ".env").read_bytes() == prior_env
    assert not (args["state_root"] / f"{RELEASE_ID}.authority.json").exists()


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


def test_steady_apply_rejects_db_backed_baseline_drift(tmp_path, monkeypatch):
    args = _fixture(tmp_path, monkeypatch)
    _prepare_candidate(args, monkeypatch)
    _seed_old_targets(args)
    plan, plan_path = _build(args)

    def drifted_baseline(**_kwargs):
        value = dict(plan["quarantine_baseline"])
        value["status_sha256"] = "e" * 64
        return value

    monkeypatch.setattr(transaction, "_validate_quarantine_baseline", drifted_baseline)
    with pytest.raises(
        transaction.SteadyReleaseTransactionError,
        match="baseline_status_changed",
    ):
        transaction.apply_plan(plan, plan_path=plan_path)
    assert not (args["state_root"] / f"{RELEASE_ID}.authority.json").exists()
    assert (args["hermes_home"] / ".env").read_text() != (
        args["candidate_root"] / "candidate.env"
    ).read_text()


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


def test_apply_tracks_replace_when_post_replace_fsync_fails(tmp_path, monkeypatch):
    args = _fixture(tmp_path, monkeypatch)
    _prepare_candidate(args, monkeypatch)
    _seed_old_targets(args)
    plan, plan_path = _build(args)
    original_sync = transaction._sync_directory
    calls = 0

    def fail_first_sync(path):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise OSError("injected post-replace fsync failure")
        original_sync(path)

    monkeypatch.setattr(transaction, "_sync_directory", fail_first_sync)
    with pytest.raises(OSError, match="post-replace fsync failure"):
        transaction.apply_plan(plan, plan_path=plan_path)
    authority_path = args["state_root"] / f"{RELEASE_ID}.authority.json"
    assert not authority_path.exists()
    automatic = json.loads(
        (args["evidence"] / "steady-test-transaction/automatic-rollback.json").read_text()
    )
    assert automatic["restored_to_pre_transaction"] is True
    assert automatic["blocked_entries"] == []
    assert automatic["restored_entries"][0]["name"] == "authority"


def test_automatic_rollback_preserves_concurrent_target_and_records_partial(
    tmp_path, monkeypatch
):
    args = _fixture(tmp_path, monkeypatch)
    _prepare_candidate(args, monkeypatch)
    _seed_old_targets(args)
    plan, plan_path = _build(args)
    replacements = 0
    concurrent = b"concurrent-owner\n"

    def replace_then_conflict(source, target):
        nonlocal replacements
        replacements += 1
        os.replace(source, target)
        if replacements == 2:
            Path(target).write_bytes(concurrent)
            Path(target).chmod(0o600)
            raise OSError("injected concurrent replacement")

    with pytest.raises(
        transaction.SteadyReleaseTransactionError,
        match="automatic_rollback_incomplete",
    ):
        transaction.apply_plan(
            plan,
            plan_path=plan_path,
            replace_func=replace_then_conflict,
        )
    authority_path = args["state_root"] / f"{RELEASE_ID}.authority.json"
    pointer_path = args["state_root"] / "ACTIVE_RCA_RELEASE.json"
    assert not authority_path.exists()
    assert pointer_path.read_bytes() == concurrent
    automatic = json.loads(
        (args["evidence"] / "steady-test-transaction/automatic-rollback.json").read_text()
    )
    assert automatic["restored_to_pre_transaction"] is False
    assert [item["name"] for item in automatic["blocked_entries"]] == ["pointer"]
    assert automatic["blocked_entries"][0]["reason"] == "after_observation_missing"


def test_manual_rollback_preserves_concurrent_target_and_records_partial(
    tmp_path, monkeypatch
):
    args = _fixture(tmp_path, monkeypatch)
    _prepare_candidate(args, monkeypatch)
    _seed_old_targets(args)
    plan, plan_path = _build(args)
    receipt = transaction.apply_plan(plan, plan_path=plan_path)
    env_path = args["hermes_home"] / ".env"
    concurrent = b"concurrent-env-owner\n"
    env_path.write_bytes(concurrent)
    env_path.chmod(0o600)
    output_path = args["evidence"] / "partial-manual-rollback.json"
    with pytest.raises(
        transaction.SteadyReleaseTransactionError,
        match="manual_rollback_incomplete",
    ):
        transaction.rollback_transaction(
            Path(receipt["receipt_path"]), output_path=output_path
        )
    assert env_path.read_bytes() == concurrent
    assert not (args["state_root"] / f"{RELEASE_ID}.authority.json").exists()
    rollback = json.loads(output_path.read_text())
    assert rollback["restored_to_pre_transaction"] is False
    assert [item["name"] for item in rollback["blocked_entries"]] == ["env"]
    assert rollback["blocked_entries"][0]["reason"] == "target_changed"


def test_manual_rollback_refuses_replaced_activation_epoch(tmp_path, monkeypatch):
    args = _fixture(tmp_path, monkeypatch)
    _prepare_candidate(args, monkeypatch)
    _seed_old_targets(args)
    plan, plan_path = _build(args)
    receipt = transaction.apply_plan(plan, plan_path=plan_path)
    env_path = args["hermes_home"] / ".env"
    installed = env_path.read_bytes()
    connection = sqlite3.connect(args["control_db"])
    connection.execute(
        "UPDATE rca_activation_epochs SET state='steady_active' WHERE is_current=1"
    )
    connection.commit()
    connection.close()
    with pytest.raises(
        transaction.SteadyReleaseTransactionError,
        match="activation_audit_invalid",
    ):
        transaction.rollback_transaction(
            Path(receipt["receipt_path"]),
            output_path=args["evidence"] / "replaced-epoch-rollback.json",
        )
    assert env_path.read_bytes() == installed


def test_manual_rollback_rechecks_after_immediately_before_restore(
    tmp_path, monkeypatch
):
    args = _fixture(tmp_path, monkeypatch)
    _prepare_candidate(args, monkeypatch)
    _seed_old_targets(args)
    plan, plan_path = _build(args)
    receipt = transaction.apply_plan(plan, plan_path=plan_path)
    env_path = args["hermes_home"] / ".env"
    concurrent = b"concurrent-between-cas-and-restore\n"
    original_observe = transaction.base._observe
    env_observations = 0

    def drift_on_second_env_observation(path, *, required):
        nonlocal env_observations
        if Path(path) == env_path:
            env_observations += 1
            if env_observations == 2:
                env_path.write_bytes(concurrent)
                env_path.chmod(0o600)
        return original_observe(path, required=required)

    monkeypatch.setattr(transaction.base, "_observe", drift_on_second_env_observation)
    output_path = args["evidence"] / "between-cas-manual-rollback.json"
    with pytest.raises(
        transaction.SteadyReleaseTransactionError,
        match="manual_rollback_incomplete",
    ):
        transaction.rollback_transaction(
            Path(receipt["receipt_path"]), output_path=output_path
        )
    assert env_path.read_bytes() == concurrent
    rollback = json.loads(output_path.read_text())
    assert [item["name"] for item in rollback["blocked_entries"]] == ["env"]
    assert rollback["blocked_entries"][0]["reason"] == "target_changed_before_restore"
