from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sqlite3
from typing import Any

import pytest

from gateway import pnc_rca_capacity_runtime as capacity_runtime
from gateway import pnc_rca_capacity_sample_evidence as capacity_evidence
from gateway import pnc_rca_capacity_transition as capacity_transition
from gateway import pnc_rca_prod_bootstrap as prod_bootstrap
from gateway.pnc_rca_control_store import (
    ActivationDeferralResult,
    RcaControlStore,
    ShadowPromotionError,
    ShadowPromotionResult,
)
from scripts import pnc_rca_activation as activation_module


EPOCH_ID = "rca-activation-20260712"
TOPIC = "feishu-project-workflow-event"
OPERATOR = "release-operator"
REASON = "approved exact activation operation"
CONFIG_SHA256 = "2" * 64
DB_IDENTITY_SHA256 = "5" * 64
START_FENCE_SHA256 = "6" * 64
RELEASE_BINDING_SHA256 = "7" * 64
PREAUTHORIZATION_FINGERPRINT = "1" * 64
PREAUTHORIZATION_RECEIPT_SHA256 = "a" * 64
PREAUTHORIZATION_CAPSULE_SHA256 = "b" * 64
PREPRODUCTION_FINGERPRINT = "c" * 64
PREPRODUCTION_RECEIPT_SHA256 = "d" * 64
PREPRODUCTION_CAPSULE_SHA256 = "e" * 64
NOW = datetime(2026, 7, 13, 2, 0, tzinfo=timezone.utc)
CAPACITY_RELEASE_ID = "release-approval-20260713"
BOOTSTRAP_EPOCH_ID = "rca-bootstrap-release-20260713"
RELEASE_BOM_SHA256 = "f" * 64
ACTIVE_RELEASE_BINDING_SHA256 = "0" * 64
BOOTSTRAP_AUTHORIZATION_SHA256 = "3" * 64
BOOTSTRAP_AUTHORIZATION_FINGERPRINT = "4" * 64
APPROVAL_EVIDENCE_SHA256 = "8" * 64
HMAC_KEY = bytes.fromhex("42" * 32)


def _write_json(tmp_path: Path, name: str, value: Any) -> Path:
    path = tmp_path / name
    path.write_text(
        json.dumps(value, ensure_ascii=False, sort_keys=True),
        encoding="utf-8",
    )
    return path


def _write_secure_json(path: Path, value: Any) -> bytes:
    raw = (
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")
    path.write_bytes(raw)
    path.chmod(0o600)
    return raw


def _activation_db_identity() -> dict[str, Any]:
    return {
        "schema_version": "pnc_rca_activation_db_identity_v1",
        "strategy": "fresh_install_preserve",
        "databases": {
            "control": {
                "path": "/var/lib/rca/control.sqlite3",
                "device": 7,
                "inode": 11,
                "schema_version": "pnc_rca_control_store_v10",
            },
            "delivery": {
                "path": "/var/lib/rca/delivery.sqlite3",
                "device": 7,
                "inode": 12,
                "schema_version": "pnc_rca_delivery_store_v6",
            },
        },
        "migration_receipt_raw_sha256": "8" * 64,
        "materialization_receipt_raw_sha256": "9" * 64,
        "host_commit": "f" * 40,
        "config_sha256": CONFIG_SHA256,
    }


def _preauthorization_input(epoch_id: str = EPOCH_ID) -> dict[str, Any]:
    db_identity = _activation_db_identity()
    start_fence = {TOPIC: {"0": 10}}
    return {
        "epoch_id": epoch_id,
        "initial_state": "safe_off",
        "preauthorization_fingerprint": PREAUTHORIZATION_FINGERPRINT,
        "preauthorization_gate_receipt_sha256": PREAUTHORIZATION_RECEIPT_SHA256,
        "preauthorization_capsule_sha256": PREAUTHORIZATION_CAPSULE_SHA256,
        "config_sha256": CONFIG_SHA256,
        "db_logical_identity": db_identity,
        "db_logical_identity_sha256": activation_module._sha256_json(db_identity),
        "partition_start_fence": start_fence,
        "partition_start_fence_sha256": activation_module._sha256_json(start_fence),
        "migration_receipt_raw_sha256": "8" * 64,
        "materialization_receipt_raw_sha256": "9" * 64,
        "broker_t0_observation_sha256": "0" * 64,
    }


def _canary_slot_plan() -> dict[str, dict[str, Any]]:
    identities: dict[str, dict[str, Any]] = {
        "manual_success": {
            "chat_id": "oc_activation_test",
            "requester_id": "ou_activation_test",
            "message_id": "om-success",
            "thread_id": "topic:om-success",
            "issue_url": "https://project.feishu.cn/g1q3/issue/detail/7041712813",
            "mode": "run_or_join",
        },
        "manual_terminal_failure": {
            "chat_id": "oc_activation_test",
            "requester_id": "ou_activation_test",
            "message_id": "om-failure",
            "thread_id": "topic:om-failure",
            "issue_url": "https://project.feishu.cn/g1q3/issue/detail/7041712814",
            "mode": "run_or_join",
        },
    }
    return {
        slot_kind: {
            "source_kind": (
                "kafka" if slot_kind == "kafka_success" else "manual"
            ),
            "entrypoint": (
                "kafka_ingest" if slot_kind == "kafka_success" else "manual_admit"
            ),
            "source_identity": identity,
            "source_identity_sha256": activation_module._sha256_json(identity),
            "max_admissions": 1,
            "expected_admission": {
                "business_key": f"business-{slot_kind}",
                "submission_key": f"submission-{slot_kind}",
                "generation": 1,
            },
            "expected_outcome": (
                "terminal_failed"
                if slot_kind == "manual_terminal_failure"
                else "success"
            ),
        }
        for slot_kind, identity in identities.items()
    }


def _preproduction_input(epoch_id: str = EPOCH_ID) -> dict[str, Any]:
    preauthorization = _preauthorization_input(epoch_id)
    canary_slot_plan = _canary_slot_plan()
    return {
        "epoch_id": epoch_id,
        "expected_state": "safe_off",
        "target_state": "preauthorized",
        "expected_preauthorization_fingerprint": PREAUTHORIZATION_FINGERPRINT,
        "expected_preauthorization_gate_receipt_sha256": (
            PREAUTHORIZATION_RECEIPT_SHA256
        ),
        "expected_preauthorization_capsule_sha256": (
            PREAUTHORIZATION_CAPSULE_SHA256
        ),
        "expected_config_sha256": CONFIG_SHA256,
        "expected_db_logical_identity_sha256": preauthorization[
            "db_logical_identity_sha256"
        ],
        "expected_partition_start_fence_sha256": preauthorization[
            "partition_start_fence_sha256"
        ],
        "kafka_proof_mode": activation_module.ACTIVATION_KAFKA_PROOF_MODE,
        "required_slot_kinds": list(
            activation_module.ACTIVATION_RELEASE_SLOT_KINDS
        ),
        "canary_slot_plan": canary_slot_plan,
        "canary_slot_plan_sha256": activation_module._sha256_json(
            canary_slot_plan
        ),
        "preproduction_fingerprint": PREPRODUCTION_FINGERPRINT,
        "preproduction_gate_receipt_sha256": PREPRODUCTION_RECEIPT_SHA256,
        "preproduction_capsule_sha256": PREPRODUCTION_CAPSULE_SHA256,
    }


@pytest.fixture(autouse=True)
def _canonical_activation_capsule_readers(monkeypatch):
    def read(path: Path, **_kwargs: Any) -> dict[str, Any]:
        return json.loads(Path(path).read_text(encoding="utf-8"))

    monkeypatch.setattr(activation_module, "_read_preauthorization_capsule", read)
    monkeypatch.setattr(activation_module, "_read_preproduction_capsule", read)


def _confirmation_capsule(
    tmp_path: Path,
    *,
    end_offset: int = 20,
    epoch_id: str = EPOCH_ID,
    config_sha256: str = CONFIG_SHA256,
    db_identity_sha256: str = DB_IDENTITY_SHA256,
    start_fence_sha256: str = START_FENCE_SHA256,
    release_binding_sha256: str = RELEASE_BINDING_SHA256,
) -> tuple[Path, Path, dict[str, Any]]:
    end_fence = {TOPIC: {"0": end_offset}}
    confirm_input = {
        "epoch_id": epoch_id,
        "expected_state": "bounded_active",
        "target_state": "confirmed",
        "config_sha256": config_sha256,
        "db_logical_identity_sha256": db_identity_sha256,
        "partition_start_fence_sha256": start_fence_sha256,
        "release_binding_sha256": release_binding_sha256,
        "partition_end_fence": end_fence,
        "partition_end_fence_sha256": activation_module._sha256_json(end_fence),
        "production_fingerprint_source": "release_gate_report.fingerprint",
        "production_gate_receipt_sha256_source": (
            "sha256(exact_written_release_gate_receipt)"
        ),
        "restart_between_gate_and_confirm": False,
    }
    fingerprint = "3" * 64
    evaluated_at = "2026-07-12T08:00:00+00:00"
    report = {
        "schema_version": "pnc_rca_release_gate_v1",
        "evaluated_at": evaluated_at,
        "mode": "production",
        "ok": True,
        "fingerprint": fingerprint,
        "checks": [
            {
                "name": "activation_writer_barrier",
                "ok": True,
                "code": "pass",
                "detail": {
                    "epoch_id": epoch_id,
                    "state": "bounded_active",
                    "release_binding_sha256": release_binding_sha256,
                    "transition_performed": False,
                    "production_confirmation_required": True,
                    "confirm_input": confirm_input,
                    "confirm_input_sha256": activation_module._sha256_json(
                        confirm_input
                    ),
                },
            }
        ],
        "blockers": [],
    }
    receipt_path = tmp_path / "release-gate.json"
    receipt_raw = _write_secure_json(receipt_path, report)
    transition = {
        key: confirm_input[key]
        for key in (
            "epoch_id",
            "expected_state",
            "target_state",
            "config_sha256",
            "db_logical_identity_sha256",
            "partition_start_fence_sha256",
            "release_binding_sha256",
            "partition_end_fence",
            "partition_end_fence_sha256",
        )
    }
    transition.update(
        {
            "production_fingerprint": fingerprint,
            "production_gate_receipt_sha256": hashlib.sha256(
                receipt_raw
            ).hexdigest(),
        }
    )
    capsule = {
        "schema_version": "pnc_rca_activation_confirmation_capsule_v2",
        "created_at": evaluated_at,
        "release_gate_receipt": {
            "path": str(receipt_path.absolute()),
            "size_bytes": len(receipt_raw),
            "raw_sha256": hashlib.sha256(receipt_raw).hexdigest(),
            "report_fingerprint": fingerprint,
        },
        "ingress_freeze_binding": {
            "schema_version": "pnc_rca_activation_ingress_freeze_binding_v1",
            "epoch_id": epoch_id,
            "health_path": str((tmp_path / "consumer-health.json").absolute()),
            "paused_at": evaluated_at,
            "freeze_receipt_sha256": "8" * 64,
            "freeze_token_sha256": "9" * 64,
            "consumer_runtime_identity_sha256": "a" * 64,
            "partition_positions_sha256": activation_module._sha256_json(
                end_fence
            ),
            "restart_required": False,
        },
        "transition_input": transition,
        "transition_input_sha256": activation_module._sha256_json(transition),
        "operator_supplied_scope_fields": [],
        "same_file_descriptor_verification_required": True,
    }
    capsule_path = tmp_path / "release-gate.activation-confirmation.json"
    _write_secure_json(capsule_path, capsule)
    return capsule_path, receipt_path, transition


def _manual_identity(
    tmp_path: Path,
    name: str,
    *,
    message_id: str,
    issue_id: int,
    mode: str = "run_or_join",
    extra: dict[str, Any] | None = None,
) -> Path:
    value: dict[str, Any] = {
        "chat_id": "oc_activation_test",
        "requester_id": "ou_activation_test",
        "message_id": message_id,
        "thread_id": f"topic:{message_id}",
        "issue_url": f"https://project.feishu.cn/g1q3/issue/detail/{issue_id}",
        "mode": mode,
    }
    value.update(extra or {})
    return _write_json(tmp_path, name, value)


def _create_args(
    db_path: Path,
    tmp_path: Path,
    *,
    epoch_id: str = EPOCH_ID,
    apply: bool = False,
) -> list[str]:
    capsule = _write_json(
        tmp_path,
        f"preauthorization-{epoch_id}.json",
        _preauthorization_input(epoch_id),
    )
    args = [
        "--control-db",
        str(db_path),
        "create",
        "--operator",
        OPERATOR,
        "--reason",
        REASON,
        "--preauthorization-capsule",
        str(capsule),
    ]
    if apply:
        args.append("--apply")
    return args


def _preauthorize_args(
    db_path: Path,
    tmp_path: Path,
    *,
    epoch_id: str = EPOCH_ID,
    apply: bool = False,
) -> list[str]:
    capsule = _write_json(
        tmp_path,
        f"preproduction-{epoch_id}.json",
        _preproduction_input(epoch_id),
    )
    args = [
        "--control-db",
        str(db_path),
        "transition-preauthorized",
        "--operator",
        OPERATOR,
        "--reason",
        REASON,
        "--preproduction-capsule",
        str(capsule),
    ]
    if apply:
        args.append("--apply")
    return args


def _authorization_capsule(tmp_path: Path, name: str = "authorization-plan.json") -> Path:
    return _write_json(tmp_path, name, _preproduction_input())


def _mutation_args(db_path: Path, command: str) -> list[str]:
    return [
        "--control-db",
        str(db_path),
        command,
        "--epoch-id",
        EPOCH_ID,
        "--operator",
        OPERATOR,
        "--reason",
        REASON,
    ]


def _bootstrap_capacity_state(**overrides: Any) -> dict[str, Any]:
    value = {
        field: None for field in capacity_transition.PERSISTED_CAPACITY_STATE_FIELDS
    }
    value.update(
        {
            "singleton_id": 1,
            "release_id": CAPACITY_RELEASE_ID,
            "bootstrap_epoch_id": BOOTSTRAP_EPOCH_ID,
            "state": capacity_transition.BOOTSTRAP_PRODUCTION,
            "generation": 1,
            "bootstrap_initialized_at": (NOW - timedelta(hours=1)).isoformat(),
            "updated_at": (NOW - timedelta(hours=1)).isoformat(),
        }
    )
    value.update(overrides)
    return value


def _steady_capacity_state() -> dict[str, Any]:
    initialized = NOW - timedelta(days=8)
    issued = NOW - timedelta(hours=1)
    created = NOW - timedelta(minutes=50)
    committed = NOW - timedelta(minutes=40)
    activated = NOW - timedelta(minutes=30)
    value = _bootstrap_capacity_state(
        state=capacity_transition.STEADY_ACTIVE,
        generation=2,
        bootstrap_initialized_at=initialized.isoformat(),
        updated_at=activated.isoformat(),
        steady_activated_at=activated.isoformat(),
        authorization_issued_at=issued.isoformat(),
        authorization_expires_at=(NOW + timedelta(minutes=30)).isoformat(),
        receipt_created_at=created.isoformat(),
        marker_committed_at=committed.isoformat(),
    )
    for field in (
        "final_ledger_sha256",
        "transition_authorization_sha256",
        "transition_authorization_fingerprint",
        "transition_receipt_sha256",
        "transition_receipt_fingerprint",
        "commit_marker_sha256",
        "commit_marker_fingerprint",
        "evidence_bundle_sha256",
        "evidence_bundle_fingerprint",
    ):
        value[field] = hashlib.sha256(field.encode()).hexdigest()
    return value


def _bootstrap_authority_values() -> tuple[dict[str, Any], dict[str, Any]]:
    binding = {
        "binding_ready": True,
        "binding_receipt_sha256": ACTIVE_RELEASE_BINDING_SHA256,
        "release_id": CAPACITY_RELEASE_ID,
        "bootstrap_epoch_id": BOOTSTRAP_EPOCH_ID,
        "release_bom_sha256": RELEASE_BOM_SHA256,
        "approval_evidence_sha256": APPROVAL_EVIDENCE_SHA256,
        "authorization_receipt_sha256": BOOTSTRAP_AUTHORIZATION_SHA256,
        "authorization_fingerprint": BOOTSTRAP_AUTHORIZATION_FINGERPRINT,
        "candidate_env_sha256": "9" * 64,
    }
    authorization = {
        "authorization_ready": True,
        "capacity_mode": "bootstrap",
        "authorized_by": OPERATOR,
        "authorization_receipt_sha256": BOOTSTRAP_AUTHORIZATION_SHA256,
        "receipt_fingerprint": BOOTSTRAP_AUTHORIZATION_FINGERPRINT,
        "bootstrap_epoch_id": BOOTSTRAP_EPOCH_ID,
        "release_approval_id": CAPACITY_RELEASE_ID,
        "release_bom_sha256": RELEASE_BOM_SHA256,
        "approval_evidence_sha256": APPROVAL_EVIDENCE_SHA256,
        "started_at": (NOW - timedelta(hours=6)).isoformat(),
        "deadline": (NOW + timedelta(days=7, hours=18)).isoformat(),
    }
    return binding, authorization


def _stub_bootstrap_authority(
    monkeypatch,
    *,
    started_at: datetime | None = None,
    deadline: datetime | None = None,
    now: datetime = NOW,
) -> tuple[dict[str, Any], dict[str, Any]]:
    binding, authorization = _bootstrap_authority_values()
    if started_at is not None:
        authorization["started_at"] = started_at.isoformat()
    if deadline is not None:
        authorization["deadline"] = deadline.isoformat()
    monkeypatch.setattr(
        activation_module.prod_bootstrap,
        "load_active_release_binding",
        lambda **_kwargs: dict(binding),
    )
    monkeypatch.setattr(
        activation_module.prod_bootstrap,
        "load_bootstrap_authorization",
        lambda **_kwargs: dict(authorization),
    )
    monkeypatch.setattr(activation_module, "_utc_now", lambda: now)
    monkeypatch.setenv(
        capacity_runtime.HMAC_ENV, "hex:" + HMAC_KEY.hex()
    )
    return binding, authorization


def _steady_bootstrap_args(db_path: Path, live_env: Path) -> list[str]:
    return [
        *_mutation_args(db_path, "transition-steady"),
        "--active-release-binding",
        str(db_path.parent / prod_bootstrap.ACTIVE_RELEASE_BINDING_NAME),
        "--live-env",
        str(live_env),
        "--release-id",
        CAPACITY_RELEASE_ID,
        "--bootstrap-epoch-id",
        BOOTSTRAP_EPOCH_ID,
    ]


def _prepare_bootstrap_args(
    db_path: Path, live_env: Path, preproduction_capsule: Path
) -> list[str]:
    args = _steady_bootstrap_args(db_path, live_env)
    command_index = args.index("transition-steady")
    args[command_index] = "prepare-bootstrap-production"
    args.extend(["--preproduction-capsule", str(preproduction_capsule)])
    return args


def _bounded_fake_store() -> "_FakeStore":
    transition = _preproduction_input()
    fake = _FakeStore("bounded_active", capacity_state=_bootstrap_capacity_state())
    fake.current.update(
        {
            "preauthorization_fingerprint": transition[
                "expected_preauthorization_fingerprint"
            ],
            "preauthorization_gate_receipt_sha256": transition[
                "expected_preauthorization_gate_receipt_sha256"
            ],
            "preauthorization_capsule_sha256": transition[
                "expected_preauthorization_capsule_sha256"
            ],
            "config_sha256": transition["expected_config_sha256"],
            "db_logical_identity_sha256": transition[
                "expected_db_logical_identity_sha256"
            ],
            "partition_start_fence_sha256": transition[
                "expected_partition_start_fence_sha256"
            ],
            "preproduction_fingerprint": transition["preproduction_fingerprint"],
            "preproduction_gate_receipt_sha256": transition[
                "preproduction_gate_receipt_sha256"
            ],
            "preproduction_capsule_sha256": transition[
                "preproduction_capsule_sha256"
            ],
        }
    )
    fake.slot_authorizations = {
        slot_kind: {
            "source_kind": transition["canary_slot_plan"][slot_kind]["source_kind"],
            "source_identity_sha256": transition["canary_slot_plan"][slot_kind][
                "source_identity_sha256"
            ],
        }
        for slot_kind in sorted(transition["canary_slot_plan"])
    }
    return fake


def _confirm_args(db_path: Path, capsule_path: Path) -> list[str]:
    return [
        "--control-db",
        str(db_path),
        "confirm",
        "--operator",
        OPERATOR,
        "--reason",
        REASON,
        "--confirmation-capsule",
        str(capsule_path),
    ]


def _stub_live_binding(monkeypatch, transition, *, release_sha256=None):
    monkeypatch.setattr(
        activation_module,
        "_live_release_binding",
        lambda _path, **_kwargs: {
            field: (
                release_sha256
                if field == "release_binding_sha256" and release_sha256
                else transition[field]
            )
            for field in (
                "epoch_id",
                "config_sha256",
                "db_logical_identity_sha256",
                "partition_start_fence_sha256",
                "release_binding_sha256",
            )
        },
    )


def _invoke(capsys: pytest.CaptureFixture[str], argv: list[str]) -> tuple[int, dict]:
    code = activation_module.main(argv)
    captured = capsys.readouterr()
    assert captured.err == ""
    lines = captured.out.splitlines()
    assert len(lines) == 1
    return code, json.loads(lines[0])


def _create_epoch(store: RcaControlStore) -> None:
    preauthorization = _preauthorization_input()
    created = store.create_activation_epoch(
        epoch_id=EPOCH_ID,
        preauthorization_fingerprint=PREAUTHORIZATION_FINGERPRINT,
        preauthorization_gate_receipt_sha256=PREAUTHORIZATION_RECEIPT_SHA256,
        preauthorization_capsule_sha256=PREAUTHORIZATION_CAPSULE_SHA256,
        config_sha256=CONFIG_SHA256,
        db_logical_identity=preauthorization["db_logical_identity"],
        partition_start_fence=preauthorization["partition_start_fence"],
        operator=OPERATOR,
        reason=REASON,
    )
    store.preauthorize_activation_epoch(
        epoch_id=EPOCH_ID,
        preproduction_fingerprint=PREPRODUCTION_FINGERPRINT,
        preproduction_gate_receipt_sha256=PREPRODUCTION_RECEIPT_SHA256,
        preproduction_capsule_sha256=PREPRODUCTION_CAPSULE_SHA256,
        expected_preauthorization_fingerprint=PREAUTHORIZATION_FINGERPRINT,
        expected_preauthorization_gate_receipt_sha256=(
            PREAUTHORIZATION_RECEIPT_SHA256
        ),
        expected_preauthorization_capsule_sha256=PREAUTHORIZATION_CAPSULE_SHA256,
        expected_config_sha256=CONFIG_SHA256,
        expected_db_logical_identity_sha256=created[
            "db_logical_identity_sha256"
        ],
        expected_partition_start_fence_sha256=created[
            "partition_start_fence_sha256"
        ],
        operator=OPERATOR,
        reason=REASON,
    )


def _authorize_all_slots(store: RcaControlStore) -> None:
    for slot, message_id, issue_id in (
        ("manual_success", "om-success", 7041712813),
        ("manual_terminal_failure", "om-failure", 7041712814),
    ):
        store.authorize_activation_slot(
            epoch_id=EPOCH_ID,
            slot_kind=slot,
            source_kind="manual",
            source_identity={
                "chat_id": "oc_activation_test",
                "requester_id": "ou_activation_test",
                "message_id": message_id,
                "thread_id": f"topic:{message_id}",
                "issue_url": (
                    f"https://project.feishu.cn/g1q3/issue/detail/{issue_id}"
                ),
                "mode": "run_or_join",
            },
            operator=OPERATOR,
            reason=REASON,
        )


def test_default_status_is_read_only_and_redacts_database_path(tmp_path, capsys):
    db_path = tmp_path / "control.sqlite3"
    store = RcaControlStore(db_path)

    code, payload = _invoke(capsys, ["--control-db", str(db_path)])

    assert code == 0
    assert payload["command"] == "status"
    assert payload["mode"] == "read_only"
    assert payload["result"]["activation"]["configured"] is False
    assert store.activation_epoch() is None
    encoded = json.dumps(payload, sort_keys=True)
    assert str(db_path) not in encoded
    assert "payload" not in encoded


def test_status_can_bind_one_exact_current_epoch(tmp_path, capsys):
    db_path = tmp_path / "control.sqlite3"
    store = RcaControlStore(db_path)
    _create_epoch(store)

    code, payload = _invoke(
        capsys,
        ["--control-db", str(db_path), "status", "--epoch-id", EPOCH_ID],
    )
    assert code == 0
    assert payload["result"]["activation"]["current_epoch"]["epoch_id"] == EPOCH_ID

    code, payload = _invoke(
        capsys,
        ["--control-db", str(db_path), "status", "--epoch-id", "other-epoch"],
    )
    assert code == 2
    assert payload["code"] == "activation_epoch_not_current"


def test_plan_never_creates_a_missing_control_database(tmp_path, capsys):
    db_path = tmp_path / "missing.sqlite3"

    code, payload = _invoke(capsys, _create_args(db_path, tmp_path))

    assert code == 2
    assert payload["code"] == "activation_control_db_unavailable"
    assert not db_path.exists()


def test_status_never_migrates_a_predecessor_database(tmp_path, capsys):
    db_path = tmp_path / "predecessor.sqlite3"
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("CREATE TABLE control_meta(key TEXT PRIMARY KEY, value TEXT NOT NULL)")
    conn.execute(
        "INSERT INTO control_meta(key, value) VALUES ('schema_version', 'v8')"
    )
    conn.commit()
    conn.close()

    code, payload = _invoke(capsys, ["--control-db", str(db_path)])

    assert code == 2
    assert payload["code"] == "activation_control_db_schema_not_current"
    conn = sqlite3.connect(db_path)
    try:
        assert conn.execute(
            "SELECT value FROM control_meta WHERE key = 'schema_version'"
        ).fetchone() == ("v8",)
        assert conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
        ).fetchall() == [("control_meta",)]
    finally:
        conn.close()


@pytest.mark.parametrize(
    "suffix", [".pnc-rca-maintenance", ".pnc-rca-tombstone"]
)
def test_status_fails_closed_while_control_database_is_installation_fenced(
    tmp_path, capsys, suffix
):
    db_path = tmp_path / "control.sqlite3"
    RcaControlStore(db_path)
    Path(f"{db_path}{suffix}").write_text("installation active", encoding="utf-8")

    code, payload = _invoke(capsys, ["--control-db", str(db_path)])

    assert code == 2
    assert payload["code"] == "activation_control_db_installation_fenced"


def test_create_is_plan_by_default_and_plan_output_is_deterministic(tmp_path, capsys):
    db_path = tmp_path / "control.sqlite3"
    store = RcaControlStore(db_path)
    args = _create_args(db_path, tmp_path)

    first_code, first = _invoke(capsys, args)
    second_code, second = _invoke(capsys, args)

    assert first_code == second_code == 0
    assert first == second
    assert first["mode"] == "plan"
    assert first["applied"] is False
    assert first["result"]["would_change"] is True
    assert store.activation_epoch() is None
    encoded = json.dumps(first, sort_keys=True)
    assert OPERATOR not in encoded
    assert REASON not in encoded


def test_create_apply_is_idempotent(tmp_path, capsys):
    db_path = tmp_path / "control.sqlite3"
    RcaControlStore(db_path)
    args = _create_args(db_path, tmp_path, apply=True)

    first_code, first = _invoke(capsys, args)
    second_code, second = _invoke(capsys, args)

    assert first_code == second_code == 0
    assert first["result"]["changed"] is True
    assert second["result"]["changed"] is False
    epoch = RcaControlStore(db_path).activation_epoch()
    assert epoch is not None
    assert epoch["epoch_id"] == EPOCH_ID
    assert epoch["state"] == "safe_off"


def test_transition_preauthorized_is_capsule_bound_plan_apply_and_idempotent(
    tmp_path, capsys
):
    db_path = tmp_path / "control.sqlite3"
    RcaControlStore(db_path)
    create_code, _created = _invoke(
        capsys, _create_args(db_path, tmp_path, apply=True)
    )
    args = _preauthorize_args(db_path, tmp_path)

    plan_code, plan = _invoke(capsys, args)
    planned_epoch = RcaControlStore(db_path).activation_epoch()
    first_code, first = _invoke(capsys, [*args, "--apply"])
    second_code, second = _invoke(capsys, [*args, "--apply"])

    assert create_code == plan_code == first_code == second_code == 0
    assert plan["mode"] == "plan"
    assert plan["result"]["would_change"] is True
    assert planned_epoch is not None
    assert planned_epoch["state"] == "safe_off"
    assert first["result"]["changed"] is True
    assert second["result"]["changed"] is False
    epoch = RcaControlStore(db_path).activation_epoch()
    assert epoch is not None
    assert epoch["state"] == "preauthorized"
    assert epoch["preproduction_fingerprint"] == PREPRODUCTION_FINGERPRINT
    assert (
        epoch["preproduction_gate_receipt_sha256"]
        == PREPRODUCTION_RECEIPT_SHA256
    )
    assert epoch["preproduction_capsule_sha256"] == PREPRODUCTION_CAPSULE_SHA256


def test_create_can_supersede_one_exact_aborted_epoch(tmp_path, capsys):
    db_path = tmp_path / "control.sqlite3"
    store = RcaControlStore(db_path)
    _create_epoch(store)
    store.transition_activation_epoch(
        epoch_id=EPOCH_ID,
        target_state="aborted",
        expected_state="preauthorized",
        operator=OPERATOR,
        reason=REASON,
    )
    next_epoch = "rca-activation-20260712-next"
    args = _create_args(db_path, tmp_path, epoch_id=next_epoch, apply=True)

    code, payload = _invoke(capsys, args)

    assert code == 0
    assert payload["result"]["changed"] is True
    current = RcaControlStore(db_path).activation_epoch()
    assert current is not None
    assert current["epoch_id"] == next_epoch
    assert current["state"] == "safe_off"


def test_authorize_plan_apply_and_idempotent_retry_use_one_exact_manual_message(
    tmp_path, capsys
):
    db_path = tmp_path / "control.sqlite3"
    store = RcaControlStore(db_path)
    _create_epoch(store)
    identity_value = {
        "chat_id": "oc_activation_test",
        "requester_id": "ou_activation_test",
        "message_id": "om-success",
        "thread_id": "topic:om-success",
        "issue_url": "https://project.feishu.cn/g1q3/issue/detail/7041712813",
        "mode": "run_or_join",
    }
    identity = _write_json(tmp_path, "manual-success.json", identity_value)
    args = [
        *_mutation_args(db_path, "authorize"),
        "--slot-kind",
        "manual_success",
        "--preproduction-capsule",
        str(_authorization_capsule(tmp_path)),
        "--manual-identity-json",
        str(identity),
    ]

    plan_code, plan = _invoke(capsys, args)
    assert plan_code == 0
    assert plan["mode"] == "plan"
    assert store.health()["activation"]["slots"]["manual_success"]["authorized"] is False

    apply_args = [*args, "--apply"]
    first_code, first = _invoke(capsys, apply_args)
    second_code, second = _invoke(capsys, apply_args)

    assert first_code == second_code == 0
    assert first["result"] == second["result"]
    assert first["result"]["source_identity_sha256"] == hashlib.sha256(
        activation_module._canonical_json(identity_value).encode()
    ).hexdigest()
    assert identity_value["message_id"] not in json.dumps(first, sort_keys=True)
    assert store.health()["activation"]["slots"]["manual_success"]["authorized"] is True


def test_authorize_rejects_valid_identity_outside_frozen_canary_plan(
    tmp_path, capsys
):
    db_path = tmp_path / "control.sqlite3"
    store = RcaControlStore(db_path)
    _create_epoch(store)

    code, payload = _invoke(
        capsys,
        [
            *_mutation_args(db_path, "authorize"),
            "--slot-kind",
            "manual_success",
            "--preproduction-capsule",
            str(_authorization_capsule(tmp_path)),
            "--manual-identity-json",
            str(
                _manual_identity(
                    tmp_path,
                    "manual-outside-plan.json",
                    message_id="om-outside-plan",
                    issue_id=7041712813,
                )
            ),
        ],
    )

    assert code == 2
    assert payload["code"] == "activation_canary_plan_identity_mismatch"
    assert store.activation_slot_authorizations(epoch_id=EPOCH_ID)[
        "manual_success"
    ]["source_identity_sha256"] is None


def test_authorize_rejects_manual_identity_extra_fields_without_echoing_payload(
    tmp_path, capsys
):
    db_path = tmp_path / "control.sqlite3"
    store = RcaControlStore(db_path)
    _create_epoch(store)
    secret = "DO-NOT-ECHO-MANUAL-PAYLOAD"
    identity = _manual_identity(
        tmp_path,
        "manual.json",
        message_id="om-secret",
        issue_id=7041712813,
        extra={"raw_payload": secret},
    )

    code, payload = _invoke(
        capsys,
        [
            *_mutation_args(db_path, "authorize"),
            "--slot-kind",
            "manual_success",
            "--preproduction-capsule",
            str(_authorization_capsule(tmp_path)),
            "--manual-identity-json",
            str(identity),
        ],
    )

    assert code == 2
    assert payload["code"] == "activation_manual_identity_fields_invalid"
    assert secret not in json.dumps(payload, sort_keys=True)
    assert store.health()["activation"]["slots"]["manual_success"]["authorized"] is False


def test_kafka_release_slot_is_not_authorizable_and_never_echoes_argument(
    tmp_path, capsys
):
    db_path = tmp_path / "control.sqlite3"
    store = RcaControlStore(db_path)
    _create_epoch(store)
    invalid = "secret-topic*:0:12"

    code, payload = _invoke(
        capsys,
        [
            *_mutation_args(db_path, "authorize"),
            "--slot-kind",
            "kafka_success",
            "--preproduction-capsule",
            str(_authorization_capsule(tmp_path)),
            "--event-uid",
            invalid,
        ],
    )

    assert code == 2
    assert payload["code"] == "activation_cli_arguments_invalid"
    assert invalid not in json.dumps(payload, sort_keys=True)


def test_transition_bounded_is_plan_apply_and_idempotent(tmp_path, capsys):
    db_path = tmp_path / "control.sqlite3"
    store = RcaControlStore(db_path)
    _create_epoch(store)
    _authorize_all_slots(store)
    args = [
        *_mutation_args(db_path, "transition-bounded"),
        "--preproduction-capsule",
        str(_authorization_capsule(tmp_path)),
    ]

    plan_code, plan = _invoke(capsys, args)
    assert plan_code == 0
    assert plan["result"]["target_state"] == "bounded_active"
    assert store.activation_epoch()["state"] == "preauthorized"  # type: ignore[index]

    first_code, first = _invoke(capsys, [*args, "--apply"])
    second_code, second = _invoke(capsys, [*args, "--apply"])

    assert first_code == second_code == 0
    assert first["result"]["changed"] is True
    assert second["result"]["changed"] is False
    assert store.activation_epoch()["state"] == "bounded_active"  # type: ignore[index]


def test_transition_bounded_rejects_authorizations_outside_frozen_plan(
    tmp_path, capsys
):
    db_path = tmp_path / "control.sqlite3"
    store = RcaControlStore(db_path)
    _create_epoch(store)
    _authorize_all_slots(store)
    drifted = _preproduction_input()
    identity = {
        **drifted["canary_slot_plan"]["manual_success"]["source_identity"],
        "message_id": "om-drifted",
        "thread_id": "topic:om-drifted",
    }
    drifted["canary_slot_plan"]["manual_success"]["source_identity"] = identity
    drifted["canary_slot_plan"]["manual_success"][
        "source_identity_sha256"
    ] = activation_module._sha256_json(identity)
    drifted["canary_slot_plan_sha256"] = activation_module._sha256_json(
        drifted["canary_slot_plan"]
    )
    capsule = _write_json(tmp_path, "drifted-authorization-plan.json", drifted)

    code, payload = _invoke(
        capsys,
        [
            *_mutation_args(db_path, "transition-bounded"),
            "--preproduction-capsule",
            str(capsule),
        ],
    )

    assert code == 2
    assert payload["code"] == "activation_canary_plan_authorizations_mismatch"
    assert store.activation_epoch()["state"] == "preauthorized"  # type: ignore[index]


class _FakeStore:
    def __init__(
        self, state: str, *, capacity_state: dict[str, Any] | None = None
    ):
        self.current: dict[str, Any] = {
            "epoch_id": EPOCH_ID,
            "state": state,
            "config_sha256": CONFIG_SHA256,
            "db_logical_identity_sha256": DB_IDENTITY_SHA256,
            "partition_start_fence_sha256": START_FENCE_SHA256,
            "partition_end_fence_sha256": "",
            "production_fingerprint": "",
            "production_gate_receipt_sha256": "",
        }
        self.transition_calls: list[dict[str, Any]] = []
        self.promotion_calls: list[tuple[str, dict[str, Any]]] = []
        self.deferral_calls: list[tuple[str, dict[str, Any]]] = []
        self.transition_error: Exception | None = None
        self.promotion_error: Exception | None = None
        self.capacity = capacity_state
        self.slot_authorizations: dict[str, Any] = {}

    def activation_epoch(self) -> dict[str, Any]:
        return dict(self.current)

    def capacity_transition_state(self) -> dict[str, Any] | None:
        return dict(self.capacity) if self.capacity is not None else None

    def activation_slot_authorizations(self, *, epoch_id: str) -> dict[str, Any]:
        assert epoch_id == EPOCH_ID
        return dict(self.slot_authorizations)

    def transition_activation_epoch(self, **kwargs: Any) -> dict[str, Any]:
        self.transition_calls.append(dict(kwargs))
        if self.transition_error is not None:
            raise self.transition_error
        target = str(kwargs["target_state"])
        self.current["state"] = target
        if target == "confirmed":
            self.current["partition_end_fence_sha256"] = activation_module._sha256_json(
                kwargs["partition_end_fence"]
            )
            self.current["production_fingerprint"] = kwargs["production_fingerprint"]
            self.current["production_gate_receipt_sha256"] = kwargs[
                "production_gate_receipt_sha256"
            ]
        return dict(self.current)

    def promote_shadow_event(self, event_uid: str, **kwargs: Any) -> ShadowPromotionResult:
        self.promotion_calls.append((event_uid, dict(kwargs)))
        if self.promotion_error is not None:
            raise self.promotion_error
        return ShadowPromotionResult(
            event_uid=event_uid,
            outbox_id=41,
            submission_key="secret-submission-key",
            status="pending",
            promoted=len(self.promotion_calls) == 1,
            audit_id=100 + len(self.promotion_calls),
        )

    def defer_activation_event(
        self, event_uid: str, **kwargs: Any
    ) -> ActivationDeferralResult:
        self.deferral_calls.append((event_uid, dict(kwargs)))
        retry = len(self.deferral_calls) > 1
        return ActivationDeferralResult(
            event_uid=event_uid,
            epoch_id=EPOCH_ID,
            outbox_id=42,
            submission_key="secret-deferred-submission-key",
            prior_status="quarantined" if retry else "shadow",
            status="quarantined",
            audit_id=200 + len(self.deferral_calls),
        )


def test_confirm_binds_end_fence_production_fingerprint_and_receipt_idempotently(
    tmp_path, capsys, monkeypatch
):
    fake = _FakeStore("bounded_active")
    monkeypatch.setattr(activation_module, "_open_store", lambda _path: fake)
    db_path = tmp_path / "control.sqlite3"
    capsule, _receipt, transition = _confirmation_capsule(tmp_path)
    canonical_calls: list[dict[str, Any]] = []
    monkeypatch.setattr(
        activation_module,
        "_live_release_binding",
        lambda _path, **_kwargs: {
            field: transition[field]
            for field in (
                "epoch_id",
                "config_sha256",
                "db_logical_identity_sha256",
                "partition_start_fence_sha256",
                "release_binding_sha256",
            )
        },
    )
    monkeypatch.setattr(
        activation_module,
        "_canonical_confirmation_transition",
        lambda **kwargs: (
            canonical_calls.append(dict(kwargs)) or dict(transition)
        ),
    )
    args = [
        "--control-db",
        str(db_path),
        "confirm",
        "--operator",
        OPERATOR,
        "--reason",
        REASON,
        "--confirmation-capsule",
        str(capsule),
    ]

    plan_code, plan = _invoke(capsys, args)
    assert plan_code == 0
    assert plan["result"]["would_change"] is True
    assert fake.transition_calls == []

    first_code, first = _invoke(capsys, [*args, "--apply"])
    second_code, second = _invoke(capsys, [*args, "--apply"])

    assert first_code == second_code == 0
    assert first["result"]["changed"] is True
    assert second["result"]["changed"] is False
    assert len(fake.transition_calls) == 2
    assert len(canonical_calls) == 3
    assert canonical_calls[0]["receipt_path"] == _receipt
    assert canonical_calls[0]["current_activation"][
        "release_binding_sha256"
    ] == RELEASE_BINDING_SHA256
    assert canonical_calls[-1]["current_activation"]["state"] == "confirmed"
    assert fake.transition_calls[0]["expected_state"] == "bounded_active"
    assert fake.transition_calls[1]["expected_state"] == "confirmed"
    assert fake.transition_calls[0]["partition_end_fence"] == {TOPIC: {"0": 20}}
    assert fake.transition_calls[0]["production_fingerprint"] == "3" * 64
    assert fake.transition_calls[0]["production_gate_receipt_sha256"] == transition[
        "production_gate_receipt_sha256"
    ]
    assert fake.transition_calls[0]["expected_config_sha256"] == CONFIG_SHA256
    assert fake.transition_calls[0]["expected_db_logical_identity_sha256"] == (
        DB_IDENTITY_SHA256
    )
    assert fake.transition_calls[0]["expected_partition_start_fence_sha256"] == (
        START_FENCE_SHA256
    )
    assert fake.transition_calls[0]["expected_release_binding_sha256"] == (
        RELEASE_BINDING_SHA256
    )


def test_confirm_rejects_capsule_end_fence_tampering(tmp_path, capsys, monkeypatch):
    fake = _FakeStore("bounded_active")
    monkeypatch.setattr(activation_module, "_open_store", lambda _path: fake)
    capsule, _receipt, transition = _confirmation_capsule(tmp_path)
    body = json.loads(capsule.read_text(encoding="utf-8"))
    body["transition_input"]["partition_end_fence"][TOPIC]["0"] = 999
    _write_secure_json(capsule, body)
    _stub_live_binding(monkeypatch, transition)
    monkeypatch.setattr(
        activation_module,
        "_canonical_confirmation_transition",
        lambda **_kwargs: activation_module._normalize_confirmation_transition(
            body["transition_input"]
        ),
    )

    code, payload = _invoke(
        capsys,
        _confirm_args(tmp_path / "control.sqlite3", capsule),
    )

    assert code == 2
    assert payload["code"] == "activation_confirmation_transition_input_invalid"
    assert fake.transition_calls == []


def test_confirm_rejects_release_receipt_changed_after_capsule(
    tmp_path, capsys, monkeypatch
):
    fake = _FakeStore("bounded_active")
    monkeypatch.setattr(activation_module, "_open_store", lambda _path: fake)
    capsule, receipt, transition = _confirmation_capsule(tmp_path)
    changed = json.loads(receipt.read_text(encoding="utf-8"))
    changed["warnings"] = ["changed-after-capsule"]
    _write_secure_json(receipt, changed)
    _stub_live_binding(monkeypatch, transition)

    code, payload = _invoke(
        capsys,
        _confirm_args(tmp_path / "control.sqlite3", capsule),
    )

    assert code == 2
    assert payload["code"] == "activation_confirmation_capsule_rejected"
    assert fake.transition_calls == []


def test_confirm_passes_live_release_binding_to_canonical_reader(
    tmp_path, capsys, monkeypatch
):
    fake = _FakeStore("bounded_active")
    monkeypatch.setattr(activation_module, "_open_store", lambda _path: fake)
    capsule, _receipt, transition = _confirmation_capsule(tmp_path)
    _stub_live_binding(monkeypatch, transition, release_sha256="8" * 64)
    observed: dict[str, Any] = {}

    def reject_drift(**kwargs):
        observed.update(kwargs)
        raise activation_module.ActivationCliError(
            "activation_confirmation_capsule_rejected"
        )

    monkeypatch.setattr(
        activation_module,
        "_canonical_confirmation_transition",
        reject_drift,
    )

    code, payload = _invoke(
        capsys,
        _confirm_args(tmp_path / "control.sqlite3", capsule),
    )

    assert code == 2
    assert payload["code"] == "activation_confirmation_capsule_rejected"
    assert observed["current_activation"]["release_binding_sha256"] == "8" * 64
    assert fake.transition_calls == []


def test_confirm_rejects_current_epoch_core_binding_drift(
    tmp_path, capsys, monkeypatch
):
    fake = _FakeStore("bounded_active")
    fake.current["config_sha256"] = "8" * 64
    monkeypatch.setattr(activation_module, "_open_store", lambda _path: fake)
    capsule, _receipt, _transition = _confirmation_capsule(tmp_path)

    code, payload = _invoke(
        capsys,
        _confirm_args(tmp_path / "control.sqlite3", capsule),
    )

    assert code == 2
    assert payload["code"] == "activation_confirmation_epoch_binding_changed"
    assert fake.transition_calls == []


def test_confirm_rejects_operator_supplied_scope_arguments(tmp_path, capsys, monkeypatch):
    fake = _FakeStore("bounded_active")
    monkeypatch.setattr(activation_module, "_open_store", lambda _path: fake)
    capsule, _receipt, _transition = _confirmation_capsule(tmp_path)

    code, payload = _invoke(
        capsys,
        [*_confirm_args(tmp_path / "control.sqlite3", capsule), "--epoch-id", EPOCH_ID],
    )

    assert code == 2
    assert payload["code"] == "activation_cli_arguments_invalid"
    assert fake.transition_calls == []


@pytest.mark.parametrize("kind", ["permissions", "symlink"])
def test_confirm_requires_owner_only_regular_capsule(
    tmp_path, capsys, monkeypatch, kind
):
    fake = _FakeStore("bounded_active")
    monkeypatch.setattr(activation_module, "_open_store", lambda _path: fake)
    capsule, _receipt, _transition = _confirmation_capsule(tmp_path)
    candidate = capsule
    if kind == "permissions":
        capsule.chmod(0o644)
        expected = "activation_confirmation_capsule_file_permissions_invalid"
    else:
        candidate = tmp_path / "capsule-link.json"
        candidate.symlink_to(capsule)
        expected = "activation_confirmation_capsule_file_not_regular"

    code, payload = _invoke(
        capsys,
        _confirm_args(tmp_path / "control.sqlite3", candidate),
    )

    assert code == 2
    assert payload["code"] == expected
    assert fake.transition_calls == []


def test_confirmed_reconcile_is_exact_epoch_event_and_has_no_canary_slot(
    tmp_path, capsys, monkeypatch
):
    fake = _FakeStore("confirmed")
    monkeypatch.setattr(activation_module, "_open_store", lambda _path: fake)
    db_path = tmp_path / "control.sqlite3"
    event_uid = f"{TOPIC}:0:18"
    args = [
        *_mutation_args(db_path, "reconcile-shadow"),
        "--event-uid",
        event_uid,
    ]

    plan_code, _plan = _invoke(capsys, args)
    assert plan_code == 0
    assert fake.promotion_calls == []

    first_code, first = _invoke(capsys, [*args, "--apply"])
    second_code, second = _invoke(capsys, [*args, "--apply"])

    assert first_code == second_code == 0
    assert first["result"]["changed"] is True
    assert second["result"]["changed"] is False
    assert fake.promotion_calls[0] == (
        event_uid,
        {
            "operator": OPERATOR,
            "reason": REASON,
            "expected_activation_epoch_id": EPOCH_ID,
            "activation_required": True,
            "activation_slot_kind": "",
        },
    )
    encoded = json.dumps(first, sort_keys=True)
    assert event_uid not in encoded
    assert "secret-submission-key" not in encoded


def test_reconcile_slot_policy_is_state_specific(tmp_path, capsys, monkeypatch):
    fake = _FakeStore("confirmed")
    monkeypatch.setattr(activation_module, "_open_store", lambda _path: fake)
    db_path = tmp_path / "control.sqlite3"
    base = [
        *_mutation_args(db_path, "reconcile-shadow"),
        "--event-uid",
        f"{TOPIC}:0:18",
    ]

    code, payload = _invoke(
        capsys, [*base, "--slot-kind", "kafka_success", "--apply"]
    )
    assert code == 2
    assert payload["code"] == "activation_confirmed_reconcile_slot_forbidden"
    assert fake.promotion_calls == []

    fake.current["state"] = "bounded_active"
    code, payload = _invoke(capsys, [*base, "--apply"])
    assert code == 2
    assert payload["code"] == "activation_bounded_reconcile_slot_required"
    assert fake.promotion_calls == []


def test_defer_event_is_exact_plan_apply_and_idempotent(tmp_path, capsys, monkeypatch):
    fake = _FakeStore("aborted")
    monkeypatch.setattr(activation_module, "_open_store", lambda _path: fake)
    db_path = tmp_path / "control.sqlite3"
    event_uid = f"{TOPIC}:0:19"
    args = [
        *_mutation_args(db_path, "defer-event"),
        "--event-uid",
        event_uid,
    ]

    plan_code, plan = _invoke(capsys, args)
    assert plan_code == 0
    assert plan["mode"] == "plan"
    assert plan["result"]["would_change"] is None
    assert fake.deferral_calls == []

    first_code, first = _invoke(capsys, [*args, "--apply"])
    second_code, second = _invoke(capsys, [*args, "--apply"])

    assert first_code == second_code == 0
    assert first["result"]["changed"] is True
    assert second["result"]["changed"] is False
    assert fake.deferral_calls[0] == (
        event_uid,
        {
            "expected_activation_epoch_id": EPOCH_ID,
            "operator": OPERATOR,
            "reason": REASON,
        },
    )
    encoded = json.dumps(first, sort_keys=True)
    assert event_uid not in encoded
    assert "secret-deferred-submission-key" not in encoded


def test_defer_event_accepts_exact_manual_message_id(
    tmp_path, capsys, monkeypatch
):
    fake = _FakeStore("aborted")
    monkeypatch.setattr(activation_module, "_open_store", lambda _path: fake)
    db_path = tmp_path / "control.sqlite3"
    args = [
        *_mutation_args(db_path, "defer-event"),
        "--message-id",
        "om_manual_exact",
    ]

    plan_code, plan = _invoke(capsys, args)
    assert plan_code == 0
    assert plan["mode"] == "plan"
    apply_code, applied = _invoke(capsys, [*args, "--apply"])
    assert apply_code == 0
    assert applied["result"]["changed"] is True
    assert fake.deferral_calls[0][0] == "om_manual_exact"


@pytest.mark.parametrize(
    ("state", "command", "target"),
    [
        ("steady_active", "abort", "aborted"),
    ],
)
def test_remaining_transitions_are_plan_by_default_and_apply_exact_state(
    tmp_path, capsys, monkeypatch, state, command, target
):
    fake = _FakeStore(state)
    monkeypatch.setattr(activation_module, "_open_store", lambda _path: fake)
    args = _mutation_args(tmp_path / "control.sqlite3", command)

    plan_code, plan = _invoke(capsys, args)
    assert plan_code == 0
    assert plan["result"]["target_state"] == target
    assert fake.transition_calls == []

    apply_code, _applied = _invoke(capsys, [*args, "--apply"])
    assert apply_code == 0
    assert fake.transition_calls[0]["expected_state"] == state
    assert fake.transition_calls[0]["target_state"] == target


def test_bootstrap_prepare_is_read_only_then_steady_only_validates_and_transitions(
    tmp_path, capsys, monkeypatch
):
    db_path = tmp_path / "control.sqlite3"
    live_env = tmp_path / ".env"
    fake = _bounded_fake_store()
    monkeypatch.setattr(activation_module, "_open_store", lambda _path: fake)
    _stub_bootstrap_authority(monkeypatch)
    prepare_args = _prepare_bootstrap_args(
        db_path, live_env, _authorization_capsule(tmp_path)
    )
    paths = capacity_runtime.CapacityRuntimePaths.from_control_db(db_path)

    plan_code, plan = _invoke(capsys, prepare_args)

    assert plan_code == 0
    assert plan["result"]["would_publish_producer_receipt"] is True
    assert not paths.state_root.exists()
    assert fake.transition_calls == []

    events: list[str] = []
    real_write = capacity_evidence.write_owner_only_create_once
    real_runtime = activation_module._validate_capacity_runtime_locked

    def write(path, value):
        events.append("producer")
        return real_write(path, value)

    def validate_runtime(**kwargs):
        events.append("runtime")
        return real_runtime(**kwargs)

    monkeypatch.setattr(capacity_evidence, "write_owner_only_create_once", write)
    monkeypatch.setattr(
        activation_module, "_validate_capacity_runtime_locked", validate_runtime
    )
    code, payload = _invoke(capsys, [*prepare_args, "--apply"])

    assert code == 0, payload
    assert events == ["producer", "runtime"]
    assert fake.current["state"] == "bounded_active"
    assert payload["result"]["runtime_effective_state"] == (
        capacity_transition.BOOTSTRAP_PRODUCTION
    )
    assert payload["result"]["producer_activation_receipt_sha256"]
    receipt, raw_sha = capacity_evidence.read_and_validate_producer_activation(
        paths.producer_activation,
        hmac_key=HMAC_KEY,
        expected_release_id=CAPACITY_RELEASE_ID,
        expected_bootstrap_epoch_id=BOOTSTRAP_EPOCH_ID,
        expected_release_bom_sha256=RELEASE_BOM_SHA256,
        expected_active_release_binding_sha256=ACTIVE_RELEASE_BINDING_SHA256,
    )
    assert raw_sha == payload["result"]["producer_activation_receipt_sha256"]
    assert receipt["activated_at"] == NOW.isoformat().replace("+00:00", "Z")

    resolver = capacity_runtime.CapacityRuntimeResolver(
        store=fake,
        control_db_path=db_path,
        release_id=CAPACITY_RELEASE_ID,
        bootstrap_epoch_id=BOOTSTRAP_EPOCH_ID,
        initial_policy="bootstrap",
        hmac_key=HMAC_KEY,
        now=lambda: NOW,
    )
    runtime_decision = resolver.observe()
    assert runtime_decision["effective_state"] == (
        capacity_transition.BOOTSTRAP_PRODUCTION
    )
    assert runtime_decision["ledger"]["sample_count"] == 0

    events.clear()
    fake.current["state"] = "confirmed"
    real_transition = fake.transition_activation_epoch

    def transition_business(**kwargs):
        events.append("business")
        return real_transition(**kwargs)

    monkeypatch.setattr(fake, "transition_activation_epoch", transition_business)
    steady_args = _steady_bootstrap_args(db_path, live_env)
    steady_code, steady = _invoke(capsys, [*steady_args, "--apply"])

    assert steady_code == 0
    assert events == ["runtime", "business"]
    assert steady["result"]["changed"] is True
    assert paths.producer_activation.read_bytes() == capacity_evidence.canonical_bytes(
        receipt
    )


def test_bootstrap_prepare_rejects_insufficient_producer_window_before_write(
    tmp_path, capsys, monkeypatch
):
    db_path = tmp_path / "control.sqlite3"
    fake = _bounded_fake_store()
    monkeypatch.setattr(activation_module, "_open_store", lambda _path: fake)
    _stub_bootstrap_authority(
        monkeypatch,
        started_at=NOW - timedelta(days=1),
        deadline=NOW + timedelta(days=7),
    )
    args = _prepare_bootstrap_args(
        db_path, tmp_path / ".env", _authorization_capsule(tmp_path)
    )
    producer_path = capacity_runtime.CapacityRuntimePaths.from_control_db(
        db_path
    ).producer_activation

    code, payload = _invoke(capsys, args)

    assert code == 2
    assert payload["code"] == "rca_capacity_producer_window_insufficient"
    assert not producer_path.exists()
    assert fake.transition_calls == []


def test_bootstrap_prepare_rechecks_window_under_lock_before_create_once(
    tmp_path, capsys, monkeypatch
):
    db_path = tmp_path / "control.sqlite3"
    fake = _bounded_fake_store()
    monkeypatch.setattr(activation_module, "_open_store", lambda _path: fake)
    _stub_bootstrap_authority(
        monkeypatch,
        started_at=NOW,
        deadline=(NOW + capacity_transition.MIN_PRODUCER_DEADLINE_REMAINING),
    )
    observed_times = iter((NOW, NOW + timedelta(microseconds=1)))
    monkeypatch.setattr(activation_module, "_utc_now", lambda: next(observed_times))
    args = _prepare_bootstrap_args(
        db_path, tmp_path / ".env", _authorization_capsule(tmp_path)
    )
    producer_path = capacity_runtime.CapacityRuntimePaths.from_control_db(
        db_path
    ).producer_activation

    code, payload = _invoke(capsys, [*args, "--apply"])

    assert code == 2
    assert payload["code"] == "rca_capacity_producer_window_insufficient"
    assert not producer_path.exists()
    assert fake.transition_calls == []


def test_bootstrap_prepare_rejects_existing_signed_late_producer_receipt(
    tmp_path, capsys, monkeypatch
):
    db_path = tmp_path / "control.sqlite3"
    fake = _bounded_fake_store()
    monkeypatch.setattr(activation_module, "_open_store", lambda _path: fake)
    binding, authorization = _stub_bootstrap_authority(
        monkeypatch,
        started_at=NOW - timedelta(days=1),
        deadline=NOW + timedelta(days=7),
    )
    producer_path = capacity_runtime.CapacityRuntimePaths.from_control_db(
        db_path
    ).producer_activation
    receipt = capacity_evidence.issue_producer_activation_receipt(
        release_id=CAPACITY_RELEASE_ID,
        bootstrap_epoch_id=BOOTSTRAP_EPOCH_ID,
        release_bom_sha256=RELEASE_BOM_SHA256,
        active_release_binding_sha256=ACTIVE_RELEASE_BINDING_SHA256,
        activated_at=NOW,
        hmac_key=HMAC_KEY,
        receipt_id=activation_module._producer_receipt_id(
            binding=binding,
            authorization=authorization,
        ),
    )
    capacity_evidence.write_owner_only_create_once(producer_path, receipt)
    original = producer_path.read_bytes()
    args = _prepare_bootstrap_args(
        db_path, tmp_path / ".env", _authorization_capsule(tmp_path)
    )

    code, payload = _invoke(capsys, args)

    assert code == 2
    assert payload["code"] == "rca_capacity_producer_window_insufficient"
    assert producer_path.read_bytes() == original
    assert fake.transition_calls == []


def test_bootstrap_prepare_late_retry_uses_historical_producer_activation(
    tmp_path, capsys, monkeypatch
):
    db_path = tmp_path / "control.sqlite3"
    fake = _bounded_fake_store()
    monkeypatch.setattr(activation_module, "_open_store", lambda _path: fake)
    deadline = NOW + capacity_transition.MIN_PRODUCER_DEADLINE_REMAINING
    _stub_bootstrap_authority(
        monkeypatch,
        started_at=NOW,
        deadline=deadline,
    )
    args = _prepare_bootstrap_args(
        db_path, tmp_path / ".env", _authorization_capsule(tmp_path)
    )
    producer_path = capacity_runtime.CapacityRuntimePaths.from_control_db(
        db_path
    ).producer_activation

    first_code, first = _invoke(capsys, [*args, "--apply"])
    assert first_code == 0, first
    original = producer_path.read_bytes()

    monkeypatch.setattr(
        activation_module,
        "_utc_now",
        lambda: deadline - capacity_transition.MIN_SAMPLE_WINDOW,
    )
    retry_code, retry = _invoke(capsys, [*args, "--apply"])

    assert retry_code == 0, retry
    assert producer_path.read_bytes() == original
    assert retry["result"]["producer_activation_receipt_sha256"] == (
        hashlib.sha256(original).hexdigest()
    )

    monkeypatch.setattr(
        activation_module,
        "_utc_now",
        lambda: (
            deadline
            - capacity_transition.MIN_SAMPLE_WINDOW
            + timedelta(microseconds=1)
        ),
    )
    late_code, late = _invoke(capsys, [*args, "--apply"])
    assert late_code == 2
    assert late["code"] == "rca_capacity_producer_horizon_insufficient"
    assert producer_path.read_bytes() == original


def test_bootstrap_prepare_recovers_after_publication_before_runtime_validation(
    tmp_path, capsys, monkeypatch
):
    db_path = tmp_path / "control.sqlite3"
    fake = _bounded_fake_store()
    monkeypatch.setattr(activation_module, "_open_store", lambda _path: fake)
    _stub_bootstrap_authority(monkeypatch)
    args = _prepare_bootstrap_args(
        db_path, tmp_path / ".env", _authorization_capsule(tmp_path)
    )
    path = capacity_runtime.CapacityRuntimePaths.from_control_db(
        db_path
    ).producer_activation
    real_runtime = activation_module._validate_capacity_runtime_locked
    calls = 0

    def crash_after_receipt(**kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("crash-after-producer")
        return real_runtime(**kwargs)

    monkeypatch.setattr(
        activation_module, "_validate_capacity_runtime_locked", crash_after_receipt
    )

    first_code, first = _invoke(capsys, [*args, "--apply"])
    assert first_code == 2, first
    assert first["code"] == "activation_cli_internal_error"
    original = path.read_bytes()

    second_code, second = _invoke(capsys, [*args, "--apply"])

    assert second_code == 0
    assert path.read_bytes() == original
    assert second["result"]["producer_activation_receipt_sha256"] == (
        hashlib.sha256(original).hexdigest()
    )


def test_bootstrap_prepare_recovers_real_link_before_unlink_crash(
    tmp_path, capsys, monkeypatch
):
    db_path = tmp_path / "control.sqlite3"
    fake = _bounded_fake_store()
    monkeypatch.setattr(activation_module, "_open_store", lambda _path: fake)
    binding, authorization = _stub_bootstrap_authority(monkeypatch)
    args = _prepare_bootstrap_args(
        db_path, tmp_path / ".env", _authorization_capsule(tmp_path)
    )
    path = capacity_runtime.CapacityRuntimePaths.from_control_db(
        db_path
    ).producer_activation
    receipt = capacity_evidence.issue_producer_activation_receipt(
        release_id=CAPACITY_RELEASE_ID,
        bootstrap_epoch_id=BOOTSTRAP_EPOCH_ID,
        release_bom_sha256=RELEASE_BOM_SHA256,
        active_release_binding_sha256=ACTIVE_RELEASE_BINDING_SHA256,
        activated_at=NOW,
        hmac_key=HMAC_KEY,
        receipt_id=activation_module._producer_receipt_id(
            binding=binding,
            authorization=authorization,
        ),
    )
    raw = capacity_evidence.canonical_bytes(receipt)
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    temporary = path.with_name(
        f".{path.name}.crashed{capacity_evidence.CREATE_ONCE_TEMP_SUFFIX}"
    )
    temporary.write_bytes(raw)
    temporary.chmod(0o600)
    os.link(temporary, path)
    assert path.stat().st_nlink == 2

    code, payload = _invoke(capsys, [*args, "--apply"])

    assert code == 0, payload
    assert not temporary.exists()
    assert path.stat().st_nlink == 1
    assert path.read_bytes() == raw
    assert payload["result"]["producer_activation_receipt_sha256"] == (
        hashlib.sha256(raw).hexdigest()
    )


def test_bootstrap_prepare_requires_bounded_exact_preproduction_scope(
    tmp_path, capsys, monkeypatch
):
    db_path = tmp_path / "control.sqlite3"
    fake = _bounded_fake_store()
    monkeypatch.setattr(activation_module, "_open_store", lambda _path: fake)
    _stub_bootstrap_authority(monkeypatch)
    capsule = _authorization_capsule(tmp_path)
    body = json.loads(capsule.read_text(encoding="utf-8"))
    body["expected_config_sha256"] = "0" * 64
    capsule.write_text(json.dumps(body), encoding="utf-8")

    mismatch_code, mismatch = _invoke(
        capsys,
        _prepare_bootstrap_args(db_path, tmp_path / ".env", capsule),
    )
    assert mismatch_code == 2
    assert mismatch["code"] == "activation_preproduction_epoch_binding_changed"

    fake.current["state"] = "confirmed"
    state_code, state = _invoke(
        capsys,
        _prepare_bootstrap_args(
            db_path, tmp_path / ".env", _authorization_capsule(tmp_path, "clean.json")
        ),
    )
    assert state_code == 2
    assert state["code"] == "activation_epoch_state_not_allowed"


@pytest.mark.parametrize(
    ("kind", "expected_code"),
    [
        ("db", "activation_capacity_origin_binding_invalid"),
        ("owner", "activation_bootstrap_authority_binding_invalid"),
        ("active_path", "activation_active_release_binding_path_invalid"),
        ("bom", "rca_active_release_binding_release_bom_invalid"),
        ("env", "rca_active_release_binding_live_env_mismatch"),
    ],
)
def test_bootstrap_steady_rejects_authority_and_origin_mismatch(
    tmp_path, capsys, monkeypatch, kind, expected_code
):
    db_path = tmp_path / "control.sqlite3"
    fake = _bounded_fake_store()
    if kind == "db":
        fake.capacity = _bootstrap_capacity_state(release_id="different-release")
    monkeypatch.setattr(activation_module, "_open_store", lambda _path: fake)
    _stub_bootstrap_authority(monkeypatch)
    args = _prepare_bootstrap_args(
        db_path, tmp_path / ".env", _authorization_capsule(tmp_path)
    )
    if kind == "owner":
        binding, authorization = _bootstrap_authority_values()
        authorization["authorized_by"] = "different-owner"
        monkeypatch.setattr(
            activation_module.prod_bootstrap,
            "load_active_release_binding",
            lambda **_kwargs: binding,
        )
        monkeypatch.setattr(
            activation_module.prod_bootstrap,
            "load_bootstrap_authorization",
            lambda **_kwargs: authorization,
        )
    elif kind == "active_path":
        index = args.index("--active-release-binding") + 1
        args[index] = str(tmp_path / "wrong-active-binding.json")
    elif kind in {"bom", "env"}:
        code = (
            "rca_active_release_binding_release_bom_invalid"
            if kind == "bom"
            else "rca_active_release_binding_live_env_mismatch"
        )

        def reject_binding(**_kwargs):
            raise prod_bootstrap.RcaBootstrapAuthorizationError(code)

        monkeypatch.setattr(
            activation_module.prod_bootstrap,
            "load_active_release_binding",
            reject_binding,
        )

    code, payload = _invoke(capsys, args)

    assert code == 2
    assert payload["code"] == expected_code
    assert fake.transition_calls == []


def test_bootstrap_prepare_accepts_exact_signed_immutable_capacity_origin(
    tmp_path, capsys, monkeypatch
):
    db_path = tmp_path / "control.sqlite3"
    origin_release_id = "origin-release-20260717"
    origin_bootstrap_epoch_id = "origin-bootstrap-20260717"
    current_release_id = "current-release-20260805"
    current_bootstrap_epoch_id = "current-bootstrap-20260805"
    fake = _bounded_fake_store()
    fake.capacity = _bootstrap_capacity_state(
        release_id=origin_release_id,
        bootstrap_epoch_id=origin_bootstrap_epoch_id,
    )
    monkeypatch.setattr(activation_module, "_open_store", lambda _path: fake)
    binding, authorization = _bootstrap_authority_values()
    binding.update(
        {
            "release_id": current_release_id,
            "bootstrap_epoch_id": current_bootstrap_epoch_id,
        }
    )
    authorization.update(
        {
            "bootstrap_epoch_id": current_bootstrap_epoch_id,
            "release_approval_id": current_release_id,
        }
    )
    monkeypatch.setattr(
        activation_module.prod_bootstrap,
        "load_active_release_binding",
        lambda **_kwargs: dict(binding),
    )
    monkeypatch.setattr(
        activation_module.prod_bootstrap,
        "load_bootstrap_authorization",
        lambda **_kwargs: dict(authorization),
    )
    monkeypatch.setattr(activation_module, "_utc_now", lambda: NOW)
    monkeypatch.setenv(capacity_runtime.HMAC_ENV, "hex:" + HMAC_KEY.hex())

    paths = capacity_runtime.CapacityRuntimePaths.from_control_db(db_path)
    bound_binding = dict(binding)
    bound_binding["_capacity_origin"] = {
        "release_id": origin_release_id,
        "bootstrap_epoch_id": origin_bootstrap_epoch_id,
    }
    producer = capacity_evidence.issue_producer_activation_receipt(
        release_id=origin_release_id,
        bootstrap_epoch_id=origin_bootstrap_epoch_id,
        release_bom_sha256=RELEASE_BOM_SHA256,
        active_release_binding_sha256=ACTIVE_RELEASE_BINDING_SHA256,
        activated_at=NOW,
        hmac_key=HMAC_KEY,
        receipt_id=activation_module._producer_receipt_id(
            binding=bound_binding,
            authorization=authorization,
        ),
    )
    producer_sha256 = capacity_evidence.write_owner_only_create_once(
        paths.producer_activation,
        producer,
    )
    compatibility = {
        "schema_version": activation_module.CAPACITY_ORIGIN_COMPAT_SCHEMA_VERSION,
        "created_at": NOW.isoformat().replace("+00:00", "Z"),
        "current_release_id": current_release_id,
        "current_bootstrap_epoch_id": current_bootstrap_epoch_id,
        "capacity_origin_release_id": origin_release_id,
        "capacity_origin_bootstrap_epoch_id": origin_bootstrap_epoch_id,
        "active_release_binding_sha256": ACTIVE_RELEASE_BINDING_SHA256,
        "release_bom_sha256": RELEASE_BOM_SHA256,
        "producer_path": str(paths.producer_activation),
        "producer_sha256": producer_sha256,
        "producer_receipt_fingerprint": producer["receipt_fingerprint"],
        "database_rows_modified": False,
        "external_effects_triggered": False,
    }
    _write_secure_json(
        paths.state_root / activation_module.CAPACITY_ORIGIN_COMPAT_NAME,
        compatibility,
    )
    args = _prepare_bootstrap_args(
        db_path,
        tmp_path / ".env",
        _authorization_capsule(tmp_path),
    )
    args[args.index("--release-id") + 1] = current_release_id
    args[args.index("--bootstrap-epoch-id") + 1] = current_bootstrap_epoch_id

    code, payload = _invoke(capsys, args)

    assert code == 0, payload
    assert payload["result"]["producer_receipt_present"] is True
    assert payload["result"]["would_publish_producer_receipt"] is False
    assert fake.transition_calls == []


def test_bootstrap_steady_cannot_bypass_governed_arguments(
    tmp_path, capsys, monkeypatch
):
    fake = _FakeStore("confirmed", capacity_state=_bootstrap_capacity_state())
    monkeypatch.setattr(activation_module, "_open_store", lambda _path: fake)

    code, payload = _invoke(
        capsys,
        _mutation_args(tmp_path / "control.sqlite3", "transition-steady"),
    )

    assert code == 2
    assert payload["code"] == "activation_release_id_required"
    assert fake.transition_calls == []


def test_bootstrap_steady_requires_gate_prepared_producer_receipt(
    tmp_path, capsys, monkeypatch
):
    db_path = tmp_path / "control.sqlite3"
    fake = _FakeStore("confirmed", capacity_state=_bootstrap_capacity_state())
    monkeypatch.setattr(activation_module, "_open_store", lambda _path: fake)
    _stub_bootstrap_authority(monkeypatch)

    code, payload = _invoke(
        capsys,
        _steady_bootstrap_args(db_path, tmp_path / ".env"),
    )

    assert code == 2
    assert payload["code"] == "activation_producer_receipt_required"
    assert fake.transition_calls == []


def test_steady_capacity_allows_future_business_release_without_rewriting_producer(
    tmp_path, capsys, monkeypatch
):
    db_path = tmp_path / "control.sqlite3"
    fake = _FakeStore("confirmed", capacity_state=_steady_capacity_state())
    monkeypatch.setattr(activation_module, "_open_store", lambda _path: fake)
    monkeypatch.setattr(
        activation_module.capacity_runtime,
        "load_capacity_hmac_key",
        lambda: HMAC_KEY,
    )
    monkeypatch.setattr(
        activation_module,
        "_validate_capacity_runtime_locked",
        lambda **_kwargs: {
            "ready": True,
            "effective_state": capacity_transition.STEADY_ACTIVE,
        },
    )
    monkeypatch.setattr(
        capacity_evidence,
        "write_owner_only_create_once",
        lambda *_args, **_kwargs: pytest.fail("producer receipt must not be written"),
    )
    args = _mutation_args(db_path, "transition-steady")

    plan_code, plan = _invoke(capsys, args)
    apply_code, applied = _invoke(capsys, [*args, "--apply"])

    assert plan_code == apply_code == 0, (plan, applied)
    assert plan["result"]["would_publish_producer_receipt"] is False
    assert applied["result"]["runtime_effective_state"] == (
        capacity_transition.STEADY_ACTIVE
    )
    assert fake.transition_calls[0]["target_state"] == "steady_active"


def test_store_errors_are_code_only_and_do_not_echo_event_payload(
    tmp_path, capsys, monkeypatch
):
    fake = _FakeStore("confirmed")
    secret = "secret-event-payload"
    event_uid = f"{TOPIC}:0:18"
    fake.promotion_error = ShadowPromotionError(
        f"shadow promotion denied for {secret}: event_not_found"
    )
    monkeypatch.setattr(activation_module, "_open_store", lambda _path: fake)

    code, payload = _invoke(
        capsys,
        [
            *_mutation_args(tmp_path / "control.sqlite3", "reconcile-shadow"),
            "--event-uid",
            event_uid,
            "--apply",
        ],
    )

    assert code == 2
    assert payload["code"] == "activation_shadow_event_not_found"
    encoded = json.dumps(payload, sort_keys=True)
    assert secret not in encoded
    assert event_uid not in encoded


def test_unknown_store_error_is_redacted(tmp_path, capsys, monkeypatch):
    fake = _FakeStore("confirmed", capacity_state=_steady_capacity_state())
    secret = "DO-NOT-ECHO-INTERNAL-EXCEPTION"
    fake.transition_error = RuntimeError(secret)
    monkeypatch.setattr(activation_module, "_open_store", lambda _path: fake)
    monkeypatch.setattr(
        activation_module.capacity_runtime,
        "load_capacity_hmac_key",
        lambda: HMAC_KEY,
    )
    monkeypatch.setattr(
        activation_module,
        "_validate_capacity_runtime_locked",
        lambda **_kwargs: {
            "ready": True,
            "effective_state": capacity_transition.STEADY_ACTIVE,
        },
    )

    code, payload = _invoke(
        capsys,
        [
            *_mutation_args(tmp_path / "control.sqlite3", "transition-steady"),
            "--apply",
        ],
    )

    assert code == 2
    assert payload["code"] == "activation_cli_internal_error"
    assert secret not in json.dumps(payload, sort_keys=True)


@pytest.mark.parametrize(
    "legacy_args",
    [
        ["--epoch-id", EPOCH_ID],
        ["--preproduction-fingerprint", PREPRODUCTION_FINGERPRINT],
        ["--config-sha256", CONFIG_SHA256],
        ["--db-identity-json", "DO-NOT-ECHO-db-identity.json"],
        ["--start-fence-json", "DO-NOT-ECHO-start-fence.json"],
    ],
)
def test_create_rejects_legacy_direct_binding_arguments(
    tmp_path, capsys, legacy_args
):
    db_path = tmp_path / "control.sqlite3"
    store = RcaControlStore(db_path)
    args = [*_create_args(db_path, tmp_path), *legacy_args]

    code, payload = _invoke(capsys, args)

    assert code == 2
    assert payload["code"] == "activation_cli_arguments_invalid"
    assert store.activation_epoch() is None
    encoded = json.dumps(payload, sort_keys=True)
    assert "DO-NOT-ECHO" not in encoded


def test_mutation_requires_operator_and_reason_without_argparse_echo(tmp_path, capsys):
    db_path = tmp_path / "control.sqlite3"
    RcaControlStore(db_path)
    secret_event = f"{TOPIC}:0:99"

    code, payload = _invoke(
        capsys,
        [
            "--control-db",
            str(db_path),
            "authorize",
            "--epoch-id",
            EPOCH_ID,
            "--slot-kind",
            "kafka_success",
            "--event-uid",
            secret_event,
        ],
    )

    assert code == 2
    assert payload["code"] == "activation_cli_arguments_invalid"
    assert secret_event not in json.dumps(payload, sort_keys=True)
