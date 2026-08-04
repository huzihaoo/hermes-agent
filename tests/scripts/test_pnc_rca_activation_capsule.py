from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

from gateway.pnc_rca_control_store import (
    CONTROL_STORE_SCHEMA_VERSION,
    KafkaRecord,
    MANUAL_TRIGGER_SCHEMA_VERSION,
    ManualRcaTriggerRequest,
    RcaControlStore,
)
from gateway.pnc_rca_delivery_store import (
    DELIVERY_STORE_SCHEMA_VERSION,
    RcaDeliveryStore,
)
from gateway.pnc_rca_kafka_contract import WorkflowEventPolicy, WorkflowTransition
from scripts import pnc_rca_activation as activation
from scripts import pnc_rca_activation_capsule as capsules


TOPIC = "feishu-project-workflow-event"
EPOCH_ID = "rca-gray-capsule-e2e"
OPERATOR = "capsule-e2e"
REASON = "exercise the real capsule builder and reader"


def _write_owner_json(path: Path, value: Any) -> bytes:
    raw = (
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")
    path.write_bytes(raw)
    path.chmod(0o600)
    return raw


def _config() -> dict[str, Any]:
    return {
        "consumer": {
            "topic": TOPIC,
            "health_path": "/tmp/activation-consumer-health.json",
            "policy": {"policy_version": "issue-created-v1"},
        },
        "release": {"lane": "gray"},
    }


def _service_configs() -> dict[str, dict[str, Any]]:
    return {
        "local.pnc.rca-kafka-consumer": dict(_config()["consumer"]),
        "local.pnc.rca-outbox-dispatcher": {
            "health_path": "/tmp/activation-outbox-health.json"
        },
        "local.pnc.rca-delivery-collector": {
            "health_path": "/tmp/activation-collector-health.json"
        },
        "local.pnc.rca-delivery-dispatcher": {
            "health_path": "/tmp/activation-dispatcher-health.json"
        },
    }


def _gateway_binding() -> dict[str, Any]:
    identity = {
        "service_label": "ai.hermes.gateway",
        "boot_time": 1_700_000_000.0,
        "executable": "/candidate/.venv/bin/python",
        "cwd": "/candidate",
        "script": "/candidate/gateway/run.py",
        "pid": 42001,
        "script_sha256": "1" * 64,
        "runtime_files_sha256": "2" * 64,
        "public_config_sha256": "3" * 64,
        "loaded_runtime_sha256": "4" * 64,
        "process_create_time": 1_785_000_000.0,
    }
    return {
        "state": "running_safe",
        "pid": 42001,
        "process_create_time": 1_785_000_000.0,
        "runtime_identity": identity,
        "runtime_identity_sha256": capsules._sha256_json(identity),
        "verified_runtime_sha256": "a" * 64,
    }


@pytest.fixture(autouse=True)
def _stub_live_runtime_probes(monkeypatch: pytest.MonkeyPatch) -> None:
    """Builder tests use synthetic process identities; probe tests are separate."""
    monkeypatch.setattr(
        capsules, "_recheck_live_gateway_binding", lambda _value, **_kwargs: None
    )
    monkeypatch.setattr(
        capsules,
        "_recheck_live_consumer_freeze",
        lambda _value, **_kwargs: {
            "runtime_identity": {
                "loaded_runtime_sha256": "3" * 64,
            }
        },
    )
    monkeypatch.setattr(
        capsules, "_recheck_live_resident_projection", lambda *_args, **_kwargs: None
    )


def _database_identity(
    control_path: Path,
    delivery_path: Path,
    *,
    config_sha256: str,
) -> dict[str, Any]:
    control_stat = control_path.stat()
    delivery_stat = delivery_path.stat()
    return {
        "schema_version": "pnc_rca_activation_db_identity_v1",
        "strategy": "existing_database_preserve",
        "databases": {
            "control": {
                "path": str(control_path.absolute()),
                "device": control_stat.st_dev,
                "inode": control_stat.st_ino,
                "schema_version": CONTROL_STORE_SCHEMA_VERSION,
                "db_instance_id": None,
                "genesis_intent_sha256": None,
            },
            "delivery": {
                "path": str(delivery_path.absolute()),
                "device": delivery_stat.st_dev,
                "inode": delivery_stat.st_ino,
                "schema_version": DELIVERY_STORE_SCHEMA_VERSION,
                "db_instance_id": None,
                "genesis_intent_sha256": None,
            },
        },
        "migration_receipt_raw_sha256": "b" * 64,
        "materialization_receipt_raw_sha256": hashlib.sha256(b"").hexdigest(),
        "host_commit": "c" * 40,
        "config_sha256": config_sha256,
    }


def _preauthorization_input(control_path: Path, delivery_path: Path) -> dict[str, Any]:
    config_sha = capsules._sha256_json(_config())
    identity = _database_identity(control_path, delivery_path, config_sha256=config_sha)
    fence = {TOPIC: {"0": 10}}
    return {
        "epoch_id": EPOCH_ID,
        "initial_state": "safe_off",
        "config_sha256": config_sha,
        "db_logical_identity": identity,
        "db_logical_identity_sha256": capsules._sha256_json(identity),
        "partition_start_fence": fence,
        "partition_start_fence_sha256": capsules._sha256_json(fence),
        "migration_receipt_raw_sha256": "b" * 64,
        "materialization_receipt_raw_sha256": hashlib.sha256(b"").hexdigest(),
        "broker_t0_observation_sha256": "d" * 64,
    }


def _manual_identity(message: str, issue_id: int, *, mode: str) -> dict[str, str]:
    return {
        "chat_id": "oc_capsule_e2e",
        "requester_id": "ou_capsule_owner",
        "message_id": message,
        "thread_id": f"topic:{message}",
        "issue_url": f"https://project.feishu.cn/g1q3/issue/detail/{issue_id}",
        "mode": mode,
    }


def _canary_plan() -> dict[str, dict[str, Any]]:
    identities: dict[str, dict[str, Any]] = {
        "manual_success": _manual_identity(
            "om_capsule_success", 7041712814, mode="run_or_join"
        ),
        "manual_terminal_failure": _manual_identity(
            "om_capsule_failure", 7041712815, mode="run_or_join"
        ),
    }
    return {
        slot: {
            "source_kind": "kafka" if slot == "kafka_success" else "manual",
            "entrypoint": (
                "kafka_ingest" if slot == "kafka_success" else "manual_admit"
            ),
            "source_identity": identity,
            "source_identity_sha256": capsules._sha256_json(identity),
            "max_admissions": 1,
            "expected_admission": {
                "business_key": f"business-{slot}",
                "submission_key": f"submission-{slot}",
                "generation": 1,
            },
            "expected_outcome": (
                "terminal_failed" if slot == "manual_terminal_failure" else "success"
            ),
        }
        for slot, identity in identities.items()
    }


def _report(
    *,
    mode: str,
    state: str,
    extra_checks: list[dict[str, Any]],
    evaluated_at: datetime,
) -> dict[str, Any]:
    report = {
        "schema_version": capsules.RELEASE_GATE_SCHEMA_VERSION,
        "evaluated_at": evaluated_at.astimezone(timezone.utc).isoformat(),
        "mode": mode,
        "ok": True,
        "fingerprint": "0" * 64,
        "config": _config(),
        "gate_policy": {"evidence_max_age_seconds": 900},
        "checks": [
            {
                "name": "contract_drift",
                "ok": True,
                "code": "pass",
                "detail": {"contract_sha256": "f" * 64},
            },
            {
                "name": "activation_epoch",
                "ok": True,
                "code": "pass",
                "detail": {"state": state},
            },
            *extra_checks,
        ],
        "blockers": [],
        "warnings": [],
        "evidence_sha256": {},
    }
    report["fingerprint"] = capsules.release_report_fingerprint(report)
    return report


def _stage_receipt(
    tmp_path: Path,
    *,
    mode: str,
    state: str,
    activation_input: dict[str, Any],
    prior_capsule: Path | None = None,
    evaluated_at: datetime | None = None,
) -> Path:
    detail: dict[str, Any] = {
        "schema_version": (
            capsules.PREAUTHORIZATION_MATERIAL_SCHEMA_VERSION
            if mode == "preauthorization"
            else capsules.PREPRODUCTION_MATERIAL_SCHEMA_VERSION
        ),
        "evidence_directory": str(tmp_path.absolute()),
        "activation_input": activation_input,
        "gateway_binding": _gateway_binding(),
    }
    if mode == "preproduction":
        assert prior_capsule is not None
        detail.update({
            "preauthorization_capsule": str(prior_capsule.absolute()),
            "canary_slot_plan": _canary_plan(),
        })
    report = _report(
        mode=mode,
        state=state,
        extra_checks=[
            {
                "name": "activation_capsule_material",
                "ok": True,
                "code": "pass",
                "detail": detail,
            }
        ],
        evaluated_at=evaluated_at or datetime.now(timezone.utc),
    )
    path = tmp_path / f"{mode}-gate.json"
    _write_owner_json(path, report)
    return path


def _activation_args(control_path: Path, command: str) -> list[str]:
    return [
        "--control-db",
        str(control_path),
        command,
        "--operator",
        OPERATOR,
        "--reason",
        REASON,
    ]


def _new_databases(tmp_path: Path) -> tuple[Path, Path, RcaControlStore]:
    control_path = tmp_path / "control.sqlite3"
    delivery_path = tmp_path / "delivery.sqlite3"
    store = RcaControlStore(control_path)
    RcaDeliveryStore(delivery_path)
    tmp_path.chmod(0o700)
    return control_path, delivery_path, store


def _build_preauthorization(
    tmp_path: Path,
    control_path: Path,
    delivery_path: Path,
    *,
    evaluated_at: datetime | None = None,
    build_now: datetime | None = None,
) -> tuple[Path, dict[str, Any]]:
    activation_input = _preauthorization_input(control_path, delivery_path)
    receipt = _stage_receipt(
        tmp_path,
        mode="preauthorization",
        state="absent",
        activation_input=activation_input,
        evaluated_at=evaluated_at,
    )
    capsule = capsules.build_preauthorization_capsule(
        receipt, control_db_path=control_path, now=build_now
    )
    return capsule, activation_input


def test_real_builders_and_activation_readers_create_and_preauthorize(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    control_path, delivery_path, store = _new_databases(tmp_path)
    preauthorization, activation_input = _build_preauthorization(
        tmp_path, control_path, delivery_path
    )

    create_args = [
        *_activation_args(control_path, "create"),
        "--preauthorization-capsule",
        str(preauthorization),
        "--apply",
    ]
    assert activation.main(create_args) == 0
    assert json.loads(capsys.readouterr().out)["ok"] is True
    assert store.activation_epoch()["state"] == "safe_off"

    preproduction_receipt = _stage_receipt(
        tmp_path,
        mode="preproduction",
        state="safe_off",
        activation_input=activation_input,
        prior_capsule=preauthorization,
    )
    assert (
        capsules.main([
            "build-preproduction",
            "--receipt",
            str(preproduction_receipt),
            "--control-db",
            str(control_path),
            "--preauthorization-capsule",
            str(preauthorization),
        ])
        == 0
    )
    capsule_payload = json.loads(capsys.readouterr().out)
    preproduction = Path(capsule_payload["path"])
    transition_args = [
        *_activation_args(control_path, "transition-preauthorized"),
        "--preproduction-capsule",
        str(preproduction),
        "--apply",
    ]
    assert activation.main(transition_args) == 0
    assert json.loads(capsys.readouterr().out)["ok"] is True
    assert store.activation_epoch()["state"] == "preauthorized"
    assert preauthorization.stat().st_mode & 0o777 == 0o600
    assert preproduction.stat().st_mode & 0o777 == 0o600
    assert capsules._pair_path(
        preauthorization_receipt := Path(
            json.loads(preauthorization.read_text())["release_gate_receipt"]["path"]
        ),
        "preauthorization",
    ).is_file()
    assert preauthorization_receipt.is_file()


@pytest.mark.parametrize("failure", ["tamper", "permissions", "pair"])
def test_preauthorization_reader_rejects_tamper_permissions_and_pair_drift(
    tmp_path: Path, failure: str
) -> None:
    control_path, delivery_path, _store = _new_databases(tmp_path)
    capsule, _input = _build_preauthorization(tmp_path, control_path, delivery_path)
    assert (
        capsules.read_preauthorization_capsule(capsule, control_db_path=control_path)[
            "epoch_id"
        ]
        == EPOCH_ID
    )

    if failure == "permissions":
        capsule.chmod(0o644)
    elif failure == "pair":
        body = json.loads(capsule.read_text())
        receipt = Path(body["release_gate_receipt"]["path"])
        capsules._pair_path(receipt, "preauthorization").chmod(0o644)
    else:
        body = json.loads(capsule.read_text())
        body["activation_input"]["broker_t0_observation_sha256"] = "9" * 64
        _write_owner_json(capsule, body)

    with pytest.raises(capsules.CapsuleError):
        capsules.read_preauthorization_capsule(capsule, control_db_path=control_path)


def test_preauthorization_reader_rejects_expired_and_wrong_database_binding(
    tmp_path: Path,
) -> None:
    control_path, delivery_path, _store = _new_databases(tmp_path)
    old = datetime.now(timezone.utc) - timedelta(hours=2)
    capsule, _input = _build_preauthorization(
        tmp_path,
        control_path,
        delivery_path,
        evaluated_at=old,
        build_now=old,
    )
    with pytest.raises(capsules.CapsuleError, match="activation_capsule_stale"):
        capsules.read_preauthorization_capsule(capsule, control_db_path=control_path)

    other_dir = tmp_path / "other"
    other_dir.mkdir(mode=0o700)
    other_control = other_dir / "control.sqlite3"
    RcaControlStore(other_control)
    with pytest.raises(
        capsules.CapsuleError, match="activation_capsule_database_binding_mismatch"
    ):
        capsules._read_preauthorization_bundle(
            capsule,
            control_db_path=other_control,
            now=old,
        )


def _policy() -> WorkflowEventPolicy:
    return WorkflowEventPolicy(
        topic=TOPIC,
        policy_version="issue-created-v1",
        project_keys=frozenset({"project-key"}),
        project_simple_names=frozenset({"g1q3"}),
        work_item_type_keys=frozenset({"problem-type"}),
        status_change_types=frozenset({"Reached"}),
        transitions=(
            WorkflowTransition(
                state_key="new-problem-state",
                pre_status=1,
                cur_status=2,
            ),
        ),
    )


def _record() -> KafkaRecord:
    payload = json.dumps(
        {
            "id": 7041712813,
            "name": "ACC braking issue",
            "nodes": [
                {
                    "state_key": "new-problem-state",
                    "node_name": "New problem",
                    "pre_status": 1,
                    "cur_status": 2,
                }
            ],
            "project_key": "project-key",
            "project_simple_name": "g1q3",
            "status_change_type": "Reached",
            "updated_at": 1_785_000_000_000,
            "work_item_type_key": "problem-type",
        },
        sort_keys=True,
    ).encode()
    return KafkaRecord(
        topic=TOPIC,
        partition=0,
        offset=10,
        value=payload,
        key=b"issue-key",
        timestamp_ms=1_785_000_000_000,
        headers=(("trace", b"capsule-e2e"),),
    )


def _manual_request(identity: dict[str, Any]) -> ManualRcaTriggerRequest:
    return ManualRcaTriggerRequest(
        schema_version=MANUAL_TRIGGER_SCHEMA_VERSION,
        issue_url=identity["issue_url"],
        mode=identity["mode"],
        reason="manual_explicit_issue_action",
        platform="feishu",
        chat_id=identity["chat_id"],
        thread_id=identity["thread_id"],
        message_id=identity["message_id"],
        requester_id=identity["requester_id"],
    )


def _prepare_bounded_canaries(
    tmp_path: Path,
    control_path: Path,
    delivery_path: Path,
    store: RcaControlStore,
) -> dict[str, Any]:
    preauthorization, activation_input = _build_preauthorization(
        tmp_path, control_path, delivery_path
    )
    preauthorization_input = capsules.read_preauthorization_capsule(
        preauthorization, control_db_path=control_path
    )
    store.create_activation_epoch(
        epoch_id=EPOCH_ID,
        preauthorization_fingerprint=preauthorization_input[
            "preauthorization_fingerprint"
        ],
        preauthorization_gate_receipt_sha256=preauthorization_input[
            "preauthorization_gate_receipt_sha256"
        ],
        preauthorization_capsule_sha256=preauthorization_input[
            "preauthorization_capsule_sha256"
        ],
        config_sha256=preauthorization_input["config_sha256"],
        db_logical_identity=preauthorization_input["db_logical_identity"],
        partition_start_fence=preauthorization_input["partition_start_fence"],
        operator=OPERATOR,
        reason=REASON,
    )
    preproduction_receipt = _stage_receipt(
        tmp_path,
        mode="preproduction",
        state="safe_off",
        activation_input=activation_input,
        prior_capsule=preauthorization,
    )
    preproduction = capsules.build_preproduction_capsule(
        preproduction_receipt,
        control_db_path=control_path,
        preauthorization_capsule=preauthorization,
    )
    transition = capsules.read_preproduction_capsule(
        preproduction,
        control_db_path=control_path,
        current_activation=store.activation_epoch(),
    )
    store.preauthorize_activation_epoch(
        epoch_id=EPOCH_ID,
        preproduction_fingerprint=transition["preproduction_fingerprint"],
        preproduction_gate_receipt_sha256=transition[
            "preproduction_gate_receipt_sha256"
        ],
        preproduction_capsule_sha256=transition["preproduction_capsule_sha256"],
        expected_preauthorization_fingerprint=transition[
            "expected_preauthorization_fingerprint"
        ],
        expected_preauthorization_gate_receipt_sha256=transition[
            "expected_preauthorization_gate_receipt_sha256"
        ],
        expected_preauthorization_capsule_sha256=transition[
            "expected_preauthorization_capsule_sha256"
        ],
        expected_config_sha256=transition["expected_config_sha256"],
        expected_db_logical_identity_sha256=transition[
            "expected_db_logical_identity_sha256"
        ],
        expected_partition_start_fence_sha256=transition[
            "expected_partition_start_fence_sha256"
        ],
        operator=OPERATOR,
        reason=REASON,
    )
    plan = _canary_plan()
    for slot, item in plan.items():
        identity = dict(item["source_identity"])
        store.authorize_activation_slot(
            epoch_id=EPOCH_ID,
            slot_kind=slot,
            source_kind=item["source_kind"],
            source_identity=identity,
            operator=OPERATOR,
            reason=REASON,
        )
    store.transition_activation_epoch(
        epoch_id=EPOCH_ID,
        expected_state="preauthorized",
        target_state="bounded_active",
        operator=OPERATOR,
        reason=REASON,
    )
    manual_results = []
    for slot in ("manual_success", "manual_terminal_failure"):
        manual_results.append(
            store.admit_manual_trigger(
                _manual_request(plan[slot]["source_identity"]),
                allowed_chat_ids={"oc_capsule_e2e"},
                submit_enabled=True,
                operator_authorized=True,
                active_policy=_policy(),
                activation_required=True,
            )
        )
    assert all(result.submission_key for result in manual_results)
    for index in range(2):
        claim = store.claim_outbox(
            lease_owner=f"capsule-canary-{index}", activation_required=True
        )
        assert claim is not None
        store.complete_outbox(
            outbox_id=claim.outbox_id,
            lease_token=claim.lease_token,
            result={"outcome": "canary_evidence_recorded"},
        )
    current = store.activation_epoch()
    assert current is not None and current["state"] == "bounded_active"
    return current


def _confirmation_receipt(
    tmp_path: Path,
    *,
    current: dict[str, Any],
    release_binding_sha256: str,
) -> Path:
    end_fence = {TOPIC: {"0": 11}}
    confirm_input = {
        "epoch_id": EPOCH_ID,
        "expected_state": "bounded_active",
        "target_state": "confirmed",
        "config_sha256": current["config_sha256"],
        "db_logical_identity_sha256": current["db_logical_identity_sha256"],
        "partition_start_fence_sha256": current["partition_start_fence_sha256"],
        "release_binding_sha256": release_binding_sha256,
        "partition_end_fence": end_fence,
        "partition_end_fence_sha256": capsules._sha256_json(end_fence),
        "production_fingerprint_source": "release_gate_report.fingerprint",
        "production_gate_receipt_sha256_source": (
            "sha256(exact_written_release_gate_receipt)"
        ),
        "restart_between_gate_and_confirm": False,
    }
    gateway = _gateway_binding()
    residents = {"worker": {"loaded_runtime_sha256": "2" * 64}}
    continuity = {
        "gateway": gateway,
        "gateway_verification": {
            "runtime_identity_sha256": gateway["runtime_identity_sha256"],
            "loaded_runtime_sha256": "3" * 64,
            "launchctl_config_sha256": "4" * 64,
            "pid": gateway["pid"],
            "process_create_time": gateway["process_create_time"],
        },
        "residents": residents,
        "residents_sha256": capsules._sha256_json(residents),
    }
    now = datetime.now(timezone.utc)
    freeze = {
        "schema_version": "pnc_rca_activation_ingress_freeze_binding_v1",
        "epoch_id": EPOCH_ID,
        "health_path": "/tmp/activation-consumer-health.json",
        "paused_at": now.isoformat(),
        "freeze_receipt_sha256": "5" * 64,
        "freeze_token_sha256": "6" * 64,
        "consumer_runtime_identity_sha256": "7" * 64,
        "partition_positions_sha256": capsules._sha256_json(end_fence),
        "restart_required": False,
    }
    report = _report(
        mode="production_bootstrap",
        state="bounded_active",
        evaluated_at=now,
        extra_checks=[
            {
                "name": "activation_writer_barrier",
                "ok": True,
                "code": "pass",
                "detail": {
                    "state": "bounded_active",
                    "production_confirmation_required": True,
                    "transition_performed": False,
                    "release_binding_sha256": release_binding_sha256,
                    "confirm_input": confirm_input,
                    "confirm_input_sha256": capsules._sha256_json(confirm_input),
                    "ingress_freeze_binding": freeze,
                },
            },
            {
                "name": "activation_runtime_continuity",
                "ok": True,
                "code": "pass",
                "detail": continuity,
            },
            {
                "name": "runtime_dependencies",
                "ok": True,
                "code": "pass",
                "detail": {"service_configs": _service_configs()},
            },
        ],
    )
    receipt = tmp_path / "confirmation-gate.json"
    _write_owner_json(receipt, report)
    return receipt


def test_confirmation_builder_reader_and_activation_use_live_release_binding(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    control_path, delivery_path, store = _new_databases(tmp_path)
    current = _prepare_bounded_canaries(tmp_path, control_path, delivery_path, store)
    end_fence = {TOPIC: {"0": 11}}
    release_binding = store.activation_release_binding_sha256(
        epoch_id=EPOCH_ID, partition_end_fence=end_fence
    )
    receipt = _confirmation_receipt(
        tmp_path,
        current=current,
        release_binding_sha256=release_binding,
    )
    capsule = capsules.build_confirmation_capsule(receipt, control_db_path=control_path)
    live = capsules.live_release_binding(
        control_path,
        epoch_id=EPOCH_ID,
        expected_config_sha256=current["config_sha256"],
        partition_end_fence=end_fence,
    )
    assert live["release_binding_sha256"] == release_binding
    assert (
        capsules.read_confirmation_capsule(
            capsule,
            receipt_path=receipt,
            control_db_path=control_path,
            current_activation=live,
        )["release_binding_sha256"]
        == release_binding
    )

    args = [
        *_activation_args(control_path, "confirm"),
        "--confirmation-capsule",
        str(capsule),
        "--apply",
    ]
    assert activation.main(args) == 0
    first = json.loads(capsys.readouterr().out)
    assert first["ok"] is True
    assert store.activation_epoch()["state"] == "confirmed"
    assert activation.main(args) == 0
    retry = json.loads(capsys.readouterr().out)
    assert retry["ok"] is True
    assert retry["result"]["changed"] is False


def test_confirmation_builder_rejects_release_binding_drift(tmp_path: Path) -> None:
    control_path, delivery_path, store = _new_databases(tmp_path)
    current = _prepare_bounded_canaries(tmp_path, control_path, delivery_path, store)
    receipt = _confirmation_receipt(
        tmp_path,
        current=current,
        release_binding_sha256="9" * 64,
    )
    with pytest.raises(
        capsules.CapsuleError,
        match="activation_capsule_confirmation_database_binding_invalid",
    ):
        capsules.build_confirmation_capsule(receipt, control_db_path=control_path)
