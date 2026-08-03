from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace
from typing import Any

import pytest

from gateway.pnc_rca_control_store import (
    CONTROL_STORE_SCHEMA_VERSION,
    RcaControlStore,
)
from gateway.pnc_rca_delivery_store import (
    DELIVERY_STORE_SCHEMA_VERSION,
    RcaDeliveryStore,
)
from gateway.pnc_rca_release_authority import canonical_json_sha256
from scripts import pnc_rca_activation as activation
from scripts import pnc_rca_activation_capsule as capsules
from scripts import pnc_rca_activation_gate as gate
from scripts.pnc_rca_schema_fingerprint import create_snapshot_receipt


TOPIC = "feishu-project-workflow-event"
EPOCH_ID = "rca-r11-producer-test"
DB_INSTANCE_ID = "9d9b8cfc-5a1e-4ddb-91c0-18145a5b0e53"
GENESIS_SHA = "7" * 64


def _write_owner(path: Path, value: Any) -> bytes:
    raw = (
        json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")
    path.write_bytes(raw)
    path.chmod(0o600)
    return raw


def _database(path: Path) -> Path:
    RcaControlStore(path)
    RcaDeliveryStore(path)
    identity = {
        "fresh_install_db_instance_id": DB_INSTANCE_ID,
        "fresh_install_genesis_intent_sha256": GENESIS_SHA,
    }
    with sqlite3.connect(path) as connection:
        for table in ("control_meta", "rca_delivery_meta"):
            connection.executemany(
                f"INSERT INTO {table}(key, value) VALUES (?, ?)",
                identity.items(),
            )
    return path.absolute()


def _canary_plan() -> dict[str, dict[str, Any]]:
    identities = {
        "kafka_success": {
            "event_uid": f"{TOPIC}:0:10",
            "topic": TOPIC,
            "partition": 0,
            "offset": 10,
        },
        "manual_success": {
            "chat_id": "oc_gate_test",
            "requester_id": "ou_gate_owner",
            "message_id": "om_gate_success",
            "thread_id": "topic:om_gate_success",
            "issue_url": "https://project.feishu.cn/g1q3/issue/detail/7041712814",
            "mode": "run_or_join",
        },
        "manual_terminal_failure": {
            "chat_id": "oc_gate_test",
            "requester_id": "ou_gate_owner",
            "message_id": "om_gate_failure",
            "thread_id": "topic:om_gate_failure",
            "issue_url": "https://project.feishu.cn/g1q3/issue/detail/7041712815",
            "mode": "run_or_join",
        },
    }
    result: dict[str, dict[str, Any]] = {}
    for slot, identity in identities.items():
        result[slot] = {
            "source_kind": "kafka" if slot == "kafka_success" else "manual",
            "entrypoint": "kafka_ingest" if slot == "kafka_success" else "manual_admit",
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
    return result


def _gateway_binding() -> dict[str, Any]:
    identity = {
        "service_label": gate.GATEWAY_SERVICE_LABEL,
        "pid": 42001,
        "process_create_time": 1_785_000_000.0,
        "boot_time": 1_700_000_000.0,
        "executable": "/candidate/.venv/bin/python",
        "script": "/candidate/gateway/run.py",
        "cwd": "/candidate",
        "script_sha256": "1" * 64,
        "runtime_files_sha256": "2" * 64,
        "public_config_sha256": "3" * 64,
        "loaded_runtime_sha256": "4" * 64,
    }
    return {
        "state": "running_safe",
        "pid": identity["pid"],
        "process_create_time": identity["process_create_time"],
        "runtime_identity": identity,
        "runtime_identity_sha256": capsules._sha256_json(identity),
        "verified_runtime_sha256": "5" * 64,
    }


def _broker(now: datetime, *, end: int = 10) -> dict[str, Any]:
    return {
        "schema_version": gate.BROKER_SCHEMA_VERSION,
        "observed_at": now.isoformat(),
        "cluster_id": "cluster-test",
        "topic": TOPIC,
        "partition_offsets": {"0": {"beginning": 1, "end": end}},
        "connection": {
            "group_id": None,
            "enable_auto_commit": False,
            "allow_auto_create_topics": False,
            "subscribe_called": False,
            "assign_called": False,
            "poll_called": False,
            "commit_called": False,
            "isolation_level": "read_committed",
        },
        "read_only_attestation": {
            "apis": ["Metadata", "ListOffsets"],
            "records_consumed": 0,
            "offsets_committed": 0,
            "external_effects_triggered": False,
        },
    }


def _config(db_path: Path, authority: dict[str, Any], baseline_sha: str) -> dict:
    release_id = authority["release_id"]
    return {
        "consumer": {
            "topic": TOPIC,
            "policy": {"policy_version": "issue-created-v1"},
            "control_db_path": str(db_path),
            "submit_enabled": True,
            "activation_required": True,
            "external_dispatch_wired": False,
        },
        "outbox_dispatcher": {
            "control_db_path": str(db_path),
            "delivery_db_path": str(db_path),
            "dispatch_enabled": True,
            "allow_feishu_writeback": False,
            "activation_required": True,
            "data_access_mode": "remote_read",
            "capacity_mode": "bootstrap",
            "release_id": release_id,
        },
        "delivery_collector": {
            "control_db_path": str(db_path),
            "enabled": True,
            "external_writes": False,
            "activation_required": True,
            "quarantine_release_id": release_id,
            "quarantine_baseline_sha256": baseline_sha,
        },
        "delivery_dispatcher": {
            "control_db_path": str(db_path),
            "enabled": False,
            "external_writes": False,
            "activation_required": True,
            "quarantine_release_id": release_id,
            "quarantine_baseline_sha256": baseline_sha,
        },
        "release": {
            "release_id": release_id,
            "authority_epoch_id": authority["authority_epoch_id"],
            "authority_sha256": canonical_json_sha256(authority),
        },
    }


def _authority(schema_receipt: dict, schema_raw_sha: str, baseline_sha: str) -> dict:
    return {
        "schema_version": "pnc_rca_release_authority_v1",
        "release_id": "rca-r11-test-release",
        "authority_epoch_id": EPOCH_ID,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "status": "approved_for_activation",
        "supersedes_authority_sha256": None,
        "faces": {
            "host_runtime": {"commit": "a" * 40, "tree": "b" * 40, "root": "/candidate"},
            "vm_worker_state": {"commit": "c" * 40, "tree": "d" * 40, "root": "/vm/worker"},
            "g1q3_rca_pipeline": {"commit": "e" * 40, "tree": "f" * 40, "root": "/vm/pipeline"},
            "mcap_data_translate": {
                "commit": "1" * 40,
                "tree": "2" * 40,
                "root": "/vm/mcap",
                "contract_sha256": "3" * 64,
            },
        },
        "control_store": {
            "schema_version": CONTROL_STORE_SCHEMA_VERSION,
            "database_instance_id": DB_INSTANCE_ID,
            "schema_fingerprint_sha256": schema_receipt["schema_fingerprint_sha256"],
            "backup_receipt_sha256": schema_raw_sha,
            "not_measured_reason": "",
        },
        "quarantine_baseline": {
            "state": "ready",
            "required": True,
            "schema_version": "pnc_rca_delivery_quarantine_baseline_v1",
            "baseline_sha256": baseline_sha,
            "not_measured_reason": "",
        },
        "side_effect_policy": {
            "mode": "canary",
            "single_active_writer": True,
            "allow_historical_requeue": False,
            "allowed_effect_kinds": ["feishu_issue_comment"],
        },
        "report_publication": {
            "canonical_base_url": "http://127.0.0.1:18081",
            "root": "/mnt/tmp",
            "manifest_schema_version": "pnc_rca_report_manifest_v1",
        },
        "feishu_capability": {
            "required_surfaces": ["issue_comment"],
            "capability_profile_sha256": "4" * 64,
            "not_measured_reason": "",
        },
    }


def _migration_result(path: Path) -> tuple[bytes, dict, dict]:
    raw = path.read_bytes()
    conditional = {
        "schema_version": "pnc_rca_combined_conditional_schema_shape_v1",
        "source_tables_sha256": "8" * 64,
        "conditions": {},
    }
    body = {
        "source_schema_version": "pnc_rca_delivery_store_v7",
        "target_schema_version": DELIVERY_STORE_SCHEMA_VERSION,
        "source_logical_projection": {"schema_sha256": "6" * 64},
        "cross_projection_preservation": {
            "source_owned_schema": {"conditional_schema_shape": conditional}
        },
    }
    return raw, body, {"migration_runtime_sha256": "9" * 64}


def _vm_observation(authority: dict[str, Any], now: datetime) -> dict[str, Any]:
    faces = {}
    for name in ("vm_worker_state", "g1q3_rca_pipeline", "mcap_data_translate"):
        faces[name] = {**authority["faces"][name], "dirty": False}
    return {
        "schema_version": gate.VM_OBSERVATION_SCHEMA_VERSION,
        "observed_at": now.isoformat(),
        "release_id": authority["release_id"],
        "authority_sha256": canonical_json_sha256(authority),
        "faces": faces,
        "capacity": {
            "resource_class": "rca_prod",
            "capacity_mode": "bootstrap",
            "rca_prod_ok": True,
            "max_concurrency": 1,
            "queue_allowed": False,
            "input_materialization": "forbidden",
            "bootstrap_authorization_sha256": "5" * 64,
        },
        "read_only_attestation": {
            "remote_mutation_performed": False,
            "external_effects_triggered": False,
        },
    }


@pytest.fixture
def producer_case(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    tmp_path.chmod(0o700)
    now = datetime.now(timezone.utc)
    db_path = _database(tmp_path / "control.sqlite3")
    snapshot = tmp_path / "snapshot.sqlite3"
    schema_path = tmp_path / "schema.json"
    schema_result = create_snapshot_receipt(
        db_path,
        snapshot_path=snapshot.absolute(),
        receipt_path=schema_path.absolute(),
    )
    baseline_path = tmp_path / "baseline.json"
    baseline_raw = _write_owner(
        baseline_path,
        {"schema_version": "pnc_rca_delivery_quarantine_baseline_v1", "ready": True},
    )
    authority = _authority(
        schema_result["receipt"],
        schema_result["receipt_raw_sha256"],
        hashlib.sha256(baseline_raw).hexdigest(),
    )
    authority_path = tmp_path / "authority.json"
    _write_owner(authority_path, authority)
    env_path = tmp_path / "candidate.env"
    live_env_path = tmp_path / "live.env"
    env_text = (
        f"HERMES_HOME={tmp_path}\n"
        "HERMES_OUTBOUND_MODE=record-only\n"
        "HERMES_RCA_KAFKA_EXPECTED_CLUSTER_ID=cluster-test\n"
    ).encode()
    env_path.write_bytes(env_text)
    live_env_path.write_bytes(env_text)
    env_path.chmod(0o600)
    live_env_path.chmod(0o600)
    migration_path = tmp_path / "migration.json"
    _write_owner(migration_path, {"test": True})
    canary_path = tmp_path / "canary-plan.json"
    _write_owner(canary_path, _canary_plan())
    manifest_path = tmp_path / "LIVE_MANIFEST.json"
    _write_owner(manifest_path, {"runtime_root": "/candidate"})
    config = _config(db_path, authority, hashlib.sha256(baseline_raw).hexdigest())

    monkeypatch.setattr(gate, "_public_config", lambda _env, _authority: config)
    monkeypatch.setattr(
        gate,
        "_verify_host_face",
        lambda _authority: {
            **_authority["faces"]["host_runtime"],
            "dirty": False,
        },
    )
    monkeypatch.setattr(
        gate,
        "_validate_migration_receipt",
        lambda path, **_kwargs: _migration_result(path),
    )
    monkeypatch.setattr(
        gate,
        "read_quarantine_baseline_status",
        lambda *_args, **_kwargs: {
            "ready": True,
            "state": "acknowledged",
            "error_code": "",
        },
    )
    monkeypatch.setattr(
        gate.ConsumerConfig,
        "from_env",
        staticmethod(lambda *_args, **_kwargs: SimpleNamespace(topic=TOPIC)),
    )
    monkeypatch.setattr(capsules, "_recheck_live_gateway_binding", lambda *_a, **_k: None)
    return {
        "tmp": tmp_path,
        "now": now,
        "db": db_path,
        "authority": authority,
        "authority_path": authority_path,
        "env": env_path,
        "live_env": live_env_path,
        "schema": schema_path,
        "migration": migration_path,
        "baseline": baseline_path,
        "canary": canary_path,
        "manifest": manifest_path,
        "gateway": _gateway_binding(),
    }


def _produce(
    case: dict[str, Any],
    *,
    mode: str,
    broker_end: int,
    preauthorization_capsule: Path | None = None,
    vm_observation: Path | None = None,
) -> dict[str, Any]:
    return gate.produce_release_gate(
        mode=mode,
        epoch_id=EPOCH_ID,
        env_path=case["env"],
        live_env_path=case["live_env"],
        authority_path=case["authority_path"],
        schema_receipt_path=case["schema"],
        migration_receipt_path=case["migration"],
        baseline_path=case["baseline"],
        canary_plan_path=case["canary"],
        live_manifest_path=case["manifest"],
        broker_receipt_path=case["tmp"] / f"{mode}-broker.json",
        receipt_path=case["tmp"] / f"{mode}-gate.json",
        evidence_dir=case["tmp"],
        preauthorization_capsule_path=preauthorization_capsule,
        vm_observation_path=vm_observation,
        now=case["now"],
        broker_collector=lambda *_args, **_kwargs: _broker(
            case["now"], end=broker_end
        ),
        gateway_collector=lambda **_kwargs: case["gateway"],
    )


def test_preauthorization_producer_builds_capsule_consumed_by_activation(
    producer_case: dict[str, Any], capsys: pytest.CaptureFixture[str]
) -> None:
    result = _produce(producer_case, mode="preauthorization", broker_end=10)
    report_path = Path(result["report_path"])
    report = json.loads(report_path.read_text(encoding="utf-8"))

    assert report_path.stat().st_mode & 0o777 == 0o600
    assert report["fingerprint"] == capsules.release_report_fingerprint(report)
    assert report["checks"][-1]["name"] == "activation_capsule_material"
    assert report["checks"][-1]["detail"]["activation_input"][
        "partition_start_fence"
    ] == {TOPIC: {"0": 10}}
    assert result["source_mutation_performed"] is False
    assert result["external_effects_triggered"] is False

    capsule = capsules.build_preauthorization_capsule(
        report_path,
        control_db_path=producer_case["db"],
        now=producer_case["now"],
    )
    args = [
        "--control-db",
        str(producer_case["db"]),
        "create",
        "--operator",
        "producer-test",
        "--reason",
        "validate the unique activation producer",
        "--preauthorization-capsule",
        str(capsule),
        "--apply",
    ]
    assert activation.main(args) == 0
    assert json.loads(capsys.readouterr().out)["ok"] is True
    assert RcaControlStore(producer_case["db"]).activation_epoch()["state"] == "safe_off"


def test_preproduction_producer_preserves_preauthorization_input_and_transitions(
    producer_case: dict[str, Any], capsys: pytest.CaptureFixture[str]
) -> None:
    preauth = _produce(producer_case, mode="preauthorization", broker_end=10)
    preauth_path = Path(preauth["report_path"])
    preauth_capsule = capsules.build_preauthorization_capsule(
        preauth_path,
        control_db_path=producer_case["db"],
        now=producer_case["now"],
    )
    create = [
        "--control-db",
        str(producer_case["db"]),
        "create",
        "--operator",
        "producer-test",
        "--reason",
        "prepare preproduction producer test",
        "--preauthorization-capsule",
        str(preauth_capsule),
        "--apply",
    ]
    assert activation.main(create) == 0
    capsys.readouterr()
    vm_path = producer_case["tmp"] / "vm.json"
    _write_owner(
        vm_path,
        _vm_observation(producer_case["authority"], producer_case["now"]),
    )

    preproduction = _produce(
        producer_case,
        mode="preproduction",
        broker_end=11,
        preauthorization_capsule=preauth_capsule,
        vm_observation=vm_path,
    )
    preproduction_path = Path(preproduction["report_path"])
    report = json.loads(preproduction_path.read_text(encoding="utf-8"))
    material = report["checks"][-1]["detail"]
    assert material["activation_input"]["partition_start_fence"] == {
        TOPIC: {"0": 10}
    }
    assert next(
        item for item in report["checks"] if item["name"] == "broker_t0"
    )["detail"]["current_partition_fence"] == {TOPIC: {"0": 11}}

    preproduction_capsule = capsules.build_preproduction_capsule(
        preproduction_path,
        control_db_path=producer_case["db"],
        preauthorization_capsule=preauth_capsule,
        now=producer_case["now"],
    )
    transition = [
        "--control-db",
        str(producer_case["db"]),
        "transition-preauthorized",
        "--operator",
        "producer-test",
        "--reason",
        "validate preproduction producer continuity",
        "--preproduction-capsule",
        str(preproduction_capsule),
        "--apply",
    ]
    assert activation.main(transition) == 0
    assert json.loads(capsys.readouterr().out)["ok"] is True
    assert RcaControlStore(producer_case["db"]).activation_epoch()["state"] == "preauthorized"


def test_broker_collector_never_joins_polls_or_commits() -> None:
    captured: dict[str, Any] = {}

    class TopicPartition:
        def __init__(self, topic: str, partition: int):
            self.topic = topic
            self.partition = partition

        def __hash__(self) -> int:
            return hash((self.topic, self.partition))

        def __eq__(self, other: object) -> bool:
            return (
                isinstance(other, TopicPartition)
                and (self.topic, self.partition) == (other.topic, other.partition)
            )

    class FakeConsumer:
        def __init__(self, **kwargs: Any):
            captured.update(kwargs)
            self._client = SimpleNamespace(
                cluster=SimpleNamespace(cluster_id="cluster-test")
            )
            self.closed = False

        def partitions_for_topic(self, _topic: str) -> set[int]:
            return {0, 1}

        def beginning_offsets(
            self, partitions: list[Any], *, timeout_ms: int
        ) -> dict:
            assert timeout_ms == 3000
            return {partition: partition.partition for partition in partitions}

        def end_offsets(self, partitions: list[Any], *, timeout_ms: int) -> dict:
            assert timeout_ms == 3000
            return {partition: partition.partition + 10 for partition in partitions}

        def close(self) -> None:
            self.closed = True

    config = SimpleNamespace(
        topic=TOPIC,
        isolation_level="read_committed",
        offset_lookup_timeout_ms=3000,
        kafka_kwargs=lambda: {
            "group_id": "must-be-overridden",
            "enable_auto_commit": True,
            "allow_auto_create_topics": False,
        },
    )
    observed = gate.collect_broker_t0(
        config,
        expected_cluster_id="cluster-test",
        consumer_factory=FakeConsumer,
        topic_partition_factory=TopicPartition,
    )

    assert captured["group_id"] is None
    assert captured["enable_auto_commit"] is False
    assert observed["partition_offsets"] == {
        "0": {"beginning": 0, "end": 10},
        "1": {"beginning": 1, "end": 11},
    }
    assert observed["connection"]["subscribe_called"] is False
    assert observed["connection"]["poll_called"] is False
    assert observed["connection"]["commit_called"] is False


def test_unsafe_external_write_config_is_rejected(producer_case: dict[str, Any]) -> None:
    config = _config(
        producer_case["db"],
        producer_case["authority"],
        producer_case["authority"]["quarantine_baseline"]["baseline_sha256"],
    )
    config["outbox_dispatcher"]["allow_feishu_writeback"] = True
    with pytest.raises(
        gate.ActivationGateError, match="rca_activation_gate_unsafe_config"
    ):
        gate._validate_safe_config(
            config,
            {
                "HERMES_OUTBOUND_MODE": "record-only",
            },
            producer_case["authority"],
        )


def test_unapproved_authority_is_rejected(producer_case: dict[str, Any]) -> None:
    authority = dict(producer_case["authority"])
    authority["status"] = "candidate_only"
    path = producer_case["tmp"] / "candidate-authority.json"
    _write_owner(path, authority)

    with pytest.raises(
        gate.ActivationGateError,
        match="rca_activation_gate_authority_not_approved",
    ):
        gate._load_authority(path)


def test_duplicate_env_authority_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "duplicate.env"
    path.write_text("HERMES_HOME=/one\nHERMES_HOME=/two\n", encoding="utf-8")
    path.chmod(0o600)

    with pytest.raises(
        gate.ActivationGateError,
        match="rca_activation_gate_env_duplicate_key",
    ):
        gate._load_environment(path.absolute())


def test_vm_observation_face_drift_is_rejected(
    producer_case: dict[str, Any],
) -> None:
    value = _vm_observation(producer_case["authority"], producer_case["now"])
    value["faces"]["g1q3_rca_pipeline"]["tree"] = "0" * 40

    with pytest.raises(
        gate.ActivationGateError,
        match="rca_activation_gate_vm_face_mismatch",
    ):
        gate._validate_vm_observation(
            value,
            authority=producer_case["authority"],
            authority_digest=canonical_json_sha256(producer_case["authority"]),
            now=producer_case["now"],
            max_age_seconds=900,
        )


def test_release_gate_schema_has_one_production_producer() -> None:
    marker = 'report["fingerprint"] = capsules.release_report_fingerprint(report)'
    producers = [
        path.name
        for path in (Path(__file__).parents[2] / "scripts").glob("*.py")
        if marker in path.read_text(encoding="utf-8")
    ]
    assert producers == ["pnc_rca_activation_gate.py"]


def test_cli_failure_returns_direct_exit_two(capsys: pytest.CaptureFixture[str]) -> None:
    assert gate.main([]) == 2
    payload = json.loads(capsys.readouterr().err)
    assert payload["ok"] is False
    assert payload["code"] == "rca_activation_gate_cli_arguments_invalid"
