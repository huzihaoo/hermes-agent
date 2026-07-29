from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import plistlib
import sqlite3
import sys

import pytest

from gateway.pnc_rca_control_store import KafkaRecord, RcaControlStore
from gateway.pnc_rca_kafka_contract import WorkflowEventPolicy, WorkflowTransition
from scripts import pnc_rca_b15_preflight as preflight


NOW = datetime(2026, 7, 29, 5, 0, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def _fixed_preflight_clock(monkeypatch):
    monkeypatch.setattr(preflight, "_utc_now", lambda: NOW)


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_json(path: Path, value: object, *, mode: int = 0o600) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")
    path.chmod(mode)


def _build_fixture(
    tmp_path: Path,
    *,
    seed_historical_outbox: bool = True,
) -> dict[str, Path]:
    runtime_dir = tmp_path / "runtime"
    launch_dir = tmp_path / "LaunchAgents"
    release_scripts = tmp_path / "release" / "scripts"
    runtime_dir.mkdir(mode=0o700)
    launch_dir.mkdir(mode=0o700)
    release_scripts.mkdir(mode=0o700, parents=True)

    record_root = tmp_path / "records"
    record_root.mkdir(mode=0o700)
    key_file = tmp_path / "record.key"
    key_file.write_bytes(b"k" * 32)
    key_file.chmod(0o600)
    natural_gate = tmp_path / "natural-gate.json"
    exact_request = tmp_path / "exact-recovery.json"
    env_path = tmp_path / ".env"
    env_path.write_text(
        "\n".join(
            [
                "HERMES_RCA_ACTIVATION_REQUIRED=true",
                "HERMES_RCA_KAFKA_ACTIVATION_REQUIRED=true",
                "HERMES_RCA_OUTBOX_ACTIVATION_REQUIRED=true",
                "HERMES_RCA_DELIVERY_COLLECTOR_ACTIVATION_REQUIRED=true",
                "HERMES_RCA_DELIVERY_DISPATCHER_ACTIVATION_REQUIRED=true",
                "HERMES_RCA_KAFKA_SUBMIT_ENABLED=true",
                "HERMES_RCA_OUTBOX_DISPATCH_ENABLED=true",
                "HERMES_RCA_OUTBOX_ALLOW_FEISHU_WRITEBACK=false",
                "HERMES_RCA_DELIVERY_DISPATCHER_ENABLED=false",
                "HERMES_OUTBOUND_MODE=record-only",
                f"HERMES_OUTBOUND_RECORD_ROOT={record_root}",
                f"HERMES_OUTBOUND_RECORD_KEY_FILE={key_file}",
                f"HERMES_RCA_KAFKA_NATURAL_CANARY_GATE_PATH={natural_gate}",
                f"HERMES_RCA_KAFKA_EXACT_RECOVERY_REQUEST_PATH={exact_request}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    env_path.chmod(0o600)

    scripts: dict[str, Path] = {}
    for name, plist_name, _health_name, _mode in preflight.SERVICE_SPECS:
        script = release_scripts / f"{name}.py"
        script.write_text(f"# {name}\n", encoding="utf-8")
        script.chmod(0o600)
        scripts[name] = script
        plist = {
            "Label": plist_name.removesuffix(".plist"),
            "ProgramArguments": [sys.executable, str(script)],
            "WorkingDirectory": str(release_scripts.parent),
        }
        plist_path = launch_dir / plist_name
        plist_path.write_bytes(plistlib.dumps(plist))
        plist_path.chmod(0o600)

    service_labels = {
        "kafka_consumer": "local.pnc.rca-kafka-consumer",
        "outbox_dispatcher": "local.pnc.rca-outbox-dispatcher",
        "delivery_collector": "local.pnc.rca-delivery-collector",
        "delivery_dispatcher": "local.pnc.rca-delivery-dispatcher",
    }
    health_configs = {
        "kafka_consumer": {
            "activation_required": True,
            "submit_enabled": True,
            "external_dispatch_wired": False,
            "natural_canary_gate_path": str(natural_gate),
            "exact_recovery_request_path": str(exact_request),
        },
        "outbox_dispatcher": {
            "activation_required": True,
            "dispatch_enabled": True,
            "allow_feishu_writeback": False,
        },
        "delivery_collector": {
            "activation_required": True,
            "enabled": True,
            "external_writes": False,
        },
        "delivery_dispatcher": {
            "activation_required": True,
            "enabled": False,
            "external_writes": False,
        },
    }
    runtime_identity = {
        name: {
            "service_label": service_labels[name],
            "pid": 1000 + index,
            "process_create_time": 2.0,
            "boot_time": 1.0,
            "executable": str(Path(sys.executable).resolve()),
            "script": str(script),
            "cwd": str(release_scripts.parent),
            "script_sha256": _sha(script),
            "runtime_files_sha256": "a" * 64,
            "public_config_sha256": preflight.canonical_json_sha256(
                health_configs[name]
            ),
            "loaded_runtime_sha256": "b" * 64,
        }
        for index, (name, script) in enumerate(scripts.items())
    }
    common_time = NOW.isoformat()
    _write_json(
        runtime_dir / "consumer_health.json",
        {
            "schema_version": "pnc_rca_kafka_consumer_health_v2",
            "state": "activation_frozen",
            "healthy": True,
            "ok": True,
            "enabled": True,
            "activation_required": True,
            "external_dispatch_wired": False,
            "heartbeat_at": common_time,
            "runtime_identity": runtime_identity["kafka_consumer"],
            "config": health_configs["kafka_consumer"],
            "assignment": {
                "assigned_partitions": ["fixture-topic:0"],
                "callback_errors": 0,
            },
            "store": {"ok": True},
            "activation_freeze": {
                "schema_version": "pnc_rca_activation_ingress_freeze_v1",
                "epoch_id": "epoch-fixture",
                "state": "partitions_paused",
                "freeze_token": "c" * 64,
                "paused_at": common_time,
                "observed_at": common_time,
                "consumer_runtime_identity_sha256": preflight.canonical_json_sha256(
                    runtime_identity["kafka_consumer"]
                ),
                "partition_positions": {"fixture-topic": {"0": 10}},
                "restart_required": False,
            },
        },
    )
    _write_json(
        runtime_dir / "outbox_dispatcher_health.json",
        {
            "schema_version": "pnc_rca_outbox_dispatcher_health_v2",
            "ok": True,
            "state": "idle",
            "healthy": True,
            "enabled": True,
            "heartbeat_at": common_time,
            "readiness_observed_at": common_time,
            "runtime_identity": runtime_identity["outbox_dispatcher"],
            "config": health_configs["outbox_dispatcher"],
            "store": {"ok": True},
            "delivery_backpressure": {"active": False},
            "workspace_runtime": {"ready": True},
            "capacity_admission": {"ready": True},
        },
    )
    _write_json(
        runtime_dir / "delivery_collector_health.json",
        {
            "schema_version": "pnc_rca_delivery_collector_health_v2",
            "state": "idle",
            "healthy": True,
            "enabled": True,
            "updated_at": common_time,
            "runtime_identity": runtime_identity["delivery_collector"],
            "config": health_configs["delivery_collector"],
            "external_writes": False,
            "dependencies": {},
            "dependency_error": "",
            "store": {"ok": True},
        },
    )
    _write_json(
        runtime_dir / "delivery_dispatcher_health.json",
        {
            "schema_version": "pnc_rca_delivery_dispatcher_health_v2",
            "state": "disabled",
            "healthy": True,
            "updated_at": common_time,
            "runtime_identity": runtime_identity["delivery_dispatcher"],
            "config": health_configs["delivery_dispatcher"],
            "store": {"ok": True},
        },
    )

    snapshot_path = tmp_path / "resource-snapshot.json"
    _write_json(
        snapshot_path,
        {
            "schema_version": "hermes-rca-prod-resource-snapshot/v1",
            "observed_at": common_time,
            "root_available_bytes": 500 * 1024**3,
            "delivery_available_bytes": 600 * 1024**3,
            "root_device": "device-root",
            "delivery_device": "device-delivery",
            "delivery_filesystem": "cifs",
            "delivery_mount_rw": True,
            "delivery_writable": True,
            "memory_available_bytes": 32 * 1024**3,
            "swap_free_ratio": 0.5,
            "load1": 0.1,
            "cpu_count": 4,
            "dnp_real": 0,
            "dnp_like": 0,
            "mcap_rss_bytes": 0,
            "mcap_process_count": 0,
        },
    )

    db_path = tmp_path / "control.sqlite3"
    store = RcaControlStore(db_path)
    topic = "feishu-project-workflow-event"
    policy = WorkflowEventPolicy(
        topic=topic,
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
    value = json.dumps(
        {
            "id": 7041712812,
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
            "updated_at": 1783650000000,
            "work_item_type_key": "problem-type",
        },
        sort_keys=True,
    ).encode()
    if seed_historical_outbox:
        store.ingest_record(
            KafkaRecord(
                topic=topic,
                partition=2,
                offset=10,
                value=value,
                key=b"issue-key",
                timestamp_ms=1783650000000,
                headers=(("trace", b"trace-1"),),
            ),
            policy=policy,
            submit_enabled=True,
        )
    epoch = store.create_activation_epoch(
        epoch_id="epoch-fixture",
        preauthorization_fingerprint="1" * 64,
        preauthorization_gate_receipt_sha256="a" * 64,
        preauthorization_capsule_sha256="b" * 64,
        config_sha256="2" * 64,
        db_logical_identity={
            "device": 7,
            "inode": 11,
            "logical_store_id": "rca-control-primary",
        },
        partition_start_fence={topic: {"2": 20}},
        operator="preflight-test",
        reason="create canonical B15 fixture",
        now=NOW,
    )
    store.preauthorize_activation_epoch(
        epoch_id="epoch-fixture",
        preproduction_fingerprint="c" * 64,
        preproduction_gate_receipt_sha256="d" * 64,
        preproduction_capsule_sha256="e" * 64,
        expected_preauthorization_fingerprint="1" * 64,
        expected_preauthorization_gate_receipt_sha256="a" * 64,
        expected_preauthorization_capsule_sha256="b" * 64,
        expected_config_sha256=epoch["config_sha256"],
        expected_db_logical_identity_sha256=epoch["db_logical_identity_sha256"],
        expected_partition_start_fence_sha256=epoch[
            "partition_start_fence_sha256"
        ],
        operator="preflight-test",
        reason="seal canonical historical hold",
        now=NOW,
    )
    store.close_dispatcher_circuit(now=NOW)
    db_path.chmod(0o600)

    return {
        "env": env_path,
        "runtime": runtime_dir,
        "launch": launch_dir,
        "db": db_path,
        "snapshot": snapshot_path,
    }


def _args(paths: dict[str, Path], receipt: Path) -> list[str]:
    return [
        "--env-file",
        str(paths["env"]),
        "--runtime-dir",
        str(paths["runtime"]),
        "--launch-agents-dir",
        str(paths["launch"]),
        "--control-db",
        str(paths["db"]),
        "--resource-snapshot",
        str(paths["snapshot"]),
        "--receipt",
        str(receipt),
    ]


def test_b15_preflight_green_receipt_is_read_only_and_recomputable(tmp_path):
    paths = _build_fixture(tmp_path)
    before = paths["db"].stat()
    receipt = tmp_path / "preflight.json"

    assert preflight.main(_args(paths, receipt)) == 0

    body = json.loads(receipt.read_text(encoding="utf-8"))
    assert body["status"] == "GREEN"
    assert body["ready"] is True
    assert body["runtime_mutation_performed"] is False
    assert all(body["gates"].values())
    hold_gate = body["gates"]["historical_outbox_and_circuit"]
    assert hold_gate["historical_hold_integrity_error"] == ""
    assert hold_gate["historical_hold_integrity"]["disposed"] is False
    assert hold_gate["historical_hold_integrity"]["matches"] is True
    assert (
        hold_gate["historical_hold_integrity"]["sealed_sha256"]
        == hold_gate["historical_hold_integrity"]["current_sha256"]
    )
    assert body["receipt_fingerprint"] == preflight.canonical_json_sha256(
        {key: value for key, value in body.items() if key != "receipt_fingerprint"}
    )
    after = paths["db"].stat()
    assert (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) == (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
    )


def test_b15_preflight_clock_override_is_not_a_cli_contract(tmp_path):
    paths = _build_fixture(tmp_path)
    with pytest.raises(SystemExit):
        preflight.build_arg_parser().parse_args(
            _args(paths, tmp_path / "rejected.json") + ["--now", NOW.isoformat()]
        )


def test_b15_preflight_uses_live_clock_for_freshness(tmp_path, monkeypatch):
    paths = _build_fixture(tmp_path)
    monkeypatch.setattr(preflight, "_utc_now", lambda: NOW + timedelta(days=2))
    receipt = tmp_path / "live-clock-stale.json"

    assert preflight.main(_args(paths, receipt)) == 2
    body = json.loads(receipt.read_text(encoding="utf-8"))
    assert body["status"] == "RED"
    assert body["gates"]["resource_snapshot"]["error_code"] == (
        "rca_prod_snapshot_stale"
    )


def test_b15_preflight_binds_kafka_freeze_to_current_epoch(tmp_path):
    paths = _build_fixture(tmp_path)
    health_path = paths["runtime"] / "consumer_health.json"
    health = json.loads(health_path.read_text(encoding="utf-8"))
    health["activation_freeze"]["epoch_id"] = "different-epoch"
    _write_json(health_path, health)
    receipt = tmp_path / "epoch-mismatch.json"

    assert preflight.main(_args(paths, receipt)) == 2
    body = json.loads(receipt.read_text(encoding="utf-8"))
    gate = body["gates"]["historical_outbox_and_circuit"]
    assert gate["kafka_epoch_binding_ready"] is False
    assert gate["ready"] is False


def test_b15_preflight_uses_each_residents_authoritative_freshness_field(tmp_path):
    paths = _build_fixture(tmp_path)
    health_path = paths["runtime"] / "outbox_dispatcher_health.json"
    health = json.loads(health_path.read_text(encoding="utf-8"))
    health["heartbeat_at"] = (NOW - timedelta(days=2)).isoformat()
    health["readiness_observed_at"] = NOW.isoformat()
    health["started_at"] = NOW.isoformat()
    _write_json(health_path, health)
    receipt = tmp_path / "stale-authoritative-heartbeat.json"

    assert preflight.main(_args(paths, receipt)) == 2
    body = json.loads(receipt.read_text(encoding="utf-8"))
    gate = body["gates"]["resident_runtime"]["outbox_dispatcher"]
    assert gate["ready"] is False
    assert gate["age_seconds"] == pytest.approx(2 * 24 * 60 * 60)


@pytest.mark.parametrize(
    "service",
    ["kafka_consumer", "outbox_dispatcher", "delivery_collector", "delivery_dispatcher"],
)
def test_b15_preflight_rejects_bogus_resident_health_contract(tmp_path, service):
    paths = _build_fixture(tmp_path)
    health_name = {
        "kafka_consumer": "consumer_health.json",
        "outbox_dispatcher": "outbox_dispatcher_health.json",
        "delivery_collector": "delivery_collector_health.json",
        "delivery_dispatcher": "delivery_dispatcher_health.json",
    }[service]
    health_path = paths["runtime"] / health_name
    health = json.loads(health_path.read_text(encoding="utf-8"))
    health["schema_version"] = "bogus-health-schema"
    health["healthy"] = False
    health["config"]["activation_required"] = False
    if service in {"kafka_consumer", "outbox_dispatcher"}:
        health["ok"] = False
    _write_json(health_path, health)
    receipt = tmp_path / f"{service}-bogus.json"

    assert preflight.main(_args(paths, receipt)) == 2
    body = json.loads(receipt.read_text(encoding="utf-8"))
    service_gate = body["gates"]["resident_runtime"][service]
    assert service_gate["ready"] is False
    assert service_gate["contract_ready"] is False
    assert "schema_version" in service_gate["contract_errors"]
    assert "healthy" in service_gate["contract_errors"]


def test_b15_preflight_malformed_snapshot_writes_red_receipt(tmp_path):
    paths = _build_fixture(tmp_path)
    _write_json(paths["snapshot"], {"rca_prod_snapshot": []})
    receipt = tmp_path / "malformed-snapshot.json"

    assert preflight.main(_args(paths, receipt)) == 2
    body = json.loads(receipt.read_text(encoding="utf-8"))
    gate = body["gates"]["resource_snapshot"]
    assert body["status"] == "RED"
    assert gate["ready"] is False
    assert gate["error_code"] == "b15_resource_snapshot_shape_invalid"
    assert gate["raw_sha256"] == _sha(paths["snapshot"])
    assert receipt.with_name(receipt.name + ".sha256").is_file()


@pytest.mark.parametrize(
    ("mutation", "expected_error"),
    [
        (
            "UPDATE rca_activation_historical_outbox_holds "
            "SET cohort_sha256 = 'ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff'",
            "historical_outbox_hold_seal",
        ),
        (
            "UPDATE rca_outbox SET payload_json = '{\"tampered\":true}'",
            "historical_outbox_hold_row_binding",
        ),
    ],
)
def test_b15_preflight_recomputes_canonical_historical_hold_integrity(
    tmp_path,
    mutation,
    expected_error,
):
    paths = _build_fixture(tmp_path)
    with sqlite3.connect(paths["db"]) as conn:
        conn.row_factory = sqlite3.Row
        RcaControlStore._drop_v13_historical_outbox_hold_triggers(conn)
        conn.execute(mutation)
        RcaControlStore._create_v13_historical_outbox_hold_schema(conn)
    receipt = tmp_path / "tampered-hold.json"

    assert preflight.main(_args(paths, receipt)) == 2
    body = json.loads(receipt.read_text(encoding="utf-8"))
    hold_gate = body["gates"]["historical_outbox_and_circuit"]
    assert hold_gate["ready"] is False
    assert expected_error in hold_gate["historical_hold_integrity_error"]


def test_b15_preflight_rejects_empty_historical_cohort(tmp_path):
    paths = _build_fixture(tmp_path, seed_historical_outbox=False)
    receipt = tmp_path / "empty-hold.json"

    assert preflight.main(_args(paths, receipt)) == 2
    body = json.loads(receipt.read_text(encoding="utf-8"))
    hold_gate = body["gates"]["historical_outbox_and_circuit"]
    assert hold_gate["ready"] is False
    assert hold_gate["historical_hold_integrity"]["sealed_count"] == 0
    assert hold_gate["historical_hold_integrity"]["matches"] is True


def test_b15_preflight_rejects_unsealed_historical_cohort_addition(tmp_path):
    paths = _build_fixture(tmp_path)
    with sqlite3.connect(paths["db"]) as conn:
        conn.row_factory = sqlite3.Row
        original = dict(conn.execute("SELECT * FROM rca_outbox").fetchone())
        original.pop("outbox_id")
        original["submission_key"] = f"{original['submission_key']}-unsealed"
        columns = tuple(original)
        conn.execute(
            f"INSERT INTO rca_outbox({', '.join(columns)}) "
            f"VALUES ({', '.join('?' for _ in columns)})",
            tuple(original[column] for column in columns),
        )
    receipt = tmp_path / "unsealed-addition.json"

    assert preflight.main(_args(paths, receipt)) == 2
    body = json.loads(receipt.read_text(encoding="utf-8"))
    hold_gate = body["gates"]["historical_outbox_and_circuit"]
    assert hold_gate["ready"] is False
    assert hold_gate["historical_hold_integrity_error"] == ""
    assert hold_gate["historical_hold_integrity"]["matches"] is False
    assert hold_gate["historical_hold_integrity"]["sealed_count"] == 1
    assert hold_gate["historical_hold_integrity"]["current_count"] == 2


def test_b15_preflight_rejects_delivery_enablement_and_missing_freeze(
    tmp_path,
):
    paths = _build_fixture(tmp_path)
    env = paths["env"].read_text(encoding="utf-8")
    paths["env"].write_text(
        env.replace("HERMES_RCA_DELIVERY_DISPATCHER_ENABLED=false", "HERMES_RCA_DELIVERY_DISPATCHER_ENABLED=true"),
        encoding="utf-8",
    )
    paths["env"].chmod(0o600)
    receipt = tmp_path / "red.json"

    assert preflight.main(_args(paths, receipt)) == 2
    body = json.loads(receipt.read_text(encoding="utf-8"))
    assert body["status"] == "RED"
    assert body["gates"]["configuration"]["ready"] is False


def test_b15_preflight_requires_explicit_writeback_disablement(tmp_path):
    paths = _build_fixture(tmp_path)
    env = paths["env"].read_text(encoding="utf-8")
    paths["env"].write_text(
        env.replace("HERMES_RCA_OUTBOX_ALLOW_FEISHU_WRITEBACK=false\n", ""),
        encoding="utf-8",
    )
    paths["env"].chmod(0o600)
    receipt = tmp_path / "missing-writeback-fence.json"

    assert preflight.main(_args(paths, receipt)) == 2
    body = json.loads(receipt.read_text(encoding="utf-8"))
    gate = body["gates"]["configuration"]
    assert gate["ready"] is False
    assert gate["outbox_allow_feishu_writeback"] is None


def test_b15_preflight_rejects_stale_resource_snapshot(tmp_path):
    paths = _build_fixture(tmp_path)
    snapshot = json.loads(paths["snapshot"].read_text(encoding="utf-8"))
    snapshot["observed_at"] = "2026-07-28T00:00:00+00:00"
    _write_json(paths["snapshot"], snapshot)
    receipt = tmp_path / "stale.json"

    assert preflight.main(_args(paths, receipt)) == 2
    body = json.loads(receipt.read_text(encoding="utf-8"))
    assert body["gates"]["resource_snapshot"]["ready"] is False
    assert body["gates"]["resource_snapshot"]["error_code"] == (
        "rca_prod_snapshot_stale"
    )


def test_b15_preflight_rejects_unsealed_historical_hold(tmp_path):
    paths = _build_fixture(tmp_path)
    with sqlite3.connect(paths["db"]) as conn:
        conn.execute("DROP TABLE rca_activation_historical_outbox_holds")
    receipt = tmp_path / "no-hold.json"

    assert preflight.main(_args(paths, receipt)) == 2
    body = json.loads(receipt.read_text(encoding="utf-8"))
    assert body["gates"]["historical_outbox_and_circuit"]["ready"] is False


def test_b15_preflight_database_error_baseline_is_recomputable(tmp_path):
    paths = _build_fixture(tmp_path)
    paths["db"].write_bytes(b"not-a-sqlite-database")
    paths["db"].chmod(0o600)
    receipt = tmp_path / "invalid-database.json"

    assert preflight.main(_args(paths, receipt)) == 2
    body = json.loads(receipt.read_text(encoding="utf-8"))
    baseline = body["external_effect_baseline"]
    assert baseline["error_code"] == "b15_control_db_query_failed"
    assert baseline["baseline_sha256"] == preflight.canonical_json_sha256(
        {key: value for key, value in baseline.items() if key != "baseline_sha256"}
    )
