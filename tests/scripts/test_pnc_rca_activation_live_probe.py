from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from scripts import pnc_rca_activation_capsule as capsules


def _gateway_binding() -> dict[str, Any]:
    identity = {
        "service_label": capsules.GATEWAY_SERVICE_LABEL,
        "pid": 1234,
        "process_create_time": 42.0,
        "boot_time": 1.0,
        "executable": "/bin/python3",
        "script": "/tmp/gateway/run.py",
        "cwd": "/tmp",
        "script_sha256": "1" * 64,
        "runtime_files_sha256": "2" * 64,
        "public_config_sha256": "3" * 64,
        "loaded_runtime_sha256": "4" * 64,
    }
    return {
        "state": "running_safe",
        "pid": 1234,
        "process_create_time": 42.0,
        "runtime_identity": identity,
        "runtime_identity_sha256": capsules._sha256_json(identity),
        "verified_runtime_sha256": "5" * 64,
    }


class _FakeProcess:
    def is_running(self) -> bool:
        return True

    def status(self) -> str:
        return "running"

    def create_time(self) -> float:
        return 42.0

    def cwd(self) -> str:
        return "/tmp"

    def exe(self) -> str:
        return "/bin/python3"

    def cmdline(self) -> list[str]:
        return ["python3", "-m", "hermes_cli.main", "gateway", "run"]

    def environ(self) -> dict[str, str]:
        return {}


class _FakeConsumerProcess(_FakeProcess):
    def create_time(self) -> float:
        return 43.0

    def cmdline(self) -> list[str]:
        return ["python3", "/tmp/scripts/pnc_rca_kafka_consumer.py"]


@pytest.fixture
def gateway_probe_fakes(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(capsules, "runtime_identity_is_valid", lambda *_a, **_k: True)
    monkeypatch.setattr(
        capsules,
        "psutil",
        SimpleNamespace(
            Process=lambda _pid: _FakeProcess(),
            STATUS_DEAD="dead",
            STATUS_ZOMBIE="zombie",
            Error=RuntimeError,
        ),
    )
    monkeypatch.setattr(capsules, "file_sha256", lambda _path: "1" * 64)
    monkeypatch.setattr(
        capsules, "runtime_file_snapshot", lambda *_a, **_k: ({}, "2" * 64)
    )


def test_gateway_probe_rejects_launchd_pid_drift(
    gateway_probe_fakes: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(capsules, "_live_launchd_pid", lambda _label: 9999)
    with pytest.raises(capsules.CapsuleError, match="gateway_restarted"):
        capsules._recheck_live_gateway_binding(_gateway_binding())


def test_gateway_probe_rejects_runtime_file_drift(
    gateway_probe_fakes: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(capsules, "_live_launchd_pid", lambda _label: 1234)
    monkeypatch.setattr(capsules, "file_sha256", lambda _path: "9" * 64)
    with pytest.raises(capsules.CapsuleError, match="runtime_changed"):
        capsules._recheck_live_gateway_binding(_gateway_binding())


def test_process_environment_allows_release_path_but_rejects_loader_overrides() -> None:
    assert not capsules._unsafe_process_environment(
        {
            "PYTHONPATH": "/tmp",
            "PYTHONUNBUFFERED": "1",
        },
        expected_root="/tmp",
    )
    assert capsules._unsafe_process_environment(
        {"PYTHONPATH": "/tmp:/malicious"}, expected_root="/tmp"
    )
    assert capsules._unsafe_process_environment({"PYTHONHOME": "/tmp"})


def _consumer_health(path: Path, *, state: str = "activation_frozen") -> dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat()
    identity = {
        "service_label": capsules.CONSUMER_SERVICE_LABEL,
        "pid": 2345,
        "process_create_time": 43.0,
        "boot_time": 1.0,
        "executable": "/bin/python3",
        "script": "/tmp/scripts/pnc_rca_kafka_consumer.py",
        "cwd": "/tmp",
        "script_sha256": "1" * 64,
        "runtime_files_sha256": "2" * 64,
        "public_config_sha256": "3" * 64,
        "loaded_runtime_sha256": "4" * 64,
    }
    positions = {"feishu-project-workflow-event": {"0": 11}}
    freeze = {
        "schema_version": capsules.ACTIVATION_FREEZE_SCHEMA_VERSION,
        "epoch_id": "epoch-1",
        "state": "partitions_paused",
        "freeze_token": "token-1",
        "paused_at": now,
        "observed_at": now,
        "consumer_runtime_identity_sha256": capsules._sha256_json(identity),
        "partition_positions": positions,
        "restart_required": False,
    }
    binding = {
        "schema_version": "pnc_rca_activation_ingress_freeze_binding_v1",
        "epoch_id": "epoch-1",
        "health_path": str(path.absolute()),
        "paused_at": now,
        "freeze_receipt_sha256": capsules._sha256_json({
            key: value for key, value in freeze.items() if key != "observed_at"
        }),
        "freeze_token_sha256": hashlib.sha256(b"token-1").hexdigest(),
        "consumer_runtime_identity_sha256": capsules._sha256_json(identity),
        "partition_positions_sha256": capsules._sha256_json(positions),
        "restart_required": False,
    }
    payload = {
        "schema_version": capsules.CONSUMER_HEALTH_SCHEMA_VERSION,
        "ok": True,
        "healthy": True,
        "enabled": True,
        "activation_required": True,
        "state": state,
        "heartbeat_at": now,
        "runtime_identity": identity,
        "config": {"activation_required": True},
        "stats": {"blocked_partitions": 0},
        "activation_freeze": freeze,
    }
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")
    path.chmod(0o600)
    return binding


def test_consumer_probe_rejects_health_state_drift(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(capsules, "runtime_identity_is_valid", lambda *_a, **_k: True)
    monkeypatch.setattr(capsules, "_live_launchd_pid", lambda _label: 2345)
    monkeypatch.setattr(
        capsules,
        "psutil",
        SimpleNamespace(
            Process=lambda _pid: _FakeConsumerProcess(),
            STATUS_DEAD="dead",
            STATUS_ZOMBIE="zombie",
            Error=RuntimeError,
        ),
    )
    path = tmp_path / "consumer-health.json"
    binding = _consumer_health(path, state="running")
    with pytest.raises(capsules.CapsuleError, match="consumer_not_frozen"):
        capsules._recheck_live_consumer_freeze(
            binding,
            epoch_id="epoch-1",
            partition_end_fence={"feishu-project-workflow-event": {"0": 11}},
        )


def test_resident_probe_rejects_loaded_runtime_drift() -> None:
    identity = {"loaded_runtime_sha256": "new"}
    with pytest.raises(capsules.CapsuleError, match="loaded_runtime_changed"):
        capsules._recheck_live_resident_projection(
            {
                "residents": {
                    "kafka_consumer_health": {
                        "runtime_identity_sha256": capsules._sha256_json(identity),
                        "loaded_runtime_sha256": "old",
                    },
                    "outbox_dispatcher_health": {},
                    "delivery_collector_health": {},
                    "delivery_dispatcher_health": {},
                }
            },
            consumer_health={"runtime_identity": identity},
        )
