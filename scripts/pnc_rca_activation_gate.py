#!/usr/bin/env python3
"""Produce the strict, read-only activation gate consumed by RCA capsules."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import io
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import subprocess
import sys
from typing import Any, Callable, Mapping, Sequence


REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import psutil  # noqa: E402
from dotenv import dotenv_values  # noqa: E402
from dotenv.parser import parse_stream  # noqa: E402

from gateway.pnc_rca_delivery_quarantine_migration import (  # noqa: E402
    QuarantineMigrationError,
    validate_combined_migration_receipt,
)
from gateway.pnc_rca_delivery_quarantine_baseline import (  # noqa: E402
    read_quarantine_baseline_status,
)
from gateway.pnc_rca_release_authority import (  # noqa: E402
    ReleaseAuthorityError,
    canonical_json_sha256 as authority_sha256,
    validate_release_authority,
)
from gateway.pnc_rca_runtime_identity import (  # noqa: E402
    GATEWAY_RCA_RUNTIME_RELATIVE_FILES,
    canonical_json_sha256,
    file_sha256,
    runtime_file_snapshot,
    runtime_identity_is_valid,
)
from scripts import pnc_rca_activation_capsule as capsules  # noqa: E402
from scripts.pnc_rca_delivery_collector import CollectorConfig  # noqa: E402
from scripts.pnc_rca_delivery_dispatcher import (  # noqa: E402
    DispatcherConfig as DeliveryDispatcherConfig,
)
from scripts.pnc_rca_kafka_consumer import ConsumerConfig  # noqa: E402
from scripts.pnc_rca_outbox_dispatcher import (  # noqa: E402
    DispatcherConfig as OutboxDispatcherConfig,
)
from scripts.pnc_rca_schema_fingerprint import (  # noqa: E402
    SchemaFingerprintError,
    verify_snapshot_receipt,
)


CLI_SCHEMA_VERSION = "pnc_rca_activation_gate_cli_v1"
BROKER_SCHEMA_VERSION = "pnc_rca_broker_t0_observation_v1"
CONTRACT_SCHEMA_VERSION = "pnc_rca_activation_contract_binding_v1"
VM_OBSERVATION_SCHEMA_VERSION = "pnc_rca_vm_release_observation_v1"
MAX_ENV_BYTES = 1024 * 1024
MAX_SUBPROCESS_BYTES = 1024 * 1024
DEFAULT_EVIDENCE_MAX_AGE_SECONDS = 900
GATEWAY_SERVICE_LABEL = "ai.hermes.gateway"
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
_EPOCH_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_PREAUTHORIZATION_INPUT_FIELDS = {
    "epoch_id",
    "initial_state",
    "config_sha256",
    "db_logical_identity",
    "db_logical_identity_sha256",
    "partition_start_fence",
    "partition_start_fence_sha256",
    "migration_receipt_raw_sha256",
    "materialization_receipt_raw_sha256",
    "broker_t0_observation_sha256",
    "canary_plan_raw_sha256",
}


class ActivationGateError(RuntimeError):
    """Stable fail-closed producer error."""

    def __init__(self, code: str, detail: str = ""):
        self.code = str(code or "rca_activation_gate_invalid")[:160]
        self.detail = str(detail or self.code)[:1000]
        super().__init__(self.code)


def _digest(value: Any, code: str) -> str:
    text = str(value or "").strip().lower()
    if _SHA256_RE.fullmatch(text) is None:
        raise ActivationGateError(code)
    return text


def _timestamp(value: Any, code: str) -> datetime:
    if not isinstance(value, str) or not value or value != value.strip():
        raise ActivationGateError(code)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ActivationGateError(code) from exc
    if parsed.tzinfo is None:
        raise ActivationGateError(code)
    return parsed.astimezone(timezone.utc)


def _fresh(
    observed_at: Any,
    *,
    now: datetime,
    max_age_seconds: int,
    code: str,
) -> datetime:
    observed = _timestamp(observed_at, code)
    age = (now - observed).total_seconds()
    if age < -30 or age > max_age_seconds:
        raise ActivationGateError(code)
    return observed


def _absolute(path: Path, code: str) -> Path:
    selected = path.expanduser()
    if not selected.is_absolute() or selected.absolute() != selected:
        raise ActivationGateError(code)
    return selected


def _owner_regular(path: Path, *, exact_mode: int | None = 0o600) -> os.stat_result:
    try:
        observed = path.lstat()
    except OSError as exc:
        raise ActivationGateError("rca_activation_gate_file_unavailable", str(path)) from exc
    if (
        stat.S_ISLNK(observed.st_mode)
        or not stat.S_ISREG(observed.st_mode)
        or observed.st_uid != os.geteuid()
        or observed.st_nlink != 1
        or observed.st_size <= 0
        or (
            exact_mode is not None
            and stat.S_IMODE(observed.st_mode) != exact_mode
        )
    ):
        raise ActivationGateError("rca_activation_gate_file_invalid", str(path))
    return observed


def _read_owner_json(path: Path, artifact: str) -> tuple[bytes, dict[str, Any]]:
    try:
        return capsules._read_owner_json(path, artifact=artifact)
    except capsules.CapsuleError as exc:
        raise ActivationGateError(exc.code) from exc


def _write_owner_json(path: Path, value: Mapping[str, Any]) -> bytes:
    try:
        return capsules._write_owner_no_clobber(path, value)
    except capsules.CapsuleError as exc:
        raise ActivationGateError(exc.code) from exc


def _load_environment(path: Path) -> tuple[bytes, dict[str, str]]:
    selected = _absolute(path, "rca_activation_gate_env_path_invalid")
    observed = _owner_regular(selected)
    if observed.st_size > MAX_ENV_BYTES:
        raise ActivationGateError("rca_activation_gate_env_file_invalid")
    raw = selected.read_bytes()
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ActivationGateError("rca_activation_gate_env_file_invalid") from exc
    seen: set[str] = set()
    for binding in parse_stream(io.StringIO(text)):
        if binding.error:
            raise ActivationGateError("rca_activation_gate_env_file_invalid")
        if binding.key is None:
            continue
        if binding.key in seen:
            raise ActivationGateError("rca_activation_gate_env_duplicate_key")
        seen.add(binding.key)
    parsed = dotenv_values(stream=io.StringIO(text), interpolate=False)
    if set(parsed) != seen or any(value is None for value in parsed.values()):
        raise ActivationGateError("rca_activation_gate_env_file_invalid")
    return raw, {str(key): str(value) for key, value in parsed.items()}


def _public_config(
    env: Mapping[str, str], authority: Mapping[str, Any]
) -> dict[str, Any]:
    home = Path(str(env.get("HERMES_HOME") or "")).expanduser()
    if not home.is_absolute():
        raise ActivationGateError("rca_activation_gate_hermes_home_invalid")
    try:
        consumer = ConsumerConfig.from_env(env, hermes_home=home)
        outbox = OutboxDispatcherConfig.from_env(env, hermes_home=home)
        collector = CollectorConfig.from_env(env, hermes_home=home)
        delivery = DeliveryDispatcherConfig.from_env(env, hermes_home=home)
    except (TypeError, ValueError) as exc:
        raise ActivationGateError(
            "rca_activation_gate_config_invalid", type(exc).__name__
        ) from exc
    return {
        "consumer": consumer.runtime_public_dict(),
        "outbox_dispatcher": outbox.public_dict(),
        "delivery_collector": collector.public_dict(),
        "delivery_dispatcher": delivery.public_dict(),
        "release": {
            "release_id": authority["release_id"],
            "authority_epoch_id": authority["authority_epoch_id"],
            "authority_sha256": authority_sha256(authority),
        },
    }


def _validate_safe_config(
    config: Mapping[str, Any],
    env: Mapping[str, str],
    authority: Mapping[str, Any],
) -> dict[str, Any]:
    consumer = config.get("consumer")
    outbox = config.get("outbox_dispatcher")
    collector = config.get("delivery_collector")
    delivery = config.get("delivery_dispatcher")
    if not all(isinstance(value, Mapping) for value in (consumer, outbox, collector, delivery)):
        raise ActivationGateError("rca_activation_gate_config_invalid")
    assert isinstance(consumer, Mapping)
    assert isinstance(outbox, Mapping)
    assert isinstance(collector, Mapping)
    assert isinstance(delivery, Mapping)
    release_id = authority["release_id"]
    baseline = authority["quarantine_baseline"]
    expected_baseline = baseline.get("baseline_sha256")
    control_paths = {
        str(consumer.get("control_db_path") or ""),
        str(outbox.get("control_db_path") or ""),
        str(outbox.get("delivery_db_path") or ""),
        str(collector.get("control_db_path") or ""),
        str(delivery.get("control_db_path") or ""),
    }
    safe = (
        env.get("HERMES_OUTBOUND_MODE") == "record-only"
        and consumer.get("submit_enabled") is True
        and consumer.get("activation_required") is True
        and consumer.get("external_dispatch_wired") is False
        and outbox.get("dispatch_enabled") is True
        and outbox.get("allow_feishu_writeback") is False
        and outbox.get("activation_required") is True
        and outbox.get("data_access_mode") == "remote_read"
        and outbox.get("capacity_mode") == "bootstrap"
        and outbox.get("release_id") == release_id
        and collector.get("enabled") is True
        and collector.get("external_writes") is False
        and collector.get("activation_required") is True
        and collector.get("quarantine_release_id") == release_id
        and collector.get("quarantine_baseline_sha256") == expected_baseline
        and delivery.get("enabled") is False
        and delivery.get("external_writes") is False
        and delivery.get("activation_required") is True
        and delivery.get("quarantine_release_id") == release_id
        and delivery.get("quarantine_baseline_sha256") == expected_baseline
        and len(control_paths) == 1
        and next(iter(control_paths)).startswith("/")
    )
    if not safe:
        raise ActivationGateError("rca_activation_gate_unsafe_config")
    return {
        "outbound_mode": "record-only",
        "activation_required": True,
        "submit_enabled": True,
        "outbox_dispatch_enabled": True,
        "feishu_writeback_enabled": False,
        "delivery_collector_enabled": True,
        "delivery_dispatcher_enabled": False,
        "external_writes": False,
        "control_db_path": next(iter(control_paths)),
        "capacity_mode": "bootstrap",
    }


def _load_authority(path: Path) -> tuple[bytes, dict[str, Any], str]:
    raw, authority = _read_owner_json(path, "rca_activation_gate_authority")
    try:
        validate_release_authority(authority)
    except ReleaseAuthorityError as exc:
        raise ActivationGateError(exc.code, exc.detail) from exc
    if authority.get("status") != "approved_for_activation":
        raise ActivationGateError("rca_activation_gate_authority_not_approved")
    return raw, authority, authority_sha256(authority)


def _git(root: Path, *arguments: str) -> str:
    try:
        completed = subprocess.run(
            ["git", "-C", str(root), *arguments],
            text=True,
            capture_output=True,
            timeout=20,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise ActivationGateError("rca_activation_gate_host_face_unavailable") from exc
    if completed.returncode != 0 or len(completed.stdout.encode("utf-8")) > MAX_SUBPROCESS_BYTES:
        raise ActivationGateError("rca_activation_gate_host_face_unavailable")
    return completed.stdout.strip()


def _verify_host_face(authority: Mapping[str, Any]) -> dict[str, Any]:
    face = authority["faces"]["host_runtime"]
    root = Path(str(face["root"])).expanduser().absolute()
    try:
        observed = root.lstat()
    except OSError as exc:
        raise ActivationGateError("rca_activation_gate_host_face_unavailable") from exc
    if stat.S_ISLNK(observed.st_mode) or not stat.S_ISDIR(observed.st_mode):
        raise ActivationGateError("rca_activation_gate_host_face_invalid")
    commit = _git(root, "rev-parse", "HEAD")
    tree = _git(root, "rev-parse", "HEAD^{tree}")
    dirty = _git(root, "status", "--porcelain", "--untracked-files=all")
    if commit != face["commit"] or tree != face["tree"] or dirty:
        raise ActivationGateError("rca_activation_gate_host_face_drift")
    return {"root": str(root), "commit": commit, "tree": tree, "dirty": False}


def _validate_schema_receipt(
    path: Path,
    *,
    control_db_path: Path,
    authority: Mapping[str, Any],
    now: datetime,
    max_age_seconds: int,
) -> tuple[bytes, dict[str, Any], dict[str, Any]]:
    try:
        verified = verify_snapshot_receipt(path)
    except SchemaFingerprintError as exc:
        raise ActivationGateError(exc.code, exc.detail) from exc
    raw, body = _read_owner_json(path, "rca_activation_gate_schema_receipt")
    _fresh(
        body.get("observation_completed_at"),
        now=now,
        max_age_seconds=max_age_seconds,
        code="rca_activation_gate_schema_receipt_stale",
    )
    source = body.get("source_database")
    identity = body.get("database_identity")
    if not isinstance(source, Mapping) or not isinstance(identity, Mapping):
        raise ActivationGateError("rca_activation_gate_schema_receipt_invalid")
    selected = control_db_path.expanduser().absolute()
    live_stat = _owner_regular(selected, exact_mode=None)
    expected = authority["control_store"]
    if (
        source.get("path") != str(selected)
        or source.get("device") != live_stat.st_dev
        or source.get("inode") != live_stat.st_ino
        or identity.get("database_instance_id")
        != expected.get("database_instance_id")
        or identity.get("control_schema_version") != expected.get("schema_version")
        or body.get("schema_fingerprint_sha256")
        != expected.get("schema_fingerprint_sha256")
        or hashlib.sha256(raw).hexdigest() != expected.get("backup_receipt_sha256")
    ):
        raise ActivationGateError("rca_activation_gate_schema_authority_mismatch")
    return raw, body, verified


def _validate_migration_receipt(
    path: Path,
    *,
    target_live_db_path: Path,
    authority: Mapping[str, Any],
) -> tuple[bytes, dict[str, Any], dict[str, Any]]:
    raw, body = _read_owner_json(path, "rca_activation_gate_migration_receipt")
    host_root = Path(authority["faces"]["host_runtime"]["root"])
    runtime_path = host_root / "gateway" / "pnc_rca_delivery_quarantine_migration.py"
    expected_runtime_sha = file_sha256(runtime_path)
    source_projection = body.get("source_logical_projection")
    if not isinstance(source_projection, Mapping):
        raise ActivationGateError("rca_activation_gate_migration_receipt_invalid")
    raw_sha = hashlib.sha256(raw).hexdigest()
    try:
        binding = validate_combined_migration_receipt(
            receipt_path=path,
            expected_sha256=raw_sha,
            target_live_db_path=target_live_db_path,
            expected_migration_runtime_sha256=expected_runtime_sha,
            expected_source_schema_sha256=str(
                source_projection.get("schema_sha256") or ""
            ),
        )
    except QuarantineMigrationError as exc:
        raise ActivationGateError(exc.code) from exc
    conditional = body.get("cross_projection_preservation")
    if not isinstance(conditional, Mapping):
        raise ActivationGateError("rca_activation_gate_migration_receipt_invalid")
    source_owned = conditional.get("source_owned_schema")
    if (
        not isinstance(source_owned, Mapping)
        or not isinstance(source_owned.get("conditional_schema_shape"), Mapping)
    ):
        raise ActivationGateError(
            "rca_activation_gate_conditional_schema_evidence_missing"
        )
    return raw, body, binding


def _validate_baseline(
    path: Path,
    authority: Mapping[str, Any],
    *,
    control_db_path: Path,
    config: Mapping[str, Any],
) -> tuple[bytes, dict[str, Any], dict[str, Any]]:
    raw, body = _read_owner_json(path, "rca_activation_gate_quarantine_baseline")
    expected = authority["quarantine_baseline"]
    if (
        expected.get("state") != "ready"
        or expected.get("required") is not True
        or body.get("schema_version") != expected.get("schema_version")
        or hashlib.sha256(raw).hexdigest() != expected.get("baseline_sha256")
    ):
        raise ActivationGateError("rca_activation_gate_quarantine_baseline_mismatch")
    collector = config.get("delivery_collector")
    if not isinstance(collector, Mapping):
        raise ActivationGateError("rca_activation_gate_config_invalid")
    status = read_quarantine_baseline_status(
        control_db_path,
        baseline_path=path,
        expected_sha256=expected["baseline_sha256"],
        expected_release_id=str(authority["release_id"]),
        bootstrap_epoch_id=str(collector.get("quarantine_bootstrap_epoch_id") or ""),
        active_release_binding_path=str(
            collector.get("quarantine_active_release_binding_path") or ""
        ),
        live_env_path=str(collector.get("quarantine_live_env_path") or ""),
    )
    if status.get("ready") is not True or status.get("state") != "acknowledged":
        raise ActivationGateError(
            str(status.get("error_code") or "rca_activation_gate_quarantine_baseline_not_ready")
        )
    return raw, body, status


def _validate_canary_plan(path: Path) -> tuple[bytes, dict[str, Any]]:
    raw, body = _read_owner_json(path, "rca_activation_gate_canary_plan")
    try:
        normalized = capsules._normalize_canary_slot_plan(body)
    except capsules.CapsuleError as exc:
        raise ActivationGateError(exc.code) from exc
    return raw, normalized


def collect_broker_t0(
    consumer: ConsumerConfig,
    *,
    expected_cluster_id: str,
    now: datetime | None = None,
    consumer_factory: Callable[..., Any] | None = None,
    topic_partition_factory: Callable[[str, int], Any] | None = None,
) -> dict[str, Any]:
    """Collect ListOffsets evidence without joining or polling a consumer group."""
    if consumer_factory is None or topic_partition_factory is None:
        try:
            kafka = __import__("kafka")
            structs = __import__("kafka.structs", fromlist=["TopicPartition"])
        except ImportError as exc:
            raise ActivationGateError(
                "rca_activation_gate_kafka_dependency_unavailable"
            ) from exc
        consumer_factory = consumer_factory or kafka.KafkaConsumer
        topic_partition_factory = topic_partition_factory or structs.TopicPartition
    kwargs = consumer.kafka_kwargs()
    kwargs.update({
        "group_id": None,
        "enable_auto_commit": False,
        "client_id": "root_cause_analysis_agent_activation_gate",
    })
    client = None
    try:
        client = consumer_factory(**kwargs)
        partitions = client.partitions_for_topic(consumer.topic)
        if not isinstance(partitions, (set, frozenset)) or not partitions:
            raise ActivationGateError("rca_activation_gate_kafka_topic_unavailable")
        partition_ids = sorted(partitions)
        if partition_ids != list(range(len(partition_ids))):
            raise ActivationGateError("rca_activation_gate_kafka_partitions_invalid")
        topic_partitions = [
            topic_partition_factory(consumer.topic, partition)
            for partition in partition_ids
        ]
        timeout_ms = consumer.offset_lookup_timeout_ms
        beginnings = client.beginning_offsets(
            topic_partitions, timeout_ms=timeout_ms
        )
        ends = client.end_offsets(topic_partitions, timeout_ms=timeout_ms)
        cluster = getattr(getattr(client, "_client", None), "cluster", None)
        cluster_id = getattr(cluster, "cluster_id", None)
        if cluster_id != expected_cluster_id:
            raise ActivationGateError("rca_activation_gate_kafka_cluster_mismatch")
        offsets: dict[str, dict[str, int]] = {}
        for partition, topic_partition in zip(
            partition_ids, topic_partitions, strict=True
        ):
            beginning = beginnings.get(topic_partition)
            end = ends.get(topic_partition)
            if (
                isinstance(beginning, bool)
                or not isinstance(beginning, int)
                or isinstance(end, bool)
                or not isinstance(end, int)
                or beginning < 0
                or end < beginning
            ):
                raise ActivationGateError("rca_activation_gate_kafka_offsets_invalid")
            offsets[str(partition)] = {"beginning": beginning, "end": end}
    except ActivationGateError:
        raise
    except Exception as exc:
        raise ActivationGateError(
            "rca_activation_gate_kafka_observation_failed", type(exc).__name__
        ) from exc
    finally:
        if client is not None:
            try:
                client.close()
            except Exception:
                pass
    observed_at = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    return {
        "schema_version": BROKER_SCHEMA_VERSION,
        "observed_at": observed_at.isoformat(),
        "cluster_id": expected_cluster_id,
        "topic": consumer.topic,
        "partition_offsets": offsets,
        "connection": {
            "group_id": None,
            "enable_auto_commit": False,
            "allow_auto_create_topics": False,
            "subscribe_called": False,
            "assign_called": False,
            "poll_called": False,
            "commit_called": False,
            "isolation_level": consumer.isolation_level,
        },
        "read_only_attestation": {
            "apis": ["Metadata", "ListOffsets"],
            "records_consumed": 0,
            "offsets_committed": 0,
            "external_effects_triggered": False,
        },
    }


_GATEWAY_CHILD_PROBE = """
import json
from gateway.pnc_rca_policy_config import manual_rca_admission_runtime_config_from_env
from gateway.pnc_rca_runtime_identity import canonical_json_sha256, gateway_loaded_runtime_sha256
from gateway.run import _g1q3_manual_runtime_public_config
config = _g1q3_manual_runtime_public_config(manual_rca_admission_runtime_config_from_env())
print(json.dumps({
    "public_config_sha256": canonical_json_sha256(config),
    "loaded_runtime_sha256": gateway_loaded_runtime_sha256(),
}, sort_keys=True))
"""


def _gateway_child_probe(
    *,
    root: Path,
    process_environment: Mapping[str, str],
    dotenv_environment: Mapping[str, str],
) -> dict[str, str]:
    virtual_env = Path(str(process_environment.get("VIRTUAL_ENV") or ""))
    python = virtual_env / "bin" / "python"
    if not virtual_env.is_absolute() or not python.is_file():
        raise ActivationGateError("rca_activation_gate_gateway_venv_invalid")
    environment = dict(process_environment)
    environment.update(dotenv_environment)
    environment["PYTHONPATH"] = str(root)
    try:
        completed = subprocess.run(
            [str(python), "-c", _GATEWAY_CHILD_PROBE],
            cwd=root,
            env=environment,
            text=True,
            capture_output=True,
            timeout=30,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise ActivationGateError("rca_activation_gate_gateway_probe_failed") from exc
    if (
        completed.returncode != 0
        or len(completed.stdout.encode("utf-8")) > MAX_SUBPROCESS_BYTES
        or len(completed.stderr.encode("utf-8")) > MAX_SUBPROCESS_BYTES
    ):
        raise ActivationGateError("rca_activation_gate_gateway_probe_failed")
    lines = [line for line in completed.stdout.splitlines() if line.strip()]
    try:
        value = json.loads(lines[-1]) if lines else None
    except json.JSONDecodeError as exc:
        raise ActivationGateError("rca_activation_gate_gateway_probe_failed") from exc
    if not isinstance(value, dict) or set(value) != {
        "public_config_sha256",
        "loaded_runtime_sha256",
    }:
        raise ActivationGateError("rca_activation_gate_gateway_probe_failed")
    return {
        "public_config_sha256": _digest(
            value["public_config_sha256"],
            "rca_activation_gate_gateway_probe_failed",
        ),
        "loaded_runtime_sha256": _digest(
            value["loaded_runtime_sha256"],
            "rca_activation_gate_gateway_probe_failed",
        ),
    }


def collect_gateway_binding(
    *,
    live_manifest: Mapping[str, Any],
    live_manifest_raw_sha256: str,
    live_env: Mapping[str, str],
    candidate_env: Mapping[str, str],
    process_factory: Callable[[int], Any] = psutil.Process,
    launchd_pid_reader: Callable[[str], int] = capsules._live_launchd_pid,
    child_probe: Callable[..., Mapping[str, str]] = _gateway_child_probe,
) -> dict[str, Any]:
    pid = launchd_pid_reader(GATEWAY_SERVICE_LABEL)
    try:
        process = process_factory(pid)
        if not process.is_running() or process.status() in {
            psutil.STATUS_DEAD,
            psutil.STATUS_ZOMBIE,
        }:
            raise ActivationGateError("rca_activation_gate_gateway_not_running")
        root = Path(process.cwd()).expanduser().absolute()
        executable = Path(process.exe()).expanduser().resolve(strict=True)
        process_environment = process.environ()
        create_time = float(process.create_time())
        cmdline = [str(item) for item in process.cmdline()]
    except ActivationGateError:
        raise
    except (OSError, ValueError, psutil.Error) as exc:
        raise ActivationGateError("rca_activation_gate_gateway_not_running") from exc
    if (
        live_manifest.get("runtime_root") != str(root)
        or "gateway" not in "\x00".join(cmdline)
        or "run" not in "\x00".join(cmdline)
    ):
        raise ActivationGateError("rca_activation_gate_gateway_manifest_mismatch")
    script = root / "gateway" / "run.py"
    live_probe = child_probe(
        root=root,
        process_environment=process_environment,
        dotenv_environment=live_env,
    )
    candidate_probe = child_probe(
        root=root,
        process_environment=process_environment,
        dotenv_environment=candidate_env,
    )
    if live_probe["public_config_sha256"] != candidate_probe["public_config_sha256"]:
        raise ActivationGateError("rca_activation_gate_gateway_config_drift")
    _hashes, runtime_sha = runtime_file_snapshot(
        root, GATEWAY_RCA_RUNTIME_RELATIVE_FILES
    )
    identity = {
        "service_label": GATEWAY_SERVICE_LABEL,
        "pid": pid,
        "process_create_time": create_time,
        "boot_time": float(psutil.boot_time()),
        "executable": str(executable),
        "script": str(script),
        "cwd": str(root),
        "script_sha256": file_sha256(script),
        "runtime_files_sha256": runtime_sha,
        "public_config_sha256": live_probe["public_config_sha256"],
        "loaded_runtime_sha256": live_probe["loaded_runtime_sha256"],
    }
    if not runtime_identity_is_valid(identity, service_label=GATEWAY_SERVICE_LABEL):
        raise ActivationGateError("rca_activation_gate_gateway_identity_invalid")
    verification = {
        "runtime_identity_sha256": canonical_json_sha256(identity),
        "live_manifest_raw_sha256": live_manifest_raw_sha256,
        "cmdline_sha256": canonical_json_sha256(cmdline),
    }
    return {
        "state": "running_safe",
        "pid": pid,
        "process_create_time": create_time,
        "runtime_identity": identity,
        "runtime_identity_sha256": verification["runtime_identity_sha256"],
        "verified_runtime_sha256": canonical_json_sha256(verification),
    }


def _activation_observation(
    control_db_path: Path,
    *,
    mode: str,
    epoch_id: str,
    activation_input: Mapping[str, Any] | None,
) -> dict[str, Any]:
    connection: sqlite3.Connection | None = None
    try:
        connection = sqlite3.connect(
            control_db_path.as_uri() + "?mode=ro", uri=True, timeout=10
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only=ON")
        rows = connection.execute(
            "SELECT epoch_id, state, is_current, config_sha256, "
            "db_logical_identity_sha256, partition_start_fence_sha256 "
            "FROM rca_activation_epochs ORDER BY created_at"
        ).fetchall()
    except sqlite3.Error as exc:
        raise ActivationGateError("rca_activation_gate_epoch_unavailable") from exc
    finally:
        if connection is not None:
            connection.close()
    current = [row for row in rows if int(row["is_current"]) == 1]
    if mode == "preauthorization":
        if rows or current:
            raise ActivationGateError("rca_activation_gate_epoch_not_absent")
        return {"state": "absent", "epoch_count": 0, "current_epoch_count": 0}
    if len(current) != 1 or activation_input is None:
        raise ActivationGateError("rca_activation_gate_epoch_not_safe_off")
    row = current[0]
    if (
        row["epoch_id"] != epoch_id
        or row["state"] != "safe_off"
        or row["config_sha256"] != activation_input["config_sha256"]
        or row["db_logical_identity_sha256"]
        != activation_input["db_logical_identity_sha256"]
        or row["partition_start_fence_sha256"]
        != activation_input["partition_start_fence_sha256"]
    ):
        raise ActivationGateError("rca_activation_gate_epoch_not_safe_off")
    return {
        "state": "safe_off",
        "epoch_id": row["epoch_id"],
        "epoch_count": len(rows),
        "current_epoch_count": 1,
        "config_sha256": row["config_sha256"],
        "db_logical_identity_sha256": row["db_logical_identity_sha256"],
        "partition_start_fence_sha256": row["partition_start_fence_sha256"],
    }


def _partition_fence(broker: Mapping[str, Any]) -> dict[str, dict[str, int]]:
    topic = str(broker.get("topic") or "")
    offsets = broker.get("partition_offsets")
    if not topic or not isinstance(offsets, Mapping) or not offsets:
        raise ActivationGateError("rca_activation_gate_kafka_offsets_invalid")
    return {
        topic: {
            str(partition): int(value["end"])
            for partition, value in sorted(offsets.items(), key=lambda item: int(item[0]))
            if isinstance(value, Mapping)
        }
    }


def _database_identity(
    *,
    schema_receipt: Mapping[str, Any],
    control_db_path: Path,
    host_commit: str,
    config_sha256: str,
    migration_receipt_raw_sha256: str,
) -> dict[str, Any]:
    if _COMMIT_RE.fullmatch(host_commit) is None:
        raise ActivationGateError("rca_activation_gate_host_commit_invalid")
    observed = _owner_regular(control_db_path, exact_mode=None)
    identity = schema_receipt["database_identity"]
    instance_sha = hashlib.sha256(
        str(identity["database_instance_id"]).encode("utf-8")
    ).hexdigest()
    database = {
        "path": str(control_db_path),
        "device": observed.st_dev,
        "inode": observed.st_ino,
        "schema_version": identity["control_schema_version"],
        "db_instance_id": instance_sha,
        "genesis_intent_sha256": identity["genesis_intent_sha256"],
    }
    delivery = {
        **database,
        "schema_version": identity["delivery_schema_version"],
    }
    return {
        "schema_version": "pnc_rca_activation_db_identity_v1",
        "strategy": "existing_database_preserve",
        "databases": {"control": database, "delivery": delivery},
        "migration_receipt_raw_sha256": migration_receipt_raw_sha256,
        "materialization_receipt_raw_sha256": hashlib.sha256(b"").hexdigest(),
        "host_commit": host_commit,
        "config_sha256": config_sha256,
    }


def _check(name: str, detail: Mapping[str, Any]) -> dict[str, Any]:
    return {"name": name, "ok": True, "code": "pass", "detail": dict(detail)}


def produce_release_gate(
    *,
    mode: str,
    epoch_id: str,
    env_path: Path,
    live_env_path: Path,
    authority_path: Path,
    schema_receipt_path: Path,
    migration_receipt_path: Path,
    baseline_path: Path,
    canary_plan_path: Path,
    live_manifest_path: Path,
    broker_receipt_path: Path,
    receipt_path: Path,
    evidence_dir: Path,
    preauthorization_capsule_path: Path | None = None,
    vm_observation_path: Path | None = None,
    now: datetime | None = None,
    broker_collector: Callable[..., Mapping[str, Any]] = collect_broker_t0,
    gateway_collector: Callable[..., Mapping[str, Any]] = collect_gateway_binding,
) -> dict[str, Any]:
    if mode not in {"preauthorization", "preproduction"}:
        raise ActivationGateError("rca_activation_gate_mode_invalid")
    if _EPOCH_RE.fullmatch(epoch_id) is None:
        raise ActivationGateError("rca_activation_gate_epoch_id_invalid")
    current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    max_age = DEFAULT_EVIDENCE_MAX_AGE_SECONDS
    try:
        evidence = capsules._secure_evidence_directory(str(evidence_dir))
    except capsules.CapsuleError as exc:
        raise ActivationGateError(exc.code) from exc
    broker_output = _absolute(
        broker_receipt_path, "rca_activation_gate_broker_output_invalid"
    )
    gate_output = _absolute(receipt_path, "rca_activation_gate_output_invalid")
    if broker_output.parent != evidence or gate_output.parent != evidence:
        raise ActivationGateError("rca_activation_gate_output_outside_evidence")

    authority_raw, authority, authority_digest = _load_authority(authority_path)
    host_face = _verify_host_face(authority)
    env_raw, env = _load_environment(env_path)
    live_env_raw, live_env = _load_environment(live_env_path)
    live_home = str(live_env_path.expanduser().absolute().parent)
    live_env.setdefault("HERMES_HOME", live_home)
    env.setdefault("HERMES_HOME", live_env["HERMES_HOME"])
    if env["HERMES_HOME"] != live_env["HERMES_HOME"]:
        raise ActivationGateError("rca_activation_gate_hermes_home_mismatch")
    config = _public_config(env, authority)
    safe_config = _validate_safe_config(config, env, authority)
    config_sha = capsules._sha256_json(config)
    control_db_path = Path(safe_config["control_db_path"]).expanduser().absolute()
    schema_raw, schema_receipt, schema_verified = _validate_schema_receipt(
        schema_receipt_path,
        control_db_path=control_db_path,
        authority=authority,
        now=current,
        max_age_seconds=max_age,
    )
    migration_raw, migration_receipt, migration_binding = (
        _validate_migration_receipt(
            migration_receipt_path,
            target_live_db_path=control_db_path,
            authority=authority,
        )
    )
    baseline_raw, baseline, baseline_status = _validate_baseline(
        baseline_path,
        authority,
        control_db_path=control_db_path,
        config=config,
    )
    canary_raw, canary_plan = _validate_canary_plan(canary_plan_path)
    live_manifest_raw, live_manifest = _read_owner_json(
        live_manifest_path, "rca_activation_gate_live_manifest"
    )

    try:
        consumer = ConsumerConfig.from_env(
            env, hermes_home=Path(env["HERMES_HOME"])
        )
    except (TypeError, ValueError) as exc:
        raise ActivationGateError(
            "rca_activation_gate_config_invalid", type(exc).__name__
        ) from exc
    expected_cluster_id = str(env.get("HERMES_RCA_KAFKA_EXPECTED_CLUSTER_ID") or "")
    if not expected_cluster_id:
        raise ActivationGateError("rca_activation_gate_expected_cluster_missing")
    broker = dict(
        broker_collector(
            consumer,
            expected_cluster_id=expected_cluster_id,
            now=current,
        )
    )
    _fresh(
        broker.get("observed_at"),
        now=current,
        max_age_seconds=max_age,
        code="rca_activation_gate_broker_observation_stale",
    )
    broker_raw = _write_owner_json(broker_output, broker)
    broker_raw_sha = hashlib.sha256(broker_raw).hexdigest()
    gateway = dict(
        gateway_collector(
            live_manifest=live_manifest,
            live_manifest_raw_sha256=hashlib.sha256(live_manifest_raw).hexdigest(),
            live_env=live_env,
            candidate_env=env,
        )
    )
    try:
        capsules._normalize_gateway_binding(gateway)
    except capsules.CapsuleError as exc:
        raise ActivationGateError(exc.code) from exc

    preauthorization_capsule: str | None = None
    if mode == "preauthorization":
        fence = _partition_fence(broker)
        migration_sha = hashlib.sha256(migration_raw).hexdigest()
        db_identity = _database_identity(
            schema_receipt=schema_receipt,
            control_db_path=control_db_path,
            host_commit=authority["faces"]["host_runtime"]["commit"],
            config_sha256=config_sha,
            migration_receipt_raw_sha256=migration_sha,
        )
        activation_input = {
            "epoch_id": epoch_id,
            "initial_state": "safe_off",
            "config_sha256": config_sha,
            "db_logical_identity": db_identity,
            "db_logical_identity_sha256": capsules._sha256_json(db_identity),
            "partition_start_fence": fence,
            "partition_start_fence_sha256": capsules._sha256_json(fence),
            "migration_receipt_raw_sha256": migration_sha,
            "materialization_receipt_raw_sha256": hashlib.sha256(b"").hexdigest(),
            "broker_t0_observation_sha256": broker_raw_sha,
            "canary_plan_raw_sha256": hashlib.sha256(canary_raw).hexdigest(),
        }
    else:
        if preauthorization_capsule_path is None:
            raise ActivationGateError(
                "rca_activation_gate_preauthorization_capsule_required"
            )
        try:
            prior = capsules.read_preauthorization_capsule(
                preauthorization_capsule_path,
                control_db_path=control_db_path,
            )
        except capsules.CapsuleError as exc:
            raise ActivationGateError(exc.code) from exc
        activation_input = {
            key: prior[key] for key in _PREAUTHORIZATION_INPUT_FIELDS
        }
        preauthorization_capsule = str(preauthorization_capsule_path.absolute())
        current_fence = _partition_fence(broker)
        start_fence = activation_input["partition_start_fence"]
        if set(current_fence) != set(start_fence) or any(
            set(current_fence[topic]) != set(start_fence[topic])
            or any(
                current_fence[topic][partition] < start_fence[topic][partition]
                for partition in start_fence[topic]
            )
            for topic in start_fence
        ):
            raise ActivationGateError("rca_activation_gate_broker_fence_regressed")
        if (
            activation_input["config_sha256"] != config_sha
            or activation_input["migration_receipt_raw_sha256"]
            != hashlib.sha256(migration_raw).hexdigest()
            or activation_input["canary_plan_raw_sha256"]
            != hashlib.sha256(canary_raw).hexdigest()
        ):
            raise ActivationGateError(
                "rca_activation_gate_preauthorization_input_drift"
            )

    activation = _activation_observation(
        control_db_path,
        mode=mode,
        epoch_id=epoch_id,
        activation_input=activation_input,
    )
    vm_detail: dict[str, Any] = {"required": False, "state": "not_applicable"}
    vm_raw = b""
    if mode == "preproduction":
        if vm_observation_path is None:
            raise ActivationGateError("rca_activation_gate_vm_observation_required")
        vm_raw, vm = _read_owner_json(
            vm_observation_path, "rca_activation_gate_vm_observation"
        )
        vm_detail = _validate_vm_observation(
            vm,
            authority=authority,
            authority_digest=authority_digest,
            now=current,
            max_age_seconds=max_age,
        )

    contract = {
        "schema_version": CONTRACT_SCHEMA_VERSION,
        "release_id": authority["release_id"],
        "authority_epoch_id": authority["authority_epoch_id"],
        "authority_sha256": authority_digest,
        "host_face": host_face,
        "config_sha256": config_sha,
        "schema_fingerprint_sha256": schema_receipt[
            "schema_fingerprint_sha256"
        ],
        "schema_receipt_raw_sha256": hashlib.sha256(schema_raw).hexdigest(),
        "migration_receipt_raw_sha256": hashlib.sha256(migration_raw).hexdigest(),
        "quarantine_baseline_raw_sha256": hashlib.sha256(baseline_raw).hexdigest(),
        "canary_plan_raw_sha256": hashlib.sha256(canary_raw).hexdigest(),
        "gateway_runtime_identity_sha256": gateway["runtime_identity_sha256"],
        "broker_observation_raw_sha256": broker_raw_sha,
        "vm_observation_raw_sha256": (
            hashlib.sha256(vm_raw).hexdigest() if vm_raw else None
        ),
    }
    material: dict[str, Any] = {
        "schema_version": (
            capsules.PREAUTHORIZATION_MATERIAL_SCHEMA_VERSION
            if mode == "preauthorization"
            else capsules.PREPRODUCTION_MATERIAL_SCHEMA_VERSION
        ),
        "evidence_directory": str(evidence),
        "activation_input": activation_input,
        "gateway_binding": gateway,
    }
    if mode == "preproduction":
        material.update({
            "preauthorization_capsule": preauthorization_capsule,
            "canary_slot_plan": canary_plan,
        })
    checks = [
        _check("contract_drift", contract),
        _check("release_authority", {
            "release_id": authority["release_id"],
            "authority_sha256": authority_digest,
            "status": authority["status"],
        }),
        _check("safe_side_effect_config", safe_config),
        _check("schema_fingerprint", {
            **schema_verified,
            "receipt_raw_sha256": hashlib.sha256(schema_raw).hexdigest(),
        }),
        _check("combined_migration", {
            "source_schema_version": migration_receipt["source_schema_version"],
            "target_schema_version": migration_receipt["target_schema_version"],
            "migration_runtime_sha256": migration_binding[
                "migration_runtime_sha256"
            ],
            "conditional_schema_shape": migration_receipt[
                "cross_projection_preservation"
            ]["source_owned_schema"]["conditional_schema_shape"],
        }),
        _check("quarantine_baseline", {
            "schema_version": baseline["schema_version"],
            "raw_sha256": hashlib.sha256(baseline_raw).hexdigest(),
            "state": baseline_status["state"],
            "ready": baseline_status["ready"],
        }),
        _check("broker_t0", {
            "schema_version": broker["schema_version"],
            "observed_at": broker["observed_at"],
            "cluster_id": broker["cluster_id"],
            "partition_start_fence": activation_input["partition_start_fence"],
            "current_partition_fence": _partition_fence(broker),
            "read_only_attestation": broker["read_only_attestation"],
        }),
        _check("gateway_runtime", {
            "pid": gateway["pid"],
            "process_create_time": gateway["process_create_time"],
            "runtime_identity_sha256": gateway["runtime_identity_sha256"],
            "verified_runtime_sha256": gateway["verified_runtime_sha256"],
        }),
        _check("vm_release", vm_detail),
        _check("activation_epoch", activation),
        _check("activation_capsule_material", material),
    ]
    evidence_sha256 = {
        "authority": hashlib.sha256(authority_raw).hexdigest(),
        "candidate_env": hashlib.sha256(env_raw).hexdigest(),
        "live_env": hashlib.sha256(live_env_raw).hexdigest(),
        "schema_receipt": hashlib.sha256(schema_raw).hexdigest(),
        "migration_receipt": hashlib.sha256(migration_raw).hexdigest(),
        "quarantine_baseline": hashlib.sha256(baseline_raw).hexdigest(),
        "canary_plan": hashlib.sha256(canary_raw).hexdigest(),
        "live_manifest": hashlib.sha256(live_manifest_raw).hexdigest(),
        "broker_observation": broker_raw_sha,
    }
    if vm_raw:
        evidence_sha256["vm_observation"] = hashlib.sha256(vm_raw).hexdigest()
    report = {
        "schema_version": capsules.RELEASE_GATE_SCHEMA_VERSION,
        "evaluated_at": current.isoformat(),
        "mode": mode,
        "ok": True,
        "fingerprint": "0" * 64,
        "config": config,
        "gate_policy": {"evidence_max_age_seconds": max_age},
        "checks": checks,
        "blockers": [],
        "warnings": [],
        "evidence_sha256": dict(sorted(evidence_sha256.items())),
    }
    try:
        report["fingerprint"] = capsules.release_report_fingerprint(report)
    except capsules.CapsuleError as exc:
        raise ActivationGateError(exc.code) from exc
    gate_raw = _write_owner_json(gate_output, report)
    return {
        "schema_version": CLI_SCHEMA_VERSION,
        "ok": True,
        "mode": mode,
        "release_id": authority["release_id"],
        "authority_sha256": authority_digest,
        "epoch_id": epoch_id,
        "report_path": str(gate_output),
        "report_raw_sha256": hashlib.sha256(gate_raw).hexdigest(),
        "report_fingerprint": report["fingerprint"],
        "broker_receipt_path": str(broker_output),
        "broker_receipt_raw_sha256": broker_raw_sha,
        "source_mutation_performed": False,
        "external_effects_triggered": False,
    }


def _validate_vm_observation(
    value: Mapping[str, Any],
    *,
    authority: Mapping[str, Any],
    authority_digest: str,
    now: datetime,
    max_age_seconds: int,
) -> dict[str, Any]:
    expected_fields = {
        "schema_version",
        "observed_at",
        "release_id",
        "authority_sha256",
        "faces",
        "capacity",
        "read_only_attestation",
    }
    if (
        set(value) != expected_fields
        or value.get("schema_version") != VM_OBSERVATION_SCHEMA_VERSION
        or value.get("release_id") != authority["release_id"]
        or value.get("authority_sha256") != authority_digest
    ):
        raise ActivationGateError("rca_activation_gate_vm_observation_invalid")
    _fresh(
        value.get("observed_at"),
        now=now,
        max_age_seconds=max_age_seconds,
        code="rca_activation_gate_vm_observation_stale",
    )
    faces = value.get("faces")
    if not isinstance(faces, Mapping) or set(faces) != {
        "vm_worker_state",
        "g1q3_rca_pipeline",
        "mcap_data_translate",
    }:
        raise ActivationGateError("rca_activation_gate_vm_observation_invalid")
    for name in faces:
        observed = faces[name]
        expected = authority["faces"][name]
        if (
            not isinstance(observed, Mapping)
            or observed.get("commit") != expected["commit"]
            or observed.get("tree") != expected["tree"]
            or observed.get("root") != expected["root"]
            or observed.get("dirty") is not False
            or (
                name == "mcap_data_translate"
                and observed.get("contract_sha256")
                != expected["contract_sha256"]
            )
        ):
            raise ActivationGateError("rca_activation_gate_vm_face_mismatch")
    capacity = value.get("capacity")
    attestation = value.get("read_only_attestation")
    if (
        not isinstance(capacity, Mapping)
        or capacity.get("resource_class") != "rca_prod"
        or capacity.get("capacity_mode") != "bootstrap"
        or capacity.get("rca_prod_ok") is not True
        or capacity.get("max_concurrency") != 1
        or capacity.get("queue_allowed") is not False
        or capacity.get("input_materialization") != "forbidden"
        or _SHA256_RE.fullmatch(
            str(capacity.get("bootstrap_authorization_sha256") or "")
        )
        is None
        or attestation
        != {
            "remote_mutation_performed": False,
            "external_effects_triggered": False,
        }
    ):
        raise ActivationGateError("rca_activation_gate_vm_capacity_invalid")
    return {
        "required": True,
        "state": "installed_verified_dormant",
        "observed_at": value["observed_at"],
        "faces": {name: dict(faces[name]) for name in sorted(faces)},
        "capacity": dict(capacity),
    }


class _SafeParser(argparse.ArgumentParser):
    def error(self, _message: str) -> None:
        raise ActivationGateError("rca_activation_gate_cli_arguments_invalid")


def _arguments(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = _SafeParser(description=__doc__)
    parser.add_argument("--mode", choices=("preauthorization", "preproduction"), required=True)
    parser.add_argument("--epoch-id", required=True)
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--live-env-file", type=Path, required=True)
    parser.add_argument("--authority", type=Path, required=True)
    parser.add_argument("--schema-receipt", type=Path, required=True)
    parser.add_argument("--migration-receipt", type=Path, required=True)
    parser.add_argument("--quarantine-baseline", type=Path, required=True)
    parser.add_argument("--canary-plan", type=Path, required=True)
    parser.add_argument("--live-manifest", type=Path, required=True)
    parser.add_argument("--broker-receipt", type=Path, required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--evidence-dir", type=Path, required=True)
    parser.add_argument("--preauthorization-capsule", type=Path)
    parser.add_argument("--vm-observation", type=Path)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    mode = "unknown"
    try:
        args = _arguments(argv)
        mode = str(args.mode)
        result = produce_release_gate(
            mode=mode,
            epoch_id=args.epoch_id,
            env_path=args.env_file,
            live_env_path=args.live_env_file,
            authority_path=args.authority,
            schema_receipt_path=args.schema_receipt,
            migration_receipt_path=args.migration_receipt,
            baseline_path=args.quarantine_baseline,
            canary_plan_path=args.canary_plan,
            live_manifest_path=args.live_manifest,
            broker_receipt_path=args.broker_receipt,
            receipt_path=args.receipt,
            evidence_dir=args.evidence_dir,
            preauthorization_capsule_path=args.preauthorization_capsule,
            vm_observation_path=args.vm_observation,
        )
        print(json.dumps(result, ensure_ascii=True, sort_keys=True))
        return 0
    except ActivationGateError as exc:
        print(
            json.dumps(
                {
                    "schema_version": CLI_SCHEMA_VERSION,
                    "ok": False,
                    "mode": mode,
                    "code": exc.code,
                    "detail": exc.detail,
                    "source_mutation_performed": False,
                    "external_effects_triggered": False,
                },
                ensure_ascii=True,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
