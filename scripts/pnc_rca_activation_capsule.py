#!/usr/bin/env python3
"""Build and validate the small immutable capsule set used by RCA activation."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import psutil
import re
import sqlite3
import stat
import subprocess
import sys
import tempfile
from typing import Any, Mapping, Sequence

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from gateway.pnc_rca_control_store import (
    ACTIVATION_KAFKA_PROOF_MODE,
    ACTIVATION_RELEASE_SLOT_KINDS,
    CONTROL_STORE_SCHEMA_VERSION,
    RcaControlStore,
)
from gateway.pnc_rca_delivery_store import DELIVERY_STORE_SCHEMA_VERSION
from gateway.pnc_rca_runtime_identity import (
    GATEWAY_RCA_RUNTIME_RELATIVE_FILES,
    MAX_HEALTH_FUTURE_SKEW_SECONDS,
    file_sha256,
    runtime_file_snapshot,
    runtime_identity_is_valid,
)


RELEASE_GATE_SCHEMA_VERSION = "pnc_rca_release_gate_v1"
PREAUTHORIZATION_CAPSULE_SCHEMA_VERSION = (
    "pnc_rca_activation_preauthorization_capsule_v2"
)
PREPRODUCTION_CAPSULE_SCHEMA_VERSION = "pnc_rca_activation_preproduction_capsule_v2"
CONFIRMATION_CAPSULE_SCHEMA_VERSION = "pnc_rca_activation_confirmation_capsule_v2"
STAGE_PAIR_COMMIT_SCHEMA_VERSION = "pnc_rca_activation_stage_pair_commit_v1"
CONFIRMATION_PAIR_COMMIT_SCHEMA_VERSION = (
    "pnc_rca_activation_confirmation_pair_commit_v1"
)
PREAUTHORIZATION_MATERIAL_SCHEMA_VERSION = (
    "pnc_rca_activation_preauthorization_material_v2"
)
PREPRODUCTION_MATERIAL_SCHEMA_VERSION = "pnc_rca_activation_preproduction_material_v2"
CAPSULE_CLI_SCHEMA_VERSION = "pnc_rca_activation_capsule_cli_v1"
MAX_CAPSULE_BYTES = 256 * 1024
MAX_RECEIPT_BYTES = 4 * 1024 * 1024
MAX_GATE_EVIDENCE_AGE_SECONDS = 900
MAX_STAGE_CAPSULE_AGE_SECONDS = 3600
LIVE_HEALTH_MAX_AGE_SECONDS = 60
GATEWAY_SERVICE_LABEL = "ai.hermes.gateway"
CONSUMER_SERVICE_LABEL = "local.pnc.rca-kafka-consumer"
CONSUMER_HEALTH_SCHEMA_VERSION = "pnc_rca_kafka_consumer_health_v2"
ACTIVATION_FREEZE_SCHEMA_VERSION = "pnc_rca_activation_ingress_freeze_v2"
_RESIDENT_LABELS = {
    "kafka_consumer_health": "local.pnc.rca-kafka-consumer",
    "outbox_dispatcher_health": "local.pnc.rca-outbox-dispatcher",
    "delivery_collector_health": "local.pnc.rca-delivery-collector",
    "delivery_dispatcher_health": "local.pnc.rca-delivery-dispatcher",
}
_RESIDENT_HEALTH_SPECS = {
    "kafka_consumer_health": (
        CONSUMER_HEALTH_SCHEMA_VERSION,
        "heartbeat_at",
    ),
    "outbox_dispatcher_health": (
        "pnc_rca_outbox_dispatcher_health_v2",
        "heartbeat_at",
    ),
    "delivery_collector_health": (
        "pnc_rca_delivery_collector_health_v2",
        "updated_at",
    ),
    "delivery_dispatcher_health": (
        "pnc_rca_delivery_dispatcher_health_v2",
        "updated_at",
    ),
}


def _process_create_time_matches(observed: Any, expected: Any) -> bool:
    try:
        return math.isclose(float(observed), float(expected), rel_tol=0.0, abs_tol=0.05)
    except (TypeError, ValueError, OverflowError):
        return False


def _unsafe_process_environment(
    environment: Mapping[str, Any],
    *,
    expected_root: str | Path | None = None,
) -> bool:
    """Reject loader overrides while allowing the release's own Python path."""
    allowed_python = {
        "PYTHONDONTWRITEBYTECODE",
        "PYTHONNOUSERSITE",
        "PYTHONUNBUFFERED",
    }
    normalized_root = None
    if expected_root is not None:
        normalized_root = str(Path(expected_root).expanduser().absolute())
    for raw_key, raw_value in environment.items():
        key = str(raw_key)
        if key.startswith(("DYLD_", "LD_")):
            return True
        if key == "PYTHONPATH":
            if normalized_root is None or str(raw_value) != normalized_root:
                return True
            continue
        if key.startswith("PYTHON") and key not in allowed_python:
            return True
    return False


_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
_EPOCH_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_EVENT_UID_RE = re.compile(
    r"^(?P<topic>[A-Za-z0-9][A-Za-z0-9._-]{0,248}):"
    r"(?P<partition>[0-9]+):(?P<offset>[0-9]+)$"
)
_ISSUE_URL_RE = re.compile(
    r"^https://project\.feishu\.cn/"
    r"[A-Za-z0-9._-]+/issue/detail/[0-9]+/*$"
)
_REPORT_FIELDS = {
    "schema_version",
    "evaluated_at",
    "mode",
    "ok",
    "fingerprint",
    "config",
    "gate_policy",
    "checks",
    "blockers",
    "warnings",
    "evidence_sha256",
}
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
}


class CapsuleError(RuntimeError):
    """A capsule, receipt, or live binding failed a bounded validation."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _sha256_raw(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _digest(value: Any, code: str) -> str:
    text = str(value or "").strip().lower()
    if _SHA256_RE.fullmatch(text) is None:
        raise CapsuleError(code)
    return text


def _text(value: Any, code: str, *, max_bytes: int = 4096) -> str:
    if not isinstance(value, str):
        raise CapsuleError(code)
    text = value.strip()
    if (
        not text
        or text != value
        or "\n" in text
        or "\r" in text
        or len(text.encode("utf-8")) > max_bytes
    ):
        raise CapsuleError(code)
    return text


def _timestamp(value: Any, code: str) -> datetime:
    text = _text(value, code, max_bytes=128)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise CapsuleError(code) from exc
    if parsed.tzinfo is None:
        raise CapsuleError(code)
    return parsed.astimezone(timezone.utc)


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise CapsuleError("activation_capsule_json_duplicate_key")
        result[key] = value
    return result


def _reject_json_constant(_value: str) -> None:
    raise CapsuleError("activation_capsule_json_non_finite_number")


def _absolute_path(value: Path | str, code: str) -> Path:
    candidate = Path(value).expanduser()
    if not candidate.is_absolute():
        raise CapsuleError(code)
    absolute = candidate.absolute()
    if str(absolute) != str(candidate):
        raise CapsuleError(code)
    return absolute


def _read_owner_json(
    path: Path,
    *,
    artifact: str,
    max_bytes: int = MAX_CAPSULE_BYTES,
) -> tuple[bytes, dict[str, Any]]:
    candidate = _absolute_path(path, f"{artifact}_path_invalid")
    descriptor = -1
    try:
        observed = candidate.lstat()
        if (
            stat.S_ISLNK(observed.st_mode)
            or not stat.S_ISREG(observed.st_mode)
            or observed.st_uid != os.getuid()
            or observed.st_nlink != 1
            or stat.S_IMODE(observed.st_mode) != 0o600
            or observed.st_size <= 0
            or observed.st_size > max_bytes
        ):
            raise CapsuleError(f"{artifact}_file_invalid")
        descriptor = os.open(
            candidate,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_uid != os.getuid()
            or opened.st_nlink != 1
            or stat.S_IMODE(opened.st_mode) != 0o600
            or (opened.st_dev, opened.st_ino, opened.st_size)
            != (observed.st_dev, observed.st_ino, observed.st_size)
        ):
            raise CapsuleError(f"{artifact}_file_invalid")
        chunks: list[bytes] = []
        remaining = int(opened.st_size)
        while remaining:
            chunk = os.read(descriptor, min(1024 * 1024, remaining))
            if not chunk:
                raise CapsuleError(f"{artifact}_file_changed")
            chunks.append(chunk)
            remaining -= len(chunk)
        if os.read(descriptor, 1):
            raise CapsuleError(f"{artifact}_file_changed")
        finished = os.fstat(descriptor)
        final = candidate.lstat()
        identity = (
            opened.st_dev,
            opened.st_ino,
            opened.st_size,
            opened.st_mtime_ns,
            opened.st_ctime_ns,
        )
        if identity != (
            finished.st_dev,
            finished.st_ino,
            finished.st_size,
            finished.st_mtime_ns,
            finished.st_ctime_ns,
        ) or identity != (
            final.st_dev,
            final.st_ino,
            final.st_size,
            final.st_mtime_ns,
            final.st_ctime_ns,
        ):
            raise CapsuleError(f"{artifact}_file_changed")
        raw = b"".join(chunks)
    except CapsuleError:
        raise
    except OSError as exc:
        raise CapsuleError(f"{artifact}_file_invalid") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    try:
        decoded = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=_reject_json_constant,
        )
    except CapsuleError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CapsuleError(f"{artifact}_json_invalid") from exc
    if not isinstance(decoded, dict) or not decoded:
        raise CapsuleError(f"{artifact}_shape_invalid")
    return raw, decoded


def _check_map(report: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    raw_checks = report.get("checks")
    if not isinstance(raw_checks, list) or not raw_checks:
        raise CapsuleError("activation_capsule_gate_checks_invalid")
    checks: dict[str, dict[str, Any]] = {}
    for raw in raw_checks:
        if not isinstance(raw, Mapping) or set(raw) != {"name", "ok", "code", "detail"}:
            raise CapsuleError("activation_capsule_gate_checks_invalid")
        name = _text(raw.get("name"), "activation_capsule_gate_checks_invalid")
        detail = raw.get("detail")
        if (
            name in checks
            or raw.get("ok") is not True
            or raw.get("code") != "pass"
            or not isinstance(detail, Mapping)
        ):
            raise CapsuleError("activation_capsule_gate_checks_invalid")
        checks[name] = dict(raw)
    return checks


def release_report_fingerprint(report: Mapping[str, Any]) -> str:
    """Return the canonical v1 gate fingerprint after strict shape validation."""
    if set(report) != _REPORT_FIELDS:
        raise CapsuleError("activation_capsule_gate_report_shape_invalid")
    _timestamp(report.get("evaluated_at"), "activation_capsule_gate_time_invalid")
    mode = report.get("mode")
    if mode not in {
        "preauthorization",
        "preproduction",
        "production_bootstrap",
        "production",
    }:
        raise CapsuleError("activation_capsule_gate_mode_invalid")
    if (
        report.get("schema_version") != RELEASE_GATE_SCHEMA_VERSION
        or report.get("ok") is not True
        or report.get("blockers") != []
        or report.get("warnings") != []
    ):
        raise CapsuleError("activation_capsule_gate_not_successful")
    config = report.get("config")
    gate_policy = report.get("gate_policy")
    evidence = report.get("evidence_sha256")
    if not isinstance(config, Mapping) or not config:
        raise CapsuleError("activation_capsule_gate_config_invalid")
    if not isinstance(gate_policy, Mapping) or not gate_policy:
        raise CapsuleError("activation_capsule_gate_policy_invalid")
    max_age = gate_policy.get("evidence_max_age_seconds")
    if (
        isinstance(max_age, bool)
        or not isinstance(max_age, int)
        or max_age < 1
        or max_age > MAX_GATE_EVIDENCE_AGE_SECONDS
    ):
        raise CapsuleError("activation_capsule_gate_policy_invalid")
    if not isinstance(evidence, Mapping):
        raise CapsuleError("activation_capsule_gate_evidence_invalid")
    normalized_evidence: dict[str, str] = {}
    for key, value in evidence.items():
        name = _text(key, "activation_capsule_gate_evidence_invalid")
        normalized_evidence[name] = _digest(
            value, "activation_capsule_gate_evidence_invalid"
        )
    checks = _check_map(report)
    if "contract_drift" not in checks:
        raise CapsuleError("activation_capsule_contract_check_missing")
    consumer = config.get("consumer")
    if not isinstance(consumer, Mapping):
        raise CapsuleError("activation_capsule_gate_config_invalid")
    policy = consumer.get("policy")
    if not isinstance(policy, Mapping):
        raise CapsuleError("activation_capsule_gate_config_invalid")
    expected_topic = _text(
        consumer.get("topic"), "activation_capsule_gate_config_invalid"
    )
    expected_rule = _text(
        policy.get("policy_version"), "activation_capsule_gate_config_invalid"
    )
    contract = checks["contract_drift"]["detail"]
    material = {
        "schema_version": RELEASE_GATE_SCHEMA_VERSION,
        "mode": mode,
        "expected_topic": expected_topic,
        "expected_rule_version": expected_rule,
        "config": dict(config),
        "gate_policy": dict(gate_policy),
        "contract": dict(contract),
        "evidence_sha256": dict(sorted(normalized_evidence.items())),
        "checks": [dict(item) for item in report["checks"]],
        "blockers": [],
        "warnings": [],
    }
    return _sha256_json(material)


def _validate_report(
    report: Mapping[str, Any],
    *,
    allowed_modes: frozenset[str],
    expected_state: str,
) -> tuple[str, dict[str, dict[str, Any]]]:
    expected_fingerprint = release_report_fingerprint(report)
    fingerprint = _digest(
        report.get("fingerprint"), "activation_capsule_gate_fingerprint_invalid"
    )
    if fingerprint != expected_fingerprint or report.get("mode") not in allowed_modes:
        raise CapsuleError("activation_capsule_gate_fingerprint_invalid")
    checks = _check_map(report)
    activation = checks.get("activation_epoch")
    if activation is None or activation["detail"].get("state") != expected_state:
        raise CapsuleError("activation_capsule_gate_activation_state_invalid")
    return fingerprint, checks


def _check_freshness(
    report: Mapping[str, Any],
    created_at: Any,
    *,
    now: datetime | None,
    max_age_seconds: int,
) -> None:
    evaluated = _timestamp(
        report.get("evaluated_at"), "activation_capsule_gate_time_invalid"
    )
    created = _timestamp(created_at, "activation_capsule_created_at_invalid")
    if created != evaluated:
        raise CapsuleError("activation_capsule_created_at_mismatch")
    current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    age = (current - created).total_seconds()
    if age < -MAX_HEALTH_FUTURE_SKEW_SECONDS:
        raise CapsuleError("activation_capsule_from_future")
    if age > max_age_seconds:
        raise CapsuleError("activation_capsule_stale")


def _capsule_path(receipt_path: Path, stage: str) -> Path:
    receipt = _absolute_path(receipt_path, "activation_capsule_receipt_path_invalid")
    if stage == "preauthorization":
        suffix = "activation-preauthorization"
    elif stage == "preproduction":
        suffix = "activation-preproduction"
    elif stage == "confirmation":
        suffix = "activation-confirmation"
    else:
        raise CapsuleError("activation_capsule_stage_invalid")
    return receipt.with_name(f"{receipt.stem}.{suffix}.json")


def _pair_path(receipt_path: Path, stage: str) -> Path:
    receipt = _absolute_path(receipt_path, "activation_capsule_receipt_path_invalid")
    if stage not in {"preauthorization", "preproduction", "confirmation"}:
        raise CapsuleError("activation_capsule_stage_invalid")
    return receipt.with_name(f"{receipt.stem}.activation-{stage}.commit.json")


def _receipt_binding(
    receipt_path: Path,
    *,
    allowed_modes: frozenset[str],
    expected_state: str,
) -> tuple[bytes, dict[str, Any], dict[str, Any], dict[str, dict[str, Any]]]:
    receipt = _absolute_path(receipt_path, "activation_capsule_receipt_path_invalid")
    raw, report = _read_owner_json(
        receipt,
        artifact="activation_capsule_gate_receipt",
        max_bytes=MAX_RECEIPT_BYTES,
    )
    fingerprint, checks = _validate_report(
        report,
        allowed_modes=allowed_modes,
        expected_state=expected_state,
    )
    binding = {
        "path": str(receipt),
        "size_bytes": len(raw),
        "raw_sha256": _sha256_raw(raw),
        "report_fingerprint": fingerprint,
    }
    return raw, report, binding, checks


def _bound_receipt_from_capsule(
    capsule_path: Path,
    capsule: Mapping[str, Any],
    *,
    stage: str,
    allowed_modes: frozenset[str],
    expected_state: str,
) -> tuple[Path, bytes, dict[str, Any], dict[str, Any], dict[str, dict[str, Any]]]:
    raw_binding = capsule.get("release_gate_receipt")
    if not isinstance(raw_binding, Mapping) or set(raw_binding) != {
        "path",
        "size_bytes",
        "raw_sha256",
        "report_fingerprint",
    }:
        raise CapsuleError("activation_capsule_receipt_binding_invalid")
    receipt_path = _absolute_path(
        _text(
            raw_binding.get("path"),
            "activation_capsule_receipt_binding_invalid",
        ),
        "activation_capsule_receipt_binding_invalid",
    )
    if _absolute_path(capsule_path, "activation_capsule_path_invalid") != _capsule_path(
        receipt_path, stage
    ):
        raise CapsuleError("activation_capsule_path_invalid")
    raw, report, expected, checks = _receipt_binding(
        receipt_path,
        allowed_modes=allowed_modes,
        expected_state=expected_state,
    )
    if dict(raw_binding) != expected:
        raise CapsuleError("activation_capsule_receipt_binding_invalid")
    return receipt_path, raw, report, expected, checks


def _normalize_fence(value: Any, code: str) -> dict[str, dict[str, int]]:
    if not isinstance(value, Mapping) or not value:
        raise CapsuleError(code)
    normalized: dict[str, dict[str, int]] = {}
    for raw_topic, raw_partitions in value.items():
        if not isinstance(raw_topic, str) or raw_topic != raw_topic.strip():
            raise CapsuleError(code)
        topic = raw_topic
        if (
            _EVENT_UID_RE.fullmatch(f"{topic}:0:0") is None
            or not isinstance(raw_partitions, Mapping)
            or not raw_partitions
        ):
            raise CapsuleError(code)
        partitions: dict[str, int] = {}
        for raw_partition, raw_offset in raw_partitions.items():
            partition = str(raw_partition)
            if (
                not partition.isdigit()
                or str(int(partition)) != partition
                or isinstance(raw_offset, bool)
                or not isinstance(raw_offset, int)
                or raw_offset < 0
            ):
                raise CapsuleError(code)
            partitions[partition] = raw_offset
        normalized[topic] = partitions
    return normalized


def _validate_database_file(
    path: Path, *, role: str, expected_schema: str
) -> os.stat_result:
    candidate = _absolute_path(path, "activation_capsule_database_path_invalid")
    try:
        observed = candidate.lstat()
    except OSError as exc:
        raise CapsuleError("activation_capsule_database_unavailable") from exc
    if stat.S_ISLNK(observed.st_mode) or not stat.S_ISREG(observed.st_mode):
        raise CapsuleError("activation_capsule_database_file_invalid")
    connection: sqlite3.Connection | None = None
    try:
        connection = sqlite3.connect(
            candidate.as_uri() + "?mode=ro", uri=True, timeout=5
        )
        connection.execute("PRAGMA query_only=ON")
        connection.execute("PRAGMA trusted_schema=OFF")
        table = "control_meta" if role == "control" else "rca_delivery_meta"
        row = connection.execute(
            f"SELECT value FROM {table} WHERE key='schema_version'"
        ).fetchone()
    except sqlite3.Error as exc:
        raise CapsuleError("activation_capsule_database_unreadable") from exc
    finally:
        if connection is not None:
            connection.close()
    if row is None or str(row[0]) != expected_schema:
        raise CapsuleError("activation_capsule_database_schema_mismatch")
    try:
        final = candidate.lstat()
    except OSError as exc:
        raise CapsuleError("activation_capsule_database_unavailable") from exc
    if (observed.st_dev, observed.st_ino) != (final.st_dev, final.st_ino):
        raise CapsuleError("activation_capsule_database_changed")
    return observed


def _normalize_db_identity(
    value: Any,
    *,
    control_db_path: Path,
) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != {
        "schema_version",
        "strategy",
        "databases",
        "migration_receipt_raw_sha256",
        "materialization_receipt_raw_sha256",
        "host_commit",
        "config_sha256",
    }:
        raise CapsuleError("activation_capsule_database_identity_shape_invalid")
    identity = dict(value)
    if identity.get("schema_version") != "pnc_rca_activation_db_identity_v1":
        raise CapsuleError("activation_capsule_database_identity_shape_invalid")
    if identity.get("strategy") not in {
        "fresh_install_preserve",
        "existing_database_preserve",
    }:
        raise CapsuleError("activation_capsule_database_identity_strategy_invalid")
    _digest(
        identity.get("migration_receipt_raw_sha256"),
        "activation_capsule_database_identity_shape_invalid",
    )
    _digest(
        identity.get("materialization_receipt_raw_sha256"),
        "activation_capsule_database_identity_shape_invalid",
    )
    if _COMMIT_RE.fullmatch(str(identity.get("host_commit") or "")) is None:
        raise CapsuleError("activation_capsule_database_identity_shape_invalid")
    _digest(
        identity.get("config_sha256"),
        "activation_capsule_database_identity_shape_invalid",
    )
    databases = identity.get("databases")
    if not isinstance(databases, Mapping) or set(databases) != {"control", "delivery"}:
        raise CapsuleError("activation_capsule_database_identity_shape_invalid")
    expected_control = _absolute_path(
        control_db_path, "activation_capsule_database_path_invalid"
    )
    for role, expected_schema in (
        ("control", CONTROL_STORE_SCHEMA_VERSION),
        ("delivery", DELIVERY_STORE_SCHEMA_VERSION),
    ):
        database = databases.get(role)
        if not isinstance(database, Mapping) or set(database) != {
            "path",
            "device",
            "inode",
            "schema_version",
            "db_instance_id",
            "genesis_intent_sha256",
        }:
            raise CapsuleError("activation_capsule_database_identity_shape_invalid")
        db_path = _absolute_path(
            _text(
                database.get("path"),
                "activation_capsule_database_identity_shape_invalid",
            ),
            "activation_capsule_database_identity_shape_invalid",
        )
        if role == "control" and db_path != expected_control:
            raise CapsuleError("activation_capsule_database_binding_mismatch")
        observed = _validate_database_file(
            db_path, role=role, expected_schema=expected_schema
        )
        if (
            database.get("schema_version") != expected_schema
            or isinstance(database.get("device"), bool)
            or not isinstance(database.get("device"), int)
            or isinstance(database.get("inode"), bool)
            or not isinstance(database.get("inode"), int)
            or int(database["device"]) != observed.st_dev
            or int(database["inode"]) != observed.st_ino
        ):
            raise CapsuleError("activation_capsule_database_binding_mismatch")
        for optional_digest in ("db_instance_id", "genesis_intent_sha256"):
            item = database.get(optional_digest)
            if item is not None:
                _digest(
                    item,
                    "activation_capsule_database_identity_shape_invalid",
                )
    try:
        encoded = _canonical_json(identity)
    except (TypeError, ValueError) as exc:
        raise CapsuleError(
            "activation_capsule_database_identity_shape_invalid"
        ) from exc
    if len(encoded.encode("utf-8")) > 4096:
        raise CapsuleError("activation_capsule_database_identity_shape_invalid")
    return identity


def _normalize_preauthorization_input(
    value: Any,
    *,
    control_db_path: Path,
) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != _PREAUTHORIZATION_INPUT_FIELDS:
        raise CapsuleError("activation_capsule_preauthorization_input_invalid")
    epoch_id = _text(
        value.get("epoch_id"), "activation_capsule_preauthorization_input_invalid"
    )
    if (
        _EPOCH_ID_RE.fullmatch(epoch_id) is None
        or value.get("initial_state") != "safe_off"
    ):
        raise CapsuleError("activation_capsule_preauthorization_input_invalid")
    db_identity = _normalize_db_identity(
        value.get("db_logical_identity"), control_db_path=control_db_path
    )
    fence = _normalize_fence(
        value.get("partition_start_fence"),
        "activation_capsule_partition_start_fence_invalid",
    )
    normalized = {
        "epoch_id": epoch_id,
        "initial_state": "safe_off",
        "config_sha256": _digest(
            value.get("config_sha256"),
            "activation_capsule_preauthorization_input_invalid",
        ),
        "db_logical_identity": db_identity,
        "db_logical_identity_sha256": _digest(
            value.get("db_logical_identity_sha256"),
            "activation_capsule_preauthorization_input_invalid",
        ),
        "partition_start_fence": fence,
        "partition_start_fence_sha256": _digest(
            value.get("partition_start_fence_sha256"),
            "activation_capsule_preauthorization_input_invalid",
        ),
    }
    for field in (
        "migration_receipt_raw_sha256",
        "materialization_receipt_raw_sha256",
        "broker_t0_observation_sha256",
    ):
        normalized[field] = _digest(
            value.get(field), "activation_capsule_preauthorization_input_invalid"
        )
    if (
        normalized["db_logical_identity_sha256"] != _sha256_json(db_identity)
        or normalized["partition_start_fence_sha256"] != _sha256_json(fence)
        or db_identity.get("config_sha256") != normalized["config_sha256"]
        or db_identity.get("migration_receipt_raw_sha256")
        != normalized["migration_receipt_raw_sha256"]
        or db_identity.get("materialization_receipt_raw_sha256")
        != normalized["materialization_receipt_raw_sha256"]
    ):
        raise CapsuleError("activation_capsule_preauthorization_binding_invalid")
    return normalized


def _normalize_gateway_binding(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping) or not value:
        raise CapsuleError("activation_capsule_gateway_binding_invalid")
    binding = dict(value)
    if binding.get("state") != "running_safe":
        raise CapsuleError("activation_capsule_gateway_binding_invalid")
    pid = binding.get("pid")
    create_time = binding.get("process_create_time")
    if (
        isinstance(pid, bool)
        or not isinstance(pid, int)
        or pid < 1
        or isinstance(create_time, bool)
        or not isinstance(create_time, (int, float))
        or create_time <= 0
    ):
        raise CapsuleError("activation_capsule_gateway_binding_invalid")
    for field in (
        "runtime_identity_sha256",
        "verified_runtime_sha256",
    ):
        _digest(binding.get(field), "activation_capsule_gateway_binding_invalid")
    runtime_identity = binding.get("runtime_identity")
    if not isinstance(runtime_identity, Mapping) or not runtime_identity:
        raise CapsuleError("activation_capsule_gateway_binding_invalid")
    if not runtime_identity_is_valid(
        runtime_identity, service_label=GATEWAY_SERVICE_LABEL
    ) or not str(runtime_identity.get("script") or "").endswith("/gateway/run.py"):
        raise CapsuleError("activation_capsule_gateway_binding_invalid")
    if binding["runtime_identity_sha256"] != _sha256_json(runtime_identity):
        raise CapsuleError("activation_capsule_gateway_binding_invalid")
    if (
        runtime_identity.get("pid") != pid
        or runtime_identity.get("process_create_time") != create_time
    ):
        raise CapsuleError("activation_capsule_gateway_binding_invalid")
    return binding


def _live_launchd_pid(
    service_label: str,
    *,
    runner: Any = subprocess.run,
) -> int:
    """Return the one running launchd PID for a resident service."""
    try:
        completed = runner(
            ["/bin/launchctl", "print", f"gui/{os.getuid()}/{service_label}"],
            text=True,
            capture_output=True,
            timeout=10,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise CapsuleError("activation_capsule_launchd_unavailable") from exc
    stdout = str(completed.stdout or "")
    if completed.returncode != 0 or len(stdout.encode("utf-8")) > 65_536:
        raise CapsuleError("activation_capsule_launchd_unavailable")
    if re.search(r"(?m)^\s*state\s*=\s*running\s*$", stdout) is None:
        raise CapsuleError("activation_capsule_launchd_not_running")
    pids = {
        int(match) for match in re.findall(r"(?m)^\s*pid\s*=\s*([0-9]+)\s*$", stdout)
    }
    if len(pids) != 1:
        raise CapsuleError("activation_capsule_launchd_pid_invalid")
    return next(iter(pids))


def _recheck_live_gateway_binding(value: Mapping[str, Any]) -> None:
    """Recheck the gateway process and loaded tree after a gate was written.

    A gate receipt is a point-in-time observation.  Activation must not consume
    it after launchd has restarted the gateway or the resident tree has drifted.
    This deliberately uses only the small runtime-identity API, keeping the
    retired release-gate module out of the activation path.
    """
    binding = _normalize_gateway_binding(value)
    identity = binding.get("runtime_identity")
    if not runtime_identity_is_valid(identity, service_label=GATEWAY_SERVICE_LABEL):
        raise CapsuleError("activation_capsule_gateway_runtime_invalid")
    assert isinstance(identity, Mapping)
    script = Path(str(identity["script"])).expanduser().absolute()
    root = Path(str(identity["cwd"])).expanduser().absolute()
    if not str(script).endswith("/gateway/run.py"):
        raise CapsuleError("activation_capsule_gateway_runtime_invalid")
    try:
        if _live_launchd_pid(GATEWAY_SERVICE_LABEL) != int(identity["pid"]):
            raise CapsuleError("activation_capsule_gateway_restarted")
        process = psutil.Process(int(identity["pid"]))
        if not process.is_running() or process.status() in {
            psutil.STATUS_DEAD,
            psutil.STATUS_ZOMBIE,
        }:
            raise CapsuleError("activation_capsule_gateway_restarted")
        if not _process_create_time_matches(
            process.create_time(), identity["process_create_time"]
        ):
            raise CapsuleError("activation_capsule_gateway_restarted")
        if Path(process.cwd()).expanduser().absolute() != root:
            raise CapsuleError("activation_capsule_gateway_restarted")
        if (
            Path(process.exe()).expanduser().absolute()
            != Path(str(identity["executable"])).expanduser().absolute()
        ):
            raise CapsuleError("activation_capsule_gateway_restarted")
        cmdline = process.cmdline()
        rendered_cmdline = "\x00".join(str(item) for item in cmdline)
        if (
            not cmdline
            or "gateway" not in rendered_cmdline
            or "run" not in rendered_cmdline
        ):
            raise CapsuleError("activation_capsule_gateway_runtime_invalid")
        if _unsafe_process_environment(process.environ(), expected_root=root):
            raise CapsuleError("activation_capsule_gateway_environment_invalid")
        if file_sha256(script) != identity["script_sha256"]:
            raise CapsuleError("activation_capsule_gateway_runtime_changed")
        _hashes, runtime_sha256 = runtime_file_snapshot(
            root, GATEWAY_RCA_RUNTIME_RELATIVE_FILES
        )
    except CapsuleError:
        raise
    except (OSError, ValueError, psutil.Error) as exc:
        raise CapsuleError("activation_capsule_gateway_restarted") from exc
    if runtime_sha256 != identity["runtime_files_sha256"]:
        raise CapsuleError("activation_capsule_gateway_runtime_changed")


def _recheck_live_resident_projection(
    value: Mapping[str, Any],
    *,
    consumer_health: Mapping[str, Any],
    consumer_health_path: str | Path | None = None,
    service_configs: Mapping[str, Any] | None = None,
    now: datetime | None = None,
) -> None:
    """Recheck resident PIDs and loaded-runtime projections from the gate."""
    residents = value.get("residents")
    if not isinstance(residents, Mapping) or set(residents) != set(_RESIDENT_LABELS):
        raise CapsuleError("activation_capsule_residents_invalid")
    if not isinstance(service_configs, Mapping):
        raise CapsuleError("activation_capsule_resident_health_config_missing")
    if consumer_health_path is None:
        raise CapsuleError("activation_capsule_resident_health_path_invalid")
    trusted_consumer_health_path = _absolute_path(
        consumer_health_path,
        "activation_capsule_resident_health_path_invalid",
    )
    consumer_identity = consumer_health.get("runtime_identity")
    if not isinstance(consumer_identity, Mapping):
        raise CapsuleError("activation_capsule_consumer_runtime_invalid")
    consumer_expected = residents.get("kafka_consumer_health")
    if isinstance(consumer_expected, Mapping):
        expected_runtime = consumer_expected.get("runtime_identity_sha256")
        expected_loaded = consumer_expected.get("loaded_runtime_sha256")
        if (
            expected_runtime is not None
            and _sha256_json(consumer_identity) != expected_runtime
        ):
            raise CapsuleError("activation_capsule_consumer_runtime_changed")
        if expected_loaded is not None and str(
            consumer_identity.get("loaded_runtime_sha256")
        ) != str(expected_loaded):
            raise CapsuleError("activation_capsule_consumer_loaded_runtime_changed")
    for artifact, expected_value in residents.items():
        if not isinstance(expected_value, Mapping):
            raise CapsuleError("activation_capsule_residents_invalid")
        required = {"pid", "process_create_time", "executable", "cwd"}
        if not required.issubset(expected_value):
            raise CapsuleError("activation_capsule_resident_identity_invalid")
        label = _RESIDENT_LABELS.get(str(artifact))
        if label is None:
            raise CapsuleError("activation_capsule_resident_label_invalid")
        pid = expected_value.get("pid")
        create_time = expected_value.get("process_create_time")
        if (
            isinstance(pid, bool)
            or not isinstance(pid, int)
            or pid < 1
            or isinstance(create_time, bool)
            or not isinstance(create_time, (int, float))
            or create_time <= 0
        ):
            raise CapsuleError("activation_capsule_resident_identity_invalid")
        try:
            if _live_launchd_pid(label) != pid:
                raise CapsuleError("activation_capsule_resident_restarted")
            process = psutil.Process(pid)
            if (
                not process.is_running()
                or process.status() in {psutil.STATUS_DEAD, psutil.STATUS_ZOMBIE}
                or not _process_create_time_matches(process.create_time(), create_time)
                or Path(process.exe()).expanduser().absolute()
                != Path(str(expected_value["executable"])).expanduser().absolute()
                or Path(process.cwd()).expanduser().absolute()
                != Path(str(expected_value["cwd"])).expanduser().absolute()
            ):
                raise CapsuleError("activation_capsule_resident_restarted")
            cmdline_sha = expected_value.get("cmdline_sha256")
            if cmdline_sha is not None:
                actual_cmdline = process.cmdline()
                if _digest(
                    cmdline_sha, "activation_capsule_resident_identity_invalid"
                ) != _sha256_json([str(item) for item in actual_cmdline]):
                    raise CapsuleError("activation_capsule_resident_runtime_changed")
            if _unsafe_process_environment(
                process.environ(), expected_root=expected_value["cwd"]
            ):
                raise CapsuleError("activation_capsule_resident_environment_invalid")
        except CapsuleError:
            raise
        except (OSError, ValueError, psutil.Error) as exc:
            raise CapsuleError("activation_capsule_resident_restarted") from exc
    observed_at = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    for artifact, label in _RESIDENT_LABELS.items():
        raw_config = service_configs.get(label)
        if not isinstance(raw_config, Mapping):
            raise CapsuleError("activation_capsule_resident_health_config_missing")
        config = dict(raw_config)
        health_path = _absolute_path(
            _text(
                config.get("health_path"),
                "activation_capsule_resident_health_path_invalid",
            ),
            "activation_capsule_resident_health_path_invalid",
        )
        if artifact == "kafka_consumer_health":
            health = consumer_health
            if health_path != trusted_consumer_health_path:
                raise CapsuleError("activation_capsule_resident_health_path_changed")
        else:
            _raw, health = _read_owner_json(
                health_path,
                artifact=f"activation_{artifact}",
            )
        schema, timestamp_field = _RESIDENT_HEALTH_SPECS[artifact]
        if health.get("schema_version") != schema:
            raise CapsuleError("activation_capsule_resident_health_invalid")
        if health.get("healthy") is not True:
            raise CapsuleError("activation_capsule_resident_unhealthy")
        if label in {
            CONSUMER_SERVICE_LABEL,
            _RESIDENT_LABELS["outbox_dispatcher_health"],
        }:
            if health.get("ok") is not True:
                raise CapsuleError("activation_capsule_resident_unhealthy")
        heartbeat = _timestamp(
            health.get(timestamp_field),
            "activation_capsule_resident_health_time_invalid",
        )
        age = (observed_at - heartbeat).total_seconds()
        if age < -MAX_HEALTH_FUTURE_SKEW_SECONDS:
            raise CapsuleError("activation_capsule_resident_health_from_future")
        if age > LIVE_HEALTH_MAX_AGE_SECONDS:
            raise CapsuleError("activation_capsule_resident_health_stale")
        health_config = health.get("config")
        identity = health.get("runtime_identity")
        if not isinstance(health_config, Mapping) or _sha256_json(
            health_config
        ) != _sha256_json(config):
            raise CapsuleError("activation_capsule_resident_health_config_changed")
        if not runtime_identity_is_valid(
            identity,
            service_label=label,
            public_config=config,
        ):
            raise CapsuleError("activation_capsule_resident_runtime_invalid")
        assert isinstance(identity, Mapping)
        expected_value = residents[artifact]
        expected_identity = _digest(
            expected_value.get("runtime_identity_sha256"),
            "activation_capsule_resident_runtime_invalid",
        )
        expected_loaded = _digest(
            expected_value.get("loaded_runtime_sha256"),
            "activation_capsule_resident_runtime_invalid",
        )
        if identity.get("loaded_runtime_sha256") != expected_loaded:
            raise CapsuleError("activation_capsule_resident_loaded_runtime_changed")
        if _sha256_json(identity) != expected_identity:
            raise CapsuleError("activation_capsule_resident_runtime_changed")


def _recheck_live_consumer_freeze(
    value: Mapping[str, Any],
    *,
    epoch_id: str,
    partition_end_fence: Mapping[str, Any],
    now: datetime | None = None,
) -> dict[str, Any]:
    """Re-read the resident consumer heartbeat and exact pause receipt."""
    binding = _normalize_ingress_freeze_binding(value, epoch_id=epoch_id)
    _raw, health = _read_owner_json(
        Path(str(binding["health_path"])),
        artifact="activation_consumer_health",
    )
    if health.get("schema_version") != CONSUMER_HEALTH_SCHEMA_VERSION:
        raise CapsuleError("activation_capsule_consumer_health_invalid")
    config = health.get("config")
    identity = health.get("runtime_identity")
    if not isinstance(config, Mapping) or not runtime_identity_is_valid(
        identity,
        service_label=CONSUMER_SERVICE_LABEL,
        public_config=config,
    ):
        raise CapsuleError("activation_capsule_consumer_runtime_invalid")
    assert isinstance(identity, Mapping)
    if _sha256_json(identity) != binding["consumer_runtime_identity_sha256"]:
        raise CapsuleError("activation_capsule_consumer_runtime_changed")
    pid = identity.get("pid")
    create_time = identity.get("process_create_time")
    if (
        isinstance(pid, bool)
        or not isinstance(pid, int)
        or pid < 1
        or isinstance(create_time, bool)
        or not isinstance(create_time, (int, float))
        or create_time <= 0
    ):
        raise CapsuleError("activation_capsule_consumer_runtime_invalid")
    try:
        if _live_launchd_pid(CONSUMER_SERVICE_LABEL) != pid:
            raise CapsuleError("activation_capsule_consumer_restarted")
        process = psutil.Process(pid)
        if (
            not process.is_running()
            or process.status() in {psutil.STATUS_DEAD, psutil.STATUS_ZOMBIE}
            or not _process_create_time_matches(process.create_time(), create_time)
            or Path(process.exe()).expanduser().absolute()
            != Path(str(identity["executable"])).expanduser().absolute()
            or Path(process.cwd()).expanduser().absolute()
            != Path(str(identity["cwd"])).expanduser().absolute()
        ):
            raise CapsuleError("activation_capsule_consumer_restarted")
        rendered_cmdline = "\x00".join(str(item) for item in process.cmdline())
        if "pnc_rca_kafka_consumer" not in rendered_cmdline:
            raise CapsuleError("activation_capsule_consumer_runtime_invalid")
        if _unsafe_process_environment(
            process.environ(), expected_root=identity["cwd"]
        ):
            raise CapsuleError("activation_capsule_consumer_environment_invalid")
    except CapsuleError:
        raise
    except (OSError, ValueError, psutil.Error) as exc:
        raise CapsuleError("activation_capsule_consumer_restarted") from exc
    if (
        health.get("ok") is not True
        or health.get("healthy") is not True
        or health.get("enabled") is not True
        or health.get("activation_required") is not True
        or health.get("state") != "activation_frozen"
    ):
        raise CapsuleError("activation_capsule_consumer_not_frozen")
    stats = health.get("stats")
    if (
        not isinstance(stats, Mapping)
        or isinstance(stats.get("blocked_partitions"), bool)
        or not isinstance(stats.get("blocked_partitions"), int)
        or stats.get("blocked_partitions") != 0
    ):
        raise CapsuleError("activation_capsule_consumer_not_frozen")
    heartbeat = _timestamp(
        health.get("heartbeat_at"), "activation_capsule_consumer_health_time_invalid"
    )
    observed_at = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    age = (observed_at - heartbeat).total_seconds()
    if age < -MAX_HEALTH_FUTURE_SKEW_SECONDS:
        raise CapsuleError("activation_capsule_consumer_health_from_future")
    if age > LIVE_HEALTH_MAX_AGE_SECONDS:
        raise CapsuleError("activation_capsule_consumer_health_stale")
    freeze = health.get("activation_freeze")
    if not isinstance(freeze, Mapping) or set(freeze) != {
        "schema_version",
        "epoch_id",
        "state",
        "freeze_token",
        "paused_at",
        "observed_at",
        "consumer_runtime_identity_sha256",
        "partition_positions",
        "restart_required",
    }:
        raise CapsuleError("activation_capsule_consumer_freeze_invalid")
    if (
        freeze.get("schema_version") != ACTIVATION_FREEZE_SCHEMA_VERSION
        or freeze.get("epoch_id") != epoch_id
        or freeze.get("state") != "partitions_paused"
        or freeze.get("restart_required") is not False
        or not isinstance(freeze.get("freeze_token"), str)
        or not str(freeze.get("freeze_token")).strip()
        or freeze.get("consumer_runtime_identity_sha256")
        != binding["consumer_runtime_identity_sha256"]
    ):
        raise CapsuleError("activation_capsule_consumer_freeze_invalid")
    _timestamp(freeze.get("paused_at"), "activation_capsule_consumer_freeze_invalid")
    freeze_observed = _timestamp(
        freeze.get("observed_at"), "activation_capsule_consumer_freeze_invalid"
    )
    freeze_age = (observed_at - freeze_observed).total_seconds()
    if freeze_age < -MAX_HEALTH_FUTURE_SKEW_SECONDS:
        raise CapsuleError("activation_capsule_consumer_freeze_from_future")
    if freeze_age > LIVE_HEALTH_MAX_AGE_SECONDS:
        raise CapsuleError("activation_capsule_consumer_freeze_stale")
    positions = _normalize_fence(
        freeze.get("partition_positions"),
        "activation_capsule_consumer_partition_positions_invalid",
    )
    expected_positions = _normalize_fence(
        partition_end_fence,
        "activation_capsule_partition_end_fence_invalid",
    )
    if positions != expected_positions:
        raise CapsuleError("activation_capsule_consumer_freeze_position_changed")
    stable_freeze = dict(freeze)
    stable_freeze.pop("observed_at", None)
    if (
        _sha256_json(stable_freeze) != binding["freeze_receipt_sha256"]
        or hashlib.sha256(str(freeze["freeze_token"]).encode("utf-8")).hexdigest()
        != binding["freeze_token_sha256"]
        or _sha256_json(positions) != binding["partition_positions_sha256"]
    ):
        raise CapsuleError("activation_capsule_consumer_freeze_binding_invalid")
    return health


def _normalize_canary_slot_plan(value: Any) -> dict[str, dict[str, Any]]:
    if not isinstance(value, Mapping) or set(value) != set(
        ACTIVATION_RELEASE_SLOT_KINDS
    ):
        raise CapsuleError("activation_capsule_canary_plan_invalid")
    normalized: dict[str, dict[str, Any]] = {}
    identity_hashes: set[str] = set()
    submissions: set[str] = set()
    for slot_kind in ACTIVATION_RELEASE_SLOT_KINDS:
        raw = value.get(slot_kind)
        if not isinstance(raw, Mapping) or set(raw) != {
            "source_kind",
            "entrypoint",
            "source_identity",
            "source_identity_sha256",
            "max_admissions",
            "expected_admission",
            "expected_outcome",
        }:
            raise CapsuleError("activation_capsule_canary_plan_invalid")
        expected_source = "kafka" if slot_kind == "kafka_success" else "manual"
        expected_entrypoint = (
            "kafka_ingest" if slot_kind == "kafka_success" else "manual_admit"
        )
        identity = raw.get("source_identity")
        if not isinstance(identity, Mapping):
            raise CapsuleError("activation_capsule_canary_plan_invalid")
        if expected_source == "kafka":
            if set(identity) != {"event_uid", "offset", "partition", "topic"}:
                raise CapsuleError("activation_capsule_canary_plan_invalid")
            match = _EVENT_UID_RE.fullmatch(str(identity.get("event_uid") or ""))
            if (
                match is None
                or isinstance(identity.get("offset"), bool)
                or not isinstance(identity.get("offset"), int)
                or isinstance(identity.get("partition"), bool)
                or not isinstance(identity.get("partition"), int)
                or int(match.group("offset")) != identity.get("offset")
                or int(match.group("partition")) != identity.get("partition")
                or match.group("topic") != identity.get("topic")
            ):
                raise CapsuleError("activation_capsule_canary_plan_invalid")
        else:
            required = {
                "chat_id",
                "requester_id",
                "message_id",
                "thread_id",
                "issue_url",
                "mode",
            }
            if set(identity) != required or any(
                not isinstance(identity.get(field), str)
                or not str(identity[field]).strip()
                or str(identity[field]).strip() != identity[field]
                for field in required
            ):
                raise CapsuleError("activation_capsule_canary_plan_invalid")
            if (
                not str(identity["requester_id"]).startswith("ou_")
                or _ISSUE_URL_RE.fullmatch(str(identity["issue_url"]).rstrip("/"))
                is None
                or identity["mode"] != "run_or_join"
            ):
                raise CapsuleError("activation_capsule_canary_plan_invalid")
        canonical_identity = dict(identity)
        identity_sha = _digest(
            raw.get("source_identity_sha256"),
            "activation_capsule_canary_plan_invalid",
        )
        admission = raw.get("expected_admission")
        if not isinstance(admission, Mapping) or set(admission) != {
            "business_key",
            "submission_key",
            "generation",
        }:
            raise CapsuleError("activation_capsule_canary_plan_invalid")
        business_key = _text(
            admission.get("business_key"), "activation_capsule_canary_plan_invalid"
        )
        submission_key = _text(
            admission.get("submission_key"), "activation_capsule_canary_plan_invalid"
        )
        generation = admission.get("generation")
        expected_outcome = raw.get("expected_outcome")
        if (
            raw.get("source_kind") != expected_source
            or raw.get("entrypoint") != expected_entrypoint
            or raw.get("max_admissions") != 1
            or identity_sha != _sha256_json(canonical_identity)
            or isinstance(generation, bool)
            or not isinstance(generation, int)
            or generation < 1
            or expected_outcome
            != (
                "terminal_failed"
                if slot_kind == "manual_terminal_failure"
                else "success"
            )
        ):
            raise CapsuleError("activation_capsule_canary_plan_invalid")
        identity_hashes.add(identity_sha)
        submissions.add(submission_key)
        normalized[slot_kind] = {
            "source_kind": expected_source,
            "entrypoint": expected_entrypoint,
            "source_identity": canonical_identity,
            "source_identity_sha256": identity_sha,
            "max_admissions": 1,
            "expected_admission": {
                "business_key": business_key,
                "submission_key": submission_key,
                "generation": generation,
            },
            "expected_outcome": expected_outcome,
        }
    if len(identity_hashes) != len(ACTIVATION_RELEASE_SLOT_KINDS) or len(
        submissions
    ) != len(
        ACTIVATION_RELEASE_SLOT_KINDS
    ):
        raise CapsuleError("activation_capsule_canary_plan_invalid")
    return normalized


def _secure_evidence_directory(value: Any) -> Path:
    path = _absolute_path(
        _text(value, "activation_capsule_evidence_directory_invalid"),
        "activation_capsule_evidence_directory_invalid",
    )
    try:
        observed = path.lstat()
    except OSError as exc:
        raise CapsuleError("activation_capsule_evidence_directory_invalid") from exc
    if (
        stat.S_ISLNK(observed.st_mode)
        or not stat.S_ISDIR(observed.st_mode)
        or observed.st_uid != os.getuid()
        or stat.S_IMODE(observed.st_mode) & 0o022
    ):
        raise CapsuleError("activation_capsule_evidence_directory_invalid")
    return path


def _material_check(
    checks: Mapping[str, Mapping[str, Any]],
    *,
    stage: str,
) -> dict[str, Any]:
    check = checks.get("activation_capsule_material")
    if check is None:
        raise CapsuleError("activation_capsule_material_check_missing")
    detail = check.get("detail")
    if not isinstance(detail, Mapping):
        raise CapsuleError("activation_capsule_material_invalid")
    expected_schema = (
        PREAUTHORIZATION_MATERIAL_SCHEMA_VERSION
        if stage == "preauthorization"
        else PREPRODUCTION_MATERIAL_SCHEMA_VERSION
    )
    expected_fields = {
        "schema_version",
        "evidence_directory",
        "activation_input",
        "gateway_binding",
    }
    if stage == "preproduction":
        expected_fields |= {"preauthorization_capsule", "canary_slot_plan"}
    if (
        set(detail) != expected_fields
        or detail.get("schema_version") != expected_schema
    ):
        raise CapsuleError("activation_capsule_material_invalid")
    return dict(detail)


def _stage_pair_material(
    *,
    stage: str,
    receipt_path: Path,
    receipt_raw: bytes,
    fingerprint: str,
    capsule_path: Path,
    capsule_raw: bytes,
    capsule: Mapping[str, Any],
) -> dict[str, Any]:
    transition_field = (
        "activation_input" if stage == "preauthorization" else "transition_input"
    )
    transition = capsule.get(transition_field)
    if not isinstance(transition, Mapping):
        raise CapsuleError("activation_capsule_pair_binding_invalid")
    return {
        "release_gate_receipt": {
            "path": str(receipt_path),
            "size_bytes": len(receipt_raw),
            "raw_sha256": _sha256_raw(receipt_raw),
            "report_fingerprint": fingerprint,
        },
        "stage_capsule": {
            "path": str(capsule_path),
            "size_bytes": len(capsule_raw),
            "raw_sha256": _sha256_raw(capsule_raw),
            "transition_sha256": _sha256_json(transition),
        },
    }


def _confirmation_pair_material(
    *,
    receipt_path: Path,
    receipt_raw: bytes,
    fingerprint: str,
    capsule_path: Path,
    capsule_raw: bytes,
    capsule: Mapping[str, Any],
) -> dict[str, Any]:
    transition = capsule.get("transition_input")
    if not isinstance(transition, Mapping):
        raise CapsuleError("activation_capsule_pair_binding_invalid")
    return {
        "release_gate_receipt": {
            "path": str(receipt_path),
            "size_bytes": len(receipt_raw),
            "raw_sha256": _sha256_raw(receipt_raw),
            "report_fingerprint": fingerprint,
        },
        "confirmation_capsule": {
            "path": str(capsule_path),
            "size_bytes": len(capsule_raw),
            "raw_sha256": _sha256_raw(capsule_raw),
            "transition_input_sha256": _sha256_json(transition),
        },
    }


def _encoded_document(value: Mapping[str, Any]) -> bytes:
    try:
        return (
            json.dumps(
                dict(value),
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
                allow_nan=False,
            )
            + "\n"
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise CapsuleError("activation_capsule_document_invalid") from exc


def _write_owner_no_clobber(path: Path, value: Mapping[str, Any]) -> bytes:
    destination = _absolute_path(path, "activation_capsule_destination_invalid")
    parent = destination.parent
    try:
        parent_stat = parent.lstat()
    except OSError as exc:
        raise CapsuleError("activation_capsule_destination_invalid") from exc
    if (
        stat.S_ISLNK(parent_stat.st_mode)
        or not stat.S_ISDIR(parent_stat.st_mode)
        or parent_stat.st_uid != os.getuid()
        or stat.S_IMODE(parent_stat.st_mode) & 0o022
    ):
        raise CapsuleError("activation_capsule_destination_invalid")
    raw = _encoded_document(value)
    if os.path.lexists(destination):
        existing, _body = _read_owner_json(
            destination,
            artifact="activation_capsule_existing",
            max_bytes=max(MAX_CAPSULE_BYTES, len(raw)),
        )
        if existing != raw:
            raise CapsuleError("activation_capsule_no_clobber_conflict")
        return existing
    temporary_name = ""
    try:
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{destination.name}.",
            suffix=".tmp",
            dir=parent,
        )
        try:
            os.fchmod(descriptor, 0o600)
            written = 0
            while written < len(raw):
                written += os.write(descriptor, raw[written:])
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        try:
            os.link(temporary_name, destination)
        except FileExistsError:
            existing, _body = _read_owner_json(
                destination,
                artifact="activation_capsule_existing",
                max_bytes=max(MAX_CAPSULE_BYTES, len(raw)),
            )
            if existing != raw:
                raise CapsuleError("activation_capsule_no_clobber_conflict")
        directory_fd = os.open(
            parent,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_DIRECTORY", 0),
        )
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except CapsuleError:
        raise
    except OSError as exc:
        raise CapsuleError("activation_capsule_publication_failed") from exc
    finally:
        if temporary_name:
            try:
                os.unlink(temporary_name)
            except FileNotFoundError:
                pass
            except OSError:
                pass
    final, _body = _read_owner_json(
        destination,
        artifact="activation_capsule_published",
        max_bytes=max(MAX_CAPSULE_BYTES, len(raw)),
    )
    if final != raw:
        raise CapsuleError("activation_capsule_publication_failed")
    return final


def _write_stage_pair(
    *,
    stage: str,
    created_at: str,
    receipt_path: Path,
    receipt_raw: bytes,
    fingerprint: str,
    capsule_path: Path,
    capsule_raw: bytes,
    capsule: Mapping[str, Any],
) -> Path:
    material = _stage_pair_material(
        stage=stage,
        receipt_path=receipt_path,
        receipt_raw=receipt_raw,
        fingerprint=fingerprint,
        capsule_path=capsule_path,
        capsule_raw=capsule_raw,
        capsule=capsule,
    )
    marker = {
        "schema_version": STAGE_PAIR_COMMIT_SCHEMA_VERSION,
        "stage": stage,
        "created_at": created_at,
        **material,
        "pair_sha256": _sha256_json(material),
        "publication_complete": True,
        "operator_supplied_scope_fields": [],
    }
    path = _pair_path(receipt_path, stage)
    _write_owner_no_clobber(path, marker)
    return path


def _read_stage_pair(
    *,
    stage: str,
    report: Mapping[str, Any],
    receipt_path: Path,
    receipt_raw: bytes,
    fingerprint: str,
    capsule_path: Path,
    capsule_raw: bytes,
    capsule: Mapping[str, Any],
) -> None:
    _raw, marker = _read_owner_json(
        _pair_path(receipt_path, stage), artifact="activation_capsule_pair_commit"
    )
    material = _stage_pair_material(
        stage=stage,
        receipt_path=receipt_path,
        receipt_raw=receipt_raw,
        fingerprint=fingerprint,
        capsule_path=capsule_path,
        capsule_raw=capsule_raw,
        capsule=capsule,
    )
    if set(marker) != {
        "schema_version",
        "stage",
        "created_at",
        "release_gate_receipt",
        "stage_capsule",
        "pair_sha256",
        "publication_complete",
        "operator_supplied_scope_fields",
    } or marker != {
        "schema_version": STAGE_PAIR_COMMIT_SCHEMA_VERSION,
        "stage": stage,
        "created_at": report["evaluated_at"],
        **material,
        "pair_sha256": _sha256_json(material),
        "publication_complete": True,
        "operator_supplied_scope_fields": [],
    }:
        raise CapsuleError("activation_capsule_pair_commit_invalid")


def _preauthorization_material(
    *,
    report: Mapping[str, Any],
    checks: Mapping[str, Mapping[str, Any]],
    control_db_path: Path,
) -> tuple[Path, dict[str, Any], dict[str, Any]]:
    material = _material_check(checks, stage="preauthorization")
    evidence_dir = _secure_evidence_directory(material["evidence_directory"])
    activation_input = _normalize_preauthorization_input(
        material["activation_input"], control_db_path=control_db_path
    )
    gateway_binding = _normalize_gateway_binding(material["gateway_binding"])
    _recheck_live_gateway_binding(gateway_binding)
    if activation_input["config_sha256"] != _sha256_json(report["config"]):
        raise CapsuleError("activation_capsule_preauthorization_binding_invalid")
    return evidence_dir, activation_input, gateway_binding


def build_preauthorization_capsule(
    receipt_path: Path,
    *,
    control_db_path: Path,
    now: datetime | None = None,
) -> Path:
    """Build the immutable preauthorization capsule and pair commit."""
    receipt = _absolute_path(receipt_path, "activation_capsule_receipt_path_invalid")
    receipt_raw, report, receipt_binding, checks = _receipt_binding(
        receipt,
        allowed_modes=frozenset({"preauthorization"}),
        expected_state="absent",
    )
    fingerprint = str(receipt_binding["report_fingerprint"])
    evidence_dir, activation_input, gateway_binding = _preauthorization_material(
        report=report,
        checks=checks,
        control_db_path=control_db_path,
    )
    _check_freshness(
        report,
        report["evaluated_at"],
        now=now,
        max_age_seconds=MAX_STAGE_CAPSULE_AGE_SECONDS,
    )
    capsule = {
        "schema_version": PREAUTHORIZATION_CAPSULE_SCHEMA_VERSION,
        "created_at": report["evaluated_at"],
        "evidence_directory": str(evidence_dir),
        "release_gate_receipt": receipt_binding,
        "activation_input": activation_input,
        "activation_input_sha256": _sha256_json(activation_input),
        "gateway_binding": gateway_binding,
        "gateway_binding_sha256": _sha256_json(gateway_binding),
        "same_file_descriptor_verification_required": True,
    }
    capsule_path = _capsule_path(receipt, "preauthorization")
    capsule_raw = _write_owner_no_clobber(capsule_path, capsule)
    _write_stage_pair(
        stage="preauthorization",
        created_at=str(report["evaluated_at"]),
        receipt_path=receipt,
        receipt_raw=receipt_raw,
        fingerprint=fingerprint,
        capsule_path=capsule_path,
        capsule_raw=capsule_raw,
        capsule=capsule,
    )
    return capsule_path


def _read_preauthorization_bundle(
    path: Path,
    *,
    control_db_path: Path,
    now: datetime | None = None,
) -> dict[str, Any]:
    capsule_path = _absolute_path(path, "activation_capsule_path_invalid")
    raw_capsule, capsule = _read_owner_json(
        capsule_path, artifact="activation_preauthorization_capsule"
    )
    if (
        set(capsule)
        != {
            "schema_version",
            "created_at",
            "evidence_directory",
            "release_gate_receipt",
            "activation_input",
            "activation_input_sha256",
            "gateway_binding",
            "gateway_binding_sha256",
            "same_file_descriptor_verification_required",
        }
        or capsule.get("schema_version") != PREAUTHORIZATION_CAPSULE_SCHEMA_VERSION
    ):
        raise CapsuleError("activation_preauthorization_capsule_shape_invalid")
    receipt, receipt_raw, report, receipt_binding, checks = _bound_receipt_from_capsule(
        capsule_path,
        capsule,
        stage="preauthorization",
        allowed_modes=frozenset({"preauthorization"}),
        expected_state="absent",
    )
    fingerprint = str(receipt_binding["report_fingerprint"])
    evidence_dir, activation_input, gateway_binding = _preauthorization_material(
        report=report,
        checks=checks,
        control_db_path=control_db_path,
    )
    if (
        capsule.get("created_at") != report.get("evaluated_at")
        or capsule.get("evidence_directory") != str(evidence_dir)
        or capsule.get("activation_input") != activation_input
        or capsule.get("activation_input_sha256") != _sha256_json(activation_input)
        or capsule.get("gateway_binding") != gateway_binding
        or capsule.get("gateway_binding_sha256") != _sha256_json(gateway_binding)
        or capsule.get("same_file_descriptor_verification_required") is not True
    ):
        raise CapsuleError("activation_preauthorization_capsule_binding_invalid")
    _check_freshness(
        report,
        capsule["created_at"],
        now=now,
        max_age_seconds=MAX_STAGE_CAPSULE_AGE_SECONDS,
    )
    _read_stage_pair(
        stage="preauthorization",
        report=report,
        receipt_path=receipt,
        receipt_raw=receipt_raw,
        fingerprint=fingerprint,
        capsule_path=capsule_path,
        capsule_raw=raw_capsule,
        capsule=capsule,
    )
    normalized = {
        **activation_input,
        "preauthorization_fingerprint": fingerprint,
        "preauthorization_gate_receipt_sha256": _sha256_raw(receipt_raw),
        "preauthorization_capsule_sha256": _sha256_raw(raw_capsule),
    }
    return {
        "capsule_path": str(capsule_path),
        "raw_capsule": raw_capsule,
        "report": report,
        "normalized": normalized,
        "gateway_binding": gateway_binding,
        "evidence_directory": str(evidence_dir),
    }


def read_preauthorization_capsule(
    path: Path,
    *,
    control_db_path: Path,
) -> dict[str, Any]:
    """Read one owner-only preauthorization capsule into activation input."""
    bundle = _read_preauthorization_bundle(path, control_db_path=control_db_path)
    return dict(bundle["normalized"])


def _live_epoch(control_db_path: Path) -> dict[str, Any] | None:
    try:
        store = RcaControlStore(
            _absolute_path(control_db_path, "activation_capsule_database_path_invalid"),
            require_current=True,
        )
        return store.activation_epoch()
    except CapsuleError:
        raise
    except Exception as exc:
        raise CapsuleError("activation_capsule_database_state_invalid") from exc


def _preproduction_material(
    *,
    report: Mapping[str, Any],
    checks: Mapping[str, Mapping[str, Any]],
    control_db_path: Path,
    prior: Mapping[str, Any],
    prior_path: Path,
) -> tuple[Path, dict[str, Any], dict[str, Any], dict[str, dict[str, Any]]]:
    material = _material_check(checks, stage="preproduction")
    evidence_dir = _secure_evidence_directory(material["evidence_directory"])
    activation_input = _normalize_preauthorization_input(
        material["activation_input"], control_db_path=control_db_path
    )
    gateway_binding = _normalize_gateway_binding(material["gateway_binding"])
    canary_plan = _normalize_canary_slot_plan(material["canary_slot_plan"])
    _recheck_live_gateway_binding(gateway_binding)
    declared_prior = _absolute_path(
        _text(
            material["preauthorization_capsule"],
            "activation_capsule_preproduction_prior_invalid",
        ),
        "activation_capsule_preproduction_prior_invalid",
    )
    prior_core = {key: prior[key] for key in sorted(_PREAUTHORIZATION_INPUT_FIELDS)}
    if (
        declared_prior != prior_path
        or activation_input != prior_core
        or activation_input["config_sha256"] != _sha256_json(report["config"])
    ):
        raise CapsuleError("activation_capsule_preproduction_scope_changed")
    return evidence_dir, activation_input, gateway_binding, canary_plan


def _preproduction_transition(
    prior: Mapping[str, Any], canary_plan: Mapping[str, Any]
) -> dict[str, Any]:
    return {
        "epoch_id": prior["epoch_id"],
        "expected_state": "safe_off",
        "target_state": "preauthorized",
        "expected_preauthorization_fingerprint": prior["preauthorization_fingerprint"],
        "expected_preauthorization_gate_receipt_sha256": prior[
            "preauthorization_gate_receipt_sha256"
        ],
        "expected_preauthorization_capsule_sha256": prior[
            "preauthorization_capsule_sha256"
        ],
        "expected_config_sha256": prior["config_sha256"],
        "expected_db_logical_identity_sha256": prior["db_logical_identity_sha256"],
        "expected_partition_start_fence_sha256": prior["partition_start_fence_sha256"],
        "kafka_proof_mode": ACTIVATION_KAFKA_PROOF_MODE,
        "required_slot_kinds": list(ACTIVATION_RELEASE_SLOT_KINDS),
        "canary_slot_plan": dict(canary_plan),
        "canary_slot_plan_sha256": _sha256_json(canary_plan),
    }


def _require_epoch_matches_prior(
    current: Mapping[str, Any] | None,
    prior: Mapping[str, Any],
    *,
    allowed_states: frozenset[str],
) -> dict[str, Any]:
    if current is None or current.get("state") not in allowed_states:
        raise CapsuleError("activation_capsule_preproduction_epoch_binding_changed")
    expected = {
        "epoch_id": prior["epoch_id"],
        "preauthorization_fingerprint": prior["preauthorization_fingerprint"],
        "preauthorization_gate_receipt_sha256": prior[
            "preauthorization_gate_receipt_sha256"
        ],
        "preauthorization_capsule_sha256": prior["preauthorization_capsule_sha256"],
        "config_sha256": prior["config_sha256"],
        "db_logical_identity_sha256": prior["db_logical_identity_sha256"],
        "partition_start_fence_sha256": prior["partition_start_fence_sha256"],
    }
    if any(current.get(key) != value for key, value in expected.items()):
        raise CapsuleError("activation_capsule_preproduction_epoch_binding_changed")
    return dict(current)


def build_preproduction_capsule(
    receipt_path: Path,
    *,
    control_db_path: Path,
    preauthorization_capsule: Path,
    now: datetime | None = None,
) -> Path:
    """Build the immutable preproduction transition capsule and pair commit."""
    receipt = _absolute_path(receipt_path, "activation_capsule_receipt_path_invalid")
    prior_path = _absolute_path(
        preauthorization_capsule, "activation_capsule_preproduction_prior_invalid"
    )
    prior_bundle = _read_preauthorization_bundle(
        prior_path, control_db_path=control_db_path, now=now
    )
    prior = dict(prior_bundle["normalized"])
    receipt_raw, report, receipt_binding, checks = _receipt_binding(
        receipt,
        allowed_modes=frozenset({"preproduction"}),
        expected_state="safe_off",
    )
    evidence_dir, activation_input, gateway_binding, canary_plan = (
        _preproduction_material(
            report=report,
            checks=checks,
            control_db_path=control_db_path,
            prior=prior,
            prior_path=prior_path,
        )
    )
    if gateway_binding != prior_bundle["gateway_binding"]:
        raise CapsuleError("activation_capsule_preproduction_gateway_changed")
    _require_epoch_matches_prior(
        _live_epoch(control_db_path), prior, allowed_states=frozenset({"safe_off"})
    )
    _check_freshness(
        report,
        report["evaluated_at"],
        now=now,
        max_age_seconds=MAX_STAGE_CAPSULE_AGE_SECONDS,
    )
    transition = _preproduction_transition(prior, canary_plan)
    prior_raw = bytes(prior_bundle["raw_capsule"])
    capsule = {
        "schema_version": PREPRODUCTION_CAPSULE_SCHEMA_VERSION,
        "created_at": report["evaluated_at"],
        "evidence_directory": str(evidence_dir),
        "release_gate_receipt": receipt_binding,
        "preauthorization_capsule": {
            "path": str(prior_path),
            "size_bytes": len(prior_raw),
            "raw_sha256": _sha256_raw(prior_raw),
        },
        "transition_input": transition,
        "transition_input_sha256": _sha256_json(transition),
        "gateway_binding": gateway_binding,
        "gateway_binding_sha256": _sha256_json(gateway_binding),
        "same_file_descriptor_verification_required": True,
    }
    capsule_path = _capsule_path(receipt, "preproduction")
    capsule_raw = _write_owner_no_clobber(capsule_path, capsule)
    _write_stage_pair(
        stage="preproduction",
        created_at=str(report["evaluated_at"]),
        receipt_path=receipt,
        receipt_raw=receipt_raw,
        fingerprint=str(receipt_binding["report_fingerprint"]),
        capsule_path=capsule_path,
        capsule_raw=capsule_raw,
        capsule=capsule,
    )
    return capsule_path


def read_preproduction_capsule(
    path: Path,
    *,
    control_db_path: Path,
    current_activation: Mapping[str, Any],
    allowed_current_states: frozenset[str] = frozenset({"safe_off", "preauthorized"}),
) -> dict[str, Any]:
    """Read one preproduction capsule and bind it to the live current epoch."""
    capsule_path = _absolute_path(path, "activation_capsule_path_invalid")
    raw_capsule, capsule = _read_owner_json(
        capsule_path, artifact="activation_preproduction_capsule"
    )
    if (
        set(capsule)
        != {
            "schema_version",
            "created_at",
            "evidence_directory",
            "release_gate_receipt",
            "preauthorization_capsule",
            "transition_input",
            "transition_input_sha256",
            "gateway_binding",
            "gateway_binding_sha256",
            "same_file_descriptor_verification_required",
        }
        or capsule.get("schema_version") != PREPRODUCTION_CAPSULE_SCHEMA_VERSION
    ):
        raise CapsuleError("activation_preproduction_capsule_shape_invalid")
    receipt, receipt_raw, report, receipt_binding, checks = _bound_receipt_from_capsule(
        capsule_path,
        capsule,
        stage="preproduction",
        allowed_modes=frozenset({"preproduction"}),
        expected_state="safe_off",
    )
    prior_binding = capsule.get("preauthorization_capsule")
    if not isinstance(prior_binding, Mapping) or set(prior_binding) != {
        "path",
        "size_bytes",
        "raw_sha256",
    }:
        raise CapsuleError("activation_capsule_preproduction_prior_invalid")
    prior_path = _absolute_path(
        _text(
            prior_binding.get("path"),
            "activation_capsule_preproduction_prior_invalid",
        ),
        "activation_capsule_preproduction_prior_invalid",
    )
    prior_bundle = _read_preauthorization_bundle(
        prior_path, control_db_path=control_db_path
    )
    prior_raw = bytes(prior_bundle["raw_capsule"])
    if dict(prior_binding) != {
        "path": str(prior_path),
        "size_bytes": len(prior_raw),
        "raw_sha256": _sha256_raw(prior_raw),
    }:
        raise CapsuleError("activation_capsule_preproduction_prior_invalid")
    prior = dict(prior_bundle["normalized"])
    evidence_dir, activation_input, gateway_binding, canary_plan = (
        _preproduction_material(
            report=report,
            checks=checks,
            control_db_path=control_db_path,
            prior=prior,
            prior_path=prior_path,
        )
    )
    transition = _preproduction_transition(prior, canary_plan)
    if (
        activation_input
        != {key: prior[key] for key in sorted(_PREAUTHORIZATION_INPUT_FIELDS)}
        or gateway_binding != prior_bundle["gateway_binding"]
        or capsule.get("created_at") != report.get("evaluated_at")
        or capsule.get("evidence_directory") != str(evidence_dir)
        or capsule.get("transition_input") != transition
        or capsule.get("transition_input_sha256") != _sha256_json(transition)
        or capsule.get("gateway_binding") != gateway_binding
        or capsule.get("gateway_binding_sha256") != _sha256_json(gateway_binding)
        or capsule.get("same_file_descriptor_verification_required") is not True
    ):
        raise CapsuleError("activation_preproduction_capsule_binding_invalid")
    _check_freshness(
        report,
        capsule["created_at"],
        now=None,
        max_age_seconds=MAX_STAGE_CAPSULE_AGE_SECONDS,
    )
    current = _require_epoch_matches_prior(
        current_activation, prior, allowed_states=allowed_current_states
    )
    live = _require_epoch_matches_prior(
        _live_epoch(control_db_path), prior, allowed_states=allowed_current_states
    )
    if any(live.get(key) != current.get(key) for key in current):
        raise CapsuleError("activation_capsule_preproduction_database_changed")
    fingerprint = str(receipt_binding["report_fingerprint"])
    normalized = {
        **transition,
        "preproduction_fingerprint": fingerprint,
        "preproduction_gate_receipt_sha256": _sha256_raw(receipt_raw),
        "preproduction_capsule_sha256": _sha256_raw(raw_capsule),
    }
    if current.get("state") == "preauthorized" and any(
        current.get(field) != normalized[field]
        for field in (
            "preproduction_fingerprint",
            "preproduction_gate_receipt_sha256",
            "preproduction_capsule_sha256",
        )
    ):
        raise CapsuleError("activation_capsule_preproduction_binding_conflict")
    _read_stage_pair(
        stage="preproduction",
        report=report,
        receipt_path=receipt,
        receipt_raw=receipt_raw,
        fingerprint=fingerprint,
        capsule_path=capsule_path,
        capsule_raw=raw_capsule,
        capsule=capsule,
    )
    return normalized


def _normalize_confirmation_input(value: Any) -> dict[str, Any]:
    expected_fields = {
        "epoch_id",
        "expected_state",
        "target_state",
        "config_sha256",
        "db_logical_identity_sha256",
        "partition_start_fence_sha256",
        "release_binding_sha256",
        "partition_end_fence",
        "partition_end_fence_sha256",
        "production_fingerprint_source",
        "production_gate_receipt_sha256_source",
        "restart_between_gate_and_confirm",
    }
    if not isinstance(value, Mapping) or set(value) != expected_fields:
        raise CapsuleError("activation_capsule_confirmation_input_invalid")
    epoch_id = _text(
        value.get("epoch_id"), "activation_capsule_confirmation_input_invalid"
    )
    if _EPOCH_ID_RE.fullmatch(epoch_id) is None:
        raise CapsuleError("activation_capsule_confirmation_input_invalid")
    end_fence = _normalize_fence(
        value.get("partition_end_fence"),
        "activation_capsule_partition_end_fence_invalid",
    )
    normalized = {
        "epoch_id": epoch_id,
        "expected_state": value.get("expected_state"),
        "target_state": value.get("target_state"),
        "config_sha256": _digest(
            value.get("config_sha256"),
            "activation_capsule_confirmation_input_invalid",
        ),
        "db_logical_identity_sha256": _digest(
            value.get("db_logical_identity_sha256"),
            "activation_capsule_confirmation_input_invalid",
        ),
        "partition_start_fence_sha256": _digest(
            value.get("partition_start_fence_sha256"),
            "activation_capsule_confirmation_input_invalid",
        ),
        "release_binding_sha256": _digest(
            value.get("release_binding_sha256"),
            "activation_capsule_confirmation_input_invalid",
        ),
        "partition_end_fence": end_fence,
        "partition_end_fence_sha256": _digest(
            value.get("partition_end_fence_sha256"),
            "activation_capsule_confirmation_input_invalid",
        ),
        "production_fingerprint_source": value.get("production_fingerprint_source"),
        "production_gate_receipt_sha256_source": value.get(
            "production_gate_receipt_sha256_source"
        ),
        "restart_between_gate_and_confirm": value.get(
            "restart_between_gate_and_confirm"
        ),
    }
    if (
        normalized["expected_state"] != "bounded_active"
        or normalized["target_state"] != "confirmed"
        or normalized["partition_end_fence_sha256"] != _sha256_json(end_fence)
        or normalized["production_fingerprint_source"]
        != "release_gate_report.fingerprint"
        or normalized["production_gate_receipt_sha256_source"]
        != "sha256(exact_written_release_gate_receipt)"
        or normalized["restart_between_gate_and_confirm"] is not False
    ):
        raise CapsuleError("activation_capsule_confirmation_input_invalid")
    return normalized


def _normalize_ingress_freeze_binding(value: Any, *, epoch_id: str) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != {
        "schema_version",
        "epoch_id",
        "health_path",
        "paused_at",
        "freeze_receipt_sha256",
        "freeze_token_sha256",
        "consumer_runtime_identity_sha256",
        "partition_positions_sha256",
        "restart_required",
    }:
        raise CapsuleError("activation_capsule_confirmation_freeze_invalid")
    binding = dict(value)
    health_path = _absolute_path(
        _text(
            binding.get("health_path"),
            "activation_capsule_confirmation_freeze_invalid",
        ),
        "activation_capsule_confirmation_freeze_invalid",
    )
    _timestamp(
        binding.get("paused_at"),
        "activation_capsule_confirmation_freeze_invalid",
    )
    if (
        binding.get("schema_version") != "pnc_rca_activation_ingress_freeze_binding_v1"
        or binding.get("epoch_id") != epoch_id
        or binding.get("health_path") != str(health_path)
        or binding.get("restart_required") is not False
    ):
        raise CapsuleError("activation_capsule_confirmation_freeze_invalid")
    for field in (
        "freeze_receipt_sha256",
        "freeze_token_sha256",
        "consumer_runtime_identity_sha256",
        "partition_positions_sha256",
    ):
        _digest(binding.get(field), "activation_capsule_confirmation_freeze_invalid")
    return binding


def _runtime_continuity_binding(
    checks: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    check = checks.get("activation_runtime_continuity")
    if check is None or not isinstance(check.get("detail"), Mapping):
        raise CapsuleError("activation_capsule_runtime_continuity_missing")
    binding = dict(check["detail"])
    if set(binding) != {
        "gateway",
        "gateway_verification",
        "residents",
        "residents_sha256",
    }:
        raise CapsuleError("activation_capsule_runtime_continuity_invalid")
    gateway = _normalize_gateway_binding(binding.get("gateway"))
    verification = binding.get("gateway_verification")
    residents = binding.get("residents")
    if (
        not isinstance(verification, Mapping)
        or not verification
        or not isinstance(residents, Mapping)
        or not residents
        or _digest(
            binding.get("residents_sha256"),
            "activation_capsule_runtime_continuity_invalid",
        )
        != _sha256_json(residents)
    ):
        raise CapsuleError("activation_capsule_runtime_continuity_invalid")
    for field in (
        "runtime_identity_sha256",
        "loaded_runtime_sha256",
        "launchctl_config_sha256",
    ):
        _digest(
            verification.get(field),
            "activation_capsule_runtime_continuity_invalid",
        )
    if (
        verification.get("runtime_identity_sha256")
        != gateway.get("runtime_identity_sha256")
        or verification.get("pid") != gateway.get("pid")
        or verification.get("process_create_time") != gateway.get("process_create_time")
    ):
        raise CapsuleError("activation_capsule_runtime_continuity_invalid")
    return {
        "gateway": gateway,
        "gateway_verification": dict(verification),
        "residents": dict(residents),
        "residents_sha256": _sha256_json(residents),
    }


def live_release_binding(
    control_db_path: Path,
    *,
    epoch_id: str,
    expected_config_sha256: str,
    partition_end_fence: Mapping[str, Any],
) -> dict[str, Any]:
    """Read the exact bounded binding through the control-store public API."""
    end_fence = _normalize_fence(
        partition_end_fence, "activation_capsule_partition_end_fence_invalid"
    )
    try:
        store = RcaControlStore(
            _absolute_path(control_db_path, "activation_capsule_database_path_invalid"),
            require_current=True,
        )
        current = store.activation_epoch()
        if (
            current is None
            or current.get("epoch_id") != epoch_id
            or current.get("state") not in {"bounded_active", "confirmed"}
            or current.get("config_sha256") != expected_config_sha256
        ):
            raise CapsuleError("activation_capsule_live_release_binding_invalid")
        release_sha = store.activation_release_binding_sha256(
            epoch_id=epoch_id,
            partition_end_fence=end_fence,
        )
    except CapsuleError:
        raise
    except Exception as exc:
        raise CapsuleError("activation_capsule_live_release_binding_invalid") from exc
    return {
        **dict(current),
        "release_binding_sha256": _digest(
            release_sha, "activation_capsule_live_release_binding_invalid"
        ),
    }


def _confirmation_material(
    *,
    report: Mapping[str, Any],
    checks: Mapping[str, Mapping[str, Any]],
    live_recheck: bool = True,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    barrier = checks.get("activation_writer_barrier")
    if barrier is None or not isinstance(barrier.get("detail"), Mapping):
        raise CapsuleError("activation_capsule_confirmation_barrier_missing")
    detail = dict(barrier["detail"])
    confirm_input = _normalize_confirmation_input(detail.get("confirm_input"))
    freeze = _normalize_ingress_freeze_binding(
        detail.get("ingress_freeze_binding"), epoch_id=confirm_input["epoch_id"]
    )
    config = report.get("config")
    consumer_config = config.get("consumer") if isinstance(config, Mapping) else None
    trusted_health_path = (
        consumer_config.get("health_path")
        if isinstance(consumer_config, Mapping)
        else None
    )
    if not isinstance(trusted_health_path, str) or not trusted_health_path:
        raise CapsuleError("activation_capsule_consumer_health_path_missing")
    if freeze["health_path"] != str(
        _absolute_path(
            trusted_health_path, "activation_capsule_consumer_health_path_invalid"
        )
    ):
        raise CapsuleError("activation_capsule_consumer_health_path_changed")
    if (
        detail.get("state") != "bounded_active"
        or detail.get("production_confirmation_required") is not True
        or detail.get("transition_performed") is not False
        or detail.get("confirm_input_sha256") != _sha256_json(confirm_input)
        or detail.get("release_binding_sha256")
        != confirm_input["release_binding_sha256"]
    ):
        raise CapsuleError("activation_capsule_confirmation_barrier_invalid")
    continuity = _runtime_continuity_binding(checks)
    if live_recheck:
        runtime_dependencies = checks.get("runtime_dependencies")
        runtime_detail = (
            runtime_dependencies.get("detail")
            if isinstance(runtime_dependencies, Mapping)
            else None
        )
        service_configs = (
            runtime_detail.get("service_configs")
            if isinstance(runtime_detail, Mapping)
            else None
        )
        if not isinstance(service_configs, Mapping):
            raise CapsuleError("activation_capsule_resident_health_config_missing")
        _recheck_live_gateway_binding(continuity["gateway"])
        consumer_health = _recheck_live_consumer_freeze(
            freeze,
            epoch_id=confirm_input["epoch_id"],
            partition_end_fence=confirm_input["partition_end_fence"],
        )
        _recheck_live_resident_projection(
            continuity,
            consumer_health=consumer_health,
            consumer_health_path=freeze["health_path"],
            service_configs=service_configs,
        )
    if (
        continuity["gateway"].get("runtime_identity_sha256")
        != freeze.get("consumer_runtime_identity_sha256")
        and detail.get("consumer_is_gateway") is True
    ):
        raise CapsuleError("activation_capsule_runtime_continuity_invalid")
    return confirm_input, freeze, continuity


def _write_confirmation_pair(
    *,
    report: Mapping[str, Any],
    receipt_path: Path,
    receipt_raw: bytes,
    fingerprint: str,
    capsule_path: Path,
    capsule_raw: bytes,
    capsule: Mapping[str, Any],
) -> Path:
    material = _confirmation_pair_material(
        receipt_path=receipt_path,
        receipt_raw=receipt_raw,
        fingerprint=fingerprint,
        capsule_path=capsule_path,
        capsule_raw=capsule_raw,
        capsule=capsule,
    )
    marker = {
        "schema_version": CONFIRMATION_PAIR_COMMIT_SCHEMA_VERSION,
        "created_at": report["evaluated_at"],
        **material,
        "pair_sha256": _sha256_json(material),
        "publication_complete": True,
        "operator_supplied_scope_fields": [],
    }
    path = _pair_path(receipt_path, "confirmation")
    _write_owner_no_clobber(path, marker)
    return path


def _read_confirmation_pair(
    *,
    report: Mapping[str, Any],
    receipt_path: Path,
    receipt_raw: bytes,
    fingerprint: str,
    capsule_path: Path,
    capsule_raw: bytes,
    capsule: Mapping[str, Any],
) -> None:
    _raw, marker = _read_owner_json(
        _pair_path(receipt_path, "confirmation"),
        artifact="activation_capsule_pair_commit",
    )
    material = _confirmation_pair_material(
        receipt_path=receipt_path,
        receipt_raw=receipt_raw,
        fingerprint=fingerprint,
        capsule_path=capsule_path,
        capsule_raw=capsule_raw,
        capsule=capsule,
    )
    expected = {
        "schema_version": CONFIRMATION_PAIR_COMMIT_SCHEMA_VERSION,
        "created_at": report["evaluated_at"],
        **material,
        "pair_sha256": _sha256_json(material),
        "publication_complete": True,
        "operator_supplied_scope_fields": [],
    }
    if set(marker) != set(expected) or marker != expected:
        raise CapsuleError("activation_capsule_pair_commit_invalid")


def build_confirmation_capsule(
    receipt_path: Path,
    *,
    control_db_path: Path,
    now: datetime | None = None,
) -> Path:
    """Build a production confirmation capsule from one frozen gate receipt."""
    receipt = _absolute_path(receipt_path, "activation_capsule_receipt_path_invalid")
    receipt_raw, report, receipt_binding, checks = _receipt_binding(
        receipt,
        allowed_modes=frozenset({"production_bootstrap", "production"}),
        expected_state="bounded_active",
    )
    confirm_input, freeze, continuity = _confirmation_material(
        report=report,
        checks=checks,
    )
    live = live_release_binding(
        control_db_path,
        epoch_id=confirm_input["epoch_id"],
        expected_config_sha256=confirm_input["config_sha256"],
        partition_end_fence=confirm_input["partition_end_fence"],
    )
    for field in (
        "epoch_id",
        "config_sha256",
        "db_logical_identity_sha256",
        "partition_start_fence_sha256",
        "release_binding_sha256",
    ):
        if live.get(field) != confirm_input[field]:
            raise CapsuleError(
                "activation_capsule_confirmation_database_binding_invalid"
            )
    _check_freshness(
        report,
        report["evaluated_at"],
        now=now,
        max_age_seconds=int(report["gate_policy"]["evidence_max_age_seconds"]),
    )
    transition = {
        field: confirm_input[field]
        for field in (
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
    transition.update({
        "production_fingerprint": receipt_binding["report_fingerprint"],
        "production_gate_receipt_sha256": _sha256_raw(receipt_raw),
    })
    capsule = {
        "schema_version": CONFIRMATION_CAPSULE_SCHEMA_VERSION,
        "created_at": report["evaluated_at"],
        "release_gate_receipt": receipt_binding,
        "ingress_freeze_binding": freeze,
        "runtime_continuity_binding": continuity,
        "transition_input": transition,
        "transition_input_sha256": _sha256_json(transition),
        "operator_supplied_scope_fields": [],
        "same_file_descriptor_verification_required": True,
    }
    capsule_path = _capsule_path(receipt, "confirmation")
    capsule_raw = _write_owner_no_clobber(capsule_path, capsule)
    _write_confirmation_pair(
        report=report,
        receipt_path=receipt,
        receipt_raw=receipt_raw,
        fingerprint=str(receipt_binding["report_fingerprint"]),
        capsule_path=capsule_path,
        capsule_raw=capsule_raw,
        capsule=capsule,
    )
    return capsule_path


def read_confirmation_scope(
    path: Path,
) -> tuple[Path, str, str, dict[str, dict[str, int]]]:
    """Read only the owner-bound fields needed for the live binding lookup."""
    capsule_path = _absolute_path(path, "activation_capsule_path_invalid")
    _raw, capsule = _read_owner_json(
        capsule_path, artifact="activation_confirmation_capsule"
    )
    receipt_binding = capsule.get("release_gate_receipt")
    transition = capsule.get("transition_input")
    if not isinstance(receipt_binding, Mapping) or not isinstance(transition, Mapping):
        raise CapsuleError("activation_capsule_confirmation_scope_invalid")
    receipt = _absolute_path(
        _text(
            receipt_binding.get("path"),
            "activation_capsule_confirmation_scope_invalid",
        ),
        "activation_capsule_confirmation_scope_invalid",
    )
    epoch_id = _text(
        transition.get("epoch_id"),
        "activation_capsule_confirmation_scope_invalid",
    )
    if _EPOCH_ID_RE.fullmatch(epoch_id) is None:
        raise CapsuleError("activation_capsule_confirmation_scope_invalid")
    config_sha = _digest(
        transition.get("config_sha256"),
        "activation_capsule_confirmation_scope_invalid",
    )
    end_fence = _normalize_fence(
        transition.get("partition_end_fence"),
        "activation_capsule_confirmation_scope_invalid",
    )
    return receipt, epoch_id, config_sha, end_fence


def read_confirmation_capsule(
    path: Path,
    *,
    receipt_path: Path,
    control_db_path: Path,
    current_activation: Mapping[str, Any],
) -> dict[str, Any]:
    """Read and revalidate the frozen confirmation capsule against live DB state."""
    capsule_path = _absolute_path(path, "activation_capsule_path_invalid")
    raw_capsule, capsule = _read_owner_json(
        capsule_path, artifact="activation_confirmation_capsule"
    )
    if (
        set(capsule)
        != {
            "schema_version",
            "created_at",
            "release_gate_receipt",
            "ingress_freeze_binding",
            "runtime_continuity_binding",
            "transition_input",
            "transition_input_sha256",
            "operator_supplied_scope_fields",
            "same_file_descriptor_verification_required",
        }
        or capsule.get("schema_version") != CONFIRMATION_CAPSULE_SCHEMA_VERSION
    ):
        raise CapsuleError("activation_confirmation_capsule_shape_invalid")
    receipt, receipt_raw, report, receipt_binding, checks = _bound_receipt_from_capsule(
        capsule_path,
        capsule,
        stage="confirmation",
        allowed_modes=frozenset({"production_bootstrap", "production"}),
        expected_state="bounded_active",
    )
    if receipt != _absolute_path(
        receipt_path, "activation_capsule_receipt_path_invalid"
    ):
        raise CapsuleError("activation_confirmation_capsule_receipt_changed")
    confirm_input, freeze, continuity = _confirmation_material(
        report=report,
        checks=checks,
        live_recheck=current_activation.get("state") != "confirmed",
    )
    observed_live = live_release_binding(
        control_db_path,
        epoch_id=confirm_input["epoch_id"],
        expected_config_sha256=confirm_input["config_sha256"],
        partition_end_fence=confirm_input["partition_end_fence"],
    )
    # The caller's opened snapshot and this independent read must both match.
    for field in (
        "epoch_id",
        "config_sha256",
        "db_logical_identity_sha256",
        "partition_start_fence_sha256",
        "release_binding_sha256",
    ):
        if observed_live.get(field) != confirm_input[field]:
            raise CapsuleError("activation_confirmation_database_binding_invalid")
        if (
            field == "release_binding_sha256"
            and current_activation.get("state") == "confirmed"
            and not current_activation.get(field)
        ):
            continue
        if current_activation.get(field) != confirm_input[field]:
            raise CapsuleError("activation_confirmation_database_binding_invalid")
    transition = {
        field: confirm_input[field]
        for field in (
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
    transition.update({
        "production_fingerprint": receipt_binding["report_fingerprint"],
        "production_gate_receipt_sha256": _sha256_raw(receipt_raw),
    })
    if (
        capsule.get("created_at") != report.get("evaluated_at")
        or capsule.get("ingress_freeze_binding") != freeze
        or capsule.get("runtime_continuity_binding") != continuity
        or capsule.get("transition_input") != transition
        or capsule.get("transition_input_sha256") != _sha256_json(transition)
        or capsule.get("operator_supplied_scope_fields") != []
        or capsule.get("same_file_descriptor_verification_required") is not True
    ):
        raise CapsuleError("activation_confirmation_capsule_binding_invalid")
    if current_activation.get("state") != "confirmed":
        _check_freshness(
            report,
            capsule["created_at"],
            now=None,
            max_age_seconds=int(report["gate_policy"]["evidence_max_age_seconds"]),
        )
    _read_confirmation_pair(
        report=report,
        receipt_path=receipt,
        receipt_raw=receipt_raw,
        fingerprint=str(receipt_binding["report_fingerprint"]),
        capsule_path=capsule_path,
        capsule_raw=raw_capsule,
        capsule=capsule,
    )
    return transition


class _SafeParser(argparse.ArgumentParser):
    def error(self, _message: str) -> None:
        raise CapsuleError("activation_capsule_cli_arguments_invalid")


def _build_parser() -> argparse.ArgumentParser:
    parser = _SafeParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    preauthorization = commands.add_parser("build-preauthorization")
    preauthorization.add_argument("--receipt", type=Path, required=True)
    preauthorization.add_argument("--control-db", type=Path, required=True)
    preproduction = commands.add_parser("build-preproduction")
    preproduction.add_argument("--receipt", type=Path, required=True)
    preproduction.add_argument("--control-db", type=Path, required=True)
    preproduction.add_argument("--preauthorization-capsule", type=Path, required=True)
    confirmation = commands.add_parser("build-confirmation")
    confirmation.add_argument("--receipt", type=Path, required=True)
    confirmation.add_argument("--control-db", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    command = "unknown"
    try:
        args = _build_parser().parse_args(argv)
        command = str(args.command)
        if command == "build-preauthorization":
            path = build_preauthorization_capsule(
                args.receipt, control_db_path=args.control_db
            )
        elif command == "build-preproduction":
            path = build_preproduction_capsule(
                args.receipt,
                control_db_path=args.control_db,
                preauthorization_capsule=args.preauthorization_capsule,
            )
        elif command == "build-confirmation":
            path = build_confirmation_capsule(
                args.receipt, control_db_path=args.control_db
            )
        else:
            raise CapsuleError("activation_capsule_cli_arguments_invalid")
        payload = {
            "command": command,
            "ok": True,
            "path": str(path),
            "schema_version": CAPSULE_CLI_SCHEMA_VERSION,
        }
        print(_canonical_json(payload))
        return 0
    except CapsuleError as exc:
        print(
            _canonical_json({
                "code": exc.code,
                "command": command,
                "ok": False,
                "schema_version": CAPSULE_CLI_SCHEMA_VERSION,
            })
        )
        return 2
    except Exception:
        print(
            _canonical_json({
                "code": "activation_capsule_internal_error",
                "command": command,
                "ok": False,
                "schema_version": CAPSULE_CLI_SCHEMA_VERSION,
            })
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
