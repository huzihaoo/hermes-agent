#!/usr/bin/env python3
"""Read-only B15 launch preflight with one recomputable evidence receipt."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import io
import json
import os
from pathlib import Path
import plistlib
import re
import sqlite3
import stat
import sys
from typing import Any, Mapping
from urllib.parse import quote

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from dotenv import dotenv_values

from gateway.pnc_rca_prod_admission import (
    MAX_TTL_SECONDS,
    MIN_DELIVERY_AVAILABLE_BYTES,
    MIN_ROOT_AVAILABLE_BYTES,
    RcaProdAdmissionError,
    _validate_snapshot,
)
from gateway.pnc_rca_control_store import (
    ACTIVATION_HISTORICAL_OUTBOX_HOLD_SCHEMA_VERSION,
    ActivationEpochError,
    RcaControlStore,
)
from gateway.pnc_rca_runtime_identity import runtime_identity_is_valid


CONTROL_STORE_SCHEMA_VERSION = "pnc_rca_control_store_v14"


def canonical_json_sha256(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()


B15_PREFLIGHT_SCHEMA_VERSION = "pnc_rca_b15_preflight_v1"
MAX_DOCUMENT_BYTES = 2 * 1024 * 1024
ACTIVATION_ENV_KEYS = (
    "HERMES_RCA_ACTIVATION_REQUIRED",
    "HERMES_RCA_KAFKA_ACTIVATION_REQUIRED",
    "HERMES_RCA_OUTBOX_ACTIVATION_REQUIRED",
    "HERMES_RCA_DELIVERY_COLLECTOR_ACTIVATION_REQUIRED",
    "HERMES_RCA_DELIVERY_DISPATCHER_ACTIVATION_REQUIRED",
)
SERVICE_SPECS = (
    (
        "kafka_consumer",
        "local.pnc.rca-kafka-consumer.plist",
        "consumer_health.json",
        "active",
    ),
    (
        "outbox_dispatcher",
        "local.pnc.rca-outbox-dispatcher.plist",
        "outbox_dispatcher_health.json",
        "active",
    ),
    (
        "delivery_collector",
        "local.pnc.rca-delivery-collector.plist",
        "delivery_collector_health.json",
        "active",
    ),
    (
        "delivery_dispatcher",
        "local.pnc.rca-delivery-dispatcher.plist",
        "delivery_dispatcher_health.json",
        "disabled",
    ),
)
HEX64_RE = re.compile(r"^[0-9a-f]{64}$")
SERVICE_HEALTH_CONTRACTS: dict[str, dict[str, Any]] = {
    "kafka_consumer": {
        "schema_version": "pnc_rca_kafka_consumer_health_v2",
        "service_label": "local.pnc.rca-kafka-consumer",
        "freshness_field": "heartbeat_at",
    },
    "outbox_dispatcher": {
        "schema_version": "pnc_rca_outbox_dispatcher_health_v2",
        "service_label": "local.pnc.rca-outbox-dispatcher",
        "freshness_field": "heartbeat_at",
    },
    "delivery_collector": {
        "schema_version": "pnc_rca_delivery_collector_health_v2",
        "service_label": "local.pnc.rca-delivery-collector",
        "freshness_field": "updated_at",
    },
    "delivery_dispatcher": {
        "schema_version": "pnc_rca_delivery_dispatcher_health_v2",
        "service_label": "local.pnc.rca-delivery-dispatcher",
        "freshness_field": "updated_at",
    },
}


class B15PreflightError(RuntimeError):
    pass


def _absolute(path: str | Path, code: str) -> Path:
    candidate = Path(path)
    if not candidate.is_absolute() or ".." in candidate.parts:
        raise B15PreflightError(code)
    return candidate.expanduser()


def _read_bytes(
    path: str | Path,
    *,
    code: str,
    max_bytes: int = MAX_DOCUMENT_BYTES,
) -> tuple[Path, bytes, os.stat_result]:
    candidate = _absolute(path, f"{code}_path_invalid")
    descriptor = -1
    try:
        before = candidate.lstat()
        if (
            stat.S_ISLNK(before.st_mode)
            or not stat.S_ISREG(before.st_mode)
            or before.st_uid != os.getuid()
            or before.st_nlink != 1
            or before.st_size <= 0
            or before.st_size > max_bytes
        ):
            raise B15PreflightError(f"{code}_file_invalid")
        descriptor = os.open(
            candidate,
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0),
        )
        opened = os.fstat(descriptor)
        identity = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
        if (
            opened.st_dev,
            opened.st_ino,
            opened.st_size,
            opened.st_mtime_ns,
        ) != identity:
            raise B15PreflightError(f"{code}_identity_changed")
        chunks: list[bytes] = []
        remaining = opened.st_size
        while remaining:
            chunk = os.read(descriptor, min(1024 * 1024, remaining))
            if not chunk:
                raise B15PreflightError(f"{code}_short_read")
            chunks.append(chunk)
            remaining -= len(chunk)
        if os.read(descriptor, 1):
            raise B15PreflightError(f"{code}_identity_changed")
        after = os.fstat(descriptor)
        visible = candidate.lstat()
        if (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
        ) != identity or (
            visible.st_dev,
            visible.st_ino,
            visible.st_size,
            visible.st_mtime_ns,
        ) != identity:
            raise B15PreflightError(f"{code}_identity_changed")
        return candidate, b"".join(chunks), before
    except B15PreflightError:
        raise
    except OSError as exc:
        raise B15PreflightError(f"{code}_unavailable") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _reject_json_constant(value: str) -> None:
    raise ValueError(value)


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate key")
        result[key] = value
    return result


def _read_json(path: str | Path, *, code: str) -> tuple[Path, dict[str, Any], str]:
    candidate, raw, _info = _read_bytes(path, code=code)
    try:
        value = json.loads(
            raw,
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=_reject_json_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise B15PreflightError(f"{code}_json_invalid") from exc
    if not isinstance(value, dict):
        raise B15PreflightError(f"{code}_shape_invalid")
    return candidate, value, hashlib.sha256(raw).hexdigest()


def _parse_timestamp(value: Any, *, code: str) -> datetime:
    text = str(value or "").strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise B15PreflightError(code) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise B15PreflightError(code)
    return parsed.astimezone(timezone.utc)


def _utc_now() -> datetime:
    """Return the only supported preflight clock: the live UTC clock.

    Deterministic tests patch this private seam; the command line has no clock
    override, so an operator cannot make stale evidence appear fresh.
    """
    return datetime.now(timezone.utc)


def _age_seconds(value: Any, *, now: datetime, code: str) -> float:
    return (now - _parse_timestamp(value, code=code)).total_seconds()


def _load_env(path: Path) -> tuple[dict[str, str], str]:
    candidate, raw, info = _read_bytes(path, code="b15_env")
    if stat.S_IMODE(info.st_mode) & 0o077:
        raise B15PreflightError("b15_env_file_permissions_invalid")
    try:
        text = raw.decode("utf-8")
        parsed = dotenv_values(stream=io.StringIO(text), interpolate=False)
    except (UnicodeDecodeError, ValueError) as exc:
        raise B15PreflightError("b15_env_invalid") from exc
    values = {str(key): str(value) for key, value in parsed.items() if value is not None}
    relevant = {
        *ACTIVATION_ENV_KEYS,
        "HERMES_RCA_KAFKA_SUBMIT_ENABLED",
        "HERMES_RCA_OUTBOX_DISPATCH_ENABLED",
        "HERMES_RCA_OUTBOX_ALLOW_FEISHU_WRITEBACK",
        "HERMES_RCA_DELIVERY_DISPATCHER_ENABLED",
        "HERMES_OUTBOUND_MODE",
        "HERMES_OUTBOUND_RECORD_ROOT",
        "HERMES_OUTBOUND_RECORD_KEY_FILE",
        "HERMES_RCA_KAFKA_NATURAL_CANARY_GATE_PATH",
        "HERMES_RCA_KAFKA_EXACT_RECOVERY_REQUEST_PATH",
    }
    for key in relevant:
        occurrences = re.findall(
            rf"(?m)^\s*(?:export\s+)?{re.escape(key)}\s*=",
            text,
        )
        if len(occurrences) > 1:
            raise B15PreflightError(f"b15_env_duplicate_key:{key}")
    return values, hashlib.sha256(raw).hexdigest()


def _private_directory(path: str, *, code: str) -> dict[str, Any]:
    candidate = _absolute(path, f"{code}_path_invalid")
    try:
        info = candidate.lstat()
    except OSError as exc:
        raise B15PreflightError(f"{code}_unavailable") from exc
    ready = (
        not stat.S_ISLNK(info.st_mode)
        and stat.S_ISDIR(info.st_mode)
        and info.st_uid == os.getuid()
        and stat.S_IMODE(info.st_mode) & 0o077 == 0
    )
    return {"path": str(candidate), "mode": oct(stat.S_IMODE(info.st_mode)), "ready": ready}


def _private_key_file(path: str) -> dict[str, Any]:
    candidate = _absolute(path, "b15_record_key_path_invalid")
    try:
        info = candidate.lstat()
    except OSError as exc:
        raise B15PreflightError("b15_record_key_unavailable") from exc
    ready = (
        not stat.S_ISLNK(info.st_mode)
        and stat.S_ISREG(info.st_mode)
        and info.st_uid == os.getuid()
        and info.st_nlink == 1
        and stat.S_IMODE(info.st_mode) == 0o600
        and 32 <= info.st_size <= 4096
    )
    return {
        "path": str(candidate),
        "mode": oct(stat.S_IMODE(info.st_mode)),
        "size": int(info.st_size),
        "ready": ready,
    }


def _configuration_gate(env: Mapping[str, str]) -> dict[str, Any]:
    activation = {key: env.get(key) for key in ACTIVATION_ENV_KEYS}
    activation_ready = all(value == "true" for value in activation.values())
    submit = {
        "HERMES_RCA_KAFKA_SUBMIT_ENABLED": env.get(
            "HERMES_RCA_KAFKA_SUBMIT_ENABLED"
        ),
        "HERMES_RCA_OUTBOX_DISPATCH_ENABLED": env.get(
            "HERMES_RCA_OUTBOX_DISPATCH_ENABLED"
        ),
    }
    record_root: dict[str, Any]
    record_key: dict[str, Any]
    try:
        record_root = _private_directory(
            str(env.get("HERMES_OUTBOUND_RECORD_ROOT") or ""),
            code="b15_record_root",
        )
    except B15PreflightError as exc:
        record_root = {"path": str(env.get("HERMES_OUTBOUND_RECORD_ROOT") or ""), "ready": False, "error": str(exc)}
    try:
        record_key = _private_key_file(
            str(env.get("HERMES_OUTBOUND_RECORD_KEY_FILE") or "")
        )
    except B15PreflightError as exc:
        record_key = {"path": str(env.get("HERMES_OUTBOUND_RECORD_KEY_FILE") or ""), "ready": False, "error": str(exc)}
    natural_path = str(
        env.get("HERMES_RCA_KAFKA_NATURAL_CANARY_GATE_PATH") or ""
    )
    exact_path = str(
        env.get("HERMES_RCA_KAFKA_EXACT_RECOVERY_REQUEST_PATH") or ""
    )
    kafka_paths_ready = bool(
        natural_path
        and exact_path
        and Path(natural_path).is_absolute()
        and Path(exact_path).is_absolute()
    )
    delivery_disabled = env.get("HERMES_RCA_DELIVERY_DISPATCHER_ENABLED") == "false"
    writeback_value = env.get("HERMES_RCA_OUTBOX_ALLOW_FEISHU_WRITEBACK")
    writeback_inert = writeback_value == "false"
    ready = bool(
        activation_ready
        and all(value == "true" for value in submit.values())
        and env.get("HERMES_OUTBOUND_MODE") == "record-only"
        and record_root.get("ready") is True
        and record_key.get("ready") is True
        and kafka_paths_ready
        and delivery_disabled
        and writeback_inert
    )
    return {
        "ready": ready,
        "activation_required": activation,
        "submission": submit,
        "outbound_mode": env.get("HERMES_OUTBOUND_MODE"),
        "record_root": record_root,
        "record_key": record_key,
        "natural_canary_gate_path": natural_path,
        "exact_recovery_request_path": exact_path,
        "kafka_gate_paths_ready": kafka_paths_ready,
        "delivery_dispatcher_enabled": env.get(
            "HERMES_RCA_DELIVERY_DISPATCHER_ENABLED"
        ),
        "outbox_allow_feishu_writeback": writeback_value,
    }


def _health_observed_at(name: str, health: Mapping[str, Any]) -> str:
    field = str(SERVICE_HEALTH_CONTRACTS[name]["freshness_field"])
    value = health.get(field)
    if not value:
        return ""
    try:
        return _parse_timestamp(value, code="b15_health_time_invalid").isoformat()
    except B15PreflightError:
        return ""


def _health_contract_errors(
    name: str,
    health: Mapping[str, Any],
    *,
    expected_mode: str,
) -> list[str]:
    """Check the safety-critical portion of each resident's health contract."""
    contract = SERVICE_HEALTH_CONTRACTS[name]
    errors: list[str] = []
    if health.get("schema_version") != contract["schema_version"]:
        errors.append("schema_version")
    if health.get("healthy") is not True:
        errors.append("healthy")
    config = health.get("config")
    if not isinstance(config, Mapping):
        errors.append("config_shape")
        config = {}
    if config.get("activation_required") is not True:
        errors.append("config.activation_required")
    if not runtime_identity_is_valid(
        health.get("runtime_identity"),
        service_label=str(contract["service_label"]),
        public_config=config,
    ):
        errors.append("runtime_identity")

    state = str(health.get("state") or "")
    if name == "kafka_consumer":
        if health.get("ok") is not True:
            errors.append("ok")
        if health.get("enabled") is not True:
            errors.append("enabled")
        if health.get("activation_required") is not True:
            errors.append("activation_required")
        if health.get("external_dispatch_wired") is not False:
            errors.append("external_dispatch_wired")
        if config.get("submit_enabled") is not True:
            errors.append("config.submit_enabled")
        if config.get("external_dispatch_wired") is not False:
            errors.append("config.external_dispatch_wired")
        if state != "activation_frozen":
            errors.append("state")
        assignment = health.get("assignment")
        if not isinstance(assignment, Mapping):
            errors.append("assignment_shape")
        elif not assignment.get("assigned_partitions") or assignment.get(
            "callback_errors"
        ) != 0:
            errors.append("assignment")
        store = health.get("store")
        if not isinstance(store, Mapping) or store.get("ok") is not True:
            errors.append("store")
    elif name == "outbox_dispatcher":
        if health.get("ok") is not True:
            errors.append("ok")
        if health.get("enabled") is not True:
            errors.append("enabled")
        if state in {"", "disabled", "error", "circuit_open"}:
            errors.append("state")
        if config.get("dispatch_enabled") is not True:
            errors.append("config.dispatch_enabled")
        if config.get("allow_feishu_writeback") is not False:
            errors.append("config.allow_feishu_writeback")
        store = health.get("store")
        if not isinstance(store, Mapping) or store.get("ok") is not True:
            errors.append("store")
        backpressure = health.get("delivery_backpressure")
        if not isinstance(backpressure, Mapping):
            errors.append("delivery_backpressure")
        workspace = health.get("workspace_runtime")
        if not isinstance(workspace, Mapping) or workspace.get("ready") is not True:
            errors.append("workspace_runtime")
        capacity = health.get("capacity_admission")
        if not isinstance(capacity, Mapping) or capacity.get("ready") is not True:
            errors.append("capacity_admission")
    elif name == "delivery_collector":
        if health.get("enabled") is not True:
            errors.append("enabled")
        if state not in {"idle", "running"}:
            errors.append("state")
        if health.get("external_writes") is not False:
            errors.append("external_writes")
        if config.get("enabled") is not True:
            errors.append("config.enabled")
        if config.get("external_writes") is not False:
            errors.append("config.external_writes")
        if health.get("dependency_error"):
            errors.append("dependency_error")
        if not isinstance(health.get("dependencies"), Mapping):
            errors.append("dependencies")
        store = health.get("store")
        if not isinstance(store, Mapping) or store.get("ok") is not True:
            errors.append("store")
    elif name == "delivery_dispatcher":
        if expected_mode != "disabled" or state != "disabled":
            errors.append("state")
        if config.get("enabled") is not False:
            errors.append("config.enabled")
        if config.get("external_writes") is not False:
            errors.append("config.external_writes")
        store = health.get("store")
        if not isinstance(store, Mapping) or store.get("ok") is not True:
            errors.append("store")
    return errors


def _resident_gate(
    *,
    launch_agents_dir: Path,
    runtime_dir: Path,
    now: datetime,
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    services: dict[str, Any] = {}
    health_documents: dict[str, dict[str, Any]] = {}
    release_roots: set[str] = set()
    for name, plist_name, health_name, expected_mode in SERVICE_SPECS:
        plist_path, plist_raw, _plist_info = _read_bytes(
            launch_agents_dir / plist_name,
            code=f"b15_{name}_plist",
        )
        try:
            plist = plistlib.loads(plist_raw)
        except Exception as exc:
            raise B15PreflightError(f"b15_{name}_plist_invalid") from exc
        arguments = plist.get("ProgramArguments")
        if not isinstance(arguments, list) or len(arguments) < 2:
            raise B15PreflightError(f"b15_{name}_program_invalid")
        try:
            executable_path = _absolute(
                str(arguments[0]),
                f"b15_{name}_executable_path_invalid",
            ).resolve(strict=True)
            working_directory = _absolute(
                str(plist.get("WorkingDirectory") or ""),
                f"b15_{name}_working_directory_invalid",
            ).resolve(strict=True)
        except OSError as exc:
            raise B15PreflightError(f"b15_{name}_runtime_path_unavailable") from exc
        script_path = _absolute(str(arguments[1]), f"b15_{name}_script_path_invalid")
        _script, script_raw, _script_info = _read_bytes(
            script_path,
            code=f"b15_{name}_script",
            max_bytes=32 * 1024 * 1024,
        )
        script_sha = hashlib.sha256(script_raw).hexdigest()
        health_path, health, health_sha = _read_json(
            runtime_dir / health_name,
            code=f"b15_{name}_health",
        )
        health_documents[name] = health
        runtime_identity = health.get("runtime_identity")
        runtime_identity = runtime_identity if isinstance(runtime_identity, Mapping) else {}
        observed_at = _health_observed_at(name, health)
        try:
            age = _age_seconds(
                observed_at,
                now=now,
                code="b15_health_time_invalid",
            )
        except B15PreflightError:
            age = float("inf")
        config = health.get("config")
        config = config if isinstance(config, Mapping) else {}
        state = str(health.get("state") or "")
        if expected_mode == "disabled":
            mode_ready = config.get("enabled") is False and state == "disabled"
        else:
            mode_ready = state not in {"", "starting", "stopped", "error", "circuit_open"}
        identity_ready = bool(
            plist.get("Label")
            == SERVICE_HEALTH_CONTRACTS[name]["service_label"]
            and runtime_identity.get("executable") == str(executable_path)
            and runtime_identity.get("cwd") == str(working_directory)
            and working_directory == script_path.parent.parent
            and runtime_identity.get("script") == str(script_path)
            and runtime_identity.get("script_sha256") == script_sha
            and HEX64_RE.fullmatch(
                str(runtime_identity.get("loaded_runtime_sha256") or "")
            )
        )
        contract_errors = _health_contract_errors(
            name,
            health,
            expected_mode=expected_mode,
        )
        contract_ready = not contract_errors
        ready = bool(
            identity_ready
            and contract_ready
            and mode_ready
            and -5 <= age <= MAX_TTL_SECONDS
        )
        release_root = str(script_path.parent.parent)
        release_roots.add(release_root)
        services[name] = {
            "ready": ready,
            "mode_ready": mode_ready,
            "identity_ready": identity_ready,
            "contract_ready": contract_ready,
            "contract_errors": contract_errors,
            "state": state,
            "freshness_field": SERVICE_HEALTH_CONTRACTS[name]["freshness_field"],
            "observed_at": observed_at,
            "age_seconds": age if age != float("inf") else None,
            "plist_path": str(plist_path),
            "plist_sha256": hashlib.sha256(plist_raw).hexdigest(),
            "executable_path": str(executable_path),
            "working_directory": str(working_directory),
            "script_path": str(script_path),
            "script_sha256": script_sha,
            "health_path": str(health_path),
            "health_sha256": health_sha,
            "loaded_runtime_sha256": str(
                runtime_identity.get("loaded_runtime_sha256") or ""
            ),
            "public_config_sha256": str(
                runtime_identity.get("public_config_sha256") or ""
            ),
        }
    services["release_binding"] = {
        "ready": len(release_roots) == 1,
        "release_roots": sorted(release_roots),
        "aggregate_sha256": canonical_json_sha256(
            {
                name: {
                    "script_path": value["script_path"],
                    "script_sha256": value["script_sha256"],
                    "loaded_runtime_sha256": value["loaded_runtime_sha256"],
                }
                for name, value in services.items()
                if isinstance(value, Mapping) and "script_path" in value
            }
        ),
    }
    services["ready"] = bool(
        services["release_binding"]["ready"]
        and all(services[name]["ready"] for name, *_rest in SERVICE_SPECS)
    )
    return services, health_documents


def _kafka_fence_gate(
    consumer_health: Mapping[str, Any],
    *,
    env: Mapping[str, str],
    now: datetime,
) -> dict[str, Any]:
    config = consumer_health.get("config")
    config = config if isinstance(config, Mapping) else {}
    freeze = consumer_health.get("activation_freeze")
    freeze = freeze if isinstance(freeze, Mapping) else {}
    positions = freeze.get("partition_positions")
    positions_ready = bool(
        isinstance(positions, Mapping)
        and positions
        and all(
            isinstance(partitions, Mapping)
            and partitions
            and all(
                str(partition).isdigit()
                and isinstance(offset, int)
                and not isinstance(offset, bool)
                and offset >= 0
                for partition, offset in partitions.items()
            )
            for partitions in positions.values()
        )
    )
    try:
        age = _age_seconds(
            freeze.get("observed_at"),
            now=now,
            code="b15_kafka_freeze_time_invalid",
        )
    except B15PreflightError:
        age = float("inf")
    runtime_identity = consumer_health.get("runtime_identity")
    runtime_identity = runtime_identity if isinstance(runtime_identity, Mapping) else {}
    identity_sha = canonical_json_sha256(dict(runtime_identity))
    ready = bool(
        freeze.get("schema_version") == "pnc_rca_activation_ingress_freeze_v2"
        and bool(str(freeze.get("epoch_id") or ""))
        and freeze.get("state") == "partitions_paused"
        and HEX64_RE.fullmatch(str(freeze.get("freeze_token") or ""))
        and freeze.get("restart_required") is False
        and freeze.get("consumer_runtime_identity_sha256") == identity_sha
        and positions_ready
        and -5 <= age <= MAX_TTL_SECONDS
        and config.get("natural_canary_gate_path")
        == env.get("HERMES_RCA_KAFKA_NATURAL_CANARY_GATE_PATH")
        and config.get("exact_recovery_request_path")
        == env.get("HERMES_RCA_KAFKA_EXACT_RECOVERY_REQUEST_PATH")
    )
    return {
        "ready": ready,
        "state": freeze.get("state"),
        "epoch_id": freeze.get("epoch_id"),
        "observed_at": freeze.get("observed_at"),
        "age_seconds": age if age != float("inf") else None,
        "partition_positions": positions if isinstance(positions, Mapping) else {},
        "runtime_identity_sha256": identity_sha,
        "natural_canary_gate_path": config.get("natural_canary_gate_path"),
        "exact_recovery_request_path": config.get("exact_recovery_request_path"),
    }


def _sqlite_tables(conn: sqlite3.Connection) -> set[str]:
    return {
        str(row[0])
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }


def _database_gate(path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    db_path = _absolute(path, "b15_control_db_path_invalid")
    try:
        before = db_path.lstat()
    except OSError as exc:
        raise B15PreflightError("b15_control_db_unavailable") from exc
    if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
        raise B15PreflightError("b15_control_db_invalid")
    uri = f"file:{quote(str(db_path), safe='/')}?mode=ro"
    conn = sqlite3.connect(uri, uri=True, isolation_level=None)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA query_only=ON")
        conn.execute("BEGIN")
        query_only = int(conn.execute("PRAGMA query_only").fetchone()[0]) == 1
        tables = _sqlite_tables(conn)
        marker = conn.execute(
            "SELECT value FROM control_meta WHERE key='schema_version'"
        ).fetchone()
        schema_version = str(marker[0]) if marker is not None else ""
        required_hold_tables = {
            "rca_activation_historical_outbox_holds",
            "rca_activation_historical_outbox_hold_items",
            "rca_activation_historical_outbox_dispositions",
            "rca_activation_historical_outbox_disposition_items",
        }
        epoch_row: sqlite3.Row | None = None
        epoch: Mapping[str, Any] | None = None
        hold: Mapping[str, Any] | None = None
        hold_items = 0
        held_pending = 0
        if "rca_activation_epochs" in tables:
            current_epochs = conn.execute(
                "SELECT * FROM rca_activation_epochs WHERE is_current=1"
            ).fetchall()
            if len(current_epochs) == 1:
                epoch_row = current_epochs[0]
                epoch = {
                    "epoch_id": str(epoch_row["epoch_id"]),
                    "state": str(epoch_row["state"]),
                }
        if epoch is not None and required_hold_tables.issubset(tables):
            row = conn.execute(
                "SELECT epoch_id,schema_version,partition_start_fence_sha256,"
                "cohort_count,cohort_sha256,sealed_at "
                "FROM rca_activation_historical_outbox_holds WHERE epoch_id=?",
                (epoch["epoch_id"],),
            ).fetchone()
            hold = dict(row) if row is not None else None
            if hold is not None:
                hold_items = int(
                    conn.execute(
                        "SELECT COUNT(*) FROM "
                        "rca_activation_historical_outbox_hold_items WHERE epoch_id=?",
                        (epoch["epoch_id"],),
                    ).fetchone()[0]
                )
                held_pending = int(
                    conn.execute(
                        "SELECT COUNT(*) FROM "
                        "rca_activation_historical_outbox_hold_items AS held "
                        "JOIN rca_outbox AS outbox ON outbox.outbox_id=held.outbox_id "
                        "WHERE held.epoch_id=? AND outbox.status='pending'",
                        (epoch["epoch_id"],),
                    ).fetchone()[0]
                )
        hold_integrity: dict[str, Any] = {}
        hold_integrity_error = ""
        if schema_version == CONTROL_STORE_SCHEMA_VERSION and epoch_row is not None:
            try:
                RcaControlStore._validate_v13_historical_outbox_hold_schema(conn)
                hold_integrity = (
                    RcaControlStore._historical_outbox_hold_evidence_tx(
                        conn,
                        epoch=epoch_row,
                        allow_current_epoch=True,
                    )
                )
            except (
                ActivationEpochError,
                RuntimeError,
                TypeError,
                ValueError,
                sqlite3.Error,
                KeyError,
                IndexError,
            ) as exc:
                hold_integrity_error = str(exc)
        epoch_state_ready = bool(
            epoch is not None
            and epoch.get("state") in {"preauthorized", "bounded_active"}
        )
        hold_ready = bool(
            schema_version == CONTROL_STORE_SCHEMA_VERSION
            and query_only
            and epoch is not None
            and epoch_state_ready
            and hold is not None
            and hold_integrity_error == ""
            and hold_integrity.get("schema_version")
            == ACTIVATION_HISTORICAL_OUTBOX_HOLD_SCHEMA_VERSION
            and hold_integrity.get("disposed") is False
            and hold_integrity.get("matches") is True
            and int(hold_integrity.get("sealed_count", -1)) > 0
            and int(hold_integrity.get("sealed_count", -1)) == hold_items
            and int(hold_integrity.get("current_count", -1)) == held_pending
        )
        circuit: dict[str, Any] = {}
        if "rca_dispatcher_circuit" in tables:
            row = conn.execute(
                "SELECT state,reason_code,reason_detail,opened_at,updated_at "
                "FROM rca_dispatcher_circuit WHERE circuit_name='submission'"
            ).fetchone()
            circuit = dict(row) if row is not None else {}
        circuit_ready = circuit.get("state") == "closed"
        effect_rows: list[dict[str, Any]] = []
        if "rca_delivery_effects" in tables:
            effect_rows = [
                dict(row)
                for row in conn.execute(
                    "SELECT effect_kind,status,write_phase,COUNT(*) AS count "
                    "FROM rca_delivery_effects "
                    "GROUP BY effect_kind,status,write_phase "
                    "ORDER BY effect_kind,status,write_phase"
                ).fetchall()
            ]
        attempt_count = (
            int(conn.execute("SELECT COUNT(*) FROM rca_delivery_attempts").fetchone()[0])
            if "rca_delivery_attempts" in tables
            else 0
        )
        conn.rollback()
        try:
            after = db_path.lstat()
        except OSError as exc:
            raise B15PreflightError("b15_control_db_identity_changed") from exc
        if (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
        ) != (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
        ):
            raise B15PreflightError("b15_control_db_identity_changed")
    except sqlite3.Error as exc:
        if conn.in_transaction:
            conn.rollback()
        raise B15PreflightError("b15_control_db_query_failed") from exc
    finally:
        conn.close()
    identity = {
        "path": str(db_path),
        "device": int(before.st_dev),
        "inode": int(before.st_ino),
        "size": int(before.st_size),
        "mtime_ns": int(before.st_mtime_ns),
    }
    database = {
        "ready": bool(hold_ready and circuit_ready),
        "query_only": query_only,
        "schema_version": schema_version,
        "expected_schema_version": CONTROL_STORE_SCHEMA_VERSION,
        "identity": identity,
        "current_epoch": dict(epoch) if epoch is not None else None,
        "current_epoch_state_ready": epoch_state_ready,
        "historical_hold": dict(hold) if hold is not None else None,
        "historical_hold_item_count": hold_items,
        "historical_held_pending_count": held_pending,
        "historical_hold_integrity": hold_integrity,
        "historical_hold_integrity_error": hold_integrity_error,
        "historical_hold_ready": hold_ready,
        "submission_circuit": circuit,
        "submission_circuit_ready": circuit_ready,
    }
    baseline = {
        "schema_version": "pnc_rca_external_effect_baseline_v1",
        "control_db_identity": identity,
        "effect_groups": effect_rows,
        "delivery_attempt_count": attempt_count,
    }
    baseline["baseline_sha256"] = canonical_json_sha256(baseline)
    return database, baseline


def _snapshot_gate(path: Path, *, now: datetime) -> dict[str, Any]:
    try:
        snapshot_path, document, raw_sha = _read_json(
            path, code="b15_resource_snapshot"
        )
    except B15PreflightError as exc:
        return {
            "ready": False,
            "error_code": str(exc),
            "path": str(path),
            "raw_sha256": "",
            "snapshot_sha256": "",
            "observed_at": None,
            "age_seconds": None,
            "max_ttl_seconds": MAX_TTL_SECONDS,
        }
    snapshot = document.get("rca_prod_snapshot", document)
    if not isinstance(snapshot, Mapping):
        return {
            "ready": False,
            "error_code": "b15_resource_snapshot_shape_invalid",
            "path": str(snapshot_path),
            "raw_sha256": raw_sha,
            "snapshot_sha256": "",
            "observed_at": None,
            "age_seconds": None,
            "max_ttl_seconds": MAX_TTL_SECONDS,
        }
    error_code = ""
    try:
        _validate_snapshot(
            snapshot,
            now=now,
            modeled_root_bytes=MIN_ROOT_AVAILABLE_BYTES,
            modeled_delivery_bytes=MIN_DELIVERY_AVAILABLE_BYTES,
        )
        ready = True
    except RcaProdAdmissionError as exc:
        ready = False
        error_code = exc.code
    try:
        age = _age_seconds(
            snapshot.get("observed_at"),
            now=now,
            code="b15_resource_snapshot_time_invalid",
        )
    except B15PreflightError:
        age = None
    return {
        "ready": ready,
        "error_code": error_code,
        "path": str(snapshot_path),
        "raw_sha256": raw_sha,
        "snapshot_sha256": canonical_json_sha256(dict(snapshot)),
        "observed_at": snapshot.get("observed_at"),
        "age_seconds": age,
        "max_ttl_seconds": MAX_TTL_SECONDS,
    }


def _write_all(descriptor: int, raw: bytes) -> None:
    view = memoryview(raw)
    while view:
        written = os.write(descriptor, view)
        if written <= 0:
            raise OSError("short write")
        view = view[written:]


def _create_immutable_file(path: Path, raw: bytes) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    descriptor = -1
    try:
        descriptor = os.open(
            temporary,
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0),
            0o400,
        )
        _write_all(descriptor, raw)
        os.fsync(descriptor)
        os.fchmod(descriptor, 0o444)
        os.close(descriptor)
        descriptor = -1
        try:
            os.link(temporary, path, follow_symlinks=False)
        except FileExistsError as exc:
            raise B15PreflightError("b15_receipt_destination_exists") from exc
        temporary.unlink()
        directory = os.open(
            path.parent,
            os.O_RDONLY
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0),
        )
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    except B15PreflightError:
        raise
    except OSError as exc:
        raise B15PreflightError("b15_receipt_write_failed") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _write_receipt(path: Path, value: Mapping[str, Any]) -> str:
    target = _absolute(path, "b15_receipt_path_invalid")
    try:
        parent = target.parent.lstat()
    except OSError as exc:
        raise B15PreflightError("b15_receipt_parent_invalid") from exc
    if (
        stat.S_ISLNK(parent.st_mode)
        or not stat.S_ISDIR(parent.st_mode)
        or parent.st_uid != os.getuid()
        or stat.S_IMODE(parent.st_mode) & 0o022
        or Path(os.path.realpath(target.parent)) != target.parent
        or os.path.lexists(target)
        or os.path.lexists(f"{target}.sha256")
    ):
        raise B15PreflightError("b15_receipt_destination_invalid")
    raw = json.dumps(
        dict(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    _create_immutable_file(target, raw)
    digest = hashlib.sha256(raw).hexdigest()
    sidecar = Path(f"{target}.sha256")
    _create_immutable_file(sidecar, f"{digest}  {target.name}\n".encode("ascii"))
    return digest


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--runtime-dir", type=Path, required=True)
    parser.add_argument("--launch-agents-dir", type=Path, required=True)
    parser.add_argument("--control-db", type=Path, required=True)
    parser.add_argument("--resource-snapshot", type=Path, required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    return parser


def run_preflight(args: argparse.Namespace) -> tuple[dict[str, Any], str]:
    now = _utc_now()
    env_path = _absolute(args.env_file, "b15_env_path_invalid")
    runtime_dir = _absolute(args.runtime_dir, "b15_runtime_dir_invalid")
    launch_agents_dir = _absolute(
        args.launch_agents_dir, "b15_launch_agents_dir_invalid"
    )
    try:
        env, env_sha = _load_env(env_path)
    except B15PreflightError:
        env, env_sha = {}, ""
    config_gate = _configuration_gate(env)
    try:
        resident_gate, health = _resident_gate(
            launch_agents_dir=launch_agents_dir,
            runtime_dir=runtime_dir,
            now=now,
        )
    except B15PreflightError as exc:
        resident_gate = {
            "ready": False,
            "error_code": str(exc),
            "services": {},
        }
        health = {}
    kafka_gate = _kafka_fence_gate(
        health.get("kafka_consumer", {}),
        env=env,
        now=now,
    )
    try:
        database_gate, effect_baseline = _database_gate(args.control_db)
    except B15PreflightError as exc:
        database_gate = {"ready": False, "error_code": str(exc)}
        effect_baseline = {
            "schema_version": "pnc_rca_external_effect_baseline_v1",
            "error_code": str(exc),
        }
        effect_baseline["baseline_sha256"] = canonical_json_sha256(effect_baseline)
    current_epoch = database_gate.get("current_epoch")
    kafka_epoch_binding_ready = bool(
        isinstance(current_epoch, Mapping)
        and str(current_epoch.get("epoch_id") or "")
        == str(kafka_gate.get("epoch_id") or "")
        and str(kafka_gate.get("epoch_id") or "")
    )
    database_gate["kafka_epoch_binding_ready"] = kafka_epoch_binding_ready
    database_gate["ready"] = bool(
        database_gate.get("ready") is True and kafka_epoch_binding_ready
    )
    snapshot_gate = _snapshot_gate(args.resource_snapshot, now=now)
    gates = {
        "configuration": config_gate,
        "resident_runtime": resident_gate,
        "historical_outbox_and_circuit": database_gate,
        "kafka_freeze": kafka_gate,
        "resource_snapshot": snapshot_gate,
    }
    ready = all(gate.get("ready") is True for gate in gates.values())
    receipt: dict[str, Any] = {
        "schema_version": B15_PREFLIGHT_SCHEMA_VERSION,
        "observed_at": now.isoformat(),
        "mode": "read_only",
        "runtime_mutation_performed": False,
        "ready": ready,
        "status": "GREEN" if ready else "RED",
        "inputs": {
            "env_file": str(env_path),
            "env_file_sha256": env_sha,
            "runtime_dir": str(runtime_dir),
            "launch_agents_dir": str(launch_agents_dir),
            "control_db": str(_absolute(args.control_db, "b15_control_db_path_invalid")),
            "resource_snapshot": str(
                _absolute(args.resource_snapshot, "b15_resource_snapshot_path_invalid")
            ),
        },
        "gates": gates,
        "external_effect_baseline": effect_baseline,
    }
    receipt["receipt_fingerprint"] = canonical_json_sha256(receipt)
    receipt_sha = _write_receipt(args.receipt, receipt)
    return receipt, receipt_sha


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    try:
        receipt, receipt_sha = run_preflight(args)
        print(
            json.dumps(
                {
                    "ok": receipt["ready"],
                    "status": receipt["status"],
                    "receipt": str(args.receipt),
                    "receipt_sha256": receipt_sha,
                    "receipt_fingerprint": receipt["receipt_fingerprint"],
                    "gates": {
                        name: gate.get("ready")
                        for name, gate in receipt["gates"].items()
                    },
                    "external_effect_baseline_sha256": receipt[
                        "external_effect_baseline"
                    ]["baseline_sha256"],
                },
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
        )
        return 0 if receipt["ready"] else 2
    except Exception as exc:
        print(
            json.dumps(
                {
                    "ok": False,
                    "error": type(exc).__name__,
                    "message": str(exc),
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
