#!/usr/bin/env python3
"""Plan, stage, or validate an RCA production environment candidate.

This tool never writes the canonical Hermes environment.  It preserves the
input dotenv document, normalizes the approved RCA production keys, and binds
the candidate bytes to the release-prepare manifest and approval receipt.
"""

from __future__ import annotations

import argparse
import base64
import binascii
import errno
import hashlib
import io
import json
import os
import re
import secrets
import stat
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from dotenv.parser import parse_stream

from gateway import pnc_rca_prod_bootstrap as prod_bootstrap


ENV_STAGE_PLAN_SCHEMA_VERSION = "pnc_rca_production_env_stage_plan_v1"
ENV_STAGE_RECEIPT_SCHEMA_VERSION = "pnc_rca_production_env_stage_receipt_v1"
RELEASE_PREPARE_MANIFEST_SCHEMA_VERSION = "pnc_rca_release_prepare_manifest_v1"
RELEASE_PREPARE_RUN_IDENTITY_SCHEMA_VERSION = (
    "pnc_rca_release_prepare_run_identity_v1"
)
RELEASE_APPROVAL_SCHEMA_VERSION = "pnc_rca_release_approval_v1"
RELEASE_APPROVAL_IDENTITY_SCHEMA_VERSION = "pnc_rca_release_approval_identity_v1"
RELEASE_APPROVAL_DECISION = "authorize_rca_production_cutover_plan"
RELEASE_APPROVAL_IDENTITY_METHOD = "kernel_owner_and_machine_binding"
RELEASE_PREPARE_MANIFEST_NAME = "release_prepare_manifest.json"
RELEASE_PREPARE_RUN_IDENTITY_NAME = "run_identity.json"
MAX_ENV_BYTES = 1024 * 1024
MAX_JSON_BYTES = 256 * 1024
MAX_APPROVAL_VALIDITY_SECONDS = 24 * 60 * 60
MAX_FUTURE_SKEW_SECONDS = 300
KEY_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
SHA256_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
NONCE_PATTERN = re.compile(r"[A-Za-z0-9_-]{16,128}\Z")
CHAT_ID_PATTERN = re.compile(r"oc_[A-Za-z0-9_-]{1,255}\Z")
USER_ID_PATTERN = re.compile(r"ou_[A-Za-z0-9_-]{1,255}\Z")
SAFE_LITERAL_PATTERN = re.compile(r"[^\x00\r\n\t #]+\Z")

CANONICAL_LIVE_ENV = Path.home() / ".hermes" / ".env"
FIXED_SERVICE_ID = "root_cause_analysis_agent"
FIXED_KAFKA_PRINCIPAL = "rca"
FIXED_KAFKA_GROUP_ID = "rca_root_cause_analysis_agent"
RELEASE_ACTION_SET = (
    "promote_host_candidate",
    "promote_scoped_workspace_closure",
    "promote_vm_candidate",
    "promote_vm_worker_candidate",
    "install_candidate_launchd_plists",
    "apply_runtime_configuration",
    "migrate_rca_stores",
    "start_rca_resident_services",
    "run_bounded_kafka_canary",
    "run_manual_success_canary",
    "run_manual_terminal_failure_canary",
    "confirm_rca_production",
    "transition_rca_steady",
    "rollback_rca_release",
)
ALLOWED_MANUAL_CHAT_IDS = frozenset(
    {
        "oc_16614f4ba25b8c88b69c0b8e9ebc2fb5",
        "oc_6cfc782212009ff4cd815349909dd423",
    }
)

PASSTHROUGH_REQUIRED_KEYS = frozenset(
    {
        "HERMES_RCA_KAFKA_BOOTSTRAP_SERVERS",
        "HERMES_RCA_KAFKA_USER",
        "HERMES_RCA_KAFKA_EXPECTED_CLUSTER_ID",
        "HERMES_RCA_KAFKA_PASSWORD",
        "HERMES_RCA_KAFKA_MIN_REPLICATION_FACTOR",
        "HERMES_RCA_KAFKA_START_OFFSETS_JSON",
        "HERMES_RCA_KAFKA_CREATION_RULE_VERSION",
        "HERMES_RCA_KAFKA_PROJECT_KEYS",
        "HERMES_RCA_KAFKA_PROJECT_SIMPLE_NAMES",
        "HERMES_RCA_KAFKA_WORK_ITEM_TYPE_KEYS",
        "HERMES_RCA_KAFKA_STATUS_CHANGE_TYPES",
        "HERMES_RCA_KAFKA_STATE_TRANSITIONS_JSON",
        "HERMES_RCA_PROD_ADMISSION_HMAC_KEY",
        "HERMES_RCA_PROD_CAPACITY_MODE",
        "HERMES_RCA_PROD_RELEASE_ID",
        "HERMES_RCA_PROD_BOOTSTRAP_EPOCH_ID",
        "HERMES_RCA_MANUAL_OPERATOR_ENABLED",
        "HERMES_RCA_MANUAL_OPERATOR_USER_IDS",
        "HERMES_RCA_MANUAL_OPERATOR_RATE_LIMIT",
        "HERMES_RCA_MANUAL_OPERATOR_RATE_WINDOW_SECONDS",
    }
)

FIXED_PRODUCTION_VALUES = {
    "HERMES_RCA_KAFKA_GROUP": FIXED_KAFKA_GROUP_ID,
    "HERMES_RCA_KAFKA_CLIENT_ID": FIXED_SERVICE_ID,
    "HERMES_RCA_KAFKA_API_VERSION": "3.9.0",
    "HERMES_RCA_KAFKA_REQUEST_TIMEOUT_MS": "120000",
    "HERMES_RCA_KAFKA_SESSION_TIMEOUT_MS": "30000",
    "HERMES_RCA_KAFKA_MAX_POLL_INTERVAL_MS": "300000",
    "HERMES_RCA_KAFKA_POLL_TIMEOUT_MS": "1000",
    "HERMES_RCA_KAFKA_MAX_POLL_RECORDS": "10",
    "HERMES_RCA_KAFKA_OFFSET_LOOKUP_TIMEOUT_MS": "3000",
    "HERMES_RCA_KAFKA_OUTBOX_HIGH_WATERMARK": "100",
    "HERMES_RCA_KAFKA_OUTBOX_RESUME_WATERMARK": "50",
    "HERMES_RCA_KAFKA_SECURITY_PROTOCOL": "SASL_PLAINTEXT",
    "HERMES_RCA_KAFKA_SASL_MECHANISM": "PLAIN",
    "HERMES_RCA_KAFKA_ISOLATION_LEVEL": "read_committed",
    "HERMES_RCA_KAFKA_AUTO_OFFSET_RESET": "none",
    "HERMES_RCA_KAFKA_SUBMIT_ENABLED": "true",
    "HERMES_RCA_KAFKA_ACTIVATION_REQUIRED": "true",
    "HERMES_RCA_MANUAL_INTAKE_ENABLED": "true",
    "HERMES_RCA_OUTBOX_DISPATCH_ENABLED": "true",
    "HERMES_RCA_OUTBOX_ACTIVATION_REQUIRED": "true",
    "HERMES_RCA_DELIVERY_COLLECTOR_ENABLED": "true",
    "HERMES_RCA_DELIVERY_COLLECTOR_ACTIVATION_REQUIRED": "true",
    "HERMES_RCA_DELIVERY_DISPATCHER_ENABLED": "true",
    "HERMES_RCA_DELIVERY_DISPATCHER_ACTIVATION_REQUIRED": "true",
    "HERMES_RCA_ACTIVATION_REQUIRED": "true",
    "HERMES_RCA_OUTBOX_SERVICE_ID": FIXED_SERVICE_ID,
    "HERMES_RCA_OUTBOX_DATA_ACCESS_MODE": "remote_read",
    "HERMES_RCA_OUTBOX_ALLOW_DOWNLOAD": "false",
    "HERMES_RCA_OUTBOX_ALLOW_FEISHU_WRITEBACK": "false",
    "HERMES_RCA_OUTBOX_STORAGE_ADMISSION_ENABLED": "true",
    "HERMES_RCA_OUTBOX_STORAGE_RESERVATION_ENABLED": "false",
    "HERMES_RCA_OUTBOX_DERIVED_CAPACITY_RESERVATION_ENABLED": "true",
    "HERMES_RCA_OUTBOX_DELIVERY_BACKPRESSURE_ENABLED": "true",
    "HERMES_RCA_PROD_CAPACITY_MODE": "bootstrap",
    "HERMES_RCA_LEGACY_AUTO_EXECUTION_DISABLED": "true",
    "HERMES_G1Q3_AUTO_DOWNLOAD_DAILY_QUOTA": "0",
    "G1Q3_GOVERNANCE_DOWNLOAD_ENABLED": "false",
    "HERMES_G1Q3_ISSUE_CAPTURE_ENABLED": "false",
}

STATE_PATH_NAMES = {
    "HERMES_RCA_KAFKA_CONTROL_DB_PATH": "control.sqlite3",
    "HERMES_RCA_KAFKA_HEALTH_PATH": "consumer_health.json",
    "HERMES_RCA_OUTBOX_CONTROL_DB_PATH": "control.sqlite3",
    "HERMES_RCA_OUTBOX_DELIVERY_DB_PATH": "control.sqlite3",
    "HERMES_RCA_OUTBOX_HEALTH_PATH": "outbox_dispatcher_health.json",
    "HERMES_RCA_DELIVERY_COLLECTOR_CONTROL_DB_PATH": "control.sqlite3",
    "HERMES_RCA_DELIVERY_COLLECTOR_HEALTH_PATH": "delivery_collector_health.json",
    "HERMES_RCA_DELIVERY_DISPATCHER_CONTROL_DB_PATH": "control.sqlite3",
    "HERMES_RCA_DELIVERY_DISPATCHER_HEALTH_PATH": "delivery_dispatcher_health.json",
}


class ProductionEnvStageError(ValueError):
    """A production environment staging invariant failed closed."""

    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


@dataclass(frozen=True)
class _OwnedFile:
    path: Path
    raw: bytes
    stat_result: os.stat_result

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.raw).hexdigest()


@dataclass(frozen=True)
class _DotenvDocument:
    owned: _OwnedFile
    values: Mapping[str, str]
    originals: tuple[tuple[str | None, str], ...]


@dataclass(frozen=True)
class StageInputs:
    input_env: Path
    release_prepare_manifest: Path
    approval_receipt: Path
    output_env: Path
    receipt: Path
    runtime_state_root: Path
    expected_topic: str


@dataclass(frozen=True)
class StageResult:
    phase: str
    body: Mapping[str, Any]
    output_written: bool = False
    receipt_written: bool = False


def _canonical_json(value: Any) -> bytes:
    try:
        raw = json.dumps(
            value,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ProductionEnvStageError("production_env_stage_json_invalid") from exc
    return raw + b"\n"


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).rstrip(b"\n")).hexdigest()


def _require_sha256(value: Any, *, code: str) -> str:
    if not isinstance(value, str) or SHA256_PATTERN.fullmatch(value) is None:
        raise ProductionEnvStageError(code)
    return value


def _read_owner_only(path: Path, *, artifact: str, maximum: int) -> _OwnedFile:
    candidate = path.expanduser().absolute()
    if not hasattr(os, "O_NOFOLLOW"):
        raise ProductionEnvStageError(f"{artifact}_no_follow_unavailable")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | os.O_NOFOLLOW
    try:
        descriptor = os.open(candidate, flags)
    except OSError as exc:
        raise ProductionEnvStageError(f"{artifact}_unavailable") from exc
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or stat.S_IMODE(before.st_mode) != 0o600
            or before.st_uid != os.geteuid()
            or before.st_nlink != 1
        ):
            raise ProductionEnvStageError(f"{artifact}_not_owner_only")
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = os.read(descriptor, min(64 * 1024, maximum + 1 - total))
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
            if total > maximum:
                raise ProductionEnvStageError(f"{artifact}_size_invalid")
        after = os.fstat(descriptor)
        try:
            lexical = os.lstat(candidate)
        except OSError as exc:
            raise ProductionEnvStageError(f"{artifact}_unstable") from exc
        if (
            stat.S_ISLNK(lexical.st_mode)
            or (after.st_dev, after.st_ino) != (lexical.st_dev, lexical.st_ino)
            or (
                before.st_dev,
                before.st_ino,
                before.st_mode,
                before.st_uid,
                before.st_nlink,
                before.st_size,
                before.st_mtime_ns,
                before.st_ctime_ns,
            )
            != (
                after.st_dev,
                after.st_ino,
                after.st_mode,
                after.st_uid,
                after.st_nlink,
                after.st_size,
                after.st_mtime_ns,
                after.st_ctime_ns,
            )
        ):
            raise ProductionEnvStageError(f"{artifact}_unstable")
        return _OwnedFile(candidate, b"".join(chunks), after)
    finally:
        os.close(descriptor)


def _strict_json(owned: _OwnedFile, *, artifact: str) -> Mapping[str, Any]:
    def unique(items: list[tuple[str, Any]]) -> dict[str, Any]:
        body: dict[str, Any] = {}
        for key, value in items:
            if key in body:
                raise ProductionEnvStageError(f"{artifact}_duplicate_key")
            body[key] = value
        return body

    try:
        value = json.loads(
            owned.raw.decode("utf-8"),
            object_pairs_hook=unique,
            parse_constant=lambda _value: (_ for _ in ()).throw(
                ProductionEnvStageError(f"{artifact}_number_invalid")
            ),
        )
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ProductionEnvStageError(f"{artifact}_json_invalid") from exc
    if not isinstance(value, dict):
        raise ProductionEnvStageError(f"{artifact}_shape_invalid")
    return value


def _parse_dotenv(
    path: Path, *, artifact: str = "production_env_input"
) -> _DotenvDocument:
    owned = _read_owner_only(path, artifact=artifact, maximum=MAX_ENV_BYTES)
    try:
        text = owned.raw.decode("utf-8")
    except UnicodeError as exc:
        raise ProductionEnvStageError(f"{artifact}_utf8_invalid") from exc
    if text.startswith("\ufeff") or "\x00" in text:
        raise ProductionEnvStageError(f"{artifact}_encoding_invalid")
    values: dict[str, str] = {}
    originals: list[tuple[str | None, str]] = []
    try:
        bindings = tuple(parse_stream(io.StringIO(text)))
    except Exception as exc:
        raise ProductionEnvStageError(f"{artifact}_syntax_invalid") from exc
    for binding in bindings:
        if binding.error:
            raise ProductionEnvStageError(f"{artifact}_syntax_invalid")
        key = binding.key
        original = binding.original.string
        originals.append((key, original))
        if key is None:
            continue
        if KEY_PATTERN.fullmatch(key) is None:
            raise ProductionEnvStageError(f"{artifact}_key_invalid")
        if key in values:
            raise ProductionEnvStageError(f"{artifact}_duplicate_key")
        if binding.value is None:
            raise ProductionEnvStageError(f"{artifact}_value_missing")
        values[key] = binding.value
    if not values:
        raise ProductionEnvStageError(f"{artifact}_empty")
    return _DotenvDocument(owned, values, tuple(originals))


def _secure_parent(path: Path, *, artifact: str) -> Path:
    if not path.is_absolute():
        raise ProductionEnvStageError(f"{artifact}_path_not_absolute")
    parent = path.parent
    try:
        info = os.lstat(parent)
    except OSError as exc:
        raise ProductionEnvStageError(f"{artifact}_parent_unavailable") from exc
    if (
        stat.S_ISLNK(info.st_mode)
        or not stat.S_ISDIR(info.st_mode)
        or info.st_uid != os.geteuid()
        or stat.S_IMODE(info.st_mode) != 0o700
    ):
        raise ProductionEnvStageError(f"{artifact}_parent_not_owner_only")
    return parent


def _resolved(path: Path) -> Path:
    try:
        return path.expanduser().absolute().resolve(strict=False)
    except OSError as exc:
        raise ProductionEnvStageError("production_env_stage_path_unresolvable") from exc


def _validate_paths(inputs: StageInputs) -> tuple[Path, Path, Path]:
    output = inputs.output_env.expanduser()
    receipt = inputs.receipt.expanduser()
    state_root = inputs.runtime_state_root.expanduser()
    if (
        not output.is_absolute()
        or not receipt.is_absolute()
        or not state_root.is_absolute()
    ):
        raise ProductionEnvStageError("production_env_stage_path_not_absolute")
    output = output.absolute()
    receipt = receipt.absolute()
    state_root = state_root.absolute()
    _secure_parent(output, artifact="production_env_output")
    _secure_parent(receipt, artifact="production_env_receipt")
    protected_sources = {
        _resolved(inputs.input_env),
        _resolved(inputs.release_prepare_manifest),
        _resolved(inputs.approval_receipt),
        _resolved(inputs.release_prepare_manifest.expanduser().absolute().parent
                  / RELEASE_PREPARE_RUN_IDENTITY_NAME),
        _resolved(CANONICAL_LIVE_ENV),
    }
    if (
        output == receipt
        or _resolved(output) in protected_sources
        or _resolved(receipt) in protected_sources
        or output.name == RELEASE_PREPARE_MANIFEST_NAME
    ):
        raise ProductionEnvStageError("production_env_stage_live_or_alias_path_forbidden")
    if any(character in str(state_root) for character in ("\x00", "\r", "\n")):
        raise ProductionEnvStageError("production_env_stage_state_root_invalid")
    return output, receipt, state_root


def _parse_timestamp(value: Any, *, code: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ProductionEnvStageError(code)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ProductionEnvStageError(code) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ProductionEnvStageError(code)
    return parsed.astimezone(timezone.utc)


def _validate_release_binding(
    *,
    document: _DotenvDocument,
    manifest_path: Path,
    approval_path: Path,
    now: datetime,
) -> Mapping[str, Any]:
    manifest_owned = _read_owner_only(
        manifest_path,
        artifact="production_env_release_prepare",
        maximum=MAX_JSON_BYTES,
    )
    manifest = _strict_json(manifest_owned, artifact="production_env_release_prepare")
    if (
        manifest.get("schema_version") != RELEASE_PREPARE_MANIFEST_SCHEMA_VERSION
        or manifest.get("complete") is not True
        or manifest.get("plan_only") is not True
    ):
        raise ProductionEnvStageError("production_env_release_prepare_invalid")
    release_id = manifest.get("release_id")
    if not isinstance(release_id, str) or not release_id.strip():
        raise ProductionEnvStageError("production_env_release_id_invalid")
    manifest_hashes = {}
    for field in (
        "approval_request_sha256",
        "release_bom_sha256",
        "workspace_runtime_sha256",
        "future_runtime_sha256",
        "action_set_sha256",
    ):
        manifest_hashes[field] = _require_sha256(
            manifest.get(field), code="production_env_release_prepare_binding_invalid"
        )

    run_descriptor = manifest.get("run_identity")
    if not isinstance(run_descriptor, Mapping):
        raise ProductionEnvStageError("production_env_run_identity_binding_missing")
    run_sha = _require_sha256(
        run_descriptor.get("sha256"), code="production_env_run_identity_sha_invalid"
    )
    run_filename = run_descriptor.get("filename")
    if run_filename != RELEASE_PREPARE_RUN_IDENTITY_NAME:
        raise ProductionEnvStageError("production_env_run_identity_filename_invalid")
    run_owned = _read_owner_only(
        manifest_owned.path.parent / RELEASE_PREPARE_RUN_IDENTITY_NAME,
        artifact="production_env_run_identity",
        maximum=MAX_JSON_BYTES,
    )
    if run_owned.sha256 != run_sha:
        raise ProductionEnvStageError("production_env_run_identity_sha_mismatch")
    run_identity = _strict_json(run_owned, artifact="production_env_run_identity")
    if (
        run_identity.get("schema_version")
        != RELEASE_PREPARE_RUN_IDENTITY_SCHEMA_VERSION
        or run_identity.get("release_id") != release_id
        or run_identity.get("plan_only") is not True
    ):
        raise ProductionEnvStageError("production_env_run_identity_invalid")
    identity_inputs = run_identity.get("inputs")
    env_descriptor = (
        identity_inputs.get("env_file") if isinstance(identity_inputs, Mapping) else None
    )
    if (
        not isinstance(env_descriptor, Mapping)
        or env_descriptor.get("sha256") != document.owned.sha256
    ):
        raise ProductionEnvStageError("production_env_input_sha_not_prepared")

    approval_owned = _read_owner_only(
        approval_path,
        artifact="production_env_approval",
        maximum=MAX_JSON_BYTES,
    )
    approval = _strict_json(approval_owned, artifact="production_env_approval")
    manifest_approval_sha = _require_sha256(
        manifest.get("approval_receipt_sha256"),
        code="production_env_approval_binding_invalid",
    )
    if approval_owned.sha256 != manifest_approval_sha:
        raise ProductionEnvStageError("production_env_approval_sha_mismatch")
    expected_approval_keys = {
        "schema_version",
        "release_id",
        "decision",
        "created_at",
        "expires_at",
        "nonce",
        "action_set",
        "action_set_sha256",
        "approval_request_sha256",
        "release_bom_sha256",
        "workspace_runtime_sha256",
        "future_runtime_sha256",
        "runtime_config_sha256",
        "t0_sha256",
        "rollback_config_sha256",
        "rollback_window_seconds",
        "identity",
    }
    if (
        set(approval) != expected_approval_keys
        or approval.get("schema_version") != RELEASE_APPROVAL_SCHEMA_VERSION
        or approval.get("decision") != RELEASE_APPROVAL_DECISION
        or approval.get("release_id") != release_id
    ):
        raise ProductionEnvStageError("production_env_approval_invalid")
    if (
        not isinstance(approval.get("nonce"), str)
        or NONCE_PATTERN.fullmatch(approval["nonce"]) is None
        or approval.get("action_set") != list(RELEASE_ACTION_SET)
        or approval.get("action_set_sha256") != _sha256_json(list(RELEASE_ACTION_SET))
        or approval.get("action_set_sha256") != manifest_hashes["action_set_sha256"]
    ):
        raise ProductionEnvStageError("production_env_approval_action_invalid")
    for field in (
        "approval_request_sha256",
        "release_bom_sha256",
        "workspace_runtime_sha256",
        "future_runtime_sha256",
    ):
        if (
            _require_sha256(
                approval.get(field), code="production_env_approval_binding_invalid"
            )
            != manifest_hashes[field]
        ):
            raise ProductionEnvStageError("production_env_approval_binding_mismatch")
    for field in (
        "runtime_config_sha256",
        "t0_sha256",
        "rollback_config_sha256",
    ):
        _require_sha256(
            approval.get(field), code="production_env_approval_binding_invalid"
        )
    rollback_window = approval.get("rollback_window_seconds")
    if (
        isinstance(rollback_window, bool)
        or not isinstance(rollback_window, int)
        or rollback_window < 1
        or rollback_window > 24 * 60 * 60
    ):
        raise ProductionEnvStageError("production_env_approval_rollback_invalid")
    identity = approval.get("identity")
    if (
        not isinstance(identity, Mapping)
        or set(identity)
        != {
            "schema_version",
            "method",
            "uid",
            "username",
            "machine_identity_source",
            "machine_identity_sha256",
        }
        or identity.get("schema_version")
        != RELEASE_APPROVAL_IDENTITY_SCHEMA_VERSION
        or identity.get("method") != RELEASE_APPROVAL_IDENTITY_METHOD
        or identity.get("uid") != os.geteuid()
        or not isinstance(identity.get("username"), str)
        or not identity["username"].strip()
        or not isinstance(identity.get("machine_identity_source"), str)
        or not identity["machine_identity_source"].strip()
    ):
        raise ProductionEnvStageError("production_env_approval_owner_mismatch")
    _require_sha256(
        identity.get("machine_identity_sha256"),
        code="production_env_approval_owner_mismatch",
    )
    created = _parse_timestamp(
        approval.get("created_at"), code="production_env_approval_timestamp_invalid"
    )
    expires = _parse_timestamp(
        approval.get("expires_at"), code="production_env_approval_timestamp_invalid"
    )
    current = now.astimezone(timezone.utc)
    validity = (expires - created).total_seconds()
    if validity <= 0 or validity > MAX_APPROVAL_VALIDITY_SECONDS:
        raise ProductionEnvStageError("production_env_approval_validity_invalid")
    if created - current > timedelta(seconds=MAX_FUTURE_SKEW_SECONDS):
        raise ProductionEnvStageError("production_env_approval_from_future")
    if current >= expires:
        raise ProductionEnvStageError("production_env_approval_expired")
    return {
        "release_id": release_id,
        "input_env_sha256": document.owned.sha256,
        "release_prepare_sha256": manifest_owned.sha256,
        "release_approval_sha256": approval_owned.sha256,
        "release_prepare_run_identity_sha256": run_owned.sha256,
        **manifest_hashes,
    }


def _required_value(
    values: Mapping[str, str], key: str, *, allow_empty: bool = False
) -> str:
    if key not in values:
        raise ProductionEnvStageError("production_env_required_key_missing")
    value = values[key]
    if not allow_empty and not value.strip():
        raise ProductionEnvStageError("production_env_required_value_missing")
    if "\x00" in value or "\r" in value or "\n" in value:
        raise ProductionEnvStageError("production_env_required_value_invalid")
    return value


def _strict_bool(value: str) -> bool:
    if value == "true":
        return True
    if value == "false":
        return False
    raise ProductionEnvStageError("production_env_boolean_invalid")


def _validate_admission_hmac_key(value: str) -> None:
    try:
        if value.startswith("hex:"):
            encoded = value.removeprefix("hex:")
            if not encoded or len(encoded) % 2:
                raise ValueError
            decoded = bytes.fromhex(encoded)
        elif value.startswith("base64:"):
            encoded = value.removeprefix("base64:")
            if not encoded:
                raise ValueError
            decoded = base64.b64decode(encoded, validate=True)
        else:
            raise ValueError
    except (ValueError, binascii.Error) as exc:
        raise ProductionEnvStageError(
            "production_env_admission_hmac_key_invalid"
        ) from exc
    if len(decoded) < 32:
        raise ProductionEnvStageError("production_env_admission_hmac_key_too_short")


def _normalized_csv(
    value: str, *, pattern: re.Pattern[str], allow_empty: bool
) -> tuple[str, ...]:
    items = tuple(sorted({item.strip() for item in value.split(",") if item.strip()}))
    if not items and not allow_empty:
        raise ProductionEnvStageError("production_env_csv_empty")
    if any(pattern.fullmatch(item) is None for item in items):
        raise ProductionEnvStageError("production_env_csv_invalid")
    return items


def _desired_values(
    values: Mapping[str, str],
    *,
    expected_topic: str,
    state_root: Path,
    release_id: str,
    release_bom_sha256: str,
    release_approval_sha256: str,
    now: datetime,
    ) -> tuple[dict[str, str], Mapping[str, Any]]:
    for key in PASSTHROUGH_REQUIRED_KEYS:
        _required_value(
            values,
            key,
            allow_empty=(key == "HERMES_RCA_MANUAL_OPERATOR_USER_IDS"),
        )
    _validate_admission_hmac_key(values["HERMES_RCA_PROD_ADMISSION_HMAC_KEY"])
    configured_release_id = _required_value(values, "HERMES_RCA_PROD_RELEASE_ID")
    bootstrap_epoch_id = _required_value(
        values, "HERMES_RCA_PROD_BOOTSTRAP_EPOCH_ID"
    )
    if configured_release_id != release_id:
        raise ProductionEnvStageError("production_env_release_id_binding_mismatch")
    try:
        bootstrap_authorization = prod_bootstrap.load_bootstrap_authorization(
            now=now,
            expected_epoch_id=bootstrap_epoch_id,
            expected_release_bom_sha256=release_bom_sha256,
            expected_release_approval_id=release_id,
            expected_approval_evidence_sha256=release_approval_sha256,
        )
    except prod_bootstrap.RcaBootstrapAuthorizationError as exc:
        raise ProductionEnvStageError(
            f"production_env_bootstrap_authorization_invalid:{exc.code}"
        ) from exc
    if SAFE_LITERAL_PATTERN.fullmatch(expected_topic) is None:
        raise ProductionEnvStageError("production_env_expected_topic_invalid")
    if _required_value(values, "HERMES_RCA_KAFKA_TOPIC") != expected_topic:
        raise ProductionEnvStageError("production_env_topic_mismatch")
    kafka_principal = _required_value(values, "HERMES_RCA_KAFKA_USER")
    if kafka_principal != FIXED_KAFKA_PRINCIPAL:
        raise ProductionEnvStageError("production_env_kafka_principal_invalid")

    chat_ids = _normalized_csv(
        _required_value(values, "HERMES_RCA_MANUAL_CHAT_IDS"),
        pattern=CHAT_ID_PATTERN,
        allow_empty=False,
    )
    if not set(chat_ids).issubset(ALLOWED_MANUAL_CHAT_IDS):
        raise ProductionEnvStageError("production_env_manual_chat_ids_invalid")
    operator_enabled = _strict_bool(
        _required_value(values, "HERMES_RCA_MANUAL_OPERATOR_ENABLED")
    )
    operator_ids = _normalized_csv(
        _required_value(
            values, "HERMES_RCA_MANUAL_OPERATOR_USER_IDS", allow_empty=True
        ),
        pattern=USER_ID_PATTERN,
        allow_empty=not operator_enabled,
    )
    if not operator_enabled and operator_ids:
        raise ProductionEnvStageError("production_env_manual_operator_ids_unbound")
    for key in (
        "HERMES_RCA_MANUAL_OPERATOR_RATE_LIMIT",
        "HERMES_RCA_MANUAL_OPERATOR_RATE_WINDOW_SECONDS",
        "HERMES_RCA_KAFKA_MIN_REPLICATION_FACTOR",
    ):
        try:
            if int(_required_value(values, key)) < 1:
                raise ValueError
        except ValueError as exc:
            raise ProductionEnvStageError(
                "production_env_positive_integer_invalid"
            ) from exc

    desired = dict(FIXED_PRODUCTION_VALUES)
    desired["HERMES_RCA_PROD_RELEASE_ID"] = release_id
    desired["HERMES_RCA_PROD_BOOTSTRAP_EPOCH_ID"] = bootstrap_epoch_id
    desired["HERMES_RCA_KAFKA_TOPIC"] = expected_topic
    desired["HERMES_RCA_KAFKA_USER"] = kafka_principal
    desired["HERMES_RCA_MANUAL_CHAT_IDS"] = ",".join(chat_ids)
    desired["HERMES_RCA_MANUAL_OPERATOR_ENABLED"] = (
        "true" if operator_enabled else "false"
    )
    desired["HERMES_RCA_MANUAL_OPERATOR_USER_IDS"] = ",".join(operator_ids)
    for key, filename in STATE_PATH_NAMES.items():
        desired[key] = str(state_root / filename)

    for key, expected in desired.items():
        observed = _required_value(
            values,
            key,
            allow_empty=(key == "HERMES_RCA_MANUAL_OPERATOR_USER_IDS"),
        )
        if observed != expected:
            raise ProductionEnvStageError("production_env_approved_value_mismatch")
        if SAFE_LITERAL_PATTERN.fullmatch(expected) is None:
            raise ProductionEnvStageError("production_env_literal_invalid")

    return desired, {
        "kafka": {
            "topic": expected_topic,
            "service_id": FIXED_SERVICE_ID,
            "api_version": "3.9.0",
            "request_timeout_ms": 120000,
            "submit_enabled": True,
            "activation_required": True,
        },
        "manual": {
            "intake_enabled": True,
            "chat_count": len(chat_ids),
            "chat_ids_sha256": _sha256_json(list(chat_ids)),
            "operator_enabled": operator_enabled,
            "operator_user_count": len(operator_ids),
            "operator_user_ids_sha256": _sha256_json(list(operator_ids)),
        },
        "stores": {
            "runtime_state_root": str(state_root),
            "control_database_shared": True,
            "health_paths_explicit": True,
        },
        "remote_read": {
            "data_access_mode": "remote_read",
            "allow_download": False,
            "input_materialization": "forbidden",
        },
        "capacity_admission": {
            "capacity_mode": "bootstrap",
            "bootstrap_epoch_id": bootstrap_authorization["bootstrap_epoch_id"],
            "bootstrap_started_at": bootstrap_authorization["started_at"],
            "bootstrap_deadline": bootstrap_authorization["deadline"],
            "bootstrap_authorization_fingerprint": bootstrap_authorization[
                "receipt_fingerprint"
            ],
            "bootstrap_authorization_sha256": bootstrap_authorization[
                "authorization_receipt_sha256"
            ],
            "release_bom_sha256": bootstrap_authorization[
                "release_bom_sha256"
            ],
            "release_approval_id": bootstrap_authorization[
                "release_approval_id"
            ],
            "approval_evidence_sha256": bootstrap_authorization[
                "approval_evidence_sha256"
            ],
        },
        "legacy": {
            "auto_execution_disabled": True,
            "download_daily_quota": 0,
            "governance_download_enabled": False,
            "issue_capture_enabled": False,
        },
    }


def _render_candidate(document: _DotenvDocument, desired: Mapping[str, str]) -> bytes:
    rendered: list[str] = []
    seen: set[str] = set()
    for key, original in document.originals:
        if key in desired:
            rendered.append(f"{key}={desired[key]}\n")
            seen.add(key)
        else:
            rendered.append(original)
    missing = sorted(set(desired) - seen)
    if missing:
        if rendered and not rendered[-1].endswith("\n"):
            rendered[-1] += "\n"
        rendered.append("\n# RCA production candidate values (staged; not live)\n")
        rendered.extend(f"{key}={desired[key]}\n" for key in missing)
    text = "".join(rendered)
    if not text.endswith("\n"):
        text += "\n"
    return text.encode("utf-8")


def _validate_candidate_config(raw: bytes) -> Mapping[str, str]:
    # Reparse the exact rendered bytes through the same structured parser without
    # materializing a temporary file or exposing any values.
    try:
        text = raw.decode("utf-8")
        bindings = tuple(parse_stream(io.StringIO(text)))
    except Exception as exc:
        raise ProductionEnvStageError("production_env_candidate_parse_invalid") from exc
    values: dict[str, str] = {}
    for binding in bindings:
        if binding.error:
            raise ProductionEnvStageError("production_env_candidate_parse_invalid")
        if binding.key is None:
            continue
        if binding.key in values or binding.value is None:
            raise ProductionEnvStageError("production_env_candidate_duplicate_or_missing")
        values[binding.key] = binding.value

    def strict_json_value(key: str) -> Any:
        def unique(items: list[tuple[str, Any]]) -> dict[str, Any]:
            result: dict[str, Any] = {}
            for item_key, value in items:
                if item_key in result:
                    raise ProductionEnvStageError(
                        "production_env_candidate_json_duplicate_key"
                    )
                result[item_key] = value
            return result

        try:
            return json.loads(
                values[key],
                object_pairs_hook=unique,
                parse_constant=lambda _value: (_ for _ in ()).throw(
                    ProductionEnvStageError(
                        "production_env_candidate_json_number_invalid"
                    )
                ),
            )
        except (KeyError, json.JSONDecodeError, RecursionError) as exc:
            raise ProductionEnvStageError(
                "production_env_candidate_json_invalid"
            ) from exc

    try:
        brokers = tuple(
            item.strip()
            for item in values["HERMES_RCA_KAFKA_BOOTSTRAP_SERVERS"].split(",")
            if item.strip()
        )
        if not brokers or any("\n" in item or "\r" in item for item in brokers):
            raise ValueError
        if not values["HERMES_RCA_KAFKA_EXPECTED_CLUSTER_ID"].strip():
            raise ValueError
        if not values["HERMES_RCA_KAFKA_PASSWORD"]:
            raise ValueError
        _validate_admission_hmac_key(values["HERMES_RCA_PROD_ADMISSION_HMAC_KEY"])
        offsets = strict_json_value("HERMES_RCA_KAFKA_START_OFFSETS_JSON")
        if (
            not isinstance(offsets, dict)
            or not offsets
            or any(
                not isinstance(partition, str)
                or not partition.isdigit()
                or isinstance(offset, bool)
                or not isinstance(offset, int)
                or offset < 0
                for partition, offset in offsets.items()
            )
        ):
            raise ValueError
        transitions = strict_json_value(
            "HERMES_RCA_KAFKA_STATE_TRANSITIONS_JSON"
        )
        if not isinstance(transitions, list) or not transitions:
            raise ValueError
        for transition in transitions:
            if (
                not isinstance(transition, dict)
                or not str(transition.get("state_key") or "").strip()
                or isinstance(transition.get("pre_status"), bool)
                or not isinstance(transition.get("pre_status"), (str, int))
                or isinstance(transition.get("cur_status"), bool)
                or not isinstance(transition.get("cur_status"), (str, int))
            ):
                raise ValueError
        for key in (
            "HERMES_RCA_KAFKA_PROJECT_KEYS",
            "HERMES_RCA_KAFKA_PROJECT_SIMPLE_NAMES",
            "HERMES_RCA_KAFKA_WORK_ITEM_TYPE_KEYS",
            "HERMES_RCA_KAFKA_STATUS_CHANGE_TYPES",
        ):
            if not tuple(item.strip() for item in values[key].split(",") if item.strip()):
                raise ValueError
        control_paths = {
            Path(values[key])
            for key in (
                "HERMES_RCA_KAFKA_CONTROL_DB_PATH",
                "HERMES_RCA_OUTBOX_CONTROL_DB_PATH",
                "HERMES_RCA_OUTBOX_DELIVERY_DB_PATH",
                "HERMES_RCA_DELIVERY_COLLECTOR_CONTROL_DB_PATH",
                "HERMES_RCA_DELIVERY_DISPATCHER_CONTROL_DB_PATH",
            )
        }
        if len(control_paths) != 1 or not next(iter(control_paths)).is_absolute():
            raise ValueError
        for key in STATE_PATH_NAMES:
            if not Path(values[key]).is_absolute():
                raise ValueError
    except (KeyError, TypeError, ValueError) as exc:
        raise ProductionEnvStageError("production_env_candidate_config_invalid") from exc
    return values


def _receipt_body(
    *,
    inputs: StageInputs,
    output: Path,
    receipt: Path,
    document: _DotenvDocument,
    candidate: bytes,
    candidate_values: Mapping[str, str],
    binding: Mapping[str, Any],
    policy: Mapping[str, Any],
    desired: Mapping[str, str],
) -> Mapping[str, Any]:
    preserved_keys = sorted(set(candidate_values) - set(desired))
    return {
        "schema_version": ENV_STAGE_RECEIPT_SCHEMA_VERSION,
        "release_id": binding["release_id"],
        "complete": True,
        "live_write_performed": False,
        "bindings": {
            "input_env": {
                "path": str(document.owned.path),
                "sha256": binding["input_env_sha256"],
            },
            "release_prepare_manifest": {
                "path": str(inputs.release_prepare_manifest.expanduser().absolute()),
                "sha256": binding["release_prepare_sha256"],
            },
            "release_prepare_run_identity_sha256": binding[
                "release_prepare_run_identity_sha256"
            ],
            "approval_request_sha256": binding["approval_request_sha256"],
            "release_bom_sha256": binding["release_bom_sha256"],
            "workspace_runtime_sha256": binding["workspace_runtime_sha256"],
            "future_runtime_sha256": binding["future_runtime_sha256"],
            "action_set_sha256": binding["action_set_sha256"],
            "release_approval": {
                "path": str(inputs.approval_receipt.expanduser().absolute()),
                "sha256": binding["release_approval_sha256"],
            },
            "bootstrap_authorization": {
                "path": str(
                    prod_bootstrap.BOOTSTRAP_AUTHORIZATION_PATH
                    .expanduser()
                    .absolute()
                ),
                "sha256": policy["capacity_admission"][
                    "bootstrap_authorization_sha256"
                ],
                "receipt_fingerprint": policy["capacity_admission"][
                    "bootstrap_authorization_fingerprint"
                ],
            },
            "candidate_env": {
                "path": str(output),
                "sha256": hashlib.sha256(candidate).hexdigest(),
                "size_bytes": len(candidate),
            },
        },
        "policy": policy,
        "document": {
            "key_count": len(candidate_values),
            "key_set_sha256": _sha256_json(sorted(candidate_values)),
            "preserved_key_count": len(preserved_keys),
            "preserved_key_set_sha256": _sha256_json(preserved_keys),
            "credentials_preserved": True,
        },
        "side_effect_contract": {
            "canonical_live_env": str(CANONICAL_LIVE_ENV),
            "canonical_active_release_binding": str(
                Path(policy["stores"]["runtime_state_root"])
                / prod_bootstrap.ACTIVE_RELEASE_BINDING_NAME
            ),
            "candidate_env": str(output),
            "receipt": str(receipt),
            "canonical_live_write_supported": False,
            "launchctl_invoked": False,
            "services_restarted": False,
        },
    }


def _revalidate_source_bindings(
    inputs: StageInputs, binding: Mapping[str, Any]
) -> None:
    sources = {
        "production_env_input": (
            inputs.input_env,
            MAX_ENV_BYTES,
            binding["input_env_sha256"],
        ),
        "production_env_release_prepare": (
            inputs.release_prepare_manifest,
            MAX_JSON_BYTES,
            binding["release_prepare_sha256"],
        ),
        "production_env_run_identity": (
            inputs.release_prepare_manifest.expanduser().absolute().parent
            / RELEASE_PREPARE_RUN_IDENTITY_NAME,
            MAX_JSON_BYTES,
            binding["release_prepare_run_identity_sha256"],
        ),
        "production_env_approval": (
            inputs.approval_receipt,
            MAX_JSON_BYTES,
            binding["release_approval_sha256"],
        ),
        "production_env_bootstrap_authorization": (
            prod_bootstrap.BOOTSTRAP_AUTHORIZATION_PATH,
            prod_bootstrap.MAX_AUTHORIZATION_FILE_BYTES,
            binding["bootstrap_authorization_sha256"],
        ),
    }
    for artifact, (path, maximum, expected_sha256) in sources.items():
        observed = _read_owner_only(path, artifact=artifact, maximum=maximum)
        if observed.sha256 != expected_sha256:
            raise ProductionEnvStageError("production_env_stage_source_changed")


def _publishable(path: Path, payload: bytes, *, artifact: str) -> bool:
    try:
        os.lstat(path)
    except FileNotFoundError:
        return True
    except OSError as exc:
        raise ProductionEnvStageError(f"{artifact}_unavailable") from exc
    existing = _read_owner_only(path, artifact=artifact, maximum=max(len(payload), 1) + 1)
    if existing.raw != payload:
        raise ProductionEnvStageError(f"{artifact}_conflict")
    return False


def _publish_no_clobber(path: Path, payload: bytes, *, artifact: str) -> bool:
    if not _publishable(path, payload, artifact=artifact):
        return False
    temporary = path.parent / f".{path.name}.{secrets.token_hex(12)}.tmp"
    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    descriptor = -1
    try:
        descriptor = os.open(temporary, flags, 0o600)
        view = memoryview(payload)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise ProductionEnvStageError(f"{artifact}_write_failed")
            view = view[written:]
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = -1
        try:
            os.link(temporary, path, follow_symlinks=False)
        except FileExistsError:
            if not _publishable(path, payload, artifact=artifact):
                return False
            raise ProductionEnvStageError(f"{artifact}_publish_race")
        os.unlink(temporary)
        directory = os.open(
            path.parent,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
        return True
    except OSError as exc:
        if exc.errno == errno.ELOOP:
            raise ProductionEnvStageError(f"{artifact}_symlink_forbidden") from exc
        raise ProductionEnvStageError(f"{artifact}_publish_failed") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


def run_production_env_stage(
    *,
    phase: str,
    inputs: StageInputs,
    now: datetime | None = None,
) -> StageResult:
    if phase not in {"plan", "stage", "validate"}:
        raise ProductionEnvStageError("production_env_stage_phase_invalid")
    output, receipt, state_root = _validate_paths(inputs)
    document = _parse_dotenv(inputs.input_env)
    binding = _validate_release_binding(
        document=document,
        manifest_path=inputs.release_prepare_manifest,
        approval_path=inputs.approval_receipt,
        now=now or datetime.now(timezone.utc),
    )
    desired, policy = _desired_values(
        document.values,
        expected_topic=inputs.expected_topic,
        state_root=state_root,
        release_id=binding["release_id"],
        release_bom_sha256=binding["release_bom_sha256"],
        release_approval_sha256=binding["release_approval_sha256"],
        now=now or datetime.now(timezone.utc),
    )
    binding = {
        **binding,
        "bootstrap_authorization_sha256": policy["capacity_admission"][
            "bootstrap_authorization_sha256"
        ],
    }
    candidate = _render_candidate(document, desired)
    candidate_values = _validate_candidate_config(candidate)
    receipt_body = _receipt_body(
        inputs=inputs,
        output=output,
        receipt=receipt,
        document=document,
        candidate=candidate,
        candidate_values=candidate_values,
        binding=binding,
        policy=policy,
        desired=desired,
    )
    receipt_raw = _canonical_json(receipt_body)
    plan = {
        "schema_version": ENV_STAGE_PLAN_SCHEMA_VERSION,
        "phase": phase,
        "ok": True,
        "release_id": binding["release_id"],
        "bindings": receipt_body["bindings"],
        "policy": policy,
        "document": receipt_body["document"],
        "side_effect_contract": receipt_body["side_effect_contract"],
        "receipt_sha256": hashlib.sha256(receipt_raw).hexdigest(),
    }
    if phase == "plan":
        _revalidate_source_bindings(inputs, binding)
        return StageResult(phase=phase, body=plan)
    if phase == "validate":
        observed_env = _read_owner_only(
            output, artifact="production_env_candidate", maximum=MAX_ENV_BYTES
        )
        observed_receipt = _read_owner_only(
            receipt, artifact="production_env_stage_receipt", maximum=MAX_JSON_BYTES
        )
        if observed_env.raw != candidate:
            raise ProductionEnvStageError("production_env_candidate_sha_mismatch")
        if observed_receipt.raw != receipt_raw:
            raise ProductionEnvStageError("production_env_stage_receipt_mismatch")
        _revalidate_source_bindings(inputs, binding)
        return StageResult(phase=phase, body=plan)

    # Preflight both destinations before publishing either artifact.
    _revalidate_source_bindings(inputs, binding)
    output_missing = _publishable(
        output, candidate, artifact="production_env_candidate"
    )
    receipt_missing = _publishable(
        receipt, receipt_raw, artifact="production_env_stage_receipt"
    )
    output_written = (
        _publish_no_clobber(output, candidate, artifact="production_env_candidate")
        if output_missing
        else False
    )
    receipt_written = (
        _publish_no_clobber(
            receipt, receipt_raw, artifact="production_env_stage_receipt"
        )
        if receipt_missing
        else False
    )
    _revalidate_source_bindings(inputs, binding)
    return StageResult(
        phase=phase,
        body=plan,
        output_written=output_written,
        receipt_written=receipt_written,
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=("plan", "stage", "validate"))
    parser.add_argument("--input-env", type=Path, required=True)
    parser.add_argument("--release-prepare-manifest", type=Path, required=True)
    parser.add_argument("--approval-receipt", type=Path, required=True)
    parser.add_argument("--output-env", type=Path, required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--runtime-state-root", type=Path, required=True)
    parser.add_argument("--expected-topic", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    try:
        args = _parser().parse_args(argv)
        result = run_production_env_stage(
            phase=args.phase,
            inputs=StageInputs(
                input_env=args.input_env,
                release_prepare_manifest=args.release_prepare_manifest,
                approval_receipt=args.approval_receipt,
                output_env=args.output_env,
                receipt=args.receipt,
                runtime_state_root=args.runtime_state_root,
                expected_topic=args.expected_topic,
            ),
        )
        print(_canonical_json(result.body).decode("utf-8"), end="")
        return 0
    except ProductionEnvStageError as exc:
        print(
            _canonical_json({"ok": False, "code": exc.code}).decode("utf-8"),
            end="",
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
