#!/usr/bin/env python3
"""Fail-closed operator CLI for one exact PNC RCA activation epoch."""

from __future__ import annotations

import argparse
from dataclasses import asdict
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import sys
from typing import Any, Mapping, Sequence

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from gateway.pnc_rca_control_store import (
    ACTIVATION_KAFKA_PROOF_MODE,
    ACTIVATION_RELEASE_SLOT_KINDS,
    ACTIVATION_SLOT_KINDS,
    CONTROL_STORE_SCHEMA_VERSION,
    ActivationEpochError,
    RcaControlStore,
    ShadowPromotionError,
)
from gateway import pnc_rca_capacity_runtime as capacity_runtime
from gateway import pnc_rca_capacity_sample_evidence as capacity_evidence
from gateway import pnc_rca_capacity_transition as capacity_transition
from gateway import pnc_rca_prod_bootstrap as prod_bootstrap
from scripts import pnc_rca_activation_capsule as activation_capsule


ACTIVATION_CLI_SCHEMA_VERSION = "pnc_rca_activation_cli_v1"
DIRECT_STEADY_BINDING_SCHEMA_VERSION = "pnc_rca_direct_steady_binding_v1"
MAX_JSON_INPUT_BYTES = 64 * 1024
CAPACITY_ORIGIN_COMPAT_SCHEMA_VERSION = "rca_capacity_origin_compat_receipt_v2"
CAPACITY_ORIGIN_COMPAT_NAME = "capacity-origin-compatibility.json"
CAPACITY_ORIGIN_COMPAT_MAX_AGE_SECONDS = 3600
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_EPOCH_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_EVENT_UID_RE = re.compile(
    r"^(?P<topic>[A-Za-z0-9][A-Za-z0-9._-]{0,248}):"
    r"(?P<partition>[0-9]+):(?P<offset>[0-9]+)$"
)
_MANUAL_MESSAGE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,499}$")
_ISSUE_URL_RE = re.compile(
    r"^https://project\.feishu\.cn/"
    r"[A-Za-z0-9._-]+/issue/detail/[0-9]+/*$"
)
_SAFE_ERROR_CODE_RE = re.compile(r"^[a-z][a-z0-9_]{2,127}$")
_MANUAL_IDENTITY_FIELDS = frozenset({
    "chat_id",
    "requester_id",
    "message_id",
    "thread_id",
    "issue_url",
    "mode",
})
_CAPACITY_ORIGIN_COMPAT_FIELDS = frozenset({
    "schema_version",
    "created_at",
    "current_release_id",
    "current_bootstrap_epoch_id",
    "capacity_origin_release_id",
    "capacity_origin_bootstrap_epoch_id",
    "active_release_binding_sha256",
    "release_bom_sha256",
    "producer_path",
    "producer_sha256",
    "producer_receipt_fingerprint",
    "database_rows_modified",
    "external_effects_triggered",
})
_DIRECT_STEADY_BINDING_FIELDS = frozenset({
    "schema_version",
    "epoch_id",
    "release_fingerprint",
    "release_binding_sha256",
    "config_sha256",
    "db_logical_identity",
    "db_logical_identity_sha256",
    "partition_start_fence",
    "partition_start_fence_sha256",
})


class ActivationCliError(RuntimeError):
    """A bounded operator input or state check failed without exposing payload."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


class _SafeArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        del message
        raise ActivationCliError("activation_cli_arguments_invalid")


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _sha256_json(value: Any) -> str:
    return _sha256_text(_canonical_json(value))


def _normalized_sha256(value: str, field: str) -> str:
    normalized = str(value or "").strip().lower()
    if _SHA256_RE.fullmatch(normalized) is None:
        raise ActivationCliError(f"activation_{field}_invalid")
    return normalized


def _normalized_epoch_id(value: str) -> str:
    normalized = str(value or "").strip()
    if _EPOCH_ID_RE.fullmatch(normalized) is None:
        raise ActivationCliError("activation_epoch_id_invalid")
    return normalized


def _normalized_audit(operator: str, reason: str) -> tuple[str, str]:
    actor = str(operator or "").strip()
    justification = str(reason or "").strip()
    if not actor or len(actor) > 200 or "\n" in actor or "\r" in actor:
        raise ActivationCliError("activation_operator_invalid")
    if not justification or len(justification.encode("utf-8")) > 1000:
        raise ActivationCliError("activation_reason_invalid")
    return actor, justification


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ActivationCliError("activation_json_duplicate_key")
        result[key] = value
    return result


def _reject_json_constant(value: str) -> None:
    del value
    raise ActivationCliError("activation_json_non_finite_number")


def _read_json_document(
    path: Path,
    field: str,
    *,
    max_bytes: int = MAX_JSON_INPUT_BYTES,
    owner_only: bool = False,
) -> tuple[dict[str, Any], bytes]:
    candidate = Path(path).expanduser()
    if not candidate.is_absolute():
        raise ActivationCliError(f"activation_{field}_path_not_absolute")
    try:
        observed = candidate.lstat()
    except OSError as exc:
        raise ActivationCliError(f"activation_{field}_file_unavailable") from exc
    if stat.S_ISLNK(observed.st_mode) or not stat.S_ISREG(observed.st_mode):
        raise ActivationCliError(f"activation_{field}_file_not_regular")
    if owner_only and (
        observed.st_uid != os.getuid()
        or observed.st_nlink != 1
        or stat.S_IMODE(observed.st_mode) != 0o600
    ):
        raise ActivationCliError(f"activation_{field}_file_permissions_invalid")
    if observed.st_size <= 0 or observed.st_size > max_bytes:
        raise ActivationCliError(f"activation_{field}_file_size_invalid")
    descriptor = -1
    try:
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
        flags |= getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(candidate, flags)
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_dev != observed.st_dev
            or opened.st_ino != observed.st_ino
        ):
            raise ActivationCliError(f"activation_{field}_file_identity_changed")
        with os.fdopen(descriptor, "rb", closefd=True) as handle:
            descriptor = -1
            raw = handle.read(max_bytes + 1)
            finished = os.fstat(handle.fileno())
        final_path = candidate.lstat()
        if (
            finished.st_dev != opened.st_dev
            or finished.st_ino != opened.st_ino
            or finished.st_size != opened.st_size
            or finished.st_mtime_ns != opened.st_mtime_ns
            or finished.st_ctime_ns != opened.st_ctime_ns
            or len(raw) != finished.st_size
            or final_path.st_dev != opened.st_dev
            or final_path.st_ino != opened.st_ino
            or final_path.st_size != opened.st_size
            or final_path.st_mtime_ns != opened.st_mtime_ns
            or final_path.st_ctime_ns != opened.st_ctime_ns
        ):
            raise ActivationCliError(f"activation_{field}_file_identity_changed")
        if len(raw) > max_bytes:
            raise ActivationCliError(f"activation_{field}_file_size_invalid")
        decoded = raw.decode("utf-8")
        value = json.loads(
            decoded,
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=_reject_json_constant,
        )
    except ActivationCliError:
        raise
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ActivationCliError(f"activation_{field}_json_invalid") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    if not isinstance(value, dict) or not value:
        raise ActivationCliError(f"activation_{field}_object_invalid")
    return value, raw


def _read_json_object(path: Path, field: str) -> dict[str, Any]:
    value, _raw = _read_json_document(path, field)
    return value


def _normalize_db_identity(value: Mapping[str, Any]) -> dict[str, Any]:
    normalized = dict(value)
    try:
        canonical = _canonical_json(normalized)
    except (TypeError, ValueError) as exc:
        raise ActivationCliError("activation_db_identity_invalid") from exc
    if len(canonical.encode("utf-8")) > 4096:
        raise ActivationCliError("activation_db_identity_too_large")
    return normalized


def _normalize_fence(value: Mapping[str, Any], field: str) -> dict[str, dict[str, int]]:
    normalized: dict[str, dict[str, int]] = {}
    for raw_topic, raw_partitions in value.items():
        if not isinstance(raw_topic, str) or raw_topic != raw_topic.strip():
            raise ActivationCliError(f"activation_{field}_invalid")
        topic = raw_topic
        if (
            _EVENT_UID_RE.fullmatch(f"{topic}:0:0") is None
            or not isinstance(raw_partitions, Mapping)
            or not raw_partitions
        ):
            raise ActivationCliError(f"activation_{field}_invalid")
        if topic in normalized:
            raise ActivationCliError(f"activation_{field}_duplicate")
        partitions: dict[str, int] = {}
        for raw_partition, raw_offset in raw_partitions.items():
            partition = str(raw_partition)
            if (
                not partition.isdigit()
                or str(int(partition)) != partition
                or isinstance(raw_offset, bool)
            ):
                raise ActivationCliError(f"activation_{field}_invalid")
            if isinstance(raw_offset, int):
                offset = raw_offset
            else:
                raise ActivationCliError(f"activation_{field}_invalid")
            if int(partition) < 0 or offset < 0:
                raise ActivationCliError(f"activation_{field}_invalid")
            canonical_partition = str(int(partition))
            if canonical_partition in partitions:
                raise ActivationCliError(f"activation_{field}_duplicate")
            partitions[canonical_partition] = offset
        normalized[topic] = partitions
    if not normalized:
        raise ActivationCliError(f"activation_{field}_invalid")
    return normalized


def _canonical_direct_steady_binding(path: Path) -> dict[str, Any]:
    """Read and normalize the owner-only direct-release binding document."""
    value, _raw = _read_json_document(
        path,
        "direct_binding",
        owner_only=True,
    )
    if set(value) != _DIRECT_STEADY_BINDING_FIELDS:
        raise ActivationCliError("activation_direct_binding_schema_invalid")
    if value.get("schema_version") != DIRECT_STEADY_BINDING_SCHEMA_VERSION:
        raise ActivationCliError("activation_direct_binding_schema_invalid")
    epoch_id = _normalized_epoch_id(str(value.get("epoch_id") or ""))
    release_fingerprint = _normalized_sha256(
        str(value.get("release_fingerprint") or ""),
        "direct_release_fingerprint",
    )
    release_binding_sha256 = _normalized_sha256(
        str(value.get("release_binding_sha256") or ""),
        "direct_release_binding_sha256",
    )
    config_sha256 = _normalized_sha256(
        str(value.get("config_sha256") or ""),
        "config_sha256",
    )
    raw_db_identity = value.get("db_logical_identity")
    if not isinstance(raw_db_identity, Mapping):
        raise ActivationCliError("activation_db_identity_invalid")
    try:
        db_identity = _normalize_db_identity(raw_db_identity)
    except (TypeError, ValueError) as exc:
        raise ActivationCliError("activation_db_identity_invalid") from exc
    db_identity_sha256 = _normalized_sha256(
        str(value.get("db_logical_identity_sha256") or ""),
        "db_logical_identity_sha256",
    )
    if db_identity_sha256 != _sha256_json(db_identity):
        raise ActivationCliError("activation_direct_binding_identity_mismatch")
    raw_start_fence = value.get("partition_start_fence")
    if not isinstance(raw_start_fence, Mapping):
        raise ActivationCliError("activation_partition_start_fence_invalid")
    start_fence = _normalize_fence(raw_start_fence, "partition_start_fence")
    start_fence_sha256 = _normalized_sha256(
        str(value.get("partition_start_fence_sha256") or ""),
        "partition_start_fence_sha256",
    )
    if start_fence_sha256 != _sha256_json(start_fence):
        raise ActivationCliError("activation_direct_binding_fence_mismatch")
    return {
        "schema_version": DIRECT_STEADY_BINDING_SCHEMA_VERSION,
        "epoch_id": epoch_id,
        "release_fingerprint": release_fingerprint,
        "release_binding_sha256": release_binding_sha256,
        "config_sha256": config_sha256,
        "db_logical_identity": db_identity,
        "db_logical_identity_sha256": db_identity_sha256,
        "partition_start_fence": start_fence,
        "partition_start_fence_sha256": start_fence_sha256,
    }


def _read_preauthorization_capsule(
    capsule_path: Path,
    *,
    control_db_path: Path,
) -> Mapping[str, Any]:
    try:
        return activation_capsule.read_preauthorization_capsule(
            capsule_path,
            control_db_path=control_db_path,
        )
    except Exception as exc:
        raise ActivationCliError(
            "activation_preauthorization_capsule_rejected"
        ) from exc


def _canonical_preauthorization_input(
    capsule_path: Path,
    *,
    control_db_path: Path,
) -> dict[str, Any]:
    value = _read_preauthorization_capsule(
        capsule_path,
        control_db_path=control_db_path,
    )
    expected_fields = {
        "epoch_id",
        "initial_state",
        "preauthorization_fingerprint",
        "preauthorization_gate_receipt_sha256",
        "preauthorization_capsule_sha256",
        "config_sha256",
        "db_logical_identity",
        "db_logical_identity_sha256",
        "partition_start_fence",
        "partition_start_fence_sha256",
        "migration_receipt_raw_sha256",
        "materialization_receipt_raw_sha256",
        "broker_t0_observation_sha256",
    }
    if not isinstance(value, Mapping) or set(value) != expected_fields:
        raise ActivationCliError("activation_preauthorization_capsule_rejected")
    raw_db_identity = value.get("db_logical_identity")
    raw_start_fence = value.get("partition_start_fence")
    if not isinstance(raw_db_identity, Mapping) or not isinstance(
        raw_start_fence, Mapping
    ):
        raise ActivationCliError("activation_preauthorization_capsule_rejected")
    db_identity = _normalize_db_identity(raw_db_identity)
    start_fence = _normalize_fence(raw_start_fence, "partition_start_fence")
    normalized = {
        "epoch_id": _normalized_epoch_id(str(value.get("epoch_id") or "")),
        "initial_state": value.get("initial_state"),
        "preauthorization_fingerprint": _normalized_sha256(
            str(value.get("preauthorization_fingerprint") or ""),
            "preauthorization_fingerprint",
        ),
        "preauthorization_gate_receipt_sha256": _normalized_sha256(
            str(value.get("preauthorization_gate_receipt_sha256") or ""),
            "preauthorization_gate_receipt_sha256",
        ),
        "preauthorization_capsule_sha256": _normalized_sha256(
            str(value.get("preauthorization_capsule_sha256") or ""),
            "preauthorization_capsule_sha256",
        ),
        "config_sha256": _normalized_sha256(
            str(value.get("config_sha256") or ""), "config_sha256"
        ),
        "db_logical_identity": db_identity,
        "db_logical_identity_sha256": _normalized_sha256(
            str(value.get("db_logical_identity_sha256") or ""),
            "db_logical_identity_sha256",
        ),
        "partition_start_fence": start_fence,
        "partition_start_fence_sha256": _normalized_sha256(
            str(value.get("partition_start_fence_sha256") or ""),
            "partition_start_fence_sha256",
        ),
        "migration_receipt_raw_sha256": _normalized_sha256(
            str(value.get("migration_receipt_raw_sha256") or ""),
            "migration_receipt_raw_sha256",
        ),
        "materialization_receipt_raw_sha256": _normalized_sha256(
            str(value.get("materialization_receipt_raw_sha256") or ""),
            "materialization_receipt_raw_sha256",
        ),
        "broker_t0_observation_sha256": _normalized_sha256(
            str(value.get("broker_t0_observation_sha256") or ""),
            "broker_t0_observation_sha256",
        ),
    }
    if (
        normalized["initial_state"] != "safe_off"
        or normalized["db_logical_identity_sha256"] != _sha256_json(db_identity)
        or normalized["partition_start_fence_sha256"] != _sha256_json(start_fence)
    ):
        raise ActivationCliError("activation_preauthorization_capsule_rejected")
    return normalized


def _read_preproduction_capsule(
    capsule_path: Path,
    *,
    control_db_path: Path,
    current_activation: Mapping[str, Any],
) -> Mapping[str, Any]:
    try:
        return activation_capsule.read_preproduction_capsule(
            capsule_path,
            control_db_path=control_db_path,
            current_activation=current_activation,
            allowed_current_states=frozenset({
                str(current_activation.get("state") or "")
            }),
        )
    except Exception as exc:
        raise ActivationCliError("activation_preproduction_capsule_rejected") from exc


def _canonical_preproduction_transition(
    capsule_path: Path,
    *,
    control_db_path: Path,
    current_activation: Mapping[str, Any],
) -> dict[str, Any]:
    value = _read_preproduction_capsule(
        capsule_path,
        control_db_path=control_db_path,
        current_activation=current_activation,
    )
    expected_fields = {
        "epoch_id",
        "expected_state",
        "target_state",
        "expected_preauthorization_fingerprint",
        "expected_preauthorization_gate_receipt_sha256",
        "expected_preauthorization_capsule_sha256",
        "expected_config_sha256",
        "expected_db_logical_identity_sha256",
        "expected_partition_start_fence_sha256",
        "kafka_proof_mode",
        "required_slot_kinds",
        "canary_slot_plan",
        "canary_slot_plan_sha256",
        "preproduction_fingerprint",
        "preproduction_gate_receipt_sha256",
        "preproduction_capsule_sha256",
    }
    if not isinstance(value, Mapping) or set(value) != expected_fields:
        raise ActivationCliError("activation_preproduction_capsule_rejected")
    normalized = {
        "epoch_id": _normalized_epoch_id(str(value.get("epoch_id") or "")),
        "expected_state": value.get("expected_state"),
        "target_state": value.get("target_state"),
        "kafka_proof_mode": value.get("kafka_proof_mode"),
        "required_slot_kinds": value.get("required_slot_kinds"),
    }
    canary_slot_plan = _normalize_canary_slot_plan(value.get("canary_slot_plan"))
    normalized["canary_slot_plan"] = canary_slot_plan
    for field in sorted(
        expected_fields
        - {
            "epoch_id",
            "expected_state",
            "target_state",
            "kafka_proof_mode",
            "required_slot_kinds",
            "canary_slot_plan",
        }
    ):
        normalized[field] = _normalized_sha256(str(value.get(field) or ""), field)
    if (
        normalized["expected_state"] != "safe_off"
        or normalized["target_state"] != "preauthorized"
        or normalized["kafka_proof_mode"] != ACTIVATION_KAFKA_PROOF_MODE
        or normalized["required_slot_kinds"]
        != list(ACTIVATION_RELEASE_SLOT_KINDS)
        or normalized["canary_slot_plan_sha256"] != _sha256_json(canary_slot_plan)
    ):
        raise ActivationCliError("activation_preproduction_capsule_rejected")
    return normalized


def _live_release_binding(
    control_db_path: Path,
    *,
    epoch_id: str,
    expected_config_sha256: str,
    partition_end_fence: Mapping[str, Any],
) -> dict[str, Any]:
    try:
        return activation_capsule.live_release_binding(
            control_db_path,
            epoch_id=epoch_id,
            expected_config_sha256=expected_config_sha256,
            partition_end_fence=partition_end_fence,
        )
    except Exception as exc:
        raise ActivationCliError(
            "activation_confirmation_database_binding_invalid"
        ) from exc


def _confirmation_capsule_scope(
    capsule_path: Path,
) -> tuple[Path, str, str, dict[str, dict[str, int]]]:
    capsule, _capsule_raw = _read_json_document(
        capsule_path,
        "confirmation_capsule",
        owner_only=True,
    )
    receipt_meta = capsule.get("release_gate_receipt")
    transition = capsule.get("transition_input")
    if not isinstance(receipt_meta, Mapping) or not isinstance(transition, Mapping):
        raise ActivationCliError("activation_confirmation_capsule_scope_invalid")
    receipt_path_value = receipt_meta.get("path")
    if not isinstance(receipt_path_value, str) or not receipt_path_value.strip():
        raise ActivationCliError("activation_confirmation_capsule_scope_invalid")
    receipt_path = Path(receipt_path_value)
    if (
        not receipt_path.is_absolute()
        or str(receipt_path.absolute()) != receipt_path_value
    ):
        raise ActivationCliError("activation_confirmation_capsule_scope_invalid")
    epoch_id = _normalized_epoch_id(str(transition.get("epoch_id") or ""))
    config_sha256 = _normalized_sha256(
        str(transition.get("config_sha256") or ""), "config_sha256"
    )
    raw_end_fence = transition.get("partition_end_fence")
    if not isinstance(raw_end_fence, Mapping):
        raise ActivationCliError("activation_confirmation_capsule_scope_invalid")
    partition_end_fence = _normalize_fence(
        raw_end_fence, "confirmation_partition_end_fence"
    )
    return receipt_path, epoch_id, config_sha256, partition_end_fence


def _normalize_confirmation_transition(value: Mapping[str, Any]) -> dict[str, Any]:
    if set(value) != {
        "epoch_id",
        "expected_state",
        "target_state",
        "config_sha256",
        "db_logical_identity_sha256",
        "partition_start_fence_sha256",
        "release_binding_sha256",
        "partition_end_fence",
        "partition_end_fence_sha256",
        "production_fingerprint",
        "production_gate_receipt_sha256",
    }:
        raise ActivationCliError("activation_confirmation_transition_input_invalid")
    end_fence_value = value.get("partition_end_fence")
    if not isinstance(end_fence_value, Mapping):
        raise ActivationCliError("activation_confirmation_transition_input_invalid")
    end_fence = _normalize_fence(end_fence_value, "partition_end_fence")
    normalized_transition = {
        "epoch_id": _normalized_epoch_id(str(value.get("epoch_id") or "")),
        "expected_state": value.get("expected_state"),
        "target_state": value.get("target_state"),
        "config_sha256": _normalized_sha256(
            str(value.get("config_sha256") or ""), "config_sha256"
        ),
        "db_logical_identity_sha256": _normalized_sha256(
            str(value.get("db_logical_identity_sha256") or ""),
            "db_logical_identity_sha256",
        ),
        "partition_start_fence_sha256": _normalized_sha256(
            str(value.get("partition_start_fence_sha256") or ""),
            "partition_start_fence_sha256",
        ),
        "release_binding_sha256": _normalized_sha256(
            str(value.get("release_binding_sha256") or ""),
            "release_binding_sha256",
        ),
        "partition_end_fence": end_fence,
        "partition_end_fence_sha256": _normalized_sha256(
            str(value.get("partition_end_fence_sha256") or ""),
            "partition_end_fence_sha256",
        ),
        "production_fingerprint": _normalized_sha256(
            str(value.get("production_fingerprint") or ""),
            "production_fingerprint",
        ),
        "production_gate_receipt_sha256": _normalized_sha256(
            str(value.get("production_gate_receipt_sha256") or ""),
            "production_gate_receipt_sha256",
        ),
    }
    if (
        normalized_transition.get("expected_state") != "bounded_active"
        or normalized_transition.get("target_state") != "confirmed"
        or normalized_transition["partition_end_fence_sha256"]
        != _sha256_json(end_fence)
    ):
        raise ActivationCliError("activation_confirmation_transition_input_invalid")
    return normalized_transition


def _canonical_confirmation_transition(
    *,
    capsule_path: Path,
    receipt_path: Path,
    control_db_path: Path,
    current_activation: Mapping[str, Any],
) -> dict[str, Any]:
    try:
        transition = activation_capsule.read_confirmation_capsule(
            capsule_path,
            receipt_path=receipt_path,
            control_db_path=control_db_path,
            current_activation=current_activation,
        )
    except Exception as exc:
        raise ActivationCliError("activation_confirmation_capsule_rejected") from exc
    if not isinstance(transition, Mapping):
        raise ActivationCliError("activation_confirmation_capsule_rejected")
    return _normalize_confirmation_transition(transition)


def _normalize_event_uid(value: str) -> tuple[str, dict[str, Any]]:
    event_uid = str(value or "").strip()
    match = _EVENT_UID_RE.fullmatch(event_uid)
    if match is None:
        raise ActivationCliError("activation_event_uid_invalid")
    if str(int(match.group("partition"))) != match.group("partition") or str(
        int(match.group("offset"))
    ) != match.group("offset"):
        raise ActivationCliError("activation_event_uid_invalid")
    normalized = {
        "event_uid": event_uid,
        "offset": int(match.group("offset")),
        "partition": int(match.group("partition")),
        "topic": match.group("topic"),
    }
    return event_uid, normalized


def _normalize_manual_identity(value: Mapping[str, Any]) -> dict[str, str]:
    if set(value) != _MANUAL_IDENTITY_FIELDS:
        raise ActivationCliError("activation_manual_identity_fields_invalid")
    normalized: dict[str, str] = {}
    for field in sorted(_MANUAL_IDENTITY_FIELDS):
        raw = value.get(field)
        if not isinstance(raw, str):
            raise ActivationCliError("activation_manual_identity_invalid")
        item = raw.strip()
        if not item or "\n" in item or "\r" in item:
            raise ActivationCliError("activation_manual_identity_invalid")
        normalized[field] = item
    normalized["issue_url"] = normalized["issue_url"].rstrip("/")
    if _ISSUE_URL_RE.fullmatch(normalized["issue_url"]) is None:
        raise ActivationCliError("activation_manual_issue_url_invalid")
    if normalized["mode"] not in {"run_or_join", "rerun", "debug"}:
        raise ActivationCliError("activation_manual_mode_invalid")
    if len(_canonical_json(normalized).encode("utf-8")) > 2048:
        raise ActivationCliError("activation_manual_identity_too_large")
    return normalized


def _normalize_canary_slot_plan(value: Any) -> dict[str, dict[str, Any]]:
    if not isinstance(value, Mapping) or set(value) != set(
        ACTIVATION_RELEASE_SLOT_KINDS
    ):
        raise ActivationCliError("activation_canary_slot_plan_invalid")
    expected_source = {
        "kafka_success": ("kafka", "kafka_ingest", "success"),
        "manual_success": ("manual", "manual_admit", "success"),
        "manual_terminal_failure": (
            "manual",
            "manual_admit",
            frozenset({"terminal_failed", "quarantined"}),
        ),
    }
    normalized: dict[str, dict[str, Any]] = {}
    for slot_kind in sorted(ACTIVATION_RELEASE_SLOT_KINDS):
        raw_slot = value.get(slot_kind)
        if not isinstance(raw_slot, Mapping) or set(raw_slot) != {
            "source_kind",
            "entrypoint",
            "source_identity",
            "source_identity_sha256",
            "max_admissions",
            "expected_admission",
            "expected_outcome",
        }:
            raise ActivationCliError("activation_canary_slot_plan_invalid")
        source_kind, entrypoint, expected_outcome = expected_source[slot_kind]
        raw_identity = raw_slot.get("source_identity")
        if not isinstance(raw_identity, Mapping):
            raise ActivationCliError("activation_canary_slot_plan_invalid")
        if source_kind == "kafka":
            if set(raw_identity) != {"event_uid", "offset", "partition", "topic"}:
                raise ActivationCliError("activation_canary_slot_plan_invalid")
            _, source_identity = _normalize_event_uid(
                str(raw_identity.get("event_uid") or "")
            )
            if (
                isinstance(raw_identity.get("offset"), bool)
                or not isinstance(raw_identity.get("offset"), int)
                or isinstance(raw_identity.get("partition"), bool)
                or not isinstance(raw_identity.get("partition"), int)
                or not isinstance(raw_identity.get("event_uid"), str)
                or not isinstance(raw_identity.get("topic"), str)
                or dict(raw_identity) != source_identity
            ):
                raise ActivationCliError("activation_canary_slot_plan_invalid")
        else:
            source_identity = _normalize_manual_identity(raw_identity)
            if source_identity["mode"] != "run_or_join" and not (
                source_identity["mode"] == "rerun"
                and source_identity["chat_id"] == "operator"
                and source_identity["thread_id"] == "operator:issue-only"
            ):
                raise ActivationCliError("activation_canary_slot_plan_invalid")
        source_sha256 = _normalized_sha256(
            str(raw_slot.get("source_identity_sha256") or ""),
            "canary_slot_source_identity_sha256",
        )
        admission = raw_slot.get("expected_admission")
        if not isinstance(admission, Mapping):
            raise ActivationCliError("activation_canary_slot_plan_invalid")
        business_key = admission.get("business_key")
        submission_key = admission.get("submission_key")
        generation = admission.get("generation")
        if (
            not isinstance(business_key, str)
            or not business_key.strip()
            or not isinstance(submission_key, str)
            or not submission_key.strip()
            or isinstance(generation, bool)
            or not isinstance(generation, int)
            or generation < 1
            or raw_slot.get("source_kind") != source_kind
            or raw_slot.get("entrypoint") != entrypoint
            or raw_slot.get("expected_outcome")
            not in (
                expected_outcome
                if isinstance(expected_outcome, frozenset)
                else {expected_outcome}
            )
            or isinstance(raw_slot.get("max_admissions"), bool)
            or raw_slot.get("max_admissions") != 1
            or source_sha256 != _sha256_json(source_identity)
        ):
            raise ActivationCliError("activation_canary_slot_plan_invalid")
        normalized[slot_kind] = {
            "source_kind": source_kind,
            "entrypoint": entrypoint,
            "source_identity": source_identity,
            "source_identity_sha256": source_sha256,
            "max_admissions": 1,
            "expected_admission": dict(admission),
            "expected_outcome": str(raw_slot.get("expected_outcome")),
        }
    if (
        len({slot["source_identity_sha256"] for slot in normalized.values()})
        != len(ACTIVATION_RELEASE_SLOT_KINDS)
        or len({
            str(slot["expected_admission"]["submission_key"])
            for slot in normalized.values()
        })
        != len(ACTIVATION_RELEASE_SLOT_KINDS)
        or len(_canonical_json(normalized).encode("utf-8")) > MAX_JSON_INPUT_BYTES
    ):
        raise ActivationCliError("activation_canary_slot_plan_invalid")
    return normalized


def _open_store(db_path: Path) -> RcaControlStore:
    candidate = Path(db_path).expanduser()
    if not candidate.is_absolute():
        raise ActivationCliError("activation_control_db_path_not_absolute")
    try:
        observed = candidate.lstat()
    except OSError as exc:
        raise ActivationCliError("activation_control_db_unavailable") from exc
    if stat.S_ISLNK(observed.st_mode) or not stat.S_ISREG(observed.st_mode):
        raise ActivationCliError("activation_control_db_not_regular")
    uri = f"{candidate.resolve(strict=True).as_uri()}?mode=ro"
    try:
        conn = sqlite3.connect(uri, uri=True)
        try:
            row = conn.execute(
                "SELECT value FROM control_meta WHERE key = 'schema_version'"
            ).fetchone()
            journal_mode = str(
                conn.execute("PRAGMA journal_mode").fetchone()[0]
            ).lower()
        finally:
            conn.close()
    except sqlite3.Error as exc:
        raise ActivationCliError("activation_control_db_schema_not_current") from exc
    if row is None or str(row[0]) != CONTROL_STORE_SCHEMA_VERSION:
        raise ActivationCliError("activation_control_db_schema_not_current")
    if journal_mode != "wal":
        raise ActivationCliError("activation_control_db_journal_mode_not_wal")
    try:
        return RcaControlStore(candidate, require_current=True)
    except RuntimeError as exc:
        if "rca_control_store_installation_marker_" in str(exc):
            raise ActivationCliError(
                "activation_control_db_installation_fenced"
            ) from exc
        raise ActivationCliError("activation_control_db_changed") from exc


def _current_epoch(
    store: RcaControlStore,
    epoch_id: str,
    allowed_states: frozenset[str],
) -> dict[str, Any]:
    current = store.activation_epoch()
    if current is None or current.get("epoch_id") != epoch_id:
        raise ActivationCliError("activation_epoch_not_current")
    if str(current.get("state") or "") not in allowed_states:
        raise ActivationCliError("activation_epoch_state_not_allowed")
    return current


def _audit_hashes(operator: str, reason: str) -> dict[str, str]:
    return {
        "operator_sha256": _sha256_text(operator),
        "reason_sha256": _sha256_text(reason),
    }


def _plan(command: str, result: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "applied": False,
        "command": command,
        "mode": "plan",
        "ok": True,
        "result": dict(result),
        "schema_version": ACTIVATION_CLI_SCHEMA_VERSION,
    }


def _applied(command: str, result: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "applied": True,
        "command": command,
        "mode": "apply",
        "ok": True,
        "result": dict(result),
        "schema_version": ACTIVATION_CLI_SCHEMA_VERSION,
    }


def _status(store: RcaControlStore, epoch_id: str = "") -> dict[str, Any]:
    health = store.health()
    activation = health.get("activation")
    if not isinstance(activation, Mapping):
        raise ActivationCliError("activation_health_contract_invalid")
    current = activation.get("current_epoch")
    if epoch_id and (
        not isinstance(current, Mapping) or current.get("epoch_id") != epoch_id
    ):
        raise ActivationCliError("activation_epoch_not_current")
    safe_activation = {
        "backlog": activation.get("backlog"),
        "configured": activation.get("configured"),
        "current_epoch": current,
        "ledger": activation.get("ledger"),
        "production_active": activation.get("production_active"),
        "slots": activation.get("slots"),
    }
    return {
        "applied": False,
        "command": "status",
        "mode": "read_only",
        "ok": True,
        "result": {
            "activation": safe_activation,
            "schema_version": health.get("schema_version"),
            "snapshot_at": health.get("snapshot_at"),
            "sqlite_data_version": health.get("sqlite_data_version"),
        },
        "schema_version": ACTIVATION_CLI_SCHEMA_VERSION,
    }


def _create(store: RcaControlStore, args: argparse.Namespace) -> dict[str, Any]:
    operator, reason = _normalized_audit(args.operator, args.reason)
    activation_input = _canonical_preauthorization_input(
        args.preauthorization_capsule,
        control_db_path=args.control_db,
    )
    epoch_id = str(activation_input["epoch_id"])
    bindings = {
        "preauthorization_fingerprint": activation_input[
            "preauthorization_fingerprint"
        ],
        "preauthorization_gate_receipt_sha256": activation_input[
            "preauthorization_gate_receipt_sha256"
        ],
        "preauthorization_capsule_sha256": activation_input[
            "preauthorization_capsule_sha256"
        ],
        "config_sha256": activation_input["config_sha256"],
        "db_logical_identity_sha256": activation_input["db_logical_identity_sha256"],
        "partition_start_fence_sha256": activation_input[
            "partition_start_fence_sha256"
        ],
    }
    prior = store.activation_epoch()
    if (
        prior is not None
        and prior.get("epoch_id") != epoch_id
        and prior.get("state") != "aborted"
    ):
        raise ActivationCliError("activation_current_epoch_exists")
    same_epoch = bool(prior is not None and prior.get("epoch_id") == epoch_id)
    identical = bool(
        same_epoch
        and all(prior.get(field) == value for field, value in bindings.items())
    )
    if same_epoch and not identical:
        raise ActivationCliError("activation_epoch_binding_conflict")
    summary = {
        **_audit_hashes(operator, reason),
        **bindings,
        "epoch_id": epoch_id,
        "initial_state": "safe_off",
        "would_change": not identical,
    }
    if not args.apply:
        return _plan("create", summary)
    epoch = store.create_activation_epoch(
        epoch_id=epoch_id,
        preauthorization_fingerprint=str(
            activation_input["preauthorization_fingerprint"]
        ),
        preauthorization_gate_receipt_sha256=str(
            activation_input["preauthorization_gate_receipt_sha256"]
        ),
        preauthorization_capsule_sha256=str(
            activation_input["preauthorization_capsule_sha256"]
        ),
        config_sha256=str(activation_input["config_sha256"]),
        db_logical_identity=activation_input["db_logical_identity"],
        partition_start_fence=activation_input["partition_start_fence"],
        operator=operator,
        reason=reason,
    )
    return _applied(
        "create",
        {
            "changed": not identical,
            "current_epoch": epoch,
            **_audit_hashes(operator, reason),
        },
    )


def _direct_steady(
    store: RcaControlStore, args: argparse.Namespace
) -> dict[str, Any]:
    operator, reason = _normalized_audit(args.operator, args.reason)
    binding = _canonical_direct_steady_binding(args.direct_binding)
    epoch_id = _normalized_epoch_id(args.epoch_id)
    if binding["epoch_id"] != epoch_id:
        raise ActivationCliError("activation_direct_binding_epoch_mismatch")

    current = store.activation_epoch()
    if current is not None:
        current_epoch_id = str(current.get("epoch_id") or "")
        current_state = str(current.get("state") or "")
        if current_epoch_id == epoch_id:
            expected = {
                "state": "steady_active",
                "preauthorization_fingerprint": binding["release_fingerprint"],
                "preauthorization_gate_receipt_sha256": binding[
                    "release_binding_sha256"
                ],
                "preauthorization_capsule_sha256": binding[
                    "release_binding_sha256"
                ],
                "preproduction_fingerprint": binding["release_fingerprint"],
                "preproduction_gate_receipt_sha256": binding[
                    "release_binding_sha256"
                ],
                "preproduction_capsule_sha256": binding[
                    "release_binding_sha256"
                ],
                "config_sha256": binding["config_sha256"],
                "db_logical_identity_sha256": binding[
                    "db_logical_identity_sha256"
                ],
                "partition_start_fence_sha256": binding[
                    "partition_start_fence_sha256"
                ],
                "partition_end_fence_sha256": binding[
                    "partition_start_fence_sha256"
                ],
                "production_fingerprint": binding["release_fingerprint"],
                "production_gate_receipt_sha256": binding[
                    "release_binding_sha256"
                ],
            }
            identical = all(current.get(field) == value for field, value in expected.items())
            if not identical:
                raise ActivationCliError("activation_direct_steady_binding_conflict")
            # A same-binding retry is a no-op, including when the operator
            # supplies a different audit label on a later invocation.
            would_change = False
        elif current_state == "aborted":
            # A different epoch may replace an aborted current pointer.
            would_change = True
        else:
            raise ActivationCliError("activation_current_epoch_exists")
    else:
        would_change = True

    summary = {
        **_audit_hashes(operator, reason),
        "activation_mode": "direct_steady",
        "config_sha256": binding["config_sha256"],
        "db_logical_identity_sha256": binding["db_logical_identity_sha256"],
        "epoch_id": epoch_id,
        "partition_start_fence_sha256": binding[
            "partition_start_fence_sha256"
        ],
        "release_fingerprint": binding["release_fingerprint"],
        "release_binding_sha256": binding[
            "release_binding_sha256"
        ],
        "would_change": would_change,
    }
    if not args.apply:
        return _plan("direct-steady", summary)
    result = store.activate_direct_steady_epoch(
        epoch_id=epoch_id,
        release_fingerprint=str(binding["release_fingerprint"]),
        release_binding_sha256=str(binding["release_binding_sha256"]),
        config_sha256=str(binding["config_sha256"]),
        db_logical_identity=binding["db_logical_identity"],
        partition_start_fence=binding["partition_start_fence"],
        operator=operator,
        reason=reason,
    )
    return _applied(
        "direct-steady",
        {
            "activation_mode": "direct_steady",
            "changed": would_change,
            "current_epoch": result,
            **_audit_hashes(operator, reason),
        },
    )


def _transition_preauthorized(
    store: RcaControlStore,
    args: argparse.Namespace,
) -> dict[str, Any]:
    operator, reason = _normalized_audit(args.operator, args.reason)
    current = store.activation_epoch()
    if current is None or str(current.get("state") or "") not in {
        "safe_off",
        "preauthorized",
    }:
        raise ActivationCliError("activation_epoch_state_not_allowed")
    transition = _canonical_preproduction_transition(
        args.preproduction_capsule,
        control_db_path=args.control_db,
        current_activation=current,
    )
    epoch_id = str(transition["epoch_id"])
    if current.get("epoch_id") != epoch_id:
        raise ActivationCliError("activation_epoch_not_current")
    expected_current = {
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
        "db_logical_identity_sha256": transition["expected_db_logical_identity_sha256"],
        "partition_start_fence_sha256": transition[
            "expected_partition_start_fence_sha256"
        ],
    }
    if any(current.get(field) != value for field, value in expected_current.items()):
        raise ActivationCliError("activation_preproduction_epoch_binding_changed")
    expected_production_binding = {
        "preproduction_fingerprint": transition["preproduction_fingerprint"],
        "preproduction_gate_receipt_sha256": transition[
            "preproduction_gate_receipt_sha256"
        ],
        "preproduction_capsule_sha256": transition["preproduction_capsule_sha256"],
    }
    identical = bool(
        current["state"] == "preauthorized"
        and all(
            current.get(field) == value
            for field, value in expected_production_binding.items()
        )
    )
    if current["state"] == "preauthorized" and not identical:
        raise ActivationCliError("activation_preproduction_binding_conflict")
    summary = {
        **_audit_hashes(operator, reason),
        **expected_production_binding,
        "epoch_id": epoch_id,
        "expected_state": current["state"],
        "target_state": "preauthorized",
        "would_change": not identical,
    }
    if not args.apply:
        return _plan("transition-preauthorized", summary)
    result = store.preauthorize_activation_epoch(
        epoch_id=epoch_id,
        preproduction_fingerprint=str(transition["preproduction_fingerprint"]),
        preproduction_gate_receipt_sha256=str(
            transition["preproduction_gate_receipt_sha256"]
        ),
        preproduction_capsule_sha256=str(transition["preproduction_capsule_sha256"]),
        expected_preauthorization_fingerprint=str(
            transition["expected_preauthorization_fingerprint"]
        ),
        expected_preauthorization_gate_receipt_sha256=str(
            transition["expected_preauthorization_gate_receipt_sha256"]
        ),
        expected_preauthorization_capsule_sha256=str(
            transition["expected_preauthorization_capsule_sha256"]
        ),
        expected_config_sha256=str(transition["expected_config_sha256"]),
        expected_db_logical_identity_sha256=str(
            transition["expected_db_logical_identity_sha256"]
        ),
        expected_partition_start_fence_sha256=str(
            transition["expected_partition_start_fence_sha256"]
        ),
        operator=operator,
        reason=reason,
    )
    return _applied(
        "transition-preauthorized",
        {
            "changed": not identical,
            "current_epoch": result,
            **_audit_hashes(operator, reason),
        },
    )


def _authorize(store: RcaControlStore, args: argparse.Namespace) -> dict[str, Any]:
    epoch_id = _normalized_epoch_id(args.epoch_id)
    operator, reason = _normalized_audit(args.operator, args.reason)
    current = _current_epoch(store, epoch_id, frozenset({"preauthorized"}))
    transition = _canonical_preproduction_transition(
        args.preproduction_capsule,
        control_db_path=args.control_db,
        current_activation=current,
    )
    _require_preproduction_binding(current, transition)
    slot = str(args.slot_kind or "").strip()
    if slot not in ACTIVATION_RELEASE_SLOT_KINDS:
        raise ActivationCliError("activation_slot_kind_invalid")
    if slot == "kafka_success":
        if not args.event_uid or args.manual_identity_json is not None:
            raise ActivationCliError("activation_slot_identity_argument_mismatch")
        _, source_identity = _normalize_event_uid(args.event_uid)
        source_kind = "kafka"
    else:
        if args.event_uid or args.manual_identity_json is None:
            raise ActivationCliError("activation_slot_identity_argument_mismatch")
        source_identity = _normalize_manual_identity(
            _read_json_object(args.manual_identity_json, "manual_identity")
        )
        source_kind = "manual"
    source_sha = _sha256_json(source_identity)
    planned = transition["canary_slot_plan"][slot]
    if (
        planned.get("source_kind") != source_kind
        or planned.get("source_identity") != source_identity
        or planned.get("source_identity_sha256") != source_sha
    ):
        raise ActivationCliError("activation_canary_plan_identity_mismatch")
    summary = {
        **_audit_hashes(operator, reason),
        "epoch_id": epoch_id,
        "expected_state": current["state"],
        "slot_kind": slot,
        "source_identity_sha256": source_sha,
        "source_kind": source_kind,
        "canary_slot_plan_sha256": transition["canary_slot_plan_sha256"],
        "would_change": None,
    }
    if not args.apply:
        return _plan("authorize", summary)
    result = store.authorize_activation_slot(
        epoch_id=epoch_id,
        slot_kind=slot,
        source_kind=source_kind,
        source_identity=source_identity,
        operator=operator,
        reason=reason,
    )
    return _applied(
        "authorize",
        {
            **result,
            **_audit_hashes(operator, reason),
        },
    )


def _require_preproduction_binding(
    current: Mapping[str, Any],
    transition: Mapping[str, Any],
) -> None:
    expected = {
        "epoch_id": transition.get("epoch_id"),
        "preauthorization_fingerprint": transition.get(
            "expected_preauthorization_fingerprint"
        ),
        "preauthorization_gate_receipt_sha256": transition.get(
            "expected_preauthorization_gate_receipt_sha256"
        ),
        "preauthorization_capsule_sha256": transition.get(
            "expected_preauthorization_capsule_sha256"
        ),
        "config_sha256": transition.get("expected_config_sha256"),
        "db_logical_identity_sha256": transition.get(
            "expected_db_logical_identity_sha256"
        ),
        "partition_start_fence_sha256": transition.get(
            "expected_partition_start_fence_sha256"
        ),
        "preproduction_fingerprint": transition.get("preproduction_fingerprint"),
        "preproduction_gate_receipt_sha256": transition.get(
            "preproduction_gate_receipt_sha256"
        ),
        "preproduction_capsule_sha256": transition.get("preproduction_capsule_sha256"),
    }
    if any(current.get(field) != value for field, value in expected.items()):
        raise ActivationCliError("activation_preproduction_epoch_binding_changed")


def _transition_bounded(
    store: RcaControlStore,
    args: argparse.Namespace,
) -> dict[str, Any]:
    epoch_id = _normalized_epoch_id(args.epoch_id)
    operator, reason = _normalized_audit(args.operator, args.reason)
    current = _current_epoch(
        store,
        epoch_id,
        frozenset({"preauthorized", "bounded_active"}),
    )
    transition = _canonical_preproduction_transition(
        args.preproduction_capsule,
        control_db_path=args.control_db,
        current_activation=current,
    )
    _require_preproduction_binding(current, transition)
    planned = transition["canary_slot_plan"]
    expected_authorizations = {
        slot_kind: {
            "source_kind": planned[slot_kind]["source_kind"],
            "source_identity_sha256": planned[slot_kind]["source_identity_sha256"],
        }
        for slot_kind in sorted(ACTIVATION_RELEASE_SLOT_KINDS)
    }
    if store.activation_slot_authorizations(epoch_id=epoch_id) != (
        expected_authorizations
    ):
        raise ActivationCliError("activation_canary_plan_authorizations_mismatch")
    summary = {
        **_audit_hashes(operator, reason),
        "epoch_id": epoch_id,
        "expected_state": current["state"],
        "target_state": "bounded_active",
        "canary_slot_plan_sha256": transition["canary_slot_plan_sha256"],
        "would_change": current["state"] != "bounded_active",
    }
    if not args.apply:
        return _plan("transition-bounded", summary)
    result = store.transition_activation_epoch(
        epoch_id=epoch_id,
        target_state="bounded_active",
        expected_state=current["state"],
        operator=operator,
        reason=reason,
    )
    return _applied(
        "transition-bounded",
        {
            "changed": current["state"] != "bounded_active",
            "current_epoch": result,
            **_audit_hashes(operator, reason),
        },
    )


def _transition(
    store: RcaControlStore,
    args: argparse.Namespace,
    *,
    command: str,
    target_state: str,
    allowed_states: frozenset[str],
) -> dict[str, Any]:
    epoch_id = _normalized_epoch_id(args.epoch_id)
    operator, reason = _normalized_audit(args.operator, args.reason)
    current = _current_epoch(store, epoch_id, allowed_states)
    summary = {
        **_audit_hashes(operator, reason),
        "epoch_id": epoch_id,
        "expected_state": current["state"],
        "target_state": target_state,
        "would_change": current["state"] != target_state,
    }
    if not args.apply:
        return _plan(command, summary)
    result = store.transition_activation_epoch(
        epoch_id=epoch_id,
        target_state=target_state,
        expected_state=current["state"],
        operator=operator,
        reason=reason,
    )
    return _applied(
        command,
        {
            "changed": current["state"] != target_state,
            "current_epoch": result,
            **_audit_hashes(operator, reason),
        },
    )


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _absolute_path_argument(value: Path | None, field: str) -> Path:
    if value is None:
        raise ActivationCliError(f"activation_{field}_required")
    candidate = Path(value).expanduser()
    if not candidate.is_absolute():
        raise ActivationCliError(f"activation_{field}_path_not_absolute")
    return candidate


def _capacity_state(store: RcaControlStore) -> dict[str, Any]:
    try:
        value = store.capacity_transition_state()
        if value is None:
            raise ActivationCliError("activation_capacity_state_missing")
        return capacity_transition.validate_persisted_capacity_state(value)
    except ActivationCliError:
        raise
    except capacity_transition.CapacityTransitionError as exc:
        raise ActivationCliError(exc.code) from exc
    except Exception as exc:
        raise ActivationCliError("activation_capacity_state_invalid") from exc


def _producer_receipt_present(path: Path) -> bool:
    try:
        path.lstat()
        return True
    except FileNotFoundError:
        return False
    except OSError as exc:
        raise ActivationCliError("activation_producer_receipt_stat_failed") from exc


def _capacity_producer_identity(
    binding: Mapping[str, Any],
) -> tuple[str, str]:
    origin = binding.get("_capacity_origin")
    if isinstance(origin, Mapping) and set(origin) == {
        "release_id",
        "bootstrap_epoch_id",
    }:
        return str(origin["release_id"]), str(origin["bootstrap_epoch_id"])
    return str(binding["release_id"]), str(binding["bootstrap_epoch_id"])


def _load_capacity_origin_compatibility(
    *,
    state: Mapping[str, Any],
    binding: Mapping[str, Any],
    authorization: Mapping[str, Any],
    paths: capacity_runtime.CapacityRuntimePaths,
    hmac_key: bytes,
    now: datetime,
) -> dict[str, str]:
    path = paths.state_root / CAPACITY_ORIGIN_COMPAT_NAME
    try:
        value, _raw = _read_json_document(
            path,
            "capacity_origin_compat",
            owner_only=True,
        )
    except ActivationCliError as exc:
        if exc.code == "activation_capacity_origin_compat_file_unavailable":
            raise ActivationCliError("activation_capacity_origin_binding_invalid") from exc
        raise
    if (
        set(value) != _CAPACITY_ORIGIN_COMPAT_FIELDS
        or value.get("schema_version") != CAPACITY_ORIGIN_COMPAT_SCHEMA_VERSION
        or value.get("database_rows_modified") is not False
        or value.get("external_effects_triggered") is not False
        or value.get("producer_path") != str(paths.producer_activation)
    ):
        raise ActivationCliError("activation_capacity_origin_compat_invalid")
    try:
        created_at = datetime.fromisoformat(
            str(value.get("created_at") or "").replace("Z", "+00:00")
        ).astimezone(timezone.utc)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ActivationCliError("activation_capacity_origin_compat_invalid") from exc
    age = (now.astimezone(timezone.utc) - created_at).total_seconds()
    if age < -5 or age > CAPACITY_ORIGIN_COMPAT_MAX_AGE_SECONDS:
        raise ActivationCliError("activation_capacity_origin_compat_stale")
    origin = {
        "release_id": str(state.get("release_id") or ""),
        "bootstrap_epoch_id": str(state.get("bootstrap_epoch_id") or ""),
    }
    expected = {
        "current_release_id": str(binding["release_id"]),
        "current_bootstrap_epoch_id": str(binding["bootstrap_epoch_id"]),
        "capacity_origin_release_id": origin["release_id"],
        "capacity_origin_bootstrap_epoch_id": origin["bootstrap_epoch_id"],
        "active_release_binding_sha256": str(binding["binding_receipt_sha256"]),
        "release_bom_sha256": str(binding["release_bom_sha256"]),
    }
    if any(value.get(field) != expected_value for field, expected_value in expected.items()):
        raise ActivationCliError("activation_capacity_origin_compat_invalid")
    bound = dict(binding)
    bound["_capacity_origin"] = origin
    try:
        producer, producer_sha256 = (
            capacity_evidence.read_and_validate_producer_activation(
                paths.producer_activation,
                hmac_key=hmac_key,
                expected_release_id=origin["release_id"],
                expected_bootstrap_epoch_id=origin["bootstrap_epoch_id"],
                expected_release_bom_sha256=str(binding["release_bom_sha256"]),
                expected_active_release_binding_sha256=str(
                    binding["binding_receipt_sha256"]
                ),
            )
        )
    except capacity_evidence.CapacitySampleEvidenceError as exc:
        raise ActivationCliError(exc.code) from exc
    if (
        value.get("producer_sha256") != producer_sha256
        or value.get("producer_receipt_fingerprint")
        != producer.get("receipt_fingerprint")
        or producer.get("receipt_id")
        != _producer_receipt_id(binding=bound, authorization=authorization)
    ):
        raise ActivationCliError("activation_capacity_origin_compat_invalid")
    return origin


def _bootstrap_authority(
    args: argparse.Namespace,
    *,
    state: Mapping[str, Any],
    operator: str,
    now: datetime,
) -> tuple[
    dict[str, Any], dict[str, Any], bytes, capacity_runtime.CapacityRuntimePaths
]:
    release_id = str(getattr(args, "release_id", "") or "").strip()
    bootstrap_epoch_id = str(getattr(args, "bootstrap_epoch_id", "") or "").strip()
    if not release_id:
        raise ActivationCliError("activation_release_id_required")
    if not bootstrap_epoch_id:
        raise ActivationCliError("activation_bootstrap_epoch_id_required")
    if state.get("generation") != 1:
        raise ActivationCliError("activation_capacity_origin_binding_invalid")
    active_binding_path = _absolute_path_argument(
        getattr(args, "active_release_binding", None), "active_release_binding"
    )
    live_env_path = _absolute_path_argument(getattr(args, "live_env", None), "live_env")
    expected_active_binding = (
        Path(args.control_db).expanduser().absolute().parent
        / prod_bootstrap.ACTIVE_RELEASE_BINDING_NAME
    )
    if active_binding_path != expected_active_binding:
        raise ActivationCliError("activation_active_release_binding_path_invalid")
    try:
        binding = prod_bootstrap.load_active_release_binding(
            path=active_binding_path,
            live_env_path=live_env_path,
            expected_release_id=release_id,
            expected_epoch_id=bootstrap_epoch_id,
        )
        authorization = prod_bootstrap.load_bootstrap_authorization(
            now=now,
            expected_epoch_id=binding["bootstrap_epoch_id"],
            expected_release_bom_sha256=binding["release_bom_sha256"],
            expected_release_approval_id=binding["release_id"],
            expected_approval_evidence_sha256=binding["approval_evidence_sha256"],
        )
    except prod_bootstrap.RcaBootstrapAuthorizationError as exc:
        raise ActivationCliError(exc.code) from exc
    if (
        authorization.get("authorization_ready") is not True
        or authorization.get("capacity_mode") != "bootstrap"
        or authorization.get("authorized_by") != operator
        or authorization.get("authorization_receipt_sha256")
        != binding.get("authorization_receipt_sha256")
        or authorization.get("receipt_fingerprint")
        != binding.get("authorization_fingerprint")
    ):
        raise ActivationCliError("activation_bootstrap_authority_binding_invalid")
    try:
        hmac_key = capacity_runtime.load_capacity_hmac_key()
    except capacity_runtime.CapacityRuntimeError as exc:
        raise ActivationCliError(exc.code) from exc
    paths = capacity_runtime.CapacityRuntimePaths.from_control_db(args.control_db)
    if (
        state.get("release_id") != release_id
        or state.get("bootstrap_epoch_id") != bootstrap_epoch_id
    ):
        origin = _load_capacity_origin_compatibility(
            state=state,
            binding=binding,
            authorization=authorization,
            paths=paths,
            hmac_key=hmac_key,
            now=now,
        )
        binding = dict(binding)
        binding["_capacity_origin"] = origin
    return binding, authorization, hmac_key, paths


def _producer_receipt_id(
    *, binding: Mapping[str, Any], authorization: Mapping[str, Any]
) -> str:
    release_id, bootstrap_epoch_id = _capacity_producer_identity(binding)
    identity = {
        "release_id": release_id,
        "bootstrap_epoch_id": bootstrap_epoch_id,
        "release_bom_sha256": binding["release_bom_sha256"],
        "active_release_binding_sha256": binding["binding_receipt_sha256"],
        "authorization_receipt_sha256": authorization["authorization_receipt_sha256"],
        "authorization_fingerprint": authorization["receipt_fingerprint"],
    }
    return f"producer-{_sha256_json(identity)[:32]}"


def _validate_producer_deadline_window(
    *,
    activated_at: datetime,
    deadline: datetime | str,
) -> dict[str, Any]:
    try:
        return capacity_transition.validate_producer_deadline_window(
            activated_at=activated_at,
            deadline=deadline,
        )
    except capacity_transition.CapacityTransitionError as exc:
        raise ActivationCliError(exc.code) from exc


def _validate_producer_live_horizon(
    *,
    observed_at: datetime,
    deadline: datetime | str,
) -> dict[str, Any]:
    try:
        return capacity_transition.validate_producer_live_horizon(
            observed_at=observed_at,
            deadline=deadline,
        )
    except capacity_transition.CapacityTransitionError as exc:
        raise ActivationCliError(exc.code) from exc


def _read_bound_producer_receipt(
    *,
    paths: capacity_runtime.CapacityRuntimePaths,
    binding: Mapping[str, Any],
    authorization: Mapping[str, Any],
    hmac_key: bytes,
    now: datetime,
) -> tuple[dict[str, Any], str]:
    release_id, bootstrap_epoch_id = _capacity_producer_identity(binding)
    try:
        receipt, raw_sha256 = capacity_evidence.read_and_validate_producer_activation(
            paths.producer_activation,
            hmac_key=hmac_key,
            expected_release_id=release_id,
            expected_bootstrap_epoch_id=bootstrap_epoch_id,
            expected_release_bom_sha256=str(binding["release_bom_sha256"]),
            expected_active_release_binding_sha256=str(
                binding["binding_receipt_sha256"]
            ),
        )
    except capacity_evidence.CapacitySampleEvidenceError as exc:
        raise ActivationCliError(exc.code) from exc
    if receipt.get("receipt_id") != _producer_receipt_id(
        binding=binding, authorization=authorization
    ):
        raise ActivationCliError("activation_producer_receipt_identity_invalid")
    try:
        activated_at = datetime.fromisoformat(
            str(receipt["activated_at"]).replace("Z", "+00:00")
        ).astimezone(timezone.utc)
        started_at = datetime.fromisoformat(
            str(authorization["started_at"]).replace("Z", "+00:00")
        ).astimezone(timezone.utc)
        deadline = datetime.fromisoformat(
            str(authorization["deadline"]).replace("Z", "+00:00")
        ).astimezone(timezone.utc)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ActivationCliError("activation_producer_receipt_time_invalid") from exc
    if not started_at <= activated_at <= min(deadline, now):
        raise ActivationCliError("activation_producer_receipt_time_invalid")
    _validate_producer_deadline_window(
        activated_at=activated_at,
        deadline=deadline,
    )
    _validate_producer_live_horizon(
        observed_at=now,
        deadline=deadline,
    )
    return receipt, raw_sha256


def _validate_capacity_runtime_locked(
    *,
    store: RcaControlStore,
    args: argparse.Namespace,
    state: Mapping[str, Any],
    hmac_key: bytes,
    now: datetime,
) -> dict[str, Any]:
    resolver = capacity_runtime.CapacityRuntimeResolver(
        store=store,
        control_db_path=args.control_db,
        release_id=str(state["release_id"]),
        bootstrap_epoch_id=str(state["bootstrap_epoch_id"]),
        initial_policy=(
            "bootstrap"
            if state["state"] == capacity_transition.BOOTSTRAP_PRODUCTION
            else "steady"
        ),
        hmac_key=hmac_key,
        now=lambda: now,
    )
    decision = resolver._resolve_locked(lock_latency_ms=0.0)
    expected = (
        {
            capacity_transition.BOOTSTRAP_PRODUCTION,
            capacity_transition.STEADY_READY,
        }
        if state["state"] == capacity_transition.BOOTSTRAP_PRODUCTION
        else {capacity_transition.STEADY_ACTIVE}
    )
    if (
        decision.get("ready") is not True
        or decision.get("effective_state") not in expected
    ):
        reason = str(decision.get("reason_code") or "")
        if _SAFE_ERROR_CODE_RE.fullmatch(reason):
            raise ActivationCliError(reason)
        raise ActivationCliError("activation_capacity_runtime_not_ready")
    return decision


def _require_bounded_preproduction_scope(
    store: RcaControlStore,
    args: argparse.Namespace,
    current: Mapping[str, Any],
) -> dict[str, Any]:
    transition = _canonical_preproduction_transition(
        args.preproduction_capsule,
        control_db_path=args.control_db,
        current_activation=current,
    )
    _require_preproduction_binding(current, transition)
    planned = transition["canary_slot_plan"]
    expected_authorizations = {
        slot_kind: {
            "source_kind": planned[slot_kind]["source_kind"],
            "source_identity_sha256": planned[slot_kind]["source_identity_sha256"],
        }
        for slot_kind in sorted(ACTIVATION_RELEASE_SLOT_KINDS)
    }
    if (
        store.activation_slot_authorizations(epoch_id=str(current["epoch_id"]))
        != expected_authorizations
    ):
        raise ActivationCliError("activation_canary_plan_authorizations_mismatch")
    return transition


def _publish_bootstrap_producer_locked(
    *,
    store: RcaControlStore,
    args: argparse.Namespace,
    state: Mapping[str, Any],
    operator: str,
    now: datetime,
) -> tuple[dict[str, Any], str, str]:
    binding, authorization, hmac_key, paths = _bootstrap_authority(
        args,
        state=state,
        operator=operator,
        now=now,
    )
    if not _producer_receipt_present(paths.producer_activation):
        _validate_producer_deadline_window(
            activated_at=now,
            deadline=str(authorization["deadline"]),
        )
        try:
            release_id, bootstrap_epoch_id = _capacity_producer_identity(binding)
            receipt = capacity_evidence.issue_producer_activation_receipt(
                release_id=release_id,
                bootstrap_epoch_id=bootstrap_epoch_id,
                release_bom_sha256=str(binding["release_bom_sha256"]),
                active_release_binding_sha256=str(binding["binding_receipt_sha256"]),
                activated_at=now,
                hmac_key=hmac_key,
                receipt_id=_producer_receipt_id(
                    binding=binding, authorization=authorization
                ),
            )
            capacity_evidence.write_owner_only_create_once(
                paths.producer_activation, receipt
            )
        except capacity_evidence.CapacitySampleEvidenceError as exc:
            raise ActivationCliError(exc.code) from exc
    receipt, producer_sha256 = _read_bound_producer_receipt(
        paths=paths,
        binding=binding,
        authorization=authorization,
        hmac_key=hmac_key,
        now=now,
    )
    decision = _validate_capacity_runtime_locked(
        store=store,
        args=args,
        state=state,
        hmac_key=hmac_key,
        now=now,
    )
    return decision, producer_sha256, str(receipt["receipt_fingerprint"])


def _prepare_bootstrap_production(
    store: RcaControlStore, args: argparse.Namespace
) -> dict[str, Any]:
    epoch_id = _normalized_epoch_id(args.epoch_id)
    operator, reason = _normalized_audit(args.operator, args.reason)
    current = _current_epoch(store, epoch_id, frozenset({"bounded_active"}))
    transition = _require_bounded_preproduction_scope(store, args, current)
    state = _capacity_state(store)
    if state["state"] != capacity_transition.BOOTSTRAP_PRODUCTION:
        raise ActivationCliError("activation_capacity_state_not_bootstrap")
    plan_now = _utc_now()
    binding, authorization, hmac_key, paths = _bootstrap_authority(
        args,
        state=state,
        operator=operator,
        now=plan_now,
    )
    producer_present = _producer_receipt_present(paths.producer_activation)
    if producer_present:
        _read_bound_producer_receipt(
            paths=paths,
            binding=binding,
            authorization=authorization,
            hmac_key=hmac_key,
            now=plan_now,
        )
    else:
        _validate_producer_deadline_window(
            activated_at=plan_now,
            deadline=str(authorization["deadline"]),
        )
    summary = {
        **_audit_hashes(operator, reason),
        "canary_slot_plan_sha256": transition["canary_slot_plan_sha256"],
        "capacity_generation": state["generation"],
        "capacity_state": state["state"],
        "epoch_id": epoch_id,
        "expected_state": current["state"],
        "producer_receipt_present": producer_present,
        "would_publish_producer_receipt": not producer_present,
    }
    if not args.apply:
        return _plan("prepare-bootstrap-production", summary)

    try:
        capacity_evidence.ensure_owner_only_lock_file(paths.global_lock)
    except capacity_evidence.CapacitySampleEvidenceError as exc:
        raise ActivationCliError(exc.code) from exc
    try:
        lock_context = capacity_transition.capacity_flock(
            paths.global_lock,
            exclusive=True,
            timeout_seconds=capacity_runtime.DEFAULT_LOCK_TIMEOUT_SECONDS,
        )
        lock_context.__enter__()
    except capacity_transition.CapacityTransitionError as exc:
        raise ActivationCliError(exc.code) from exc
    try:
        now = _utc_now()
        current = _current_epoch(store, epoch_id, frozenset({"bounded_active"}))
        _require_bounded_preproduction_scope(store, args, current)
        state = _capacity_state(store)
        if state["state"] != capacity_transition.BOOTSTRAP_PRODUCTION:
            raise ActivationCliError("activation_capacity_state_not_bootstrap")
        decision, producer_sha256, producer_fingerprint = (
            _publish_bootstrap_producer_locked(
                store=store,
                args=args,
                state=state,
                operator=operator,
                now=now,
            )
        )
    finally:
        lock_context.__exit__(None, None, None)
    return _applied(
        "prepare-bootstrap-production",
        {
            **summary,
            "producer_activation_receipt_sha256": producer_sha256,
            "producer_activation_receipt_fingerprint": producer_fingerprint,
            "producer_receipt_present": True,
            "runtime_effective_state": decision["effective_state"],
            "would_publish_producer_receipt": False,
        },
    )


def _transition_steady(
    store: RcaControlStore, args: argparse.Namespace
) -> dict[str, Any]:
    epoch_id = _normalized_epoch_id(args.epoch_id)
    operator, reason = _normalized_audit(args.operator, args.reason)
    current = _current_epoch(store, epoch_id, frozenset({"confirmed", "steady_active"}))
    state = _capacity_state(store)
    if state["state"] not in {
        capacity_transition.BOOTSTRAP_PRODUCTION,
        capacity_transition.STEADY_ACTIVE,
    }:
        raise ActivationCliError("activation_capacity_state_invalid")

    binding: dict[str, Any] | None = None
    authorization: dict[str, Any] | None = None
    hmac_key: bytes | None = None
    paths = capacity_runtime.CapacityRuntimePaths.from_control_db(args.control_db)
    producer_present = _producer_receipt_present(paths.producer_activation)
    if state["state"] == capacity_transition.BOOTSTRAP_PRODUCTION:
        binding, authorization, hmac_key, paths = _bootstrap_authority(
            args,
            state=state,
            operator=operator,
            now=_utc_now(),
        )
        if not producer_present:
            raise ActivationCliError("activation_producer_receipt_required")
        _read_bound_producer_receipt(
            paths=paths,
            binding=binding,
            authorization=authorization,
            hmac_key=hmac_key,
            now=_utc_now(),
        )

    summary = {
        **_audit_hashes(operator, reason),
        "capacity_generation": state["generation"],
        "capacity_state": state["state"],
        "epoch_id": epoch_id,
        "expected_state": current["state"],
        "producer_receipt_present": producer_present,
        "target_state": "steady_active",
        "would_change": current["state"] != "steady_active",
        "would_publish_producer_receipt": bool(
            state["state"] == capacity_transition.BOOTSTRAP_PRODUCTION
            and not producer_present
        ),
    }
    if not args.apply:
        return _plan("transition-steady", summary)

    try:
        capacity_evidence.ensure_owner_only_lock_file(paths.global_lock)
    except capacity_evidence.CapacitySampleEvidenceError as exc:
        raise ActivationCliError(exc.code) from exc
    try:
        lock_context = capacity_transition.capacity_flock(
            paths.global_lock,
            exclusive=True,
            timeout_seconds=capacity_runtime.DEFAULT_LOCK_TIMEOUT_SECONDS,
        )
        lock_context.__enter__()
    except capacity_transition.CapacityTransitionError as exc:
        raise ActivationCliError(exc.code) from exc
    try:
        now = _utc_now()
        state = _capacity_state(store)
        current = _current_epoch(
            store, epoch_id, frozenset({"confirmed", "steady_active"})
        )
        producer_sha256 = ""
        producer_fingerprint = ""
        if state["state"] == capacity_transition.BOOTSTRAP_PRODUCTION:
            binding, authorization, hmac_key, paths = _bootstrap_authority(
                args,
                state=state,
                operator=operator,
                now=now,
            )
            if not _producer_receipt_present(paths.producer_activation):
                raise ActivationCliError("activation_producer_receipt_required")
            receipt, producer_sha256 = _read_bound_producer_receipt(
                paths=paths,
                binding=binding,
                authorization=authorization,
                hmac_key=hmac_key,
                now=now,
            )
            producer_fingerprint = str(receipt["receipt_fingerprint"])
        elif state["state"] == capacity_transition.STEADY_ACTIVE:
            try:
                hmac_key = capacity_runtime.load_capacity_hmac_key()
            except capacity_runtime.CapacityRuntimeError as exc:
                raise ActivationCliError(exc.code) from exc
        else:
            raise ActivationCliError("activation_capacity_state_invalid")
        if hmac_key is None:
            raise ActivationCliError("activation_capacity_hmac_key_invalid")
        decision = _validate_capacity_runtime_locked(
            store=store,
            args=args,
            state=state,
            hmac_key=hmac_key,
            now=now,
        )
        result = store.transition_activation_epoch(
            epoch_id=epoch_id,
            target_state="steady_active",
            expected_state=current["state"],
            operator=operator,
            reason=reason,
        )
    finally:
        lock_context.__exit__(None, None, None)
    return _applied(
        "transition-steady",
        {
            **summary,
            "capacity_generation": state["generation"],
            "capacity_state": state["state"],
            "changed": current["state"] != "steady_active",
            "current_epoch": result,
            "producer_activation_receipt_sha256": producer_sha256,
            "producer_activation_receipt_fingerprint": producer_fingerprint,
            "runtime_effective_state": decision["effective_state"],
            "would_publish_producer_receipt": False,
        },
    )


def _confirm(store: RcaControlStore, args: argparse.Namespace) -> dict[str, Any]:
    operator, reason = _normalized_audit(args.operator, args.reason)
    receipt_path, epoch_id, config_sha256, partition_end_fence = (
        _confirmation_capsule_scope(args.confirmation_capsule)
    )
    current = _current_epoch(
        store, epoch_id, frozenset({"bounded_active", "confirmed"})
    )
    if current.get("config_sha256") != config_sha256:
        raise ActivationCliError("activation_confirmation_epoch_binding_changed")
    validation_activation: Mapping[str, Any] = current
    if current["state"] == "bounded_active":
        validation_activation = _live_release_binding(
            args.control_db,
            epoch_id=epoch_id,
            expected_config_sha256=config_sha256,
            partition_end_fence=partition_end_fence,
        )
    transition = _canonical_confirmation_transition(
        capsule_path=args.confirmation_capsule,
        receipt_path=receipt_path,
        control_db_path=args.control_db,
        current_activation=validation_activation,
    )
    for field in (
        "config_sha256",
        "db_logical_identity_sha256",
        "partition_start_fence_sha256",
    ):
        if current.get(field) != transition[field]:
            raise ActivationCliError("activation_confirmation_epoch_binding_changed")
    end_fence = transition["partition_end_fence"]
    end_fence_sha = str(transition["partition_end_fence_sha256"])
    production = str(transition["production_fingerprint"])
    receipt = str(transition["production_gate_receipt_sha256"])
    identical = bool(
        current["state"] == "confirmed"
        and current.get("partition_end_fence_sha256") == end_fence_sha
        and current.get("production_fingerprint") == production
        and current.get("production_gate_receipt_sha256") == receipt
    )
    if current["state"] == "confirmed" and not identical:
        raise ActivationCliError("activation_confirmation_binding_conflict")
    summary = {
        **_audit_hashes(operator, reason),
        "epoch_id": epoch_id,
        "expected_state": current["state"],
        "partition_end_fence_sha256": end_fence_sha,
        "production_fingerprint": production,
        "production_gate_receipt_sha256": receipt,
        "target_state": "confirmed",
        "would_change": not identical,
    }
    if not args.apply:
        return _plan("confirm", summary)
    result = store.transition_activation_epoch(
        epoch_id=epoch_id,
        target_state="confirmed",
        expected_state=current["state"],
        partition_end_fence=end_fence,
        production_fingerprint=production,
        production_gate_receipt_sha256=receipt,
        expected_config_sha256=str(transition["config_sha256"]),
        expected_db_logical_identity_sha256=str(
            transition["db_logical_identity_sha256"]
        ),
        expected_partition_start_fence_sha256=str(
            transition["partition_start_fence_sha256"]
        ),
        expected_release_binding_sha256=str(transition["release_binding_sha256"]),
        operator=operator,
        reason=reason,
    )
    return _applied(
        "confirm",
        {
            "changed": not identical,
            "current_epoch": result,
            **_audit_hashes(operator, reason),
        },
    )


def _reconcile_shadow(
    store: RcaControlStore, args: argparse.Namespace
) -> dict[str, Any]:
    epoch_id = _normalized_epoch_id(args.epoch_id)
    operator, reason = _normalized_audit(args.operator, args.reason)
    event_uid, _ = _normalize_event_uid(args.event_uid)
    current = _current_epoch(
        store, epoch_id, frozenset({"bounded_active", "confirmed"})
    )
    slot = str(args.slot_kind or "").strip()
    if current["state"] == "confirmed" and slot:
        raise ActivationCliError("activation_confirmed_reconcile_slot_forbidden")
    if current["state"] == "bounded_active" and slot != "kafka_success":
        raise ActivationCliError("activation_bounded_reconcile_slot_required")
    summary = {
        **_audit_hashes(operator, reason),
        "epoch_id": epoch_id,
        "event_uid_sha256": _sha256_text(event_uid),
        "expected_state": current["state"],
        "slot_kind": slot,
        "would_change": None,
    }
    if not args.apply:
        return _plan("reconcile-shadow", summary)
    promoted = store.promote_shadow_event(
        event_uid,
        operator=operator,
        reason=reason,
        expected_activation_epoch_id=epoch_id,
        activation_required=True,
        activation_slot_kind=slot,
    )
    result = asdict(promoted)
    return _applied(
        "reconcile-shadow",
        {
            "audit_id": result["audit_id"],
            "changed": bool(result["promoted"]),
            "event_uid_sha256": _sha256_text(str(result["event_uid"])),
            "outbox_id": result["outbox_id"],
            "status": result["status"],
            "submission_key_sha256": _sha256_text(str(result["submission_key"])),
            **_audit_hashes(operator, reason),
        },
    )


def _defer_event(store: RcaControlStore, args: argparse.Namespace) -> dict[str, Any]:
    epoch_id = _normalized_epoch_id(args.epoch_id)
    operator, reason = _normalized_audit(args.operator, args.reason)
    if args.message_id:
        event_uid = str(args.message_id).strip()
        if _MANUAL_MESSAGE_ID_RE.fullmatch(event_uid) is None:
            raise ActivationCliError("activation_deferred_message_id_invalid")
    else:
        event_uid, _ = _normalize_event_uid(args.event_uid)
    current = _current_epoch(
        store,
        epoch_id,
        frozenset({
            "safe_off",
            "preauthorized",
            "bounded_active",
            "confirmed",
            "aborted",
        }),
    )
    summary = {
        **_audit_hashes(operator, reason),
        "epoch_id": epoch_id,
        "event_uid_sha256": _sha256_text(event_uid),
        "expected_state": current["state"],
        "would_change": None,
    }
    if not args.apply:
        return _plan("defer-event", summary)
    deferred = asdict(
        store.defer_activation_event(
            event_uid,
            expected_activation_epoch_id=epoch_id,
            operator=operator,
            reason=reason,
        )
    )
    return _applied(
        "defer-event",
        {
            "audit_id": deferred["audit_id"],
            "changed": deferred["prior_status"] != "quarantined",
            "epoch_id": deferred["epoch_id"],
            "event_uid_sha256": _sha256_text(str(deferred["event_uid"])),
            "outbox_id": deferred["outbox_id"],
            "prior_status": deferred["prior_status"],
            "status": deferred["status"],
            "submission_key_sha256": _sha256_text(str(deferred["submission_key"])),
            **_audit_hashes(operator, reason),
        },
    )


def _abort(store: RcaControlStore, args: argparse.Namespace) -> dict[str, Any]:
    return _transition(
        store,
        args,
        command="abort",
        target_state="aborted",
        allowed_states=frozenset({
            "safe_off",
            "preauthorized",
            "bounded_active",
            "confirmed",
            "steady_active",
            "aborted",
        }),
    )


def _add_mutation_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--epoch-id", required=True)
    parser.add_argument("--operator", required=True)
    parser.add_argument("--reason", required=True)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Perform the mutation. Without this flag the command is a plan only.",
    )


def _add_confirmation_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--operator", required=True)
    parser.add_argument("--reason", required=True)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Perform the mutation. Without this flag the command is a plan only.",
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = _SafeArgumentParser(description=__doc__)
    parser.add_argument("--control-db", type=Path, required=True)
    commands = parser.add_subparsers(dest="command", parser_class=_SafeArgumentParser)

    status_parser = commands.add_parser("status")
    status_parser.add_argument("--epoch-id", default="")

    create = commands.add_parser("create")
    _add_confirmation_arguments(create)
    create.add_argument("--preauthorization-capsule", type=Path, required=True)

    preauthorized = commands.add_parser("transition-preauthorized")
    _add_confirmation_arguments(preauthorized)
    preauthorized.add_argument("--preproduction-capsule", type=Path, required=True)

    authorize = commands.add_parser("authorize")
    _add_mutation_arguments(authorize)
    authorize.add_argument(
        "--slot-kind", choices=ACTIVATION_RELEASE_SLOT_KINDS, required=True
    )
    authorize.add_argument("--preproduction-capsule", type=Path, required=True)
    identity = authorize.add_mutually_exclusive_group(required=True)
    identity.add_argument("--event-uid")
    identity.add_argument("--manual-identity-json", type=Path)

    bounded = commands.add_parser("transition-bounded")
    _add_mutation_arguments(bounded)
    bounded.add_argument("--preproduction-capsule", type=Path, required=True)

    confirm = commands.add_parser("confirm")
    _add_confirmation_arguments(confirm)
    confirm.add_argument("--confirmation-capsule", type=Path, required=True)

    reconcile = commands.add_parser("reconcile-shadow")
    _add_mutation_arguments(reconcile)
    reconcile.add_argument("--event-uid", required=True)
    reconcile.add_argument("--slot-kind", choices=ACTIVATION_SLOT_KINDS, default="")

    defer = commands.add_parser("defer-event")
    _add_mutation_arguments(defer)
    identity = defer.add_mutually_exclusive_group(required=True)
    identity.add_argument("--event-uid")
    identity.add_argument(
        "--message-id",
        help="exact Feishu manual message_id already bound to this epoch",
    )

    prepare_bootstrap = commands.add_parser("prepare-bootstrap-production")
    _add_mutation_arguments(prepare_bootstrap)
    prepare_bootstrap.add_argument("--preproduction-capsule", type=Path, required=True)
    prepare_bootstrap.add_argument("--active-release-binding", type=Path, required=True)
    prepare_bootstrap.add_argument("--live-env", type=Path, required=True)
    prepare_bootstrap.add_argument("--release-id", required=True)
    prepare_bootstrap.add_argument("--bootstrap-epoch-id", required=True)

    steady = commands.add_parser("transition-steady")
    _add_mutation_arguments(steady)
    steady.add_argument("--active-release-binding", type=Path)
    steady.add_argument("--live-env", type=Path)
    steady.add_argument("--release-id")
    steady.add_argument("--bootstrap-epoch-id")

    direct_steady = commands.add_parser("direct-steady")
    _add_mutation_arguments(direct_steady)
    direct_steady.add_argument("--direct-binding", type=Path, required=True)

    abort = commands.add_parser("abort")
    _add_mutation_arguments(abort)
    return parser


def _safe_exception_code(exc: BaseException) -> str:
    if isinstance(exc, ActivationCliError):
        return exc.code
    message = str(exc)
    if isinstance(exc, ActivationEpochError) and _SAFE_ERROR_CODE_RE.fullmatch(message):
        return message
    if isinstance(exc, ShadowPromotionError):
        match = re.search(r": ([a-z][a-z0-9_]{2,127})$", message)
        if match is not None:
            return f"activation_shadow_{match.group(1)}"
        return "activation_shadow_promotion_denied"
    return "activation_cli_internal_error"


def _emit(payload: Mapping[str, Any]) -> None:
    print(_canonical_json(dict(payload)))


def main(argv: Sequence[str] | None = None) -> int:
    command = "unknown"
    try:
        args = _build_parser().parse_args(argv)
        command = str(args.command or "status")
        store = _open_store(args.control_db)
        if command == "status":
            requested_epoch = str(getattr(args, "epoch_id", "") or "")
            epoch_id = _normalized_epoch_id(requested_epoch) if requested_epoch else ""
            payload = _status(store, epoch_id)
        elif command == "create":
            payload = _create(store, args)
        elif command == "transition-preauthorized":
            payload = _transition_preauthorized(store, args)
        elif command == "authorize":
            payload = _authorize(store, args)
        elif command == "transition-bounded":
            payload = _transition_bounded(store, args)
        elif command == "confirm":
            payload = _confirm(store, args)
        elif command == "reconcile-shadow":
            payload = _reconcile_shadow(store, args)
        elif command == "defer-event":
            payload = _defer_event(store, args)
        elif command == "prepare-bootstrap-production":
            payload = _prepare_bootstrap_production(store, args)
        elif command == "transition-steady":
            payload = _transition_steady(store, args)
        elif command == "direct-steady":
            payload = _direct_steady(store, args)
        elif command == "abort":
            payload = _abort(store, args)
        else:
            raise ActivationCliError("activation_cli_command_invalid")
    except SystemExit:
        raise
    except Exception as exc:
        _emit({
            "code": _safe_exception_code(exc),
            "command": command,
            "ok": False,
            "schema_version": ACTIVATION_CLI_SCHEMA_VERSION,
        })
        return 2
    _emit(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
