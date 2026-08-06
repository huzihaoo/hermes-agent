#!/usr/bin/env python3
"""Install the record-only half of one RCA successor release transaction."""

from __future__ import annotations

import argparse
from contextlib import contextmanager
import copy
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import plistlib
import re
import sqlite3
import stat
import tempfile
from typing import Any, Iterator, Mapping, Sequence
import uuid

import yaml

from gateway import pnc_rca_delivery_quarantine_baseline as quarantine_baseline
from gateway import pnc_rca_prod_bootstrap as bootstrap
from gateway import pnc_rca_release_authority as authority
from scripts import pnc_rca_release_transaction as base


PROFILE_SCHEMA_VERSION = "pnc_rca_steady_release_profile_v1"
PLAN_SCHEMA_VERSION = "pnc_rca_steady_release_transaction_plan_v1"
RECEIPT_SCHEMA_VERSION = "pnc_rca_steady_release_transaction_receipt_v1"
ROLLBACK_SCHEMA_VERSION = "pnc_rca_steady_release_transaction_rollback_v1"
CLI_SCHEMA_VERSION = "pnc_rca_steady_release_transaction_cli_v1"
LOCK_NAME = ".pnc-rca-release-transaction.lock"
PROFILE_NAME = "steady-release-profile.json"
STALE_RELAY_TASK_ID = "rca-r11-safe-off-no-task"

PROFILE_FIELDS = frozenset(
    {
        "schema_version",
        "release_id",
        "authority_sha256",
        "source",
        "activation",
        "read_only_plist_anchors",
    }
)
PROFILE_SOURCE_FIELDS = frozenset({"commit", "tree"})
PROFILE_ACTIVATION_FIELDS = frozenset(
    {
        "predecessor_epoch_id",
        "predecessor_state",
        "predecessor_binding_fingerprint",
        "successor_epoch_id",
    }
)
PROFILE_ANCHOR_FIELDS = frozenset({"path", "sha256"})
READ_ONLY_PLIST_NAMES = (
    "ai.hermes.gateway.plist",
    "local.pnc.rca-kafka-consumer.plist",
    "local.pnc.rca-outbox-dispatcher.plist",
)
ACTIVATION_BINDING_FIELDS = frozenset(
    {
        "epoch_id",
        "state",
        "binding_fingerprint",
        "transition_audit_id",
        "transitioned_at",
    }
)
DISPATCHER_RELEASE_KEYS = frozenset(
    {
        "HERMES_RCA_DELIVERY_DISPATCHER_INVENTORY_PIN",
        "HERMES_RCA_DELIVERY_DISPATCHER_OBSERVABILITY_ENABLED",
        "HERMES_RCA_DELIVERY_DISPATCHER_OBSERVATION_RELEASE_ID",
    }
)
OBSERVATION_FIELDS = frozenset(
    {
        "exists",
        "sha256",
        "mode",
        "size_bytes",
        "device",
        "inode",
        "mtime_ns",
        "ctime_ns",
    }
)
ENTRY_FIELDS = frozenset(
    {
        "name",
        "kind",
        "source_path",
        "source",
        "staged_path",
        "target_path",
        "target_mode",
        "before",
        "rollback_path",
    }
)
PLAN_FIELDS = frozenset(
    {
        "schema_version",
        "cli_schema_version",
        "transaction_id",
        "planned_at",
        "release_id",
        "authority_sha256",
        "authority_epoch_id",
        "source_root",
        "source_commit",
        "source_tree",
        "candidate_root",
        "home",
        "hermes_home",
        "control_db",
        "state_root",
        "transaction_dir",
        "rollback_dir",
        "steady_profile",
        "read_only_plist_anchors",
        "activation_binding",
        "quarantine_baseline",
        "entries",
        "mutation_performed",
    }
)
HEX64_RE = re.compile(r"^[0-9a-f]{64}$")
IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")


class SteadyReleaseTransactionError(RuntimeError):
    def __init__(self, code: str) -> None:
        self.code = str(code or "pnc_steady_release_transaction_invalid")[:160]
        super().__init__(self.code)


def _fail(code: str, exc: Exception | None = None) -> None:
    error = SteadyReleaseTransactionError(code)
    if exc is None:
        raise error
    raise error from exc


def _canonical_bytes(value: Mapping[str, Any]) -> bytes:
    try:
        return (
            json.dumps(
                dict(value),
                ensure_ascii=True,
                separators=(",", ":"),
                sort_keys=True,
                allow_nan=False,
            )
            + "\n"
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError, RecursionError) as exc:
        _fail("pnc_steady_release_transaction_json_invalid", exc)


def _canonical_sha256(value: Any) -> str:
    try:
        raw = json.dumps(
            value,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError, RecursionError) as exc:
        _fail("pnc_steady_release_transaction_activation_invalid", exc)
    return hashlib.sha256(raw).hexdigest()


def _validate_db_path(path: Path) -> Path:
    selected = path.expanduser()
    if not selected.is_absolute() or selected.absolute() != selected:
        _fail("pnc_steady_release_transaction_control_db_invalid")
    try:
        metadata = selected.lstat()
        resolved = selected.resolve(strict=True)
    except OSError as exc:
        _fail("pnc_steady_release_transaction_control_db_invalid", exc)
    if (
        resolved != selected
        or stat.S_ISLNK(metadata.st_mode)
        or not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != os.geteuid()
        or metadata.st_nlink != 1
        or metadata.st_size < 1
        or stat.S_IMODE(metadata.st_mode) & 0o022
    ):
        _fail("pnc_steady_release_transaction_control_db_invalid")
    return selected


@contextmanager
def _read_only_db(path: Path) -> Iterator[sqlite3.Connection]:
    selected = _validate_db_path(path)
    try:
        connection = sqlite3.connect(
            f"{selected.as_uri()}?mode=ro",
            uri=True,
            timeout=5,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only=ON")
        connection.execute("BEGIN")
    except sqlite3.Error as exc:
        _fail("pnc_steady_release_transaction_control_db_invalid", exc)
    try:
        yield connection
    finally:
        try:
            if connection.in_transaction:
                connection.rollback()
        finally:
            connection.close()


def _activation_fingerprint_material(
    epoch: Mapping[str, Any],
    *,
    slot_bindings: Sequence[Mapping[str, Any]],
    from_state: str,
    to_state: str,
) -> dict[str, Any]:
    return {
        "config_sha256": str(epoch["config_sha256"]),
        "db_logical_identity_sha256": str(epoch["db_logical_identity_sha256"]),
        "epoch_id": str(epoch["epoch_id"]),
        "from_state": from_state,
        "partition_end_fence_sha256": str(
            epoch["partition_end_fence_sha256"] or ""
        ),
        "partition_start_fence_sha256": str(
            epoch["partition_start_fence_sha256"]
        ),
        "preauthorization_capsule_sha256": str(
            epoch["preauthorization_capsule_sha256"]
        ),
        "preauthorization_fingerprint": str(
            epoch["preauthorization_fingerprint"]
        ),
        "preauthorization_gate_receipt_sha256": str(
            epoch["preauthorization_gate_receipt_sha256"]
        ),
        "preproduction_capsule_sha256": str(
            epoch["preproduction_capsule_sha256"] or ""
        ),
        "preproduction_fingerprint": str(
            epoch["preproduction_fingerprint"] or ""
        ),
        "preproduction_gate_receipt_sha256": str(
            epoch["preproduction_gate_receipt_sha256"] or ""
        ),
        "production_fingerprint": str(epoch["production_fingerprint"] or ""),
        "production_gate_receipt_sha256": str(
            epoch["production_gate_receipt_sha256"] or ""
        ),
        "slot_bindings_sha256": _canonical_sha256(list(slot_bindings)),
        "to_state": to_state,
    }


def _read_activation_binding(control_db: Path) -> dict[str, Any]:
    try:
        with _read_only_db(control_db) as connection:
            epochs = connection.execute(
                "SELECT * FROM rca_activation_epochs WHERE is_current = 1"
            ).fetchall()
            if len(epochs) != 1:
                _fail("pnc_steady_release_transaction_activation_not_current")
            epoch = epochs[0]
            epoch_id = str(epoch["epoch_id"] or "")
            if (
                IDENTIFIER_RE.fullmatch(epoch_id) is None
                or str(epoch["state"] or "") != "aborted"
            ):
                _fail("pnc_steady_release_transaction_activation_not_aborted")
            audits = connection.execute(
                """
                SELECT audit_id, from_state, to_state, binding_fingerprint,
                       transitioned_at
                  FROM rca_activation_transition_audit
                 WHERE epoch_id = ? AND to_state = 'aborted'
              ORDER BY audit_id
                """,
                (epoch_id,),
            ).fetchall()
            if len(audits) != 1 or str(audits[0]["from_state"]) != "steady_active":
                _fail("pnc_steady_release_transaction_activation_audit_invalid")
            slots = [
                {
                    "authorized_identity_sha256": str(
                        row["authorized_identity_sha256"] or ""
                    ),
                    "authorized_operator": str(row["authorized_operator"] or ""),
                    "authorized_reason": str(row["authorized_reason"] or ""),
                    "authorized_source_kind": str(
                        row["authorized_source_kind"] or ""
                    ),
                    "consumed_ledger_id": int(row["consumed_ledger_id"] or 0),
                    "slot_kind": str(row["slot_kind"]),
                }
                for row in connection.execute(
                    """
                    SELECT slot_kind, authorized_source_kind,
                           authorized_identity_sha256, authorized_operator,
                           authorized_reason, consumed_ledger_id
                      FROM rca_activation_budget_slots
                     WHERE epoch_id = ? ORDER BY slot_kind
                    """,
                    (epoch_id,),
                ).fetchall()
            ]
            audit = audits[0]
            expected = _canonical_sha256(
                _activation_fingerprint_material(
                    epoch,
                    slot_bindings=slots,
                    from_state=str(audit["from_state"]),
                    to_state=str(audit["to_state"]),
                )
            )
            observed = str(audit["binding_fingerprint"] or "").lower()
            if HEX64_RE.fullmatch(observed) is None or observed != expected:
                _fail(
                    "pnc_steady_release_transaction_activation_fingerprint_invalid"
                )
            return {
                "epoch_id": epoch_id,
                "state": "aborted",
                "binding_fingerprint": observed,
                "transition_audit_id": int(audit["audit_id"]),
                "transitioned_at": str(audit["transitioned_at"]),
            }
    except SteadyReleaseTransactionError:
        raise
    except (KeyError, TypeError, ValueError, sqlite3.Error) as exc:
        _fail("pnc_steady_release_transaction_activation_invalid", exc)


def _validate_profile(
    *,
    path: Path,
    home: Path,
    release_id: str,
    authority_sha256: str,
    provenance: Mapping[str, str],
    activation_binding: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    raw, profile_observation = base._read_file(
        path,
        code="pnc_steady_release_transaction_profile_invalid",
        required_mode=0o600,
    )
    value = base._json(
        raw, code="pnc_steady_release_transaction_profile_invalid"
    )
    source = value.get("source")
    activation = value.get("activation")
    expected_activation = {
        "predecessor_epoch_id": activation_binding["epoch_id"],
        "predecessor_state": activation_binding["state"],
        "predecessor_binding_fingerprint": activation_binding[
            "binding_fingerprint"
        ],
        "successor_epoch_id": str(
            activation.get("successor_epoch_id") if isinstance(activation, Mapping) else ""
        ),
    }
    anchors = value.get("read_only_plist_anchors")
    expected_anchor_values: dict[str, Any] = {}
    for name in READ_ONLY_PLIST_NAMES:
        target = home / "Library" / "LaunchAgents" / name
        if (
            not isinstance(anchors, Mapping)
            or name not in anchors
            or not isinstance(anchors[name], Mapping)
            or set(anchors[name]) != PROFILE_ANCHOR_FIELDS
            or Path(str(anchors[name].get("path") or "")) != target
            or HEX64_RE.fullmatch(str(anchors[name].get("sha256") or "")) is None
        ):
            _fail("pnc_steady_release_transaction_profile_anchor_invalid")
        try:
            anchor_observation = base._observe(target, required=True)
        except base.ReleaseTransactionError as exc:
            _fail("pnc_steady_release_transaction_profile_anchor_invalid", exc)
        if anchor_observation["sha256"] != anchors[name]["sha256"]:
            _fail("pnc_steady_release_transaction_profile_anchor_changed")
        expected_anchor_values[name] = {
            "path": str(target),
            "sha256": str(anchors[name]["sha256"]),
            "observation": anchor_observation,
        }
    if (
        raw != _canonical_bytes(value)
        or set(value) != PROFILE_FIELDS
        or value.get("schema_version") != PROFILE_SCHEMA_VERSION
        or value.get("release_id") != release_id
        or value.get("authority_sha256") != authority_sha256
        or not isinstance(source, Mapping)
        or set(source) != PROFILE_SOURCE_FIELDS
        or dict(source) != dict(provenance)
        or not isinstance(activation, Mapping)
        or set(activation) != PROFILE_ACTIVATION_FIELDS
        or activation.get("predecessor_epoch_id")
        != expected_activation["predecessor_epoch_id"]
        or activation.get("predecessor_state") != "aborted"
        or activation.get("predecessor_binding_fingerprint")
        != expected_activation["predecessor_binding_fingerprint"]
        or IDENTIFIER_RE.fullmatch(str(activation.get("successor_epoch_id") or ""))
        is None
        or activation.get("successor_epoch_id")
        == expected_activation["predecessor_epoch_id"]
    ):
        _fail("pnc_steady_release_transaction_profile_invalid")
    return value, {"exists": True, **profile_observation}, expected_anchor_values


def _parse_plist(raw: bytes, *, label: str, hermes_home: Path) -> dict[str, Any]:
    try:
        value = plistlib.loads(raw)
    except (ValueError, plistlib.InvalidFileException) as exc:
        _fail("pnc_steady_release_transaction_plist_invalid", exc)
    if not isinstance(value, dict) or value.get("Label") != label:
        _fail("pnc_steady_release_transaction_plist_invalid")
    arguments = value.get("ProgramArguments")
    launcher = str(hermes_home / "runtime" / "governance-tools" / "pnc_live_exec.py")
    if not isinstance(arguments, list) or arguments[:3] != [
        "/usr/bin/python3",
        launcher,
        label,
    ]:
        _fail("pnc_steady_release_transaction_plist_runtime_pinned")
    serialized = json.dumps(value, ensure_ascii=True, sort_keys=True)
    if any(marker in serialized for marker in ("/runtime/releases/", "/runtime/venvs/")):
        _fail("pnc_steady_release_transaction_plist_runtime_pinned")
    if not isinstance(value.get("EnvironmentVariables"), dict):
        _fail("pnc_steady_release_transaction_plist_environment_invalid")
    return value


def _relay_expected(source: Mapping[str, Any]) -> dict[str, Any]:
    expected = copy.deepcopy(dict(source))
    arguments = expected["ProgramArguments"]
    matches = [
        index
        for index in range(len(arguments) - 1)
        if arguments[index : index + 2] == ["--task-id", STALE_RELAY_TASK_ID]
    ]
    if len(matches) > 1:
        _fail("pnc_steady_release_transaction_relay_task_id_invalid")
    if matches:
        index = matches[0]
        del arguments[index : index + 2]
    if "--task-id" in arguments:
        _fail("pnc_steady_release_transaction_relay_task_id_invalid")
    expected["EnvironmentVariables"]["HERMES_OUTBOUND_MODE"] = "record-only"
    return expected


def _dispatcher_expected(
    source: Mapping[str, Any], candidate: Mapping[str, Any], *, release_id: str
) -> dict[str, Any]:
    environment = candidate["EnvironmentVariables"]
    inventory_pin = str(
        environment.get("HERMES_RCA_DELIVERY_DISPATCHER_INVENTORY_PIN") or ""
    ).lower()
    if HEX64_RE.fullmatch(inventory_pin) is None:
        _fail("pnc_steady_release_transaction_dispatcher_inventory_invalid")
    expected = copy.deepcopy(dict(source))
    expected_environment = expected["EnvironmentVariables"]
    expected_environment.update(
        {
            "HERMES_OUTBOUND_MODE": "record-only",
            "HERMES_RCA_DELIVERY_DISPATCHER_ENABLED": "false",
            "HERMES_RCA_DELIVERY_DISPATCHER_INVENTORY_PIN": inventory_pin,
            "HERMES_RCA_DELIVERY_DISPATCHER_OBSERVABILITY_ENABLED": "true",
            "HERMES_RCA_DELIVERY_DISPATCHER_OBSERVATION_RELEASE_ID": release_id,
        }
    )
    return expected


def _validate_candidate_plist(
    *,
    candidate_raw: bytes,
    source_raw: bytes,
    name: str,
    release_id: str,
    hermes_home: Path,
) -> None:
    label = name[:-6]
    source = _parse_plist(source_raw, label=label, hermes_home=hermes_home)
    candidate = _parse_plist(candidate_raw, label=label, hermes_home=hermes_home)
    environment = candidate["EnvironmentVariables"]
    scoped = set(environment) & base.RELEASE_SCOPED_PLIST_KEYS
    if label == "local.pnc.rca-delivery-dispatcher":
        if scoped - DISPATCHER_RELEASE_KEYS:
            _fail("pnc_steady_release_transaction_plist_release_pin")
        expected = _dispatcher_expected(source, candidate, release_id=release_id)
    elif label == "local.pnc.completion-notice-relay":
        if scoped:
            _fail("pnc_steady_release_transaction_plist_release_pin")
        expected = _relay_expected(source)
    else:
        if scoped:
            _fail("pnc_steady_release_transaction_plist_release_pin")
        expected = source
    if candidate != expected:
        _fail("pnc_steady_release_transaction_plist_source_mismatch")


def _validate_quarantine_baseline(
    *,
    control_db: Path,
    env: Mapping[str, str],
    env_raw: bytes,
    binding: Mapping[str, Any],
    authority_value: Mapping[str, Any],
    release_id: str,
) -> dict[str, Any]:
    path_text = str(
        env.get("HERMES_RCA_DELIVERY_QUARANTINE_BASELINE_PATH") or ""
    )
    expected_sha256 = str(
        env.get("HERMES_RCA_DELIVERY_QUARANTINE_BASELINE_SHA256") or ""
    ).lower()
    baseline_path = Path(path_text).expanduser()
    if (
        not baseline_path.is_absolute()
        or HEX64_RE.fullmatch(expected_sha256) is None
    ):
        _fail("pnc_steady_release_transaction_baseline_invalid")
    raw, observation = base._read_file(
        baseline_path,
        code="pnc_steady_release_transaction_baseline_invalid",
        maximum=quarantine_baseline.MAX_BASELINE_BYTES,
        required_mode=0o600,
    )
    value = base._json(raw, code="pnc_steady_release_transaction_baseline_invalid")
    baseline_authority = authority_value.get("quarantine_baseline")
    if (
        raw != quarantine_baseline.canonical_quarantine_baseline_bytes(value)
        or observation["sha256"] != expected_sha256
        or not isinstance(baseline_authority, Mapping)
        or baseline_authority.get("state") != "ready"
        or baseline_authority.get("required") is not True
        or baseline_authority.get("schema_version")
        != quarantine_baseline.BASELINE_SCHEMA_VERSION
        or baseline_authority.get("baseline_sha256") != expected_sha256
        or value.get("release_id") != release_id
    ):
        _fail("pnc_steady_release_transaction_baseline_invalid")
    capacity = binding["policy"]["capacity_admission"]
    with tempfile.TemporaryDirectory(
        prefix="pnc-rca-steady-baseline-"
    ) as temporary_name:
        temporary = Path(temporary_name)
        os.chmod(temporary, 0o700)
        temp_binding = temporary / "active-release-binding.json"
        temp_env = temporary / "live.env"
        validation_binding = copy.deepcopy(dict(binding))
        validation_binding["side_effect_contract"] = {
            "canonical_active_release_binding": str(temp_binding),
            "canonical_live_env": str(temp_env),
        }
        temp_binding.write_bytes(base._pretty(validation_binding))
        temp_binding.chmod(0o600)
        temp_env.write_bytes(env_raw)
        temp_env.chmod(0o600)
        try:
            with _read_only_db(control_db) as connection:
                status = quarantine_baseline.quarantine_baseline_status_tx(
                    connection,
                    db_path=control_db,
                    baseline_path=baseline_path,
                    expected_sha256=expected_sha256,
                    expected_release_id=release_id,
                    bootstrap_epoch_id=str(capacity["bootstrap_epoch_id"]),
                    active_release_binding_path=temp_binding,
                    live_env_path=temp_env,
                )
        except (KeyError, quarantine_baseline.DeliveryQuarantineBaselineError) as exc:
            _fail("pnc_steady_release_transaction_baseline_invalid", exc)
    identity = status.get("baseline_identity")
    if (
        status.get("ready") is not True
        or status.get("state") != "acknowledged"
        or not isinstance(identity, Mapping)
        or identity.get("release_id") != release_id
        or identity.get("release_bom_sha256")
        != capacity.get("release_bom_sha256")
        or identity.get("approval_evidence_sha256")
        != capacity.get("approval_evidence_sha256")
        or identity.get("candidate_env_sha256") != hashlib.sha256(env_raw).hexdigest()
        or HEX64_RE.fullmatch(
            str(identity.get("db_logical_identity_sha256") or "")
        )
        is None
    ):
        _fail("pnc_steady_release_transaction_baseline_invalid")
    return {
        "path": str(baseline_path),
        "observation": {"exists": True, **observation},
        "baseline_id": str(identity["baseline_id"]),
        "baseline_fingerprint": str(identity["baseline_fingerprint"]),
        "status_sha256": hashlib.sha256(
            _canonical_sha256_input(_stable_baseline_status(status))
        ).hexdigest(),
        "db_logical_identity_sha256": str(
            identity["db_logical_identity_sha256"]
        ),
    }


def _canonical_sha256_input(value: Mapping[str, Any]) -> bytes:
    return json.dumps(
        dict(value),
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _stable_baseline_status(status: Mapping[str, Any]) -> dict[str, Any]:
    stable = copy.deepcopy(dict(status))
    identity = stable.get("baseline_identity")
    if isinstance(identity, Mapping):
        identity = dict(identity)
        identity.pop("active_release_binding_sha256", None)
        stable["baseline_identity"] = identity
    return stable


def _candidate_paths(candidate_root: Path) -> dict[str, Path]:
    return {**base._candidate_paths(candidate_root), "profile": candidate_root / PROFILE_NAME}


def _validate_candidate(
    *,
    candidate_root: Path,
    source_root: Path,
    home: Path,
    hermes_home: Path,
    control_db: Path,
    activation_binding: Mapping[str, Any],
) -> dict[str, Any]:
    provenance = base._source_provenance(source_root)
    paths = _candidate_paths(candidate_root)
    authority_raw, _authority_observation = base._read_file(
        paths["authority"],
        code="pnc_steady_release_transaction_authority_invalid",
        required_mode=0o600,
    )
    authority_value = base._json(
        authority_raw, code="pnc_steady_release_transaction_authority_invalid"
    )
    try:
        authority.validate_release_authority(authority_value)
    except authority.ReleaseAuthorityError as exc:
        _fail("pnc_steady_release_transaction_authority_invalid", exc)
    release_id = str(authority_value.get("release_id") or "")
    authority_sha256 = authority.canonical_json_sha256(authority_value)
    authority_epoch_id = str(authority_value.get("authority_epoch_id") or "")
    host_face = authority_value.get("faces", {}).get("host_runtime", {})
    if (
        IDENTIFIER_RE.fullmatch(release_id) is None
        or authority_value.get("status") != "approved_for_activation"
        or host_face.get("commit") != provenance["commit"]
        or host_face.get("tree") != provenance["tree"]
    ):
        _fail("pnc_steady_release_transaction_authority_invalid")
    state_root = hermes_home / "runtime" / "pnc_agent" / "feishu_issue_kafka_rca"
    authority_target = state_root / f"{release_id}.authority.json"
    pointer_raw, _pointer_observation = base._read_file(
        paths["pointer"],
        code="pnc_steady_release_transaction_pointer_invalid",
        required_mode=0o600,
    )
    pointer = base._json(
        pointer_raw, code="pnc_steady_release_transaction_pointer_invalid"
    )
    try:
        authority.validate_active_pointer(
            pointer, authority_value, expected_authority_path=authority_target
        )
    except authority.ReleaseAuthorityError as exc:
        _fail("pnc_steady_release_transaction_pointer_invalid", exc)
    if pointer.get("state") != "active":
        _fail("pnc_steady_release_transaction_pointer_invalid")

    manifest_raw, _manifest_observation = base._read_file(
        paths["manifest"],
        code="pnc_steady_release_transaction_manifest_invalid",
        required_mode=0o600,
    )
    manifest = base._json(
        manifest_raw, code="pnc_steady_release_transaction_manifest_invalid"
    )
    config_raw, config_observation = base._read_file(
        paths["config"],
        code="pnc_steady_release_transaction_config_invalid",
        required_mode=0o600,
    )
    try:
        config_value = yaml.safe_load(config_raw.decode("utf-8"))
        config_semantic = hashlib.sha256(
            json.dumps(
                config_value,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
    except (UnicodeDecodeError, yaml.YAMLError, TypeError, ValueError) as exc:
        _fail("pnc_steady_release_transaction_config_invalid", exc)
    config_target = hermes_home / "config.yaml"
    if (
        manifest.get("config_path") != str(config_target)
        or manifest.get("config_sha256") != config_observation["sha256"]
        or manifest.get("config_semantic_sha256") != config_semantic
    ):
        _fail("pnc_steady_release_transaction_config_invalid")
    env_raw, env_observation = base._read_file(
        paths["env"],
        code="pnc_steady_release_transaction_env_invalid",
        required_mode=0o600,
    )
    env = base._env_map(env_raw)
    if (
        env.get("HERMES_RCA_PROD_RELEASE_ID") != release_id
        or env.get("HERMES_RCA_PROD_CAPACITY_MODE") != "bootstrap"
        or env.get("HERMES_OUTBOUND_MODE") != "record-only"
        or env.get("HERMES_RCA_OUTBOX_ALLOW_FEISHU_WRITEBACK") != "false"
        or env.get("HERMES_RCA_DELIVERY_DISPATCHER_ENABLED") != "false"
        or manifest.get("env_sha256") != env_observation["sha256"]
    ):
        _fail("pnc_steady_release_transaction_env_invalid")
    binding_raw, _binding_observation = base._read_file(
        paths["binding"],
        code="pnc_steady_release_transaction_binding_invalid",
        required_mode=0o600,
    )
    binding = base._json(
        binding_raw, code="pnc_steady_release_transaction_binding_invalid"
    )
    binding_target = state_root / bootstrap.ACTIVE_RELEASE_BINDING_NAME
    env_target = hermes_home / ".env"
    try:
        base._validate_binding(
            binding,
            release_id=release_id,
            authority_sha256=authority_sha256,
            authority_epoch_id=authority_epoch_id,
            env_sha256=env_observation["sha256"],
            binding_path=binding_target,
            env_path=env_target,
        )
    except base.ReleaseTransactionError as exc:
        _fail("pnc_steady_release_transaction_binding_invalid", exc)
    authorization_raw, _authorization_observation = base._read_file(
        paths["bootstrap_authorization"],
        code="pnc_steady_release_transaction_bootstrap_authorization_invalid",
        required_mode=0o600,
    )
    authorization_value = base._json(
        authorization_raw,
        code="pnc_steady_release_transaction_bootstrap_authorization_invalid",
    )
    if authorization_raw != bootstrap.canonical_bytes(authorization_value):
        _fail("pnc_steady_release_transaction_bootstrap_authorization_invalid")
    capacity = binding["policy"]["capacity_admission"]
    try:
        bootstrap.validate_bootstrap_authorization(
            authorization_value,
            expected_epoch_id=capacity["bootstrap_epoch_id"],
            expected_release_bom_sha256=capacity["release_bom_sha256"],
            expected_release_approval_id=release_id,
            expected_approval_evidence_sha256=capacity["approval_evidence_sha256"],
            authorization_receipt_sha256=hashlib.sha256(authorization_raw).hexdigest(),
        )
    except (KeyError, bootstrap.RcaBootstrapAuthorizationError) as exc:
        _fail(
            "pnc_steady_release_transaction_bootstrap_authorization_invalid", exc
        )
    try:
        projection = authority.audit_release_projections(
            authority_value,
            pointer=pointer,
            authority_path=authority_target,
            live_manifest=manifest,
            active_binding=binding,
            control_store_path=control_db,
        )
    except (authority.ReleaseAuthorityError, OSError, ValueError) as exc:
        _fail("pnc_steady_release_transaction_projection_invalid", exc)
    if not projection.get("ok"):
        _fail("pnc_steady_release_transaction_projection_invalid")
    _profile, profile_observation, read_only_anchors = _validate_profile(
        path=paths["profile"],
        home=home,
        release_id=release_id,
        authority_sha256=authority_sha256,
        provenance=provenance,
        activation_binding=activation_binding,
    )
    baseline = _validate_quarantine_baseline(
        control_db=control_db,
        env=env,
        env_raw=env_raw,
        binding=binding,
        authority_value=authority_value,
        release_id=release_id,
    )
    launch_dir = home / "Library" / "LaunchAgents"
    for name in base.PLIST_NAMES:
        candidate_raw, _candidate_observation = base._read_file(
            paths[name],
            code="pnc_steady_release_transaction_plist_invalid",
            required_mode=0o600,
        )
        source_raw, _source_observation = base._read_file(
            source_root / name,
            code="pnc_steady_release_transaction_plist_source_invalid",
        )
        _validate_candidate_plist(
            candidate_raw=candidate_raw,
            source_raw=source_raw,
            name=name,
            release_id=release_id,
            hermes_home=hermes_home,
        )
    return {
        "paths": paths,
        "provenance": provenance,
        "release_id": release_id,
        "authority_sha256": authority_sha256,
        "authority_epoch_id": authority_epoch_id,
        "authority_target": authority_target,
        "state_root": state_root,
        "config_target": config_target,
        "binding_target": binding_target,
        "env_target": env_target,
        "launch_dir": launch_dir,
        "profile_observation": profile_observation,
        "read_only_anchors": read_only_anchors,
        "baseline": baseline,
    }


def _source_specs(validated: Mapping[str, Any]) -> list[tuple[str, Path, Path, int, str]]:
    paths = validated["paths"]
    specs = [
        ("authority", paths["authority"], validated["authority_target"], 0o400, "authority"),
        ("pointer", paths["pointer"], validated["state_root"] / "ACTIVE_RCA_RELEASE.json", 0o600, "pointer"),
        ("manifest", paths["manifest"], validated["state_root"].parents[1] / "LIVE_MANIFEST.json", 0o600, "manifest"),
        ("config", paths["config"], validated["config_target"], 0o600, "config"),
        ("binding", paths["binding"], validated["binding_target"], 0o600, "binding"),
        ("env", paths["env"], validated["env_target"], 0o600, "env"),
        (
            "bootstrap_authorization",
            paths["bootstrap_authorization"],
            bootstrap.BOOTSTRAP_AUTHORIZATION_PATH.expanduser().absolute(),
            0o600,
            "bootstrap_authorization",
        ),
    ]
    specs.extend(
        (
            name,
            paths[name],
            validated["launch_dir"] / name,
            0o644,
            "plist",
        )
        for name in base.PLIST_NAMES
    )
    return specs


def build_plan(
    *,
    candidate_root: Path,
    source_root: Path,
    home: Path,
    hermes_home: Path,
    control_db: Path,
    evidence_root: Path,
    transaction_id: str | None = None,
) -> tuple[dict[str, Any], Path]:
    candidate_root = base._directory(candidate_root)
    source_root = base._directory(source_root)
    home = base._directory(home)
    hermes_home = base._directory(hermes_home)
    state_root = base._directory(
        hermes_home / "runtime" / "pnc_agent" / "feishu_issue_kafka_rca"
    )
    evidence_root = base._directory(evidence_root, create=True)
    control_db = _validate_db_path(control_db)
    activation_binding = _read_activation_binding(control_db)
    validated = _validate_candidate(
        candidate_root=candidate_root,
        source_root=source_root,
        home=home,
        hermes_home=hermes_home,
        control_db=control_db,
        activation_binding=activation_binding,
    )
    if validated["state_root"] != state_root:
        _fail("pnc_steady_release_transaction_state_root_invalid")
    if _read_activation_binding(control_db) != activation_binding:
        _fail("pnc_steady_release_transaction_activation_changed")
    selected_id = transaction_id or (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        + "-"
        + uuid.uuid4().hex[:12]
    )
    if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", selected_id) is None:
        _fail("pnc_steady_release_transaction_id_invalid")
    transaction_dir = evidence_root / selected_id
    rollback_dir = transaction_dir / "rollback"
    staged_dir = transaction_dir / "staged"
    base._directory(transaction_dir, create=True)
    base._directory(rollback_dir, create=True)
    base._directory(staged_dir, create=True)
    entries: list[dict[str, Any]] = []
    for index, (name, source, target, mode, kind) in enumerate(
        _source_specs(validated)
    ):
        raw, source_observation = base._read_file(
            source,
            code="pnc_steady_release_transaction_candidate_invalid",
            required_mode=0o600,
        )
        source_observation = {"exists": True, **source_observation}
        staged = staged_dir / f"{index:02d}-{name}.blob"
        base._write_new(staged, raw, mode=mode)
        before = base._observe(target, required=False)
        if name == "authority" and before["exists"]:
            _fail("pnc_steady_release_transaction_authority_target_exists")
        entries.append(
            {
                "name": name,
                "kind": kind,
                "source_path": str(source),
                "source": source_observation,
                "staged_path": str(staged),
                "target_path": str(target),
                "target_mode": format(mode, "04o"),
                "before": before,
                "rollback_path": str(rollback_dir / f"{index:02d}-{name}.before"),
            }
        )
    plan = {
        "schema_version": PLAN_SCHEMA_VERSION,
        "cli_schema_version": CLI_SCHEMA_VERSION,
        "transaction_id": selected_id,
        "planned_at": datetime.now(timezone.utc).isoformat(),
        "release_id": validated["release_id"],
        "authority_sha256": validated["authority_sha256"],
        "authority_epoch_id": validated["authority_epoch_id"],
        "source_root": str(source_root),
        "source_commit": validated["provenance"]["commit"],
        "source_tree": validated["provenance"]["tree"],
        "candidate_root": str(candidate_root),
        "home": str(home),
        "hermes_home": str(hermes_home),
        "control_db": str(control_db),
        "state_root": str(state_root),
        "transaction_dir": str(transaction_dir),
        "rollback_dir": str(rollback_dir),
        "steady_profile": {
            "path": str(validated["paths"]["profile"]),
            "observation": validated["profile_observation"],
        },
        "read_only_plist_anchors": validated["read_only_anchors"],
        "activation_binding": activation_binding,
        "quarantine_baseline": validated["baseline"],
        "entries": entries,
        "mutation_performed": False,
    }
    _validate_plan(plan)
    plan_path = transaction_dir / "plan.json"
    base._write_new(plan_path, base._pretty(plan), mode=0o600)
    return plan, plan_path


def _valid_observation(value: Any) -> bool:
    if not isinstance(value, Mapping) or set(value) != OBSERVATION_FIELDS:
        return False
    exists = value.get("exists")
    if exists is False:
        return all(
            value.get(key) is None
            for key in ("sha256", "mode", "device", "inode", "mtime_ns", "ctime_ns")
        ) and value.get("size_bytes") == 0
    return (
        exists is True
        and isinstance(value.get("sha256"), str)
        and HEX64_RE.fullmatch(value["sha256"]) is not None
        and isinstance(value.get("mode"), str)
        and re.fullmatch(r"0[0-7]{3}", value["mode"]) is not None
        and all(
            isinstance(value.get(key), int) and not isinstance(value.get(key), bool)
            for key in ("size_bytes", "device", "inode", "mtime_ns", "ctime_ns")
        )
    )


def _validate_plan(plan: Mapping[str, Any]) -> None:
    if (
        set(plan) != PLAN_FIELDS
        or plan.get("schema_version") != PLAN_SCHEMA_VERSION
        or plan.get("cli_schema_version") != CLI_SCHEMA_VERSION
        or plan.get("mutation_performed") is not False
        or IDENTIFIER_RE.fullmatch(str(plan.get("release_id") or "")) is None
        or HEX64_RE.fullmatch(str(plan.get("authority_sha256") or "")) is None
        or not isinstance(plan.get("activation_binding"), Mapping)
        or set(plan["activation_binding"]) != ACTIVATION_BINDING_FIELDS
        or plan["activation_binding"].get("state") != "aborted"
        or HEX64_RE.fullmatch(
            str(plan["activation_binding"].get("binding_fingerprint") or "")
        )
        is None
    ):
        _fail("pnc_steady_release_transaction_plan_invalid")
    candidate_root = Path(str(plan["candidate_root"]))
    home = Path(str(plan["home"]))
    hermes_home = Path(str(plan["hermes_home"]))
    state_root = Path(str(plan["state_root"]))
    transaction_dir = Path(str(plan["transaction_dir"]))
    rollback_dir = Path(str(plan["rollback_dir"]))
    source_root = Path(str(plan["source_root"]))
    for path in (
        candidate_root,
        home,
        hermes_home,
        state_root,
        transaction_dir,
        rollback_dir,
        source_root,
        Path(str(plan["control_db"])),
    ):
        if not path.is_absolute() or path.absolute() != path:
            _fail("pnc_steady_release_transaction_plan_invalid")
    if (
        state_root
        != hermes_home / "runtime" / "pnc_agent" / "feishu_issue_kafka_rca"
        or rollback_dir != transaction_dir / "rollback"
        or transaction_dir.name != plan.get("transaction_id")
    ):
        _fail("pnc_steady_release_transaction_plan_invalid")
    profile = plan.get("steady_profile")
    baseline = plan.get("quarantine_baseline")
    if (
        not isinstance(profile, Mapping)
        or set(profile) != {"path", "observation"}
        or Path(str(profile["path"])) != candidate_root / PROFILE_NAME
        or not _valid_observation(profile["observation"])
        or not isinstance(baseline, Mapping)
        or set(baseline)
        != {
            "path",
            "observation",
            "baseline_id",
            "baseline_fingerprint",
            "status_sha256",
            "db_logical_identity_sha256",
        }
        or not Path(str(baseline["path"])).is_absolute()
        or not _valid_observation(baseline["observation"])
        or HEX64_RE.fullmatch(str(baseline["baseline_fingerprint"] or "")) is None
        or HEX64_RE.fullmatch(str(baseline["status_sha256"] or "")) is None
        or HEX64_RE.fullmatch(
            str(baseline["db_logical_identity_sha256"] or "")
        )
        is None
    ):
        _fail("pnc_steady_release_transaction_plan_invalid")
    anchors = plan.get("read_only_plist_anchors")
    if not isinstance(anchors, Mapping) or set(anchors) != set(READ_ONLY_PLIST_NAMES):
        _fail("pnc_steady_release_transaction_plan_invalid")
    for name in READ_ONLY_PLIST_NAMES:
        item = anchors.get(name)
        expected_path = home / "Library" / "LaunchAgents" / name
        if (
            not isinstance(item, Mapping)
            or set(item) != {"path", "sha256", "observation"}
            or Path(str(item.get("path") or "")) != expected_path
            or HEX64_RE.fullmatch(str(item.get("sha256") or "")) is None
            or not _valid_observation(item.get("observation"))
            or item["observation"].get("sha256") != item.get("sha256")
        ):
            _fail("pnc_steady_release_transaction_plan_invalid")
    release_id = str(plan["release_id"])
    paths = _candidate_paths(candidate_root)
    targets = {
        "authority": state_root / f"{release_id}.authority.json",
        "pointer": state_root / "ACTIVE_RCA_RELEASE.json",
        "manifest": hermes_home / "runtime" / "LIVE_MANIFEST.json",
        "config": hermes_home / "config.yaml",
        "binding": state_root / bootstrap.ACTIVE_RELEASE_BINDING_NAME,
        "env": hermes_home / ".env",
        "bootstrap_authorization": bootstrap.BOOTSTRAP_AUTHORIZATION_PATH.expanduser().absolute(),
        **{
            name: home / "Library" / "LaunchAgents" / name
            for name in base.PLIST_NAMES
        },
    }
    kinds = {
        "authority": "authority",
        "pointer": "pointer",
        "manifest": "manifest",
        "config": "config",
        "binding": "binding",
        "env": "env",
        "bootstrap_authorization": "bootstrap_authorization",
        **{name: "plist" for name in base.PLIST_NAMES},
    }
    modes = {
        "authority": "0400",
        "pointer": "0600",
        "manifest": "0600",
        "config": "0600",
        "binding": "0600",
        "env": "0600",
        "bootstrap_authorization": "0600",
        **{name: "0644" for name in base.PLIST_NAMES},
    }
    expected_names = [
        "authority",
        "pointer",
        "manifest",
        "config",
        "binding",
        "env",
        "bootstrap_authorization",
        *base.PLIST_NAMES,
    ]
    entries = plan.get("entries")
    if (
        not isinstance(entries, list)
        or [entry.get("name") for entry in entries if isinstance(entry, Mapping)]
        != expected_names
        or len(entries) != len(expected_names)
    ):
        _fail("pnc_steady_release_transaction_plan_invalid")
    for index, entry in enumerate(entries):
        name = expected_names[index]
        if (
            not isinstance(entry, Mapping)
            or set(entry) != ENTRY_FIELDS
            or entry.get("name") != name
            or entry.get("kind") != kinds[name]
            or Path(str(entry.get("source_path"))) != paths[name]
            or Path(str(entry.get("target_path"))) != targets[name]
            or entry.get("target_mode") != modes[name]
            or Path(str(entry.get("staged_path")))
            != transaction_dir / "staged" / f"{index:02d}-{name}.blob"
            or Path(str(entry.get("rollback_path")))
            != rollback_dir / f"{index:02d}-{name}.before"
            or not _valid_observation(entry.get("source"))
            or not _valid_observation(entry.get("before"))
        ):
            _fail("pnc_steady_release_transaction_plan_invalid")
    if entries[0]["before"]["exists"] is not False:
        _fail("pnc_steady_release_transaction_plan_invalid")


def _read_plan(path: Path) -> tuple[bytes, dict[str, Any]]:
    raw, _observation = base._read_file(
        path,
        code="pnc_steady_release_transaction_plan_invalid",
        required_mode=0o600,
    )
    value = base._json(raw, code="pnc_steady_release_transaction_plan_invalid")
    if raw != base._pretty(value):
        _fail("pnc_steady_release_transaction_plan_invalid")
    _validate_plan(value)
    if path != Path(value["transaction_dir"]) / "plan.json":
        _fail("pnc_steady_release_transaction_plan_invalid")
    return raw, value


def _acquire_lock(state_root: Path, transaction_id: str) -> tuple[Path, int]:
    lock = state_root / LOCK_NAME
    descriptor = -1
    try:
        descriptor = os.open(
            lock,
            os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_CLOEXEC", 0),
            0o600,
        )
        os.write(descriptor, transaction_id.encode("ascii"))
        os.fsync(descriptor)
        return lock, descriptor
    except FileExistsError as exc:
        _fail("pnc_steady_release_transaction_lock_held", exc)
    except OSError as exc:
        if descriptor >= 0:
            os.close(descriptor)
            lock.unlink(missing_ok=True)
        _fail("pnc_steady_release_transaction_lock_invalid", exc)


def _release_lock(lock: Path, descriptor: int) -> None:
    try:
        opened = os.fstat(descriptor)
        current = lock.lstat()
        if (current.st_dev, current.st_ino) != (opened.st_dev, opened.st_ino):
            _fail("pnc_steady_release_transaction_lock_changed")
        lock.unlink()
    finally:
        os.close(descriptor)


def _effects() -> dict[str, bool]:
    return {
        "database_mutation": False,
        "task_submission": False,
        "kafka_consume": False,
        "feishu_write": False,
        "resident_restart": False,
    }


def _sync_directory(path: Path) -> None:
    descriptor = os.open(
        path,
        os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_CLOEXEC", 0),
    )
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _replace_from_staged(
    entry: Mapping[str, Any],
    *,
    transaction_id: str,
    replace_func,
    after_observations: dict[str, Mapping[str, Any]],
) -> None:
    raw, staged_observation = base._read_file(
        Path(entry["staged_path"]),
        code="pnc_steady_release_transaction_staged_invalid",
    )
    if (
        staged_observation["sha256"] != entry["source"]["sha256"]
        or staged_observation["mode"] != entry["target_mode"]
    ):
        _fail("pnc_steady_release_transaction_staged_changed")
    target = Path(entry["target_path"])
    temporary = target.parent / (
        f".{target.name}.steady-{transaction_id}.tmp"
    )
    base._write_new(temporary, raw, mode=int(entry["target_mode"], 8))
    try:
        replace_func(temporary, target)
        observed = _owned_after_observation(entry)
        if observed is None:
            _fail("pnc_steady_release_transaction_verify_failed")
        after_observations[str(entry["name"])] = observed
        _sync_directory(target.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _owned_after_observation(
    entry: Mapping[str, Any],
) -> dict[str, Any] | None:
    try:
        observed = base._observe(Path(entry["target_path"]), required=True)
    except base.ReleaseTransactionError:
        return None
    if (
        observed["sha256"] != entry["source"]["sha256"]
        or observed["mode"] != entry["target_mode"]
    ):
        return None
    return observed


def _revalidate_candidate_baseline(plan: Mapping[str, Any]) -> None:
    """Re-read the candidate projection and DB-backed baseline under the lock."""
    validated = _validate_candidate(
        candidate_root=Path(plan["candidate_root"]),
        source_root=Path(plan["source_root"]),
        home=Path(plan["home"]),
        hermes_home=Path(plan["hermes_home"]),
        control_db=Path(plan["control_db"]),
        activation_binding=plan["activation_binding"],
    )
    if validated["baseline"] != plan["quarantine_baseline"]:
        _fail("pnc_steady_release_transaction_baseline_status_changed")


def _restore_attempted_no_clobber(
    plan: Mapping[str, Any],
    *,
    attempted_names: Sequence[str],
    after_observations: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    entries = {str(entry["name"]): entry for entry in plan["entries"]}
    restored: list[dict[str, Any]] = []
    blocked: list[dict[str, Any]] = []
    # Keep the manifest as the final visibility switch during rollback.  The
    # supporting files must be coherent before the old manifest is published.
    restore_order = [
        name for name in reversed(tuple(attempted_names)) if name != "manifest"
    ]
    if "manifest" in attempted_names:
        restore_order.append("manifest")
    for name in restore_order:
        entry = entries[name]
        target = Path(entry["target_path"])
        before = entry["before"]
        try:
            current = base._observe(target, required=False)
        except base.ReleaseTransactionError as exc:
            blocked.append(
                {"name": name, "reason": "target_unobservable", "error": exc.code}
            )
            continue
        expected_after = after_observations.get(name)
        if expected_after is None:
            if base._same_observation(current, before):
                restored.append(
                    {"name": name, "action": "not_written", "observed": current}
                )
            else:
                blocked.append(
                    {
                        "name": name,
                        "reason": "after_observation_missing",
                        "observed": current,
                    }
                )
            continue
        if not base._same_observation(current, expected_after):
            blocked.append(
                {"name": name, "reason": "target_changed", "observed": current}
            )
            continue
        if before["exists"]:
            try:
                rollback_raw, rollback_observation = base._read_file(
                    Path(entry["rollback_path"]),
                    code="pnc_steady_release_transaction_rollback_invalid",
                )
            except base.ReleaseTransactionError as exc:
                blocked.append(
                    {
                        "name": name,
                        "reason": "rollback_blob_unavailable",
                        "error": exc.code,
                    }
                )
                continue
            if (
                rollback_observation["sha256"] != before["sha256"]
                or rollback_observation["mode"] != before["mode"]
            ):
                blocked.append(
                    {
                        "name": name,
                        "reason": "rollback_blob_changed",
                        "observed": rollback_observation,
                    }
                )
                continue
            try:
                before_restore = base._observe(target, required=True)
            except base.ReleaseTransactionError as exc:
                blocked.append(
                    {
                        "name": name,
                        "reason": "target_unobservable_before_restore",
                        "error": exc.code,
                    }
                )
                continue
            if not base._same_observation(before_restore, expected_after):
                blocked.append(
                    {"name": name, "reason": "target_changed_before_restore"}
                )
                continue
            # Re-check immediately after preparing the rollback blob.  This
            # narrows the external-writer window and fails closed if the
            # target changed while the blob was being read/prepared.
            try:
                before_replace = base._observe(target, required=True)
            except base.ReleaseTransactionError as exc:
                blocked.append(
                    {
                        "name": name,
                        "reason": "target_unobservable_before_restore",
                        "error": exc.code,
                    }
                )
                continue
            if not base._same_observation(before_replace, expected_after):
                blocked.append(
                    {"name": name, "reason": "target_changed_before_restore"}
                )
                continue
            restore_error: Exception | None = None
            try:
                base._replace(target, rollback_raw, mode=int(before["mode"], 8))
            except Exception as exc:  # replacement may have completed before fsync failed
                restore_error = exc
            try:
                observed = base._observe(target, required=True)
            except base.ReleaseTransactionError as exc:
                blocked.append(
                    {"name": name, "reason": "restore_unobservable", "error": exc.code}
                )
                continue
            if (
                observed["sha256"] != before["sha256"]
                or observed["mode"] != before["mode"]
            ):
                blocked.append(
                    {
                        "name": name,
                        "reason": "restore_failed",
                        "error": getattr(
                            restore_error,
                            "code",
                            type(restore_error).__name__ if restore_error else "",
                        ),
                        "observed": observed,
                    }
                )
                continue
            restored.append(
                {
                    "name": name,
                    "action": (
                        "restored_after_error" if restore_error else "restored"
                    ),
                    "observed": observed,
                }
            )
            continue
        try:
            before_remove = base._observe(target, required=True)
        except base.ReleaseTransactionError as exc:
            blocked.append(
                {
                    "name": name,
                    "reason": "target_unobservable_before_restore",
                    "error": exc.code,
                }
            )
            continue
        if not base._same_observation(before_remove, expected_after):
            blocked.append(
                {"name": name, "reason": "target_changed_before_restore"}
            )
            continue
        remove_error: Exception | None = None
        try:
            target.unlink()
            _sync_directory(target.parent)
        except Exception as exc:
            remove_error = exc
        try:
            observed = base._observe(target, required=False)
        except base.ReleaseTransactionError as exc:
            blocked.append(
                {
                    "name": name,
                    "reason": "remove_unobservable",
                    "error": exc.code,
                }
            )
            continue
        if observed["exists"]:
            blocked.append(
                {
                    "name": name,
                    "reason": "remove_failed",
                    "error": getattr(
                        remove_error,
                        "code",
                        type(remove_error).__name__ if remove_error else "",
                    ),
                    "observed": observed,
                }
            )
            continue
        restored.append(
            {
                "name": name,
                "action": "removed_after_error" if remove_error else "removed",
                "observed": observed,
            }
        )
    return {"restored": restored, "blocked": blocked}


def apply_plan(
    plan: Mapping[str, Any], *, plan_path: Path, replace_func=os.replace
) -> dict[str, Any]:
    _validate_plan(plan)
    if plan_path != Path(plan["transaction_dir"]) / "plan.json":
        _fail("pnc_steady_release_transaction_plan_invalid")
    plan_raw, on_disk_plan = _read_plan(plan_path)
    if dict(plan) != on_disk_plan:
        _fail("pnc_steady_release_transaction_plan_changed")
    plan = on_disk_plan
    state_root = base._directory(Path(plan["state_root"]))
    lock, lock_fd = _acquire_lock(state_root, str(plan["transaction_id"]))
    attempted_names: list[str] = []
    after_by_name: dict[str, Mapping[str, Any]] = {}
    try:
        locked_plan_raw, locked_plan = _read_plan(plan_path)
        if locked_plan_raw != plan_raw or locked_plan != plan:
            _fail("pnc_steady_release_transaction_plan_changed")
        provenance = base._source_provenance(Path(plan["source_root"]))
        if provenance != {
            "commit": plan["source_commit"],
            "tree": plan["source_tree"],
        }:
            _fail("pnc_steady_release_transaction_source_changed")
        profile = plan["steady_profile"]
        baseline = plan["quarantine_baseline"]
        if not base._same_observation(
            base._observe(Path(profile["path"]), required=True),
            profile["observation"],
        ):
            _fail("pnc_steady_release_transaction_profile_changed")
        if not base._same_observation(
            base._observe(Path(baseline["path"]), required=True),
            baseline["observation"],
        ):
            _fail("pnc_steady_release_transaction_baseline_changed")
        for anchor in plan["read_only_plist_anchors"].values():
            if not base._same_observation(
                base._observe(Path(anchor["path"]), required=True),
                anchor["observation"],
            ):
                _fail("pnc_steady_release_transaction_profile_anchor_changed")
        if _read_activation_binding(Path(plan["control_db"])) != dict(
            plan["activation_binding"]
        ):
            _fail("pnc_steady_release_transaction_activation_changed")
        _revalidate_candidate_baseline(plan)
        for entry in plan["entries"]:
            current = base._observe(Path(entry["target_path"]), required=False)
            if not base._same_observation(current, entry["before"]):
                _fail("pnc_steady_release_transaction_target_changed")
            _raw, staged_observation = base._read_file(
                Path(entry["staged_path"]),
                code="pnc_steady_release_transaction_staged_invalid",
            )
            if staged_observation["sha256"] != entry["source"]["sha256"]:
                _fail("pnc_steady_release_transaction_staged_changed")
        base._backup(plan)
        for entry in plan["entries"]:
            if not base._same_observation(
                base._observe(Path(entry["target_path"]), required=False),
                entry["before"],
            ):
                _fail("pnc_steady_release_transaction_target_changed")
        ordered = sorted(
            plan["entries"], key=lambda item: 1 if item["kind"] == "manifest" else 0
        )
        for entry in ordered:
            if not base._same_observation(
                base._observe(Path(entry["target_path"]), required=False),
                entry["before"],
            ):
                _fail("pnc_steady_release_transaction_target_changed")
            name = str(entry["name"])
            attempted_names.append(name)
            _replace_from_staged(
                entry,
                transaction_id=str(plan["transaction_id"]),
                replace_func=replace_func,
                after_observations=after_by_name,
            )
        after = []
        for entry in plan["entries"]:
            observed = base._observe(Path(entry["target_path"]), required=True)
            if not base._same_observation(
                observed, after_by_name[str(entry["name"])]
            ):
                _fail("pnc_steady_release_transaction_verify_failed")
            after.append({"name": entry["name"], "observed": observed})
        if _read_activation_binding(Path(plan["control_db"])) != dict(
            plan["activation_binding"]
        ):
            _fail("pnc_steady_release_transaction_activation_changed")
        _revalidate_candidate_baseline(plan)
        receipt = {
            "schema_version": RECEIPT_SCHEMA_VERSION,
            "transaction_id": plan["transaction_id"],
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "release_id": plan["release_id"],
            "authority_sha256": plan["authority_sha256"],
            "authority_epoch_id": plan["authority_epoch_id"],
            "source_commit": plan["source_commit"],
            "source_tree": plan["source_tree"],
            "activation_binding": dict(plan["activation_binding"]),
            "steady_profile": dict(plan["steady_profile"]),
            "quarantine_baseline": dict(plan["quarantine_baseline"]),
            "plan_path": str(plan_path),
            "plan_raw_sha256": hashlib.sha256(plan_raw).hexdigest(),
            "entries": after,
            "mutation_performed": True,
            "rollback_performed": False,
            "production_effects": _effects(),
            "verification": "pass",
        }
        receipt_path = Path(plan["transaction_dir"]) / "receipt.json"
        base._write_new(receipt_path, base._pretty(receipt), mode=0o600)
        receipt["receipt_path"] = str(receipt_path)
        receipt["receipt_raw_sha256"] = hashlib.sha256(
            receipt_path.read_bytes()
        ).hexdigest()
        return receipt
    except BaseException as original:
        if attempted_names:
            rollback_result = _restore_attempted_no_clobber(
                plan,
                attempted_names=attempted_names,
                after_observations=after_by_name,
            )
            automatic = {
                "schema_version": ROLLBACK_SCHEMA_VERSION,
                "transaction_id": plan["transaction_id"],
                "rolled_back_at": datetime.now(timezone.utc).isoformat(),
                "restored_to_pre_transaction": not rollback_result["blocked"],
                "filesystem_restored_to_pre_transaction": not rollback_result[
                    "blocked"
                ],
                "runtime_state": "unchanged_by_transaction",
                "overall_release_state_restored": False,
                "restored_entries": rollback_result["restored"],
                "blocked_entries": rollback_result["blocked"],
                "original_error": getattr(
                    original, "code", "pnc_steady_release_transaction_apply_failed"
                ),
                "production_effects": _effects(),
            }
            base._write_new(
                Path(plan["transaction_dir"]) / "automatic-rollback.json",
                base._pretty(automatic),
                mode=0o600,
            )
            if rollback_result["blocked"]:
                _fail(
                    "pnc_steady_release_transaction_automatic_rollback_incomplete",
                    original,
                )
        raise
    finally:
        _release_lock(lock, lock_fd)


def rollback_transaction(receipt_path: Path, *, output_path: Path) -> dict[str, Any]:
    receipt_raw, _receipt_observation = base._read_file(
        receipt_path,
        code="pnc_steady_release_transaction_receipt_invalid",
        required_mode=0o600,
    )
    receipt = base._json(
        receipt_raw, code="pnc_steady_release_transaction_receipt_invalid"
    )
    if (
        receipt_raw != base._pretty(receipt)
        or receipt.get("schema_version") != RECEIPT_SCHEMA_VERSION
        or receipt.get("mutation_performed") is not True
        or receipt.get("rollback_performed") is not False
        or receipt.get("verification") != "pass"
        or receipt.get("production_effects") != _effects()
        or not output_path.is_absolute()
    ):
        _fail("pnc_steady_release_transaction_receipt_invalid")
    plan_raw, plan = _read_plan(Path(str(receipt.get("plan_path") or "")))
    if (
        hashlib.sha256(plan_raw).hexdigest() != receipt.get("plan_raw_sha256")
        or receipt.get("transaction_id") != plan["transaction_id"]
        or receipt.get("release_id") != plan["release_id"]
        or receipt.get("authority_sha256") != plan["authority_sha256"]
        or receipt.get("authority_epoch_id") != plan["authority_epoch_id"]
        or receipt.get("activation_binding") != plan["activation_binding"]
        or receipt_path != Path(plan["transaction_dir"]) / "receipt.json"
    ):
        _fail("pnc_steady_release_transaction_receipt_binding_invalid")
    receipt_entries = receipt.get("entries")
    if not isinstance(receipt_entries, list) or len(receipt_entries) != len(
        plan["entries"]
    ):
        _fail("pnc_steady_release_transaction_receipt_binding_invalid")
    after_by_name: dict[str, Mapping[str, Any]] = {}
    for item, entry in zip(receipt_entries, plan["entries"], strict=True):
        if (
            not isinstance(item, Mapping)
            or set(item) != {"name", "observed"}
            or item.get("name") != entry["name"]
            or not _valid_observation(item.get("observed"))
            or item["observed"].get("exists") is not True
            or item["observed"].get("sha256") != entry["source"]["sha256"]
            or item["observed"].get("mode") != entry["target_mode"]
        ):
            _fail("pnc_steady_release_transaction_receipt_binding_invalid")
        after_by_name[str(entry["name"])] = item["observed"]
    state_root = base._directory(Path(plan["state_root"]))
    lock, lock_fd = _acquire_lock(state_root, str(plan["transaction_id"]))
    try:
        locked_receipt_raw, _locked_receipt_observation = base._read_file(
            receipt_path,
            code="pnc_steady_release_transaction_receipt_invalid",
            required_mode=0o600,
        )
        locked_receipt = base._json(
            locked_receipt_raw,
            code="pnc_steady_release_transaction_receipt_invalid",
        )
        locked_plan_raw, locked_plan = _read_plan(Path(receipt["plan_path"]))
        if (
            locked_receipt_raw != receipt_raw
            or locked_receipt != receipt
            or locked_plan_raw != plan_raw
            or locked_plan != plan
        ):
            _fail("pnc_steady_release_transaction_receipt_binding_invalid")
        # A filesystem rollback is only safe while the exact aborted
        # predecessor remains current.  Never restore over a newer epoch.
        if _read_activation_binding(Path(plan["control_db"])) != dict(
            plan["activation_binding"]
        ):
            _fail("pnc_steady_release_transaction_activation_changed")
        result_detail = _restore_attempted_no_clobber(
            plan,
            attempted_names=[str(entry["name"]) for entry in plan["entries"]],
            after_observations=after_by_name,
        )
        result = {
            "schema_version": ROLLBACK_SCHEMA_VERSION,
            "transaction_id": plan["transaction_id"],
            "rolled_back_at": datetime.now(timezone.utc).isoformat(),
            "restored_to_pre_transaction": not result_detail["blocked"],
            "filesystem_restored_to_pre_transaction": not result_detail["blocked"],
            "runtime_state": "unchanged_by_transaction",
            "overall_release_state_restored": False,
            "restored_entries": result_detail["restored"],
            "blocked_entries": result_detail["blocked"],
            "production_effects": _effects(),
        }
        base._write_new(output_path, base._pretty(result), mode=0o600)
        if result_detail["blocked"]:
            _fail("pnc_steady_release_transaction_manual_rollback_incomplete")
        return result
    finally:
        _release_lock(lock, lock_fd)


def _arguments(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    plan = commands.add_parser("plan")
    plan.add_argument("--candidate-root", type=Path, required=True)
    plan.add_argument("--source-root", type=Path, required=True)
    plan.add_argument("--home", type=Path, default=Path.home())
    plan.add_argument("--hermes-home", type=Path, default=Path.home() / ".hermes")
    plan.add_argument("--control-db", type=Path, required=True)
    plan.add_argument("--evidence-root", type=Path, required=True)
    plan.add_argument("--transaction-id")
    apply = commands.add_parser("apply")
    apply.add_argument("--plan", type=Path, required=True)
    rollback = commands.add_parser("rollback")
    rollback.add_argument("--receipt", type=Path, required=True)
    rollback.add_argument("--output", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    try:
        args = _arguments(argv)
        if args.command == "plan":
            result, path = build_plan(
                candidate_root=args.candidate_root.expanduser().absolute(),
                source_root=args.source_root.expanduser().absolute(),
                home=args.home.expanduser().absolute(),
                hermes_home=args.hermes_home.expanduser().absolute(),
                control_db=args.control_db.expanduser().absolute(),
                evidence_root=args.evidence_root.expanduser().absolute(),
                transaction_id=args.transaction_id,
            )
            print(json.dumps({"ok": True, "plan_path": str(path), **result}, sort_keys=True))
            return 0
        if args.command == "apply":
            _raw, plan = _read_plan(args.plan.expanduser().absolute())
            result = apply_plan(plan, plan_path=args.plan.expanduser().absolute())
            print(json.dumps({"ok": True, **result}, sort_keys=True))
            return 0
        result = rollback_transaction(
            args.receipt.expanduser().absolute(),
            output_path=args.output.expanduser().absolute(),
        )
        print(json.dumps({"ok": True, **result}, sort_keys=True))
        return 0
    except (
        SteadyReleaseTransactionError,
        base.ReleaseTransactionError,
        OSError,
        ValueError,
    ) as exc:
        print(
            json.dumps(
                {
                    "ok": False,
                    "code": getattr(
                        exc, "code", "pnc_steady_release_transaction_invalid"
                    ),
                },
                sort_keys=True,
            )
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
