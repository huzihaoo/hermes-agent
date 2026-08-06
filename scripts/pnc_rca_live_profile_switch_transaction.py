#!/usr/bin/env python3
"""Atomically switch one installed RCA release from record-only to live profile."""

from __future__ import annotations

import argparse
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import plistlib
import re
import sqlite3
import tempfile
from typing import Any, Mapping, Sequence
import uuid

import yaml

from gateway import pnc_rca_delivery_quarantine_baseline as quarantine_baseline
from gateway import pnc_rca_prod_bootstrap as bootstrap
from gateway import pnc_rca_release_authority as authority
from scripts import pnc_rca_release_transaction as base
from scripts import pnc_rca_steady_release_transaction as steady


PROFILE_SCHEMA_VERSION = "pnc_rca_live_profile_switch_v1"
SCHEMA_VERSION = "pnc_rca_live_profile_switch_transaction_v1"
RECEIPT_SCHEMA_VERSION = "pnc_rca_live_profile_switch_receipt_v1"
ROLLBACK_SCHEMA_VERSION = "pnc_rca_live_profile_switch_rollback_v1"
CLI_SCHEMA_VERSION = "pnc_rca_live_profile_switch_cli_v1"
LOCK_NAME = ".pnc-rca-release-transaction.lock"
HEX64_RE = re.compile(r"^[0-9a-f]{64}$")
IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
TARGET_NAMES = (
    "env",
    "binding",
    "manifest",
    "local.pnc.completion-notice-relay.plist",
    "local.pnc.rca-delivery-dispatcher.plist",
)
ANCHOR_NAMES = (
    "active-pointer",
    "authority",
    "config",
    "bootstrap-authorization",
    "local.pnc.vm-task-sync.plist",
    "local.pnc.rca-delivery-collector.plist",
    "ai.hermes.gateway.plist",
    "local.pnc.rca-kafka-consumer.plist",
    "local.pnc.rca-outbox-dispatcher.plist",
)
TARGET_MODES = {
    "env": 0o600,
    "binding": 0o600,
    "manifest": 0o600,
    "local.pnc.completion-notice-relay.plist": 0o644,
    "local.pnc.rca-delivery-dispatcher.plist": 0o644,
}
INVENTORY_PIN = "9fea0306752d005f58937e08202c9ce094e52056794549201259f214fe885880"
STALE_RELAY_TASK_ID = "rca-r11-safe-off-no-task"
PROFILE_FIELDS = frozenset(
    {
        "schema_version",
        "release_id",
        "authority_sha256",
        "source",
        "initial_mode",
        "live_mode",
        "activation_epoch_id",
        "required_activation_state",
        "live_profile_root",
        "live_env_sha256",
        "live_binding_sha256",
        "live_manifest_sha256",
        "read_only_plist_anchors",
        "production_effects",
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
        "source_path",
        "source",
        "staged_path",
        "staged",
        "target_path",
        "target_mode",
        "before",
        "rollback_path",
    }
)
ACTIVATION_BINDING_FIELDS = frozenset({"epoch_id", "state", "updated_at"})
ARTIFACT_BINDING_FIELDS = frozenset({"path", "observation"})
BASELINE_BINDING_FIELDS = frozenset(
    {
        "path",
        "observation",
        "baseline_id",
        "baseline_fingerprint",
        "status_sha256",
        "db_logical_identity_sha256",
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
        "candidate_root",
        "source_root",
        "source_commit",
        "source_tree",
        "home",
        "hermes_home",
        "control_db",
        "state_root",
        "transaction_dir",
        "rollback_dir",
        "mode_switch_profile",
        "authority",
        "initial_profile",
        "live_profile",
        "read_only_plist_anchors",
        "activation_binding",
        "quarantine_baseline",
        "entries",
        "mutation_performed",
        "production_effects",
    }
)
RECEIPT_FIELDS = frozenset(
    {
        "schema_version",
        "transaction_id",
        "release_id",
        "authority_sha256",
        "authority_epoch_id",
        "completed_at",
        "activation_binding",
        "quarantine_baseline",
        "plan_path",
        "plan_raw_sha256",
        "entries",
        "mutation_performed",
        "rollback_performed",
        "production_effects",
        "verification",
    }
)


class LiveProfileSwitchError(RuntimeError):
    def __init__(self, code: str) -> None:
        self.code = str(code or "pnc_rca_live_profile_switch_invalid")[:160]
        super().__init__(self.code)


def _fail(code: str, exc: Exception | None = None) -> None:
    error = LiveProfileSwitchError(code)
    if exc is None:
        raise error
    raise error from exc


def _canonical(value: Any) -> bytes:
    try:
        return (
            json.dumps(
                value,
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
            + "\n"
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError, RecursionError) as exc:
        _fail("pnc_rca_live_profile_switch_json_invalid", exc)


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _json(path: Path) -> tuple[bytes, dict[str, Any]]:
    raw, _observation = base._read_file(
        path,
        code="pnc_rca_live_profile_switch_file_invalid",
        required_mode=0o600,
    )
    value = base._json(raw, code="pnc_rca_live_profile_switch_json_invalid")
    if not isinstance(value, dict):
        _fail("pnc_rca_live_profile_switch_json_invalid")
    return raw, value


def _json_artifact(path: Path) -> tuple[bytes, dict[str, Any], dict[str, Any]]:
    raw, observation = base._read_file(
        path,
        code="pnc_rca_live_profile_switch_file_invalid",
        required_mode=0o600,
    )
    value = base._json(raw, code="pnc_rca_live_profile_switch_json_invalid")
    if not isinstance(value, dict):
        _fail("pnc_rca_live_profile_switch_json_invalid")
    return raw, value, {"exists": True, **observation}


def _env(path: Path) -> tuple[bytes, dict[str, str], dict[str, Any]]:
    raw, obs = base._read_file(
        path,
        code="pnc_rca_live_profile_switch_env_invalid",
        required_mode=0o600,
    )
    try:
        value = base._env_map(raw)
    except base.ReleaseTransactionError as exc:
        _fail("pnc_rca_live_profile_switch_env_invalid", exc)
    return raw, value, obs


def _plist(path: Path, *, required_mode: int = 0o644) -> tuple[bytes, dict[str, Any], dict[str, Any]]:
    raw, obs = base._read_file(
        path,
        code="pnc_rca_live_profile_switch_plist_invalid",
        required_mode=required_mode,
    )
    try:
        value = plistlib.loads(raw)
    except (ValueError, plistlib.InvalidFileException) as exc:
        _fail("pnc_rca_live_profile_switch_plist_invalid", exc)
    if not isinstance(value, dict):
        _fail("pnc_rca_live_profile_switch_plist_invalid")
    return raw, value, obs


def _effects() -> dict[str, bool]:
    return {
        "database_mutation": False,
        "task_submission": False,
        "kafka_consume": False,
        "feishu_write": False,
        "resident_restart": False,
    }


def _project_env(raw: bytes) -> bytes:
    replacements = {
        "HERMES_OUTBOUND_MODE": "live",
        "HERMES_RCA_DELIVERY_DISPATCHER_ENABLED": "true",
    }
    try:
        lines = raw.decode("utf-8").splitlines(keepends=True)
    except UnicodeDecodeError as exc:
        _fail("pnc_rca_live_profile_switch_env_invalid", exc)
    seen: set[str] = set()
    projected: list[str] = []
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in line:
            projected.append(line)
            continue
        key = line.split("=", 1)[0].strip()
        if key not in replacements:
            projected.append(line)
            continue
        if key in seen:
            _fail("pnc_rca_live_profile_switch_env_invalid")
        suffix = "\n" if line.endswith("\n") else ""
        projected.append(f"{key}={replacements[key]}{suffix}")
        seen.add(key)
    if seen != set(replacements):
        _fail("pnc_rca_live_profile_switch_env_invalid")
    return "".join(projected).encode("utf-8")


def _artifact_binding(path: Path, observation: Mapping[str, Any]) -> dict[str, Any]:
    return {"path": str(path), "observation": {"exists": True, **dict(observation)}}


def _project_plist(
    raw: bytes,
    *,
    label: str,
    outbound: str,
    enabled: bool,
    hermes_home: Path,
    release_id: str,
    dispatcher_environment: Mapping[str, str] | None = None,
) -> bytes:
    try:
        value = plistlib.loads(raw)
    except (ValueError, plistlib.InvalidFileException) as exc:
        _fail("pnc_rca_live_profile_switch_plist_invalid", exc)
    if not isinstance(value, dict) or value.get("Label") != label:
        _fail("pnc_rca_live_profile_switch_plist_invalid")
    args = value.get("ProgramArguments")
    launcher = str(hermes_home / "runtime" / "governance-tools" / "pnc_live_exec.py")
    if not isinstance(args, list) or args[:3] != ["/usr/bin/python3", launcher, label]:
        _fail("pnc_rca_live_profile_switch_plist_runtime_pinned")
    if any(
        marker in json.dumps(value, ensure_ascii=True, sort_keys=True)
        for marker in ("/runtime/releases/", "/runtime/venvs/")
    ):
        _fail("pnc_rca_live_profile_switch_plist_runtime_pinned")
    environment = value.get("EnvironmentVariables")
    if not isinstance(environment, dict):
        _fail("pnc_rca_live_profile_switch_plist_environment_invalid")
    if label == "local.pnc.completion-notice-relay":
        task_id_count = args.count("--task-id")
        if task_id_count > 1:
            _fail("pnc_rca_live_profile_switch_relay_task_id_invalid")
        if task_id_count == 1:
            index = args.index("--task-id")
            if index + 1 >= len(args) or args[index + 1] != STALE_RELAY_TASK_ID:
                _fail("pnc_rca_live_profile_switch_relay_task_id_invalid")
            del args[index : index + 2]
        environment["HERMES_OUTBOUND_MODE"] = outbound
    elif label == "local.pnc.rca-delivery-dispatcher":
        if dispatcher_environment is None:
            _fail("pnc_rca_live_profile_switch_dispatcher_environment_invalid")
        inventory_pin = str(
            dispatcher_environment.get("HERMES_RCA_DELIVERY_DISPATCHER_INVENTORY_PIN")
            or ""
        ).lower()
        observed_release = str(
            dispatcher_environment.get(
                "HERMES_RCA_DELIVERY_DISPATCHER_OBSERVATION_RELEASE_ID"
            )
            or ""
        )
        observed_enabled = str(
            dispatcher_environment.get("HERMES_RCA_DELIVERY_DISPATCHER_ENABLED") or ""
        )
        observed_observability = str(
            dispatcher_environment.get(
                "HERMES_RCA_DELIVERY_DISPATCHER_OBSERVABILITY_ENABLED"
            )
            or ""
        )
        if (
            HEX64_RE.fullmatch(inventory_pin) is None
            or observed_release != release_id
            or observed_enabled != ("true" if enabled else "false")
            or observed_observability != "true"
        ):
            _fail("pnc_rca_live_profile_switch_dispatcher_environment_invalid")
        environment.update(
            {
                "HERMES_OUTBOUND_MODE": outbound,
                "HERMES_RCA_DELIVERY_DISPATCHER_ENABLED": "true" if enabled else "false",
                "HERMES_RCA_DELIVERY_DISPATCHER_INVENTORY_PIN": inventory_pin,
                "HERMES_RCA_DELIVERY_DISPATCHER_OBSERVABILITY_ENABLED": "true",
                "HERMES_RCA_DELIVERY_DISPATCHER_OBSERVATION_RELEASE_ID": release_id,
            }
        )
    return plistlib.dumps(value, fmt=plistlib.FMT_XML, sort_keys=True)


def _paths(candidate_root: Path, home: Path, hermes_home: Path) -> dict[str, Path]:
    live_root = candidate_root / "live-profile"
    launch = home / "Library" / "LaunchAgents"
    state = hermes_home / "runtime" / "pnc_agent" / "feishu_issue_kafka_rca"
    return {
        "profile": candidate_root / "mode-switch-profile.json",
        "authority": candidate_root / "authority.json",
        "env_candidate": candidate_root / "candidate.env",
        "binding_candidate": candidate_root / "active-release-binding.json",
        "manifest_candidate": candidate_root / "LIVE_MANIFEST.json",
        "env_live": live_root / "candidate.env",
        "binding_live": live_root / "active-release-binding.json",
        "manifest_live": live_root / "LIVE_MANIFEST.json",
        "relay_live": live_root / "local.pnc.completion-notice-relay.plist",
        "dispatcher_live": live_root / "local.pnc.rca-delivery-dispatcher.plist",
        "env_target": hermes_home / ".env",
        "binding_target": state / "active-release-binding.json",
        "manifest_target": hermes_home / "runtime" / "LIVE_MANIFEST.json",
        "relay_target": launch / "local.pnc.completion-notice-relay.plist",
        "dispatcher_target": launch / "local.pnc.rca-delivery-dispatcher.plist",
        "active_pointer_anchor": state / "ACTIVE_RCA_RELEASE.json",
        "authority_anchor": state / "authority.placeholder.json",
        "config_anchor": hermes_home / "config.yaml",
        "bootstrap_authorization_anchor": bootstrap.BOOTSTRAP_AUTHORIZATION_PATH.expanduser().absolute(),
        "vm_sync_anchor": launch / "local.pnc.vm-task-sync.plist",
        "collector_anchor": launch / "local.pnc.rca-delivery-collector.plist",
        "gateway_anchor": launch / "ai.hermes.gateway.plist",
        "kafka_anchor": launch / "local.pnc.rca-kafka-consumer.plist",
        "outbox_anchor": launch / "local.pnc.rca-outbox-dispatcher.plist",
    }


def _activation_binding(control_db: Path, *, epoch_id: str) -> dict[str, Any]:
    try:
        with steady._read_only_db(control_db) as connection:
            rows = connection.execute(
                "SELECT epoch_id, state, is_current, updated_at FROM rca_activation_epochs "
                "WHERE is_current = 1"
            ).fetchall()
    except (OSError, sqlite3.Error, steady.SteadyReleaseTransactionError) as exc:
        _fail("pnc_rca_live_profile_switch_activation_invalid", exc)
    if (
        len(rows) != 1
        or str(rows[0]["epoch_id"] or "") != epoch_id
        or str(rows[0]["state"] or "") != "bounded_active"
        or int(rows[0]["is_current"] or 0) != 1
    ):
        _fail("pnc_rca_live_profile_switch_activation_not_bounded")
    return {
        "epoch_id": str(rows[0]["epoch_id"]),
        "state": str(rows[0]["state"]),
        "updated_at": str(rows[0]["updated_at"]),
    }


def _anchor_paths(*, home: Path, hermes_home: Path, release_id: str) -> dict[str, Path]:
    state_root = hermes_home / "runtime" / "pnc_agent" / "feishu_issue_kafka_rca"
    launch = home / "Library" / "LaunchAgents"
    return {
        "active-pointer": state_root / "ACTIVE_RCA_RELEASE.json",
        "authority": state_root / f"{release_id}.authority.json",
        "config": hermes_home / "config.yaml",
        "bootstrap-authorization": bootstrap.BOOTSTRAP_AUTHORIZATION_PATH.expanduser().absolute(),
        "local.pnc.vm-task-sync.plist": launch / "local.pnc.vm-task-sync.plist",
        "local.pnc.rca-delivery-collector.plist": launch / "local.pnc.rca-delivery-collector.plist",
        "ai.hermes.gateway.plist": launch / "ai.hermes.gateway.plist",
        "local.pnc.rca-kafka-consumer.plist": launch / "local.pnc.rca-kafka-consumer.plist",
        "local.pnc.rca-outbox-dispatcher.plist": launch / "local.pnc.rca-outbox-dispatcher.plist",
    }


def _validate_release_state(
    *,
    candidate_root: Path,
    source_root: Path,
    home: Path,
    hermes_home: Path,
    control_db: Path,
    installed_mode: str,
) -> dict[str, Any]:
    if installed_mode not in {"record-only", "live"}:
        _fail("pnc_rca_live_profile_switch_installed_mode_invalid")
    paths = _paths(candidate_root, home, hermes_home)
    _profile_raw, profile, profile_obs = _json_artifact(paths["profile"])
    _authority_raw, authority_value, authority_obs = _json_artifact(paths["authority"])
    try:
        authority.validate_release_authority(authority_value)
    except authority.ReleaseAuthorityError as exc:
        _fail("pnc_rca_live_profile_switch_authority_invalid", exc)
    release_id = str(authority_value.get("release_id") or "")
    authority_sha = authority.canonical_json_sha256(authority_value)
    authority_epoch_id = str(authority_value.get("authority_epoch_id") or "")
    provenance = base._source_provenance(source_root)
    host_face = authority_value.get("faces", {}).get("host_runtime", {})
    anchors = profile.get("read_only_plist_anchors")
    if (
        set(profile) != PROFILE_FIELDS
        or profile.get("schema_version") != PROFILE_SCHEMA_VERSION
        or profile.get("release_id") != release_id
        or profile.get("authority_sha256") != authority_sha
        or profile.get("source") != provenance
        or profile.get("initial_mode") != "record-only"
        or profile.get("live_mode") != "live"
        or profile.get("required_activation_state") != "bounded_active"
        or IDENTIFIER_RE.fullmatch(str(profile.get("activation_epoch_id") or "")) is None
        or profile.get("live_profile_root") != str(candidate_root / "live-profile")
        or profile.get("production_effects") != _effects()
        or not isinstance(anchors, Mapping)
        or set(anchors) != set(ANCHOR_NAMES)
        or IDENTIFIER_RE.fullmatch(release_id) is None
        or authority_value.get("status") != "approved_for_activation"
        or host_face.get("commit") != provenance["commit"]
        or host_face.get("tree") != provenance["tree"]
        or host_face.get("root") != str(source_root)
    ):
        _fail("pnc_rca_live_profile_switch_profile_invalid")
    activation_binding = _activation_binding(
        control_db, epoch_id=str(profile["activation_epoch_id"])
    )
    state_root = hermes_home / "runtime" / "pnc_agent" / "feishu_issue_kafka_rca"

    initial_env_raw, initial_env, initial_env_obs = _env(paths["env_candidate"])
    initial_binding_raw, initial_binding, initial_binding_obs = _json_artifact(
        paths["binding_candidate"]
    )
    initial_manifest_raw, initial_manifest, initial_manifest_obs = _json_artifact(
        paths["manifest_candidate"]
    )
    live_env_raw, live_env, live_env_obs = _env(paths["env_live"])
    live_binding_raw, live_binding, live_binding_obs = _json_artifact(paths["binding_live"])
    live_manifest_raw, live_manifest, live_manifest_obs = _json_artifact(paths["manifest_live"])
    required_common = {
        "HERMES_HOME": str(hermes_home),
        "HERMES_RCA_OUTBOX_ALLOW_FEISHU_WRITEBACK": "false",
        "HERMES_RCA_PROD_RELEASE_ID": release_id,
        "HERMES_RCA_PROD_CAPACITY_MODE": "bootstrap",
        "HERMES_RCA_DELIVERY_DISPATCHER_INVENTORY_PIN": None,
        "HERMES_RCA_DELIVERY_DISPATCHER_OBSERVABILITY_ENABLED": "true",
        "HERMES_RCA_DELIVERY_DISPATCHER_OBSERVATION_RELEASE_ID": release_id,
    }
    if (
        any(
            value is not None and initial_env.get(key) != value
            for key, value in required_common.items()
        )
        or HEX64_RE.fullmatch(
            str(initial_env.get("HERMES_RCA_DELIVERY_DISPATCHER_INVENTORY_PIN") or "")
        ) is None
        or initial_env.get("HERMES_OUTBOUND_MODE") != "record-only"
        or initial_env.get("HERMES_RCA_DELIVERY_DISPATCHER_ENABLED") != "false"
        or any(
            value is not None and live_env.get(key) != value
            for key, value in required_common.items()
        )
        or live_env.get("HERMES_RCA_DELIVERY_DISPATCHER_INVENTORY_PIN")
        != initial_env.get("HERMES_RCA_DELIVERY_DISPATCHER_INVENTORY_PIN")
        or live_env.get("HERMES_OUTBOUND_MODE") != "live"
        or live_env.get("HERMES_RCA_DELIVERY_DISPATCHER_ENABLED") != "true"
        or live_env_raw != _project_env(initial_env_raw)
        or live_env_obs["sha256"] != profile.get("live_env_sha256")
    ):
        _fail("pnc_rca_live_profile_switch_env_projection_invalid")
    canonical_db_keys = (
        "HERMES_RCA_DELIVERY_COLLECTOR_CONTROL_DB_PATH",
        "HERMES_RCA_DELIVERY_DISPATCHER_CONTROL_DB_PATH",
        "HERMES_RCA_KAFKA_CONTROL_DB_PATH",
        "HERMES_RCA_OUTBOX_CONTROL_DB_PATH",
        "HERMES_RCA_OUTBOX_DELIVERY_DB_PATH",
    )
    if any(
        env.get(key) != str(control_db)
        for env in (initial_env, live_env)
        for key in canonical_db_keys
    ):
        _fail("pnc_rca_live_profile_switch_env_path_invalid")
    try:
        expected_live_binding = deepcopy(initial_binding)
        expected_live_binding["bindings"]["candidate_env"]["sha256"] = live_env_obs["sha256"]
    except (KeyError, TypeError) as exc:
        _fail("pnc_rca_live_profile_switch_binding_invalid", exc)
    if live_binding != expected_live_binding or _sha(live_binding_raw) != profile.get(
        "live_binding_sha256"
    ):
        _fail("pnc_rca_live_profile_switch_binding_projection_invalid")
    try:
        capacity = initial_binding["policy"]["capacity_admission"]
        bootstrap_epoch_id = str(capacity["bootstrap_epoch_id"])
        if (
            initial_env.get("HERMES_RCA_PROD_BOOTSTRAP_EPOCH_ID") != bootstrap_epoch_id
            or live_env.get("HERMES_RCA_PROD_BOOTSTRAP_EPOCH_ID") != bootstrap_epoch_id
            or live_binding["policy"]["capacity_admission"] != capacity
        ):
            _fail("pnc_rca_live_profile_switch_capacity_binding_invalid")
        authorization_path = candidate_root / "bootstrap-authorization.json"
        _authorization_raw, authorization, authorization_obs = _json_artifact(
            authorization_path
        )
        bootstrap.validate_bootstrap_authorization(
            authorization,
            expected_epoch_id=bootstrap_epoch_id,
            expected_release_bom_sha256=str(capacity["release_bom_sha256"]),
            expected_release_approval_id=release_id,
            expected_approval_evidence_sha256=str(capacity["approval_evidence_sha256"]),
            authorization_receipt_sha256=authorization_obs["sha256"],
        )
        if (
            authorization_obs["sha256"] != capacity["bootstrap_authorization_sha256"]
            or bootstrap.authorization_fingerprint(authorization)
            != capacity["bootstrap_authorization_fingerprint"]
        ):
            _fail("pnc_rca_live_profile_switch_capacity_binding_invalid")
    except (KeyError, TypeError, bootstrap.RcaBootstrapAuthorizationError) as exc:
        _fail("pnc_rca_live_profile_switch_capacity_binding_invalid", exc)
    candidate_config_path = candidate_root / "config.yaml"
    config_target = hermes_home / "config.yaml"
    config_raw, _config_obs = base._read_file(
        candidate_config_path,
        code="pnc_rca_live_profile_switch_config_invalid",
        required_mode=0o600,
    )
    try:
        config_semantic_sha = _sha(
            json.dumps(
                yaml.safe_load(config_raw.decode("utf-8")),
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        )
    except (UnicodeDecodeError, TypeError, ValueError, yaml.YAMLError) as exc:
        _fail("pnc_rca_live_profile_switch_config_invalid", exc)
    try:
        expected_live_manifest = deepcopy(initial_manifest)
        expected_live_manifest["env_sha256"] = live_env_obs["sha256"]
        expected_live_manifest["gateway_release_binding"][
            "rca_platform_active_binding_sha256"
        ] = live_binding_obs["sha256"]
    except (KeyError, TypeError) as exc:
        _fail("pnc_rca_live_profile_switch_manifest_invalid", exc)
    if live_manifest != expected_live_manifest or _sha(live_manifest_raw) != profile.get(
        "live_manifest_sha256"
    ):
        _fail("pnc_rca_live_profile_switch_manifest_projection_invalid")
    for binding, env_obs in (
        (initial_binding, initial_env_obs),
        (live_binding, live_env_obs),
    ):
        try:
            base._validate_binding(
                binding,
                release_id=release_id,
                authority_sha256=authority_sha,
                authority_epoch_id=authority_epoch_id,
                env_sha256=env_obs["sha256"],
                binding_path=state_root / "active-release-binding.json",
                env_path=hermes_home / ".env",
            )
        except base.ReleaseTransactionError as exc:
            _fail("pnc_rca_live_profile_switch_binding_invalid", exc)
    for manifest, env_obs, binding_obs in (
        (initial_manifest, initial_env_obs, initial_binding_obs),
        (live_manifest, live_env_obs, live_binding_obs),
    ):
        gateway_release = manifest.get("gateway_release_binding")
        gateway_capacity = (
            gateway_release.get("capacity_admission")
            if isinstance(gateway_release, Mapping)
            else None
        )
        capacity = initial_binding["policy"]["capacity_admission"]
        if (
            manifest.get("config_path") != str(config_target)
            or manifest.get("config_sha256") != _sha(config_raw)
            or manifest.get("config_semantic_sha256") != config_semantic_sha
            or manifest.get("env_sha256") != env_obs["sha256"]
            or manifest.get("runtime_root")
            != authority_value["faces"]["host_runtime"]["root"]
            or manifest.get("rca_release_authority", {}).get("release_id") != release_id
            or manifest.get("gateway_release_binding", {}).get(
                "rca_platform_active_binding_sha256"
            )
            != binding_obs["sha256"]
            or not isinstance(gateway_capacity, Mapping)
            or gateway_capacity.get("release_id") != release_id
            or gateway_capacity.get("bootstrap_epoch_id") != capacity["bootstrap_epoch_id"]
            or gateway_capacity.get("bootstrap_authorization_sha256")
            != capacity["bootstrap_authorization_sha256"]
            or gateway_capacity.get("bootstrap_authorization_fingerprint")
            != capacity["bootstrap_authorization_fingerprint"]
            or gateway_capacity.get("release_bom_sha256") != capacity["release_bom_sha256"]
            or gateway_release.get("rca_platform_bootstrap_authorization_sha256")
            != capacity["bootstrap_authorization_sha256"]
            or gateway_release.get("rca_platform_owner_approval_sha256")
            != capacity["approval_evidence_sha256"]
            or gateway_release.get("rca_platform_release_bom_sha256")
            != capacity["release_bom_sha256"]
            or gateway_release.get("rca_platform_release_id") != release_id
        ):
            _fail("pnc_rca_live_profile_switch_manifest_invalid")
    pointer = _json(candidate_root / "ACTIVE_RCA_RELEASE.json")[1]
    for manifest, binding in (
        (initial_manifest, initial_binding),
        (live_manifest, live_binding),
    ):
        try:
            projection = authority.audit_release_projections(
                authority_value,
                pointer=pointer,
                authority_path=state_root / f"{release_id}.authority.json",
                live_manifest=manifest,
                active_binding=binding,
                control_store_path=control_db,
            )
        except (authority.ReleaseAuthorityError, OSError, ValueError) as exc:
            _fail("pnc_rca_live_profile_switch_projection_invalid", exc)
        if not projection.get("ok"):
            _fail("pnc_rca_live_profile_switch_projection_invalid")

    try:
        baseline_settings = quarantine_baseline.quarantine_baseline_settings(
            live_env,
            hermes_home=hermes_home,
            control_db_path=control_db,
        )
    except ValueError as exc:
        _fail("pnc_rca_live_profile_switch_baseline_invalid", exc)
    baseline_path = baseline_settings.baseline_path
    baseline_raw, baseline, baseline_obs = _json_artifact(baseline_path)
    baseline_authority = authority_value.get("quarantine_baseline")
    baseline_sha = _sha(baseline_raw)
    if (
        not isinstance(baseline_authority, Mapping)
        or baseline_authority.get("state") != "ready"
        or baseline_authority.get("baseline_sha256") != baseline_sha
        or baseline_settings.baseline_sha256 != baseline_sha
        or baseline_settings.release_id != release_id
        or baseline.get("release_id") != release_id
        or HEX64_RE.fullmatch(str(baseline.get("baseline_fingerprint") or "")) is None
    ):
        _fail("pnc_rca_live_profile_switch_baseline_invalid")
    try:
        with tempfile.TemporaryDirectory(
            prefix=".pnc-rca-live-profile-validation-", dir=candidate_root.parent
        ) as temporary_name:
            temporary = Path(temporary_name)
            temp_binding = temporary / "active-release-binding.json"
            temp_env = temporary / ".env"
            validation_binding = deepcopy(live_binding)
            validation_binding["side_effect_contract"] = {
                "canonical_active_release_binding": str(temp_binding),
                "canonical_live_env": str(temp_env),
            }
            base._write_new(temp_binding, base._pretty(validation_binding), mode=0o600)
            base._write_new(temp_env, live_env_raw, mode=0o600)
            with steady._read_only_db(control_db) as connection:
                status = quarantine_baseline.quarantine_baseline_status_tx(
                    connection,
                    db_path=control_db,
                    baseline_path=baseline_path,
                    expected_sha256=baseline_sha,
                    expected_release_id=release_id,
                    bootstrap_epoch_id=str(
                        live_binding["policy"]["capacity_admission"]["bootstrap_epoch_id"]
                    ),
                    active_release_binding_path=temp_binding,
                    live_env_path=temp_env,
                )
    except (
        KeyError,
        OSError,
        steady.SteadyReleaseTransactionError,
        quarantine_baseline.DeliveryQuarantineBaselineError,
    ) as exc:
        _fail("pnc_rca_live_profile_switch_baseline_invalid", exc)
    baseline_identity = status.get("baseline_identity")
    if (
        status.get("ready") is not True
        or not isinstance(baseline_identity, Mapping)
        or HEX64_RE.fullmatch(
            str(baseline_identity.get("db_logical_identity_sha256") or "")
        )
        is None
    ):
        _fail("pnc_rca_live_profile_switch_baseline_invalid")
    baseline_binding = {
        "path": str(baseline_path),
        "observation": baseline_obs,
        "baseline_id": str(baseline.get("baseline_id") or ""),
        "baseline_fingerprint": str(baseline["baseline_fingerprint"]),
        "status_sha256": _sha(_canonical(status)),
        "db_logical_identity_sha256": str(
            baseline_identity["db_logical_identity_sha256"]
        ),
    }

    anchor_observations: dict[str, Any] = {}
    expected_anchor_paths = _anchor_paths(
        home=home, hermes_home=hermes_home, release_id=release_id
    )
    anchor_source_paths = {
        "active-pointer": candidate_root / "ACTIVE_RCA_RELEASE.json",
        "authority": candidate_root / "authority.json",
        "config": candidate_root / "config.yaml",
        "bootstrap-authorization": candidate_root / "bootstrap-authorization.json",
        "local.pnc.vm-task-sync.plist": candidate_root / "local.pnc.vm-task-sync.plist",
        "local.pnc.rca-delivery-collector.plist": candidate_root
        / "local.pnc.rca-delivery-collector.plist",
        "ai.hermes.gateway.plist": expected_anchor_paths["ai.hermes.gateway.plist"],
        "local.pnc.rca-kafka-consumer.plist": expected_anchor_paths[
            "local.pnc.rca-kafka-consumer.plist"
        ],
        "local.pnc.rca-outbox-dispatcher.plist": expected_anchor_paths[
            "local.pnc.rca-outbox-dispatcher.plist"
        ],
    }
    for name in ANCHOR_NAMES:
        item = anchors.get(name)
        target = expected_anchor_paths[name]
        if (
            not isinstance(item, Mapping)
            or set(item) != {"path", "sha256"}
            or item.get("path") != str(target)
            or HEX64_RE.fullmatch(str(item.get("sha256") or "")) is None
        ):
            _fail("pnc_rca_live_profile_switch_anchor_invalid")
        observed = base._observe(target, required=True)
        source_raw = base._read_file(
            anchor_source_paths[name],
            code="pnc_rca_live_profile_switch_anchor_source_invalid",
            required_mode=(0o600 if name in ANCHOR_NAMES[:6] else None),
        )[0]
        if (
            observed["sha256"] != item["sha256"]
            or observed["sha256"] != _sha(source_raw)
        ):
            _fail("pnc_rca_live_profile_switch_anchor_changed")
        anchor_observations[name] = {
            "path": str(target),
            "sha256": item["sha256"],
            "observation": observed,
        }

    initial_profile = {
        "env": _artifact_binding(paths["env_candidate"], initial_env_obs),
        "binding": _artifact_binding(paths["binding_candidate"], initial_binding_obs),
        "manifest": _artifact_binding(paths["manifest_candidate"], initial_manifest_obs),
    }
    live_profile = {
        "env": _artifact_binding(paths["env_live"], live_env_obs),
        "binding": _artifact_binding(paths["binding_live"], live_binding_obs),
        "manifest": _artifact_binding(paths["manifest_live"], live_manifest_obs),
    }
    for name, target_key, live_key in (
        ("local.pnc.completion-notice-relay.plist", "relay_target", "relay_live"),
        ("local.pnc.rca-delivery-dispatcher.plist", "dispatcher_target", "dispatcher_live"),
    ):
        source_raw = base._read_file(
            source_root / name,
            code="pnc_rca_live_profile_switch_plist_source_invalid",
        )[0]
        initial_path = candidate_root / name
        initial_raw, _initial_value, initial_obs = _plist(
            initial_path, required_mode=0o600
        )
        live_path = paths[live_key]
        live_raw, _live_value, live_obs = _plist(live_path, required_mode=0o600)
        expected_initial = _project_plist(
            source_raw,
            label=name[:-6],
            outbound="record-only",
            enabled=False,
            hermes_home=hermes_home,
            release_id=release_id,
            dispatcher_environment=initial_env,
        )
        expected_live = _project_plist(
            source_raw,
            label=name[:-6],
            outbound="live",
            enabled=True,
            hermes_home=hermes_home,
            release_id=release_id,
            dispatcher_environment=live_env,
        )
        if initial_raw != expected_initial or live_raw != expected_live:
            _fail("pnc_rca_live_profile_switch_candidate_plist_invalid")
        initial_profile[name] = _artifact_binding(initial_path, initial_obs)
        live_profile[name] = _artifact_binding(live_path, live_obs)

    expected_profile = initial_profile if installed_mode == "record-only" else live_profile
    target_paths = {
        "env": paths["env_target"],
        "binding": paths["binding_target"],
        "manifest": paths["manifest_target"],
        "local.pnc.completion-notice-relay.plist": paths["relay_target"],
        "local.pnc.rca-delivery-dispatcher.plist": paths["dispatcher_target"],
    }
    for name, target in target_paths.items():
        target_raw, _target_obs = base._read_file(
            target,
            code="pnc_rca_live_profile_switch_current_profile_invalid",
            required_mode=TARGET_MODES[name],
        )
        expected_raw = base._read_file(
            Path(expected_profile[name]["path"]),
            code="pnc_rca_live_profile_switch_candidate_invalid",
            required_mode=0o600,
        )[0]
        if target_raw != expected_raw:
            _fail("pnc_rca_live_profile_switch_current_profile_invalid")

    return {
        "release_id": release_id,
        "authority_sha256": authority_sha,
        "authority_epoch_id": authority_epoch_id,
        "provenance": provenance,
        "paths": paths,
        "mode_switch_profile": _artifact_binding(paths["profile"], profile_obs),
        "authority": _artifact_binding(paths["authority"], authority_obs),
        "initial_profile": initial_profile,
        "live_profile": live_profile,
        "read_only_plist_anchors": anchor_observations,
        "activation_binding": activation_binding,
        "quarantine_baseline": baseline_binding,
        "target_paths": target_paths,
    }


def build_plan(
    *,
    candidate_root: Path,
    source_root: Path,
    home: Path,
    hermes_home: Path,
    control_db: Path,
    transaction_id: str | None = None,
) -> tuple[dict[str, Any], Path]:
    validated = _validate_release_state(
        candidate_root=candidate_root,
        source_root=source_root,
        home=home,
        hermes_home=hermes_home,
        control_db=control_db,
        installed_mode="record-only",
    )
    selected_id = transaction_id or (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        + "-"
        + uuid.uuid4().hex[:12]
    )
    if IDENTIFIER_RE.fullmatch(selected_id) is None:
        _fail("pnc_rca_live_profile_switch_transaction_id_invalid")
    transaction_dir = candidate_root.parent / "mode-switch" / selected_id
    rollback_dir = transaction_dir / "rollback"
    staged_dir = transaction_dir / "staged"
    base._directory(transaction_dir, create=True)
    base._directory(rollback_dir, create=True)
    base._directory(staged_dir, create=True)
    entries: list[dict[str, Any]] = []
    for index, name in enumerate(TARGET_NAMES):
        source = Path(validated["live_profile"][name]["path"])
        raw, source_obs = base._read_file(
            source,
            code="pnc_rca_live_profile_switch_candidate_invalid",
            required_mode=0o600,
        )
        staged = staged_dir / f"{index:02d}-{name}.blob"
        base._write_new(staged, raw, mode=TARGET_MODES[name])
        staged_observation = base._observe(staged, required=True)
        entries.append(
            {
                "name": name,
                "source_path": str(source),
                "source": {"exists": True, **source_obs},
                "staged_path": str(staged),
                "staged": staged_observation,
                "target_path": str(validated["target_paths"][name]),
                "target_mode": format(TARGET_MODES[name], "04o"),
                "before": base._observe(validated["target_paths"][name], required=True),
                "rollback_path": str(rollback_dir / f"{index:02d}-{name}.before"),
            }
        )
    state_root = hermes_home / "runtime" / "pnc_agent" / "feishu_issue_kafka_rca"
    plan = {
        "schema_version": SCHEMA_VERSION,
        "cli_schema_version": CLI_SCHEMA_VERSION,
        "transaction_id": selected_id,
        "planned_at": datetime.now(timezone.utc).isoformat(),
        "release_id": validated["release_id"],
        "authority_sha256": validated["authority_sha256"],
        "authority_epoch_id": validated["authority_epoch_id"],
        "candidate_root": str(candidate_root),
        "source_root": str(source_root),
        "source_commit": validated["provenance"]["commit"],
        "source_tree": validated["provenance"]["tree"],
        "home": str(home),
        "hermes_home": str(hermes_home),
        "control_db": str(control_db),
        "state_root": str(state_root),
        "transaction_dir": str(transaction_dir),
        "rollback_dir": str(rollback_dir),
        "mode_switch_profile": validated["mode_switch_profile"],
        "authority": validated["authority"],
        "initial_profile": validated["initial_profile"],
        "live_profile": validated["live_profile"],
        "read_only_plist_anchors": validated["read_only_plist_anchors"],
        "activation_binding": validated["activation_binding"],
        "quarantine_baseline": validated["quarantine_baseline"],
        "entries": entries,
        "mutation_performed": False,
        "production_effects": _effects(),
    }
    _validate_plan(plan)
    plan_path = transaction_dir / "plan.json"
    base._write_new(plan_path, base._pretty(plan), mode=0o600)
    return plan, plan_path


def _valid_observation(value: Any, *, required: bool | None = None) -> bool:
    if not isinstance(value, Mapping) or set(value) != OBSERVATION_FIELDS:
        return False
    exists = value.get("exists")
    if required is not None and exists is not required:
        return False
    if exists is False:
        return all(
            value.get(key) is None
            for key in ("sha256", "mode", "device", "inode", "mtime_ns", "ctime_ns")
        ) and value.get("size_bytes") == 0
    return (
        exists is True
        and HEX64_RE.fullmatch(str(value.get("sha256") or "")) is not None
        and re.fullmatch(r"0[0-7]{3}", str(value.get("mode") or "")) is not None
        and all(
            isinstance(value.get(key), int) and not isinstance(value.get(key), bool)
            for key in ("size_bytes", "device", "inode", "mtime_ns", "ctime_ns")
        )
    )


def _valid_artifact_binding(value: Any, *, expected_path: Path) -> bool:
    return (
        isinstance(value, Mapping)
        and set(value) == ARTIFACT_BINDING_FIELDS
        and Path(str(value.get("path") or "")) == expected_path
        and _valid_observation(value.get("observation"), required=True)
    )


def _validate_plan(plan: Mapping[str, Any]) -> None:
    activation = plan.get("activation_binding")
    if (
        set(plan) != PLAN_FIELDS
        or plan.get("schema_version") != SCHEMA_VERSION
        or plan.get("cli_schema_version") != CLI_SCHEMA_VERSION
        or plan.get("mutation_performed") is not False
        or plan.get("production_effects") != _effects()
        or IDENTIFIER_RE.fullmatch(str(plan.get("transaction_id") or "")) is None
        or IDENTIFIER_RE.fullmatch(str(plan.get("release_id") or "")) is None
        or IDENTIFIER_RE.fullmatch(str(plan.get("authority_epoch_id") or "")) is None
        or HEX64_RE.fullmatch(str(plan.get("authority_sha256") or "")) is None
        or re.fullmatch(r"[0-9a-f]{40,64}", str(plan.get("source_commit") or "")) is None
        or re.fullmatch(r"[0-9a-f]{40,64}", str(plan.get("source_tree") or "")) is None
        or not isinstance(plan.get("planned_at"), str)
        or not isinstance(activation, Mapping)
        or set(activation) != ACTIVATION_BINDING_FIELDS
        or activation.get("state") != "bounded_active"
        or IDENTIFIER_RE.fullmatch(str(activation.get("epoch_id") or "")) is None
        or not isinstance(activation.get("updated_at"), str)
        or not activation.get("updated_at")
    ):
        _fail("pnc_rca_live_profile_switch_plan_invalid")
    candidate_root = Path(str(plan["candidate_root"]))
    source_root = Path(str(plan["source_root"]))
    home = Path(str(plan["home"]))
    hermes_home = Path(str(plan["hermes_home"]))
    control_db = Path(str(plan["control_db"]))
    state_root = Path(str(plan["state_root"]))
    transaction_dir = Path(str(plan["transaction_dir"]))
    rollback_dir = Path(str(plan["rollback_dir"]))
    for path in (
        candidate_root,
        source_root,
        home,
        hermes_home,
        control_db,
        state_root,
        transaction_dir,
        rollback_dir,
    ):
        if not path.is_absolute() or path.absolute() != path:
            _fail("pnc_rca_live_profile_switch_plan_invalid")
    if (
        state_root
        != hermes_home / "runtime" / "pnc_agent" / "feishu_issue_kafka_rca"
        or transaction_dir
        != candidate_root.parent / "mode-switch" / str(plan["transaction_id"])
        or rollback_dir != transaction_dir / "rollback"
    ):
        _fail("pnc_rca_live_profile_switch_plan_invalid")
    if not _valid_artifact_binding(
        plan.get("mode_switch_profile"), expected_path=candidate_root / "mode-switch-profile.json"
    ) or not _valid_artifact_binding(
        plan.get("authority"), expected_path=candidate_root / "authority.json"
    ):
        _fail("pnc_rca_live_profile_switch_plan_invalid")

    profile_paths = {
        "initial_profile": {
            "env": candidate_root / "candidate.env",
            "binding": candidate_root / "active-release-binding.json",
            "manifest": candidate_root / "LIVE_MANIFEST.json",
            "local.pnc.completion-notice-relay.plist": candidate_root
            / "local.pnc.completion-notice-relay.plist",
            "local.pnc.rca-delivery-dispatcher.plist": candidate_root
            / "local.pnc.rca-delivery-dispatcher.plist",
        },
        "live_profile": {
            "env": candidate_root / "live-profile" / "candidate.env",
            "binding": candidate_root / "live-profile" / "active-release-binding.json",
            "manifest": candidate_root / "live-profile" / "LIVE_MANIFEST.json",
            "local.pnc.completion-notice-relay.plist": candidate_root
            / "live-profile"
            / "local.pnc.completion-notice-relay.plist",
            "local.pnc.rca-delivery-dispatcher.plist": candidate_root
            / "live-profile"
            / "local.pnc.rca-delivery-dispatcher.plist",
        },
    }
    for profile_name, expected_paths in profile_paths.items():
        profile = plan.get(profile_name)
        if not isinstance(profile, Mapping) or set(profile) != set(TARGET_NAMES):
            _fail("pnc_rca_live_profile_switch_plan_invalid")
        for name in TARGET_NAMES:
            if not _valid_artifact_binding(
                profile.get(name), expected_path=expected_paths[name]
            ):
                _fail("pnc_rca_live_profile_switch_plan_invalid")

    baseline = plan.get("quarantine_baseline")
    if (
        not isinstance(baseline, Mapping)
        or set(baseline) != BASELINE_BINDING_FIELDS
        or not Path(str(baseline.get("path") or "")).is_absolute()
        or not _valid_observation(baseline.get("observation"), required=True)
        or HEX64_RE.fullmatch(str(baseline.get("baseline_fingerprint") or "")) is None
        or HEX64_RE.fullmatch(str(baseline.get("status_sha256") or "")) is None
        or HEX64_RE.fullmatch(
            str(baseline.get("db_logical_identity_sha256") or "")
        )
        is None
        or not isinstance(baseline.get("baseline_id"), str)
        or not baseline.get("baseline_id")
    ):
        _fail("pnc_rca_live_profile_switch_plan_invalid")
    anchors = plan.get("read_only_plist_anchors")
    expected_anchor_paths = _anchor_paths(
        home=home, hermes_home=hermes_home, release_id=str(plan["release_id"])
    )
    if not isinstance(anchors, Mapping) or set(anchors) != set(ANCHOR_NAMES):
        _fail("pnc_rca_live_profile_switch_plan_invalid")
    for name in ANCHOR_NAMES:
        item = anchors.get(name)
        if (
            not isinstance(item, Mapping)
            or set(item) != {"path", "sha256", "observation"}
            or Path(str(item.get("path") or "")) != expected_anchor_paths[name]
            or HEX64_RE.fullmatch(str(item.get("sha256") or "")) is None
            or not _valid_observation(item.get("observation"), required=True)
            or item["observation"].get("sha256") != item.get("sha256")
        ):
            _fail("pnc_rca_live_profile_switch_plan_invalid")

    target_paths = {
        "env": hermes_home / ".env",
        "binding": state_root / "active-release-binding.json",
        "manifest": hermes_home / "runtime" / "LIVE_MANIFEST.json",
        "local.pnc.completion-notice-relay.plist": home
        / "Library"
        / "LaunchAgents"
        / "local.pnc.completion-notice-relay.plist",
        "local.pnc.rca-delivery-dispatcher.plist": home
        / "Library"
        / "LaunchAgents"
        / "local.pnc.rca-delivery-dispatcher.plist",
    }
    entries = plan.get("entries")
    if (
        not isinstance(entries, list)
        or len(entries) != len(TARGET_NAMES)
        or [entry.get("name") for entry in entries if isinstance(entry, Mapping)]
        != list(TARGET_NAMES)
    ):
        _fail("pnc_rca_live_profile_switch_plan_invalid")
    for index, entry in enumerate(entries):
        name = TARGET_NAMES[index]
        live_item = plan["live_profile"][name]
        if (
            not isinstance(entry, Mapping)
            or set(entry) != ENTRY_FIELDS
            or entry.get("name") != name
            or Path(str(entry.get("source_path") or "")) != Path(live_item["path"])
            or entry.get("source") != live_item["observation"]
            or Path(str(entry.get("staged_path") or ""))
            != transaction_dir / "staged" / f"{index:02d}-{name}.blob"
            or not _valid_observation(entry.get("staged"), required=True)
            or entry["staged"].get("sha256") != entry["source"].get("sha256")
            or entry["staged"].get("mode") != format(TARGET_MODES[name], "04o")
            or Path(str(entry.get("target_path") or "")) != target_paths[name]
            or entry.get("target_mode") != format(TARGET_MODES[name], "04o")
            or not _valid_observation(entry.get("before"), required=True)
            or Path(str(entry.get("rollback_path") or ""))
            != rollback_dir / f"{index:02d}-{name}.before"
        ):
            _fail("pnc_rca_live_profile_switch_plan_invalid")


def _read_plan(path: Path) -> tuple[bytes, dict[str, Any]]:
    raw, _observation = base._read_file(
        path,
        code="pnc_rca_live_profile_switch_plan_invalid",
        required_mode=0o600,
    )
    value = base._json(raw, code="pnc_rca_live_profile_switch_plan_invalid")
    if raw != base._pretty(value):
        _fail("pnc_rca_live_profile_switch_plan_invalid")
    _validate_plan(value)
    if path != Path(value["transaction_dir"]) / "plan.json":
        _fail("pnc_rca_live_profile_switch_plan_invalid")
    return raw, value


def _locked_validation(plan: Mapping[str, Any], *, installed_mode: str) -> None:
    validated = _validate_release_state(
        candidate_root=Path(plan["candidate_root"]),
        source_root=Path(plan["source_root"]),
        home=Path(plan["home"]),
        hermes_home=Path(plan["hermes_home"]),
        control_db=Path(plan["control_db"]),
        installed_mode=installed_mode,
    )
    expected = {
        "release_id": plan["release_id"],
        "authority_sha256": plan["authority_sha256"],
        "authority_epoch_id": plan["authority_epoch_id"],
        "provenance": {
            "commit": plan["source_commit"],
            "tree": plan["source_tree"],
        },
        "mode_switch_profile": plan["mode_switch_profile"],
        "authority": plan["authority"],
        "initial_profile": plan["initial_profile"],
        "live_profile": plan["live_profile"],
        "read_only_plist_anchors": plan["read_only_plist_anchors"],
        "activation_binding": plan["activation_binding"],
        "quarantine_baseline": plan["quarantine_baseline"],
    }
    observed = {key: validated[key] for key in expected}
    if observed != expected:
        _fail("pnc_rca_live_profile_switch_validation_changed")


def _acquire_lock(state_root: Path, transaction_id: str) -> tuple[Path, int]:
    lock = state_root / LOCK_NAME
    fd = -1
    try:
        fd = os.open(
            lock,
            os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_CLOEXEC", 0),
            0o600,
        )
        os.write(fd, transaction_id.encode("ascii"))
        os.fsync(fd)
        return lock, fd
    except FileExistsError as exc:
        _fail("pnc_rca_live_profile_switch_lock_held", exc)
    except OSError as exc:
        if fd >= 0:
            os.close(fd)
            lock.unlink(missing_ok=True)
        _fail("pnc_rca_live_profile_switch_lock_invalid", exc)


def _release_lock(lock: Path, fd: int) -> None:
    opened = os.fstat(fd)
    try:
        current = lock.lstat()
        if (current.st_dev, current.st_ino) != (opened.st_dev, opened.st_ino):
            _fail("pnc_rca_live_profile_switch_lock_changed")
        lock.unlink()
    finally:
        os.close(fd)


def _restore_written_no_clobber(
    plan: Mapping[str, Any],
    *,
    written_names: Sequence[str],
    after_observations: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    entries = {str(entry["name"]): entry for entry in plan["entries"]}
    restored: list[dict[str, Any]] = []
    blocked: list[dict[str, Any]] = []
    restore_order = [name for name in reversed(tuple(written_names)) if name != "manifest"]
    if "manifest" in written_names:
        restore_order.append("manifest")
    for name in restore_order:
        entry = entries[name]
        target = Path(entry["target_path"])
        try:
            current = base._observe(target, required=False)
        except base.ReleaseTransactionError as exc:
            blocked.append(
                {
                    "name": name,
                    "reason": "target_unobservable",
                    "error": exc.code,
                }
            )
            continue
        expected_after = after_observations.get(name)
        if expected_after is None and base._same_observation(current, entry["before"]):
            restored.append({"name": name, "observed": current, "action": "not_written"})
            continue
        if expected_after is None:
            blocked.append(
                {
                    "name": name,
                    "reason": "after_identity_unknown",
                    "observed": current,
                }
            )
            continue
        owned = base._same_observation(current, expected_after)
        if not owned:
            blocked.append({"name": name, "reason": "target_changed", "observed": current})
            continue
        try:
            rollback_raw, rollback_observation = base._read_file(
                Path(entry["rollback_path"]),
                code="pnc_rca_live_profile_switch_rollback_invalid",
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
        before = entry["before"]
        if (
            rollback_observation["sha256"] != before["sha256"]
            or rollback_observation["mode"] != before["mode"]
        ):
            blocked.append(
                {"name": name, "reason": "rollback_blob_changed", "observed": rollback_observation}
            )
            continue
        try:
            base._replace(target, rollback_raw, mode=int(before["mode"], 8))
            restored_observation = base._observe(target, required=True)
        except base.ReleaseTransactionError as exc:
            blocked.append(
                {
                    "name": name,
                    "reason": "restore_failed",
                    "error": exc.code,
                }
            )
            continue
        if (
            restored_observation["sha256"] != before["sha256"]
            or restored_observation["mode"] != before["mode"]
        ):
            blocked.append(
                {"name": name, "reason": "restore_verification_failed", "observed": restored_observation}
            )
            continue
        restored.append({"name": name, "observed": restored_observation, "action": "restored"})
    return {"restored": restored, "blocked": blocked}


def _apply(plan: Mapping[str, Any], *, plan_path: Path) -> dict[str, Any]:
    _validate_plan(plan)
    plan_raw, on_disk_plan = _read_plan(plan_path)
    if dict(plan) != on_disk_plan:
        _fail("pnc_rca_live_profile_switch_plan_changed")
    plan = on_disk_plan
    state_root = base._directory(Path(plan["state_root"]))
    lock, fd = _acquire_lock(state_root, str(plan["transaction_id"]))
    attempted_names: list[str] = []
    after_by_name: dict[str, Mapping[str, Any]] = {}
    try:
        locked_plan_raw, locked_plan = _read_plan(plan_path)
        if locked_plan_raw != plan_raw or locked_plan != plan:
            _fail("pnc_rca_live_profile_switch_plan_changed")
        _locked_validation(plan, installed_mode="record-only")
        for entry in plan["entries"]:
            if not base._same_observation(
                base._observe(Path(entry["target_path"]), required=True), entry["before"]
            ):
                _fail("pnc_rca_live_profile_switch_target_changed")
            _raw, staged_observation = base._read_file(
                Path(entry["staged_path"]),
                code="pnc_rca_live_profile_switch_staged_invalid",
            )
            staged_observation = {"exists": True, **staged_observation}
            if not base._same_observation(staged_observation, entry["staged"]):
                _fail("pnc_rca_live_profile_switch_staged_changed")
        base._backup(plan)
        for entry in plan["entries"]:
            if not base._same_observation(
                base._observe(Path(entry["target_path"]), required=True), entry["before"]
            ):
                _fail("pnc_rca_live_profile_switch_target_changed")
        ordered = sorted(plan["entries"], key=lambda item: item["name"] == "manifest")
        for entry in ordered:
            if not base._same_observation(
                base._observe(Path(entry["target_path"]), required=True), entry["before"]
            ):
                _fail("pnc_rca_live_profile_switch_target_changed")
            raw, staged_observation = base._read_file(
                Path(entry["staged_path"]),
                code="pnc_rca_live_profile_switch_staged_invalid",
            )
            if (
                staged_observation["sha256"] != entry["source"]["sha256"]
                or staged_observation["mode"] != entry["target_mode"]
            ):
                _fail("pnc_rca_live_profile_switch_staged_changed")
            target = Path(entry["target_path"])
            attempted_names.append(str(entry["name"]))
            base._replace(target, raw, mode=int(entry["target_mode"], 8))
            after = base._observe(target, required=True)
            if (
                after["sha256"] != entry["source"]["sha256"]
                or after["mode"] != entry["target_mode"]
            ):
                _fail("pnc_rca_live_profile_switch_verify_failed")
            after_by_name[str(entry["name"])] = after
        _locked_validation(plan, installed_mode="live")
        after_entries = [
            {"name": entry["name"], "observed": dict(after_by_name[entry["name"]])}
            for entry in plan["entries"]
        ]
        receipt = {
            "schema_version": RECEIPT_SCHEMA_VERSION,
            "transaction_id": plan["transaction_id"],
            "release_id": plan["release_id"],
            "authority_sha256": plan["authority_sha256"],
            "authority_epoch_id": plan["authority_epoch_id"],
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "activation_binding": dict(plan["activation_binding"]),
            "quarantine_baseline": dict(plan["quarantine_baseline"]),
            "plan_path": str(plan_path),
            "plan_raw_sha256": _sha(plan_raw),
            "entries": after_entries,
            "mutation_performed": True,
            "rollback_performed": False,
            "production_effects": _effects(),
            "verification": "pass",
        }
        receipt_path = Path(plan["transaction_dir"]) / "receipt.json"
        base._write_new(receipt_path, base._pretty(receipt), mode=0o600)
        receipt["receipt_path"] = str(receipt_path)
        receipt["receipt_raw_sha256"] = _sha(
            base._read_file(
                receipt_path,
                code="pnc_rca_live_profile_switch_receipt_invalid",
                required_mode=0o600,
            )[0]
        )
        return receipt
    except BaseException as original:
        if attempted_names:
            rollback_result = _restore_written_no_clobber(
                plan,
                written_names=attempted_names,
                after_observations=after_by_name,
            )
            automatic = {
                "schema_version": ROLLBACK_SCHEMA_VERSION,
                "transaction_id": plan["transaction_id"],
                "rolled_back_at": datetime.now(timezone.utc).isoformat(),
                "filesystem_restored_to_pre_transaction": not rollback_result["blocked"],
                "runtime_state": "unchanged_by_transaction",
                "restored_entries": rollback_result["restored"],
                "blocked_entries": rollback_result["blocked"],
                "original_error": getattr(
                    original, "code", "pnc_rca_live_profile_switch_apply_failed"
                ),
                "production_effects": _effects(),
            }
            base._write_new(
                Path(plan["transaction_dir"]) / "automatic-rollback.json",
                base._pretty(automatic),
                mode=0o600,
            )
            if rollback_result["blocked"]:
                _fail("pnc_rca_live_profile_switch_automatic_rollback_incomplete", original)
        raise
    finally:
        _release_lock(lock, fd)


def _read_receipt(path: Path) -> tuple[bytes, dict[str, Any]]:
    raw, _observation = base._read_file(
        path,
        code="pnc_rca_live_profile_switch_receipt_invalid",
        required_mode=0o600,
    )
    receipt = base._json(raw, code="pnc_rca_live_profile_switch_receipt_invalid")
    if (
        raw != base._pretty(receipt)
        or set(receipt) != RECEIPT_FIELDS
        or receipt.get("schema_version") != RECEIPT_SCHEMA_VERSION
        or receipt.get("mutation_performed") is not True
        or receipt.get("rollback_performed") is not False
        or receipt.get("verification") != "pass"
        or receipt.get("production_effects") != _effects()
        or HEX64_RE.fullmatch(str(receipt.get("plan_raw_sha256") or "")) is None
    ):
        _fail("pnc_rca_live_profile_switch_receipt_invalid")
    return raw, receipt


def rollback(receipt_path: Path, *, output_path: Path) -> dict[str, Any]:
    _receipt_raw, receipt = _read_receipt(receipt_path)
    plan_path = Path(str(receipt["plan_path"]))
    plan_raw, plan = _read_plan(plan_path)
    if (
        _sha(plan_raw) != receipt.get("plan_raw_sha256")
        or receipt.get("transaction_id") != plan["transaction_id"]
        or receipt.get("release_id") != plan["release_id"]
        or receipt.get("authority_sha256") != plan["authority_sha256"]
        or receipt.get("authority_epoch_id") != plan["authority_epoch_id"]
        or receipt.get("activation_binding") != plan["activation_binding"]
        or receipt.get("quarantine_baseline") != plan["quarantine_baseline"]
        or receipt_path != Path(plan["transaction_dir"]) / "receipt.json"
        or not output_path.is_absolute()
    ):
        _fail("pnc_rca_live_profile_switch_receipt_binding_invalid")
    receipt_entries = receipt.get("entries")
    if not isinstance(receipt_entries, list) or len(receipt_entries) != len(
        plan["entries"]
    ):
        _fail("pnc_rca_live_profile_switch_receipt_binding_invalid")
    after_by_name: dict[str, Mapping[str, Any]] = {}
    for item, entry in zip(receipt_entries, plan["entries"], strict=True):
        if (
            not isinstance(item, Mapping)
            or set(item) != {"name", "observed"}
            or item.get("name") != entry["name"]
            or not _valid_observation(item.get("observed"), required=True)
        ):
            _fail("pnc_rca_live_profile_switch_receipt_binding_invalid")
        after_by_name[str(entry["name"])] = item["observed"]
    state_root = base._directory(Path(plan["state_root"]))
    lock, fd = _acquire_lock(state_root, str(plan["transaction_id"]))
    try:
        current_receipt_raw, current_receipt = _read_receipt(receipt_path)
        current_plan_raw, current_plan = _read_plan(plan_path)
        if (
            current_receipt != receipt
            or current_plan != plan
            or current_plan_raw != plan_raw
            or _sha(current_plan_raw) != current_receipt["plan_raw_sha256"]
        ):
            _fail("pnc_rca_live_profile_switch_receipt_binding_invalid")
        _locked_validation(plan, installed_mode="live")
        for entry in plan["entries"]:
            if not base._same_observation(
                base._observe(Path(entry["target_path"]), required=True),
                after_by_name[str(entry["name"])],
            ):
                _fail("pnc_rca_live_profile_switch_rollback_target_changed")
        result_detail = _restore_written_no_clobber(
            plan,
            written_names=[str(entry["name"]) for entry in plan["entries"]],
            after_observations=after_by_name,
        )
        result = {
            "schema_version": ROLLBACK_SCHEMA_VERSION,
            "transaction_id": plan["transaction_id"],
            "rolled_back_at": datetime.now(timezone.utc).isoformat(),
            "filesystem_restored_to_pre_transaction": not result_detail["blocked"],
            "overall_release_state_restored": False,
            "runtime_state": "unverified_requires_operator_quiesce_reload_and_readback",
            "restored_entries": result_detail["restored"],
            "blocked_entries": result_detail["blocked"],
            "production_effects": _effects(),
        }
        base._write_new(output_path, base._pretty(result), mode=0o600)
        if result_detail["blocked"]:
            _fail("pnc_rca_live_profile_switch_manual_rollback_incomplete")
        return result
    finally:
        _release_lock(lock, fd)


def _args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    plan = commands.add_parser("plan")
    plan.add_argument("--candidate-root", type=Path, required=True)
    plan.add_argument("--source-root", type=Path, required=True)
    plan.add_argument("--home", type=Path, default=Path.home())
    plan.add_argument("--hermes-home", type=Path, default=Path.home() / ".hermes")
    plan.add_argument("--control-db", type=Path, required=True)
    plan.add_argument("--transaction-id")
    apply = commands.add_parser("apply")
    apply.add_argument("--plan", type=Path, required=True)
    rollback_cmd = commands.add_parser("rollback")
    rollback_cmd.add_argument("--receipt", type=Path, required=True)
    rollback_cmd.add_argument("--output", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    try:
        args = _args(argv)
        if args.command == "plan":
            plan, path = build_plan(
                candidate_root=args.candidate_root.expanduser().absolute(),
                source_root=args.source_root.expanduser().absolute(),
                home=args.home.expanduser().absolute(),
                hermes_home=args.hermes_home.expanduser().absolute(),
                control_db=args.control_db.expanduser().absolute(),
                transaction_id=args.transaction_id,
            )
            print(json.dumps({"ok": True, "plan_path": str(path), **plan}, sort_keys=True))
            return 0
        if args.command == "apply":
            _plan_raw, plan = _read_plan(args.plan.expanduser().absolute())
            print(json.dumps({"ok": True, **_apply(plan, plan_path=args.plan.expanduser().absolute())}, sort_keys=True))
            return 0
        print(json.dumps({"ok": True, **rollback(args.receipt.expanduser().absolute(), output_path=args.output.expanduser().absolute())}, sort_keys=True))
        return 0
    except (LiveProfileSwitchError, base.ReleaseTransactionError, OSError, ValueError) as exc:
        print(json.dumps({"ok": False, "code": getattr(exc, "code", "pnc_rca_live_profile_switch_invalid")}, sort_keys=True))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
