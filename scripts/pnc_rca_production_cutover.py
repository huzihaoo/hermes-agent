#!/usr/bin/env python3
"""Plan, validate, or execute an exact RCA production cutover.

The command line intentionally exposes only read-only plan/validate modes.
Programmatic apply requires an explicit active cutover lease, a production gate
validator, and a system adapter.  This module contains no default live adapter,
no shell-string runner, and no direct canonical-path mutation primitive.
"""

from __future__ import annotations

import argparse
import contextlib
import fcntl
import hashlib
import json
import os
import re
import secrets
import socket
import stat
import sys
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Protocol


PLAN_SCHEMA_VERSION = "pnc_rca_production_cutover_plan_v1"
GATE_VALIDATION_SCHEMA_VERSION = "pnc_rca_production_cutover_gate_validation_v1"
AUTHORIZATION_SCHEMA_VERSION = "pnc_rca_production_cutover_authorization_v1"
AUTHORIZATION_IDENTITY_SCHEMA_VERSION = (
    "pnc_rca_production_cutover_authorization_identity_v1"
)
JOURNAL_IDENTITY_SCHEMA_VERSION = "pnc_rca_production_cutover_journal_identity_v1"
STEP_INTENT_SCHEMA_VERSION = "pnc_rca_production_cutover_step_intent_v1"
STEP_RESULT_SCHEMA_VERSION = "pnc_rca_production_cutover_step_result_v1"
SNAPSHOT_SCHEMA_VERSION = "pnc_rca_production_cutover_snapshot_v1"
FAILURE_SCHEMA_VERSION = "pnc_rca_production_cutover_failure_v1"
ROLLBACK_SCHEMA_VERSION = "pnc_rca_production_cutover_rollback_v1"
COMPLETE_SCHEMA_VERSION = "pnc_rca_production_cutover_complete_v1"
COMMAND_PREFLIGHT_SCHEMA_VERSION = "pnc_rca_production_cutover_command_preflight_v1"
PAYLOAD_DESCRIPTOR_SCHEMA_VERSION = "pnc_rca_production_cutover_payload_descriptor_v1"
ROLLBACK_INTENT_SCHEMA_VERSION = "pnc_rca_production_cutover_rollback_intent_v1"
NONCE_CONSUMPTION_SCHEMA_VERSION = "pnc_rca_production_cutover_nonce_consumption_v1"
MACHINE_IDENTITY_SCHEMA_VERSION = "pnc_rca_production_cutover_machine_identity_v1"
RECOVERY_AUTHORIZATION_SCHEMA_VERSION = "pnc_rca_production_recovery_authorization_v1"
RECOVERY_INTENT_SCHEMA_VERSION = "pnc_rca_production_recovery_intent_v1"
RECOVERY_DONE_SCHEMA_VERSION = "pnc_rca_production_recovery_done_v1"

RELEASE_PREPARE_SCHEMA_VERSION = "pnc_rca_release_prepare_manifest_v1"
RELEASE_APPROVAL_SCHEMA_VERSION = "pnc_rca_release_approval_v1"
WRITER_STOP_SCHEMA_VERSION = "pnc_rca_gateway_writer_stop_receipt_v1"
FEISHU_HOLD_SCHEMA_VERSION = "pnc_rca_feishu_ingress_hold_apply_receipt_v1"
FEISHU_HOLD_PLAN_SCHEMA_VERSION = "pnc_rca_feishu_ingress_hold_plan_v1"
FEISHU_HOLD_APPROVAL_SCHEMA_VERSION = "pnc_rca_feishu_ingress_hold_approval_v1"
FEISHU_HOLD_CUTOVER_SCHEMA_VERSION = "pnc_rca_feishu_ingress_hold_cutover_v2"
ENV_STAGE_SCHEMA_VERSION = "pnc_rca_production_env_stage_receipt_v1"
RUNTIME_STAGE_SCHEMA_VERSION = "pnc_rca_runtime_stage_manifest_v1"
WORKSPACE_RUNTIME_SCHEMA_VERSION = "pnc_rca_workspace_runtime_bundle_v1"

AUTHORIZATION_DECISION = "authorize_exact_rca_production_cutover"
RECOVERY_AUTHORIZATION_DECISION = "authorize_exact_rca_production_recovery"
MAX_JSON_BYTES = 8 * 1024 * 1024
MAX_AUTHORIZATION_VALIDITY_SECONDS = 2 * 60 * 60
MAX_RECOVERY_AUTHORIZATION_VALIDITY_SECONDS = 30 * 60
MAX_FUTURE_SKEW_SECONDS = 300
SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
RELEASE_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{7,127}\Z")
NONCE_RE = re.compile(r"[A-Za-z0-9_-]{16,128}\Z")

CANONICAL_RUNTIME_ROOT = Path("/Users/songying/.hermes/runtime/hermes-live")
CANONICAL_WORKSPACE_ROOT = Path("/Users/songying/.hermes/runtime/rca-workspace-runtime")
CANONICAL_ENV_PATH = Path("/Users/songying/.hermes/.env")
CANONICAL_LAUNCH_AGENTS_ROOT = Path("/Users/songying/Library/LaunchAgents")
CANONICAL_NONCE_LEDGER_ROOT = Path(
    "/Users/songying/.hermes/runtime/rca-cutover-authorization-nonces"
)
ACTIVE_RELEASE_BINDING_NAME = "active-release-binding.json"
CUTOVER_ADAPTER_EXECUTABLE = "/Users/songying/.local/bin/pnc-rca-cutover-adapter"

SERVICE_LABELS = (
    "ai.hermes.gateway",
    "local.pnc.completion-notice-relay",
    "local.pnc.vm-task-sync",
    "local.pnc.rca-kafka-consumer",
    "local.pnc.rca-outbox-dispatcher",
    "local.pnc.rca-delivery-collector",
    "local.pnc.rca-delivery-dispatcher",
)
WRITER_LABELS = SERVICE_LABELS
GATEWAY_AUX_LABELS = SERVICE_LABELS[:3]
RESIDENT_LABELS = SERVICE_LABELS[3:]
CANDIDATE_PLISTS = (
    "local.pnc.completion-notice-relay.candidate.plist",
    "local.pnc.vm-task-sync.candidate.plist",
    "local.pnc.rca-kafka-consumer.candidate.plist",
    "local.pnc.rca-outbox-dispatcher.candidate.plist",
    "local.pnc.rca-delivery-collector.candidate.plist",
    "local.pnc.rca-delivery-dispatcher.candidate.plist",
)

STEP_NAMES = (
    "snapshot_live",
    "stop_writers",
    "install_feishu_sidecar",
    "install_runtime",
    "install_workspace",
    "install_environment",
    "install_plists",
    "start_gateway_aux",
    "verify_gateway_aux",
    "transition_bounded_activation",
    "start_residents",
    "verify_services",
)
MUTATING_STEPS = frozenset({
    "stop_writers",
    "install_feishu_sidecar",
    "install_runtime",
    "install_workspace",
    "install_environment",
    "install_plists",
    "start_gateway_aux",
    "transition_bounded_activation",
    "start_residents",
})
CUTOVER_ACTION_SET = (
    "acquire_global_cutover_lease",
    "revalidate_exact_release_bindings",
    "snapshot_live_runtime_environment_plists",
    "stop_all_bound_writers",
    "install_exact_feishu_hold_sidecar",
    "install_complete_runtime_without_deleting_old_runtime",
    "install_workspace_runtime",
    "install_candidate_environment",
    "install_complete_candidate_plist_set",
    "bootstrap_or_kickstart_gateway_and_aux_services",
    "verify_gateway_and_aux_services",
    "transition_exact_activation_to_bounded_active",
    "bootstrap_residents_in_gate_authorized_order",
    "verify_each_service_pid_runtime_and_health",
    "rollback_exact_snapshot_on_failure",
)

ARTIFACT_FIELDS = (
    "release_prepare_manifest",
    "approval_receipt",
    "writer_stop_receipt",
    "feishu_hold_plan",
    "feishu_hold_approval_receipt",
    "feishu_hold_cutover_binding",
    "feishu_hold_receipt",
    "env_stage_receipt",
    "runtime_stage_manifest",
    "workspace_runtime_manifest",
    "cutover_authorization_receipt",
)


class ProductionCutoverError(ValueError):
    """A production cutover invariant failed closed."""

    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


class CutoverCrash(BaseException):
    """Testable hard-crash boundary; ordinary apply does not catch this type."""


@dataclass(frozen=True)
class CutoverInputs:
    release_prepare_manifest: Path
    approval_receipt: Path
    writer_stop_receipt: Path
    feishu_hold_plan: Path
    feishu_hold_approval_receipt: Path
    feishu_hold_cutover_binding: Path
    feishu_hold_receipt: Path
    env_stage_receipt: Path
    runtime_stage_manifest: Path
    workspace_runtime_manifest: Path
    cutover_authorization_receipt: Path
    cutover_lease_fingerprint: str
    journal_root: Path


@dataclass(frozen=True)
class _OwnedJson:
    path: Path
    raw: bytes
    body: Mapping[str, Any]

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.raw).hexdigest()


@dataclass(frozen=True)
class _StablePayloadFile:
    path: Path
    raw: bytes
    identity: Mapping[str, int]

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.raw).hexdigest()


@dataclass(frozen=True)
class ArtifactBundle:
    artifacts: Mapping[str, _OwnedJson]

    @property
    def bodies(self) -> Mapping[str, Mapping[str, Any]]:
        return {name: artifact.body for name, artifact in self.artifacts.items()}

    @property
    def sha256(self) -> Mapping[str, str]:
        return {name: artifact.sha256 for name, artifact in self.artifacts.items()}


@dataclass(frozen=True)
class CutoverResult:
    phase: str
    body: Mapping[str, Any]
    resumed: bool = False


class CutoverLease(Protocol):
    fingerprint: str
    token: str
    body: Mapping[str, Any]

    def assert_active(self) -> None: ...

    def __enter__(self) -> CutoverLease: ...

    def __exit__(self, *args: object) -> None: ...


class CutoverSystemAdapter(Protocol):
    """All live observations and mutations cross this injected boundary."""

    def observe_live_identity(self) -> Mapping[str, Any]: ...

    def preflight_step(
        self,
        step: str,
        *,
        expected_identity_sha256: str,
        plan: Mapping[str, Any],
        payload_descriptors: Mapping[str, Any],
        lease_fingerprint: str,
        lease_token: str,
    ) -> Mapping[str, Any]: ...

    def execute_step(
        self,
        step: str,
        *,
        expected_identity_sha256: str,
        plan: Mapping[str, Any],
        planned_commands: Sequence[Sequence[str]],
        payload_descriptors: Mapping[str, Any],
        lease_fingerprint: str,
        lease_token: str,
    ) -> Mapping[str, Any]: ...

    def rollback(
        self,
        *,
        snapshot: Mapping[str, Any],
        expected_identity_sha256: str,
        plan: Mapping[str, Any],
        planned_commands: Sequence[Sequence[str]],
        lease_fingerprint: str,
        lease_token: str,
    ) -> Mapping[str, Any]: ...


GateValidator = Callable[..., Mapping[str, Any]]
MachineIdentityProvider = Callable[[], str]


def _canonical_json(value: Any) -> bytes:
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
    except (TypeError, ValueError) as exc:
        raise ProductionCutoverError("production_cutover_json_invalid") from exc


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).rstrip(b"\n")).hexdigest()


def _require_sha256(value: Any, *, code: str) -> str:
    if not isinstance(value, str) or SHA256_RE.fullmatch(value) is None:
        raise ProductionCutoverError(code)
    return value


def _strict_json(raw: bytes, *, artifact: str) -> Mapping[str, Any]:
    def unique(items: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in items:
            if key in result:
                raise ProductionCutoverError(f"{artifact}_duplicate_key")
            result[key] = value
        return result

    try:
        body = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=unique,
            parse_constant=lambda _value: (_ for _ in ()).throw(
                ProductionCutoverError(f"{artifact}_number_invalid")
            ),
        )
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ProductionCutoverError(f"{artifact}_json_invalid") from exc
    if not isinstance(body, dict):
        raise ProductionCutoverError(f"{artifact}_shape_invalid")
    return body


def _read_owned_json(path: Path, *, artifact: str) -> _OwnedJson:
    selected = path.expanduser().absolute()
    if not hasattr(os, "O_NOFOLLOW"):
        raise ProductionCutoverError(f"{artifact}_no_follow_unavailable")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | os.O_NOFOLLOW
    try:
        descriptor = os.open(selected, flags)
    except OSError as exc:
        raise ProductionCutoverError(f"{artifact}_unavailable") from exc
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or stat.S_IMODE(before.st_mode) != 0o600
            or before.st_uid != os.geteuid()
            or before.st_nlink != 1
            or before.st_size < 1
            or before.st_size > MAX_JSON_BYTES
        ):
            raise ProductionCutoverError(f"{artifact}_identity_invalid")
        raw = b""
        while len(raw) <= MAX_JSON_BYTES:
            chunk = os.read(descriptor, min(64 * 1024, MAX_JSON_BYTES + 1 - len(raw)))
            if not chunk:
                break
            raw += chunk
        if len(raw) > MAX_JSON_BYTES:
            raise ProductionCutoverError(f"{artifact}_size_invalid")
        after = os.fstat(descriptor)
        lexical = os.lstat(selected)
        fields = (
            "st_dev",
            "st_ino",
            "st_mode",
            "st_uid",
            "st_nlink",
            "st_size",
            "st_mtime_ns",
            "st_ctime_ns",
        )
        if stat.S_ISLNK(lexical.st_mode) or any(
            getattr(before, field) != getattr(after, field)
            or getattr(before, field) != getattr(lexical, field)
            for field in fields
        ):
            raise ProductionCutoverError(f"{artifact}_unstable")
    except OSError as exc:
        raise ProductionCutoverError(f"{artifact}_unstable") from exc
    finally:
        os.close(descriptor)
    return _OwnedJson(selected, raw, _strict_json(raw, artifact=artifact))


def _stat_fields(value: os.stat_result) -> dict[str, int]:
    return {
        field: int(getattr(value, field))
        for field in (
            "st_dev",
            "st_ino",
            "st_mode",
            "st_uid",
            "st_nlink",
            "st_size",
            "st_mtime_ns",
            "st_ctime_ns",
        )
    }


def _read_stable_payload_file(
    path: Path,
    *,
    artifact: str,
    expected_mode: int,
    max_bytes: int = MAX_JSON_BYTES,
) -> _StablePayloadFile:
    """Hash one candidate from the same descriptor used for identity checks."""
    selected = path.expanduser().absolute()
    if not hasattr(os, "O_NOFOLLOW"):
        raise ProductionCutoverError(f"{artifact}_no_follow_unavailable")
    try:
        descriptor = os.open(
            selected,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | os.O_NOFOLLOW,
        )
    except OSError as exc:
        raise ProductionCutoverError(f"{artifact}_unavailable") from exc
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or stat.S_IMODE(before.st_mode) != expected_mode
            or before.st_uid != os.geteuid()
            or before.st_nlink != 1
            or before.st_size < 0
            or before.st_size > max_bytes
        ):
            raise ProductionCutoverError(f"{artifact}_identity_invalid")
        chunks: list[bytes] = []
        remaining = before.st_size
        while remaining:
            chunk = os.read(descriptor, min(1024 * 1024, remaining))
            if not chunk:
                raise ProductionCutoverError(f"{artifact}_unstable")
            chunks.append(chunk)
            remaining -= len(chunk)
        if os.read(descriptor, 1):
            raise ProductionCutoverError(f"{artifact}_unstable")
        after = os.fstat(descriptor)
        lexical = os.lstat(selected)
        identity = _stat_fields(before)
        if (
            stat.S_ISLNK(lexical.st_mode)
            or identity != _stat_fields(after)
            or identity != _stat_fields(lexical)
        ):
            raise ProductionCutoverError(f"{artifact}_unstable")
    except OSError as exc:
        raise ProductionCutoverError(f"{artifact}_unstable") from exc
    finally:
        os.close(descriptor)
    return _StablePayloadFile(selected, b"".join(chunks), identity)


def _load_artifacts(inputs: CutoverInputs) -> ArtifactBundle:
    return ArtifactBundle({
        field: _read_owned_json(
            getattr(inputs, field), artifact=f"production_cutover_{field}"
        )
        for field in ARTIFACT_FIELDS
    })


def _parse_time(value: Any, *, code: str) -> datetime:
    if not isinstance(value, str) or not value:
        raise ProductionCutoverError(code)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ProductionCutoverError(code) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ProductionCutoverError(code)
    return parsed.astimezone(timezone.utc)


def _default_machine_identity_sha256() -> str:
    """Compute the live host identity without invoking a shell or reading secrets."""
    uname = os.uname()
    body = {
        "schema_version": MACHINE_IDENTITY_SCHEMA_VERSION,
        "uid": os.geteuid(),
        "hostname": socket.gethostname(),
        "node": uname.nodename,
        "system": uname.sysname,
        "release": uname.release,
        "machine": uname.machine,
        "hardware_node": f"{uuid.getnode():012x}",
    }
    return _sha256_json(body)


def _lease_execution_identity(lease: CutoverLease) -> Mapping[str, Any]:
    token = getattr(lease, "token", None)
    fingerprint = getattr(lease, "fingerprint", None)
    body = getattr(lease, "body", None)
    holder = body.get("holder") if isinstance(body, Mapping) else None
    if (
        not isinstance(token, str)
        or len(token) < 16
        or SHA256_RE.fullmatch(str(fingerprint or "")) is None
        or not isinstance(holder, Mapping)
        or isinstance(holder.get("pid"), bool)
        or not isinstance(holder.get("pid"), int)
        or holder["pid"] <= 0
    ):
        raise ProductionCutoverError("production_cutover_lease_identity_invalid")
    return {
        "fingerprint": fingerprint,
        "token_sha256": hashlib.sha256(token.encode("utf-8")).hexdigest(),
        "holder": json.loads(json.dumps(holder)),
        "holder_sha256": _sha256_json(holder),
    }


def _authorization_bindings(
    bundle: ArtifactBundle,
    *,
    lease_fingerprint: str,
) -> Mapping[str, str]:
    prepare = bundle.bodies["release_prepare_manifest"]
    env_stage = bundle.bodies["env_stage_receipt"]
    candidate = env_stage.get("bindings", {}).get("candidate_env")
    if not isinstance(candidate, Mapping):
        raise ProductionCutoverError("production_cutover_env_candidate_binding_missing")
    return {
        "release_prepare_manifest_sha256": bundle.sha256["release_prepare_manifest"],
        "approval_receipt_sha256": bundle.sha256["approval_receipt"],
        "release_bom_sha256": _require_sha256(
            prepare.get("release_bom_sha256"),
            code="production_cutover_release_bom_invalid",
        ),
        "cutover_lease_fingerprint": lease_fingerprint,
        "writer_stop_receipt_sha256": bundle.sha256["writer_stop_receipt"],
        "feishu_hold_plan_sha256": bundle.sha256["feishu_hold_plan"],
        "feishu_hold_approval_receipt_sha256": bundle.sha256[
            "feishu_hold_approval_receipt"
        ],
        "feishu_hold_cutover_binding_sha256": bundle.sha256[
            "feishu_hold_cutover_binding"
        ],
        "feishu_hold_receipt_sha256": bundle.sha256["feishu_hold_receipt"],
        "env_stage_receipt_sha256": bundle.sha256["env_stage_receipt"],
        "candidate_env_sha256": _require_sha256(
            candidate.get("sha256"), code="production_cutover_candidate_env_sha_invalid"
        ),
        "runtime_stage_manifest_sha256": bundle.sha256["runtime_stage_manifest"],
        "workspace_runtime_manifest_sha256": bundle.sha256[
            "workspace_runtime_manifest"
        ],
    }


def _validate_authorization(
    bundle: ArtifactBundle,
    *,
    lease_fingerprint: str,
    machine_identity_sha256: str,
    now: datetime,
) -> Mapping[str, Any]:
    owned = bundle.artifacts["cutover_authorization_receipt"]
    body = owned.body
    if set(body) != {
        "schema_version",
        "release_id",
        "decision",
        "created_at",
        "expires_at",
        "nonce",
        "action_set",
        "action_set_sha256",
        "bindings",
        "identity",
    }:
        raise ProductionCutoverError("production_cutover_authorization_shape_invalid")
    release_id = body.get("release_id")
    if (
        body.get("schema_version") != AUTHORIZATION_SCHEMA_VERSION
        or body.get("decision") != AUTHORIZATION_DECISION
        or not isinstance(release_id, str)
        or RELEASE_ID_RE.fullmatch(release_id) is None
        or body.get("action_set") != list(CUTOVER_ACTION_SET)
        or body.get("action_set_sha256") != _sha256_json(list(CUTOVER_ACTION_SET))
    ):
        raise ProductionCutoverError("production_cutover_authorization_invalid")
    nonce = body.get("nonce")
    if not isinstance(nonce, str) or NONCE_RE.fullmatch(nonce) is None:
        raise ProductionCutoverError("production_cutover_authorization_nonce_invalid")
    identity = body.get("identity")
    if (
        not isinstance(identity, Mapping)
        or set(identity)
        != {
            "schema_version",
            "method",
            "uid",
            "username",
            "machine_identity_sha256",
        }
        or identity.get("schema_version") != AUTHORIZATION_IDENTITY_SCHEMA_VERSION
        or identity.get("method") != "kernel_owner_and_machine_binding"
        or identity.get("uid") != os.geteuid()
        or not isinstance(identity.get("username"), str)
        or not identity["username"].strip()
    ):
        raise ProductionCutoverError(
            "production_cutover_authorization_identity_invalid"
        )
    live_machine_identity = _require_sha256(
        machine_identity_sha256,
        code="production_cutover_machine_identity_invalid",
    )
    if identity.get("machine_identity_sha256") != live_machine_identity:
        raise ProductionCutoverError(
            "production_cutover_authorization_machine_mismatch"
        )
    expected = {
        **_authorization_bindings(bundle, lease_fingerprint=lease_fingerprint),
        "expected_live_identity_sha256": _require_sha256(
            body.get("bindings", {}).get("expected_live_identity_sha256"),
            code="production_cutover_expected_live_identity_invalid",
        ),
    }
    if body.get("bindings") != expected:
        raise ProductionCutoverError(
            "production_cutover_authorization_binding_mismatch"
        )
    created = _parse_time(
        body.get("created_at"), code="production_cutover_authorization_time_invalid"
    )
    expires = _parse_time(
        body.get("expires_at"), code="production_cutover_authorization_time_invalid"
    )
    current = now.astimezone(timezone.utc)
    if (
        expires <= created
        or (expires - created).total_seconds() > MAX_AUTHORIZATION_VALIDITY_SECONDS
        or created - current > timedelta(seconds=MAX_FUTURE_SKEW_SECONDS)
        or current >= expires
    ):
        raise ProductionCutoverError("production_cutover_authorization_expired")
    return {
        "release_id": release_id,
        "receipt_sha256": owned.sha256,
        "bindings": expected,
        "expires_at": expires.isoformat(),
        "nonce": nonce,
        "machine_identity_sha256": live_machine_identity,
    }


def _validate_local_artifact_chain(
    bundle: ArtifactBundle,
    *,
    lease_fingerprint: str,
    machine_identity_sha256: str,
    now: datetime,
) -> Mapping[str, Any]:
    bodies = bundle.bodies
    prepare = bodies["release_prepare_manifest"]
    approval = bodies["approval_receipt"]
    writer = bodies["writer_stop_receipt"]
    hold_plan = bodies["feishu_hold_plan"]
    hold_approval = bodies["feishu_hold_approval_receipt"]
    hold_cutover = bodies["feishu_hold_cutover_binding"]
    hold = bodies["feishu_hold_receipt"]
    env_stage = bodies["env_stage_receipt"]
    runtime = bodies["runtime_stage_manifest"]
    workspace = bodies["workspace_runtime_manifest"]
    if (
        prepare.get("schema_version") != RELEASE_PREPARE_SCHEMA_VERSION
        or prepare.get("complete") is not True
        or prepare.get("plan_only") is not True
    ):
        raise ProductionCutoverError("production_cutover_release_prepare_invalid")
    release_id = prepare.get("release_id")
    if not isinstance(release_id, str) or RELEASE_ID_RE.fullmatch(release_id) is None:
        raise ProductionCutoverError("production_cutover_release_id_invalid")
    if (
        approval.get("schema_version") != RELEASE_APPROVAL_SCHEMA_VERSION
        or approval.get("release_id") != release_id
        or prepare.get("approval_receipt_sha256") != bundle.sha256["approval_receipt"]
    ):
        raise ProductionCutoverError("production_cutover_release_approval_invalid")
    if (
        writer.get("schema_version") != WRITER_STOP_SCHEMA_VERSION
        or writer.get("release_id") != release_id
        or writer.get("production_effects_executed") is not False
        or writer.get("lease_fingerprint") != lease_fingerprint
        or writer.get("release_prepare_manifest_sha256")
        != bundle.sha256["release_prepare_manifest"]
        or writer.get("approval_receipt_sha256")
        != bundle.sha256["feishu_hold_approval_receipt"]
    ):
        raise ProductionCutoverError("production_cutover_writer_stop_invalid")
    writer_observation = writer.get("writer_stop_observation")
    if (
        not isinstance(writer_observation, Mapping)
        or writer_observation.get("launchd", {}).get("pid") is not None
        or writer_observation.get("process_census", {}).get("matching_processes") != []
    ):
        raise ProductionCutoverError("production_cutover_writer_not_stopped")
    if (
        hold_plan.get("schema_version") != FEISHU_HOLD_PLAN_SCHEMA_VERSION
        or hold_plan.get("production_effects_executed") is not False
        or hold_plan.get("phase") != "plan"
        or hold_approval.get("schema_version") != FEISHU_HOLD_APPROVAL_SCHEMA_VERSION
        or hold_approval.get("hold_id") != hold_plan.get("hold_id")
        or hold_approval.get("plan_sha256") != bundle.sha256["feishu_hold_plan"]
        or hold_cutover.get("schema_version") != FEISHU_HOLD_CUTOVER_SCHEMA_VERSION
        or hold_cutover.get("hold_id") != hold_plan.get("hold_id")
        or hold_cutover.get("release_id") != release_id
        or hold_cutover.get("plan_sha256") != bundle.sha256["feishu_hold_plan"]
        or hold_cutover.get("writer_stop_receipt_sha256")
        != bundle.sha256["writer_stop_receipt"]
        or hold_cutover.get("cutover_lease_fingerprint") != lease_fingerprint
        or hold_cutover.get("release_prepare_manifest_sha256")
        != bundle.sha256["release_prepare_manifest"]
        # Current ingress-hold v2 names this release approval even though its
        # lower-level gate binds the distinct hold approval. The global cutover
        # gate must bind both approvals separately.
        or hold_cutover.get("release_approval_receipt_sha256")
        != bundle.sha256["feishu_hold_approval_receipt"]
    ):
        raise ProductionCutoverError("production_cutover_feishu_hold_chain_invalid")
    if (
        hold.get("schema_version") != FEISHU_HOLD_SCHEMA_VERSION
        or hold.get("ok") is not True
        or hold.get("production_effects_executed") is not False
        or hold.get("live_sidecar_written") is not False
        or hold.get("writer_stop", {}).get("receipt_sha256")
        != bundle.sha256["writer_stop_receipt"]
        or hold.get("writer_stop", {}).get("lease_fingerprint") != lease_fingerprint
        or hold.get("cutover", {}).get("release_id") != release_id
        or hold.get("plan_sha256") != bundle.sha256["feishu_hold_plan"]
        or hold.get("approval", {}).get("receipt_sha256")
        != bundle.sha256["feishu_hold_approval_receipt"]
        or hold.get("gate_validation", {}).get("cutover_binding_sha256")
        != bundle.sha256["feishu_hold_cutover_binding"]
    ):
        raise ProductionCutoverError("production_cutover_feishu_hold_invalid")
    env_bindings = env_stage.get("bindings")
    side_effect_contract = env_stage.get("side_effect_contract")
    if (
        env_stage.get("schema_version") != ENV_STAGE_SCHEMA_VERSION
        or env_stage.get("release_id") != release_id
        or env_stage.get("complete") is not True
        or env_stage.get("live_write_performed") is not False
        or not isinstance(env_bindings, Mapping)
        or env_bindings.get("release_prepare_manifest", {}).get("sha256")
        != bundle.sha256["release_prepare_manifest"]
        or env_bindings.get("release_approval", {}).get("sha256")
        != bundle.sha256["approval_receipt"]
        or env_bindings.get("release_bom_sha256") != prepare.get("release_bom_sha256")
        or not isinstance(side_effect_contract, Mapping)
        or side_effect_contract.get("canonical_live_env") != str(CANONICAL_ENV_PATH)
    ):
        raise ProductionCutoverError("production_cutover_env_stage_invalid")
    candidate_path = Path(str(env_bindings.get("candidate_env", {}).get("path") or ""))
    if not candidate_path.is_absolute() or candidate_path == CANONICAL_ENV_PATH:
        raise ProductionCutoverError("production_cutover_candidate_env_path_invalid")
    active_binding_path = Path(
        str(side_effect_contract.get("canonical_active_release_binding") or "")
    )
    if (
        not active_binding_path.is_absolute()
        or active_binding_path == CANONICAL_ENV_PATH
        or active_binding_path.name != ACTIVE_RELEASE_BINDING_NAME
    ):
        raise ProductionCutoverError(
            "production_cutover_active_release_binding_path_invalid"
        )
    if (
        runtime.get("schema_version") != RUNTIME_STAGE_SCHEMA_VERSION
        or runtime.get("complete") is not True
        or runtime.get("production_effects_executed") is not False
        or runtime.get("live_install_performed") is not False
        or runtime.get("future_canonical_projection", {}).get("canonical_live_root")
        != str(CANONICAL_RUNTIME_ROOT)
    ):
        raise ProductionCutoverError("production_cutover_runtime_stage_invalid")
    candidate_plists = runtime.get("future_canonical_projection", {}).get(
        "candidate_plist_sha256"
    )
    if not isinstance(candidate_plists, Mapping) or set(candidate_plists) != set(
        CANDIDATE_PLISTS
    ):
        raise ProductionCutoverError("production_cutover_candidate_plists_invalid")
    if workspace.get("schema_version") != WORKSPACE_RUNTIME_SCHEMA_VERSION:
        raise ProductionCutoverError("production_cutover_workspace_runtime_invalid")
    authorization = _validate_authorization(
        bundle,
        lease_fingerprint=lease_fingerprint,
        machine_identity_sha256=machine_identity_sha256,
        now=now,
    )
    if authorization["release_id"] != release_id:
        raise ProductionCutoverError(
            "production_cutover_authorization_release_mismatch"
        )
    return authorization


def _plan_payload_bindings(bundle: ArtifactBundle) -> Mapping[str, Any]:
    env_stage = bundle.bodies["env_stage_receipt"]
    side_effect_contract = env_stage["side_effect_contract"]
    return {
        "candidate_environment": {
            "source_path": env_stage["bindings"]["candidate_env"]["path"],
            "sha256": env_stage["bindings"]["candidate_env"]["sha256"],
            "canonical_path": str(CANONICAL_ENV_PATH),
        },
        "active_release_binding": {
            "source_path": str(bundle.artifacts["env_stage_receipt"].path),
            "sha256": bundle.sha256["env_stage_receipt"],
            "canonical_path": side_effect_contract["canonical_active_release_binding"],
        },
        "feishu_sidecar": {
            "source_path": bundle.bodies["feishu_hold_receipt"]["future_install"][
                "staged_source"
            ],
            "sha256": bundle.bodies["feishu_hold_receipt"]["future_install"][
                "staged_sha256"
            ],
            "canonical_path": bundle.bodies["feishu_hold_receipt"]["future_install"][
                "canonical_sidecar_path"
            ],
        },
        "runtime": {
            "staging_root": bundle.bodies["runtime_stage_manifest"].get("staging_root"),
            "content_sha256": bundle.bodies["runtime_stage_manifest"].get(
                "content_sha256"
            ),
            "canonical_path": str(CANONICAL_RUNTIME_ROOT),
            "candidate_plist_sha256": dict(
                bundle.bodies["runtime_stage_manifest"]["future_canonical_projection"][
                    "candidate_plist_sha256"
                ]
            ),
        },
        "workspace": {
            "staging_root": str(
                bundle.artifacts["workspace_runtime_manifest"].path.parent
            ),
            "closure_sha256": bundle.bodies["workspace_runtime_manifest"].get(
                "closure_sha256"
            ),
            "canonical_path": str(CANONICAL_WORKSPACE_ROOT),
        },
    }


def _default_gate_validator(**kwargs: Any) -> Mapping[str, Any]:
    try:
        from scripts import pnc_rca_release_gate as release_gate
    except Exception as exc:
        raise ProductionCutoverError("production_cutover_gate_import_failed") from exc
    validator = getattr(
        release_gate, "validate_rca_cutover_execution_authorization", None
    )
    if not callable(validator):
        raise ProductionCutoverError("production_cutover_gate_hook_unavailable")
    try:
        result = validator(**kwargs)
    except Exception as exc:
        raise ProductionCutoverError("production_cutover_gate_rejected") from exc
    if not isinstance(result, Mapping):
        raise ProductionCutoverError("production_cutover_gate_result_invalid")
    return result


def _validate_gate_result(
    value: Mapping[str, Any],
    *,
    bundle: ArtifactBundle,
    authorization: Mapping[str, Any],
    lease_fingerprint: str,
    requested_step: str,
) -> Mapping[str, Any]:
    expected_keys = {
        "schema_version",
        "ok",
        "release_id",
        "release_prepare_manifest_sha256",
        "release_approval_receipt_sha256",
        "release_bom_sha256",
        "cutover_lease_fingerprint",
        "writer_stop_receipt_sha256",
        "feishu_hold_plan_sha256",
        "feishu_hold_approval_receipt_sha256",
        "feishu_hold_cutover_binding_sha256",
        "feishu_hold_receipt_sha256",
        "env_stage_receipt_sha256",
        "active_release_binding_path",
        "runtime_stage_manifest_sha256",
        "workspace_runtime_manifest_sha256",
        "cutover_authorization_receipt_sha256",
        "expected_live_identity_sha256",
        "rollback_live_identity_sha256",
        "target_live_identity_sha256",
        "runtime_content_sha256",
        "workspace_runtime_sha256",
        "candidate_env_sha256",
        "feishu_sidecar_sha256",
        "candidate_plist_set_sha256",
        "activation_contract_sha256",
        "gateway_aux_start_order",
        "resident_start_order",
        "allowed_next_step",
        "authorization_expires_at",
    }
    if (
        set(value) != expected_keys
        or value.get("schema_version") != (GATE_VALIDATION_SCHEMA_VERSION)
        or value.get("ok") is not True
    ):
        raise ProductionCutoverError("production_cutover_gate_result_invalid")
    auth_bindings = authorization["bindings"]
    expected = {
        "release_id": authorization["release_id"],
        "release_prepare_manifest_sha256": bundle.sha256["release_prepare_manifest"],
        "release_approval_receipt_sha256": bundle.sha256["approval_receipt"],
        "release_bom_sha256": auth_bindings["release_bom_sha256"],
        "cutover_lease_fingerprint": lease_fingerprint,
        "writer_stop_receipt_sha256": bundle.sha256["writer_stop_receipt"],
        "feishu_hold_plan_sha256": bundle.sha256["feishu_hold_plan"],
        "feishu_hold_approval_receipt_sha256": bundle.sha256[
            "feishu_hold_approval_receipt"
        ],
        "feishu_hold_cutover_binding_sha256": bundle.sha256[
            "feishu_hold_cutover_binding"
        ],
        "feishu_hold_receipt_sha256": bundle.sha256["feishu_hold_receipt"],
        "env_stage_receipt_sha256": bundle.sha256["env_stage_receipt"],
        "active_release_binding_path": bundle.bodies["env_stage_receipt"][
            "side_effect_contract"
        ]["canonical_active_release_binding"],
        "runtime_stage_manifest_sha256": bundle.sha256["runtime_stage_manifest"],
        "workspace_runtime_manifest_sha256": bundle.sha256[
            "workspace_runtime_manifest"
        ],
        "cutover_authorization_receipt_sha256": bundle.sha256[
            "cutover_authorization_receipt"
        ],
        "expected_live_identity_sha256": auth_bindings["expected_live_identity_sha256"],
        "candidate_env_sha256": auth_bindings["candidate_env_sha256"],
        "feishu_sidecar_sha256": _require_sha256(
            bundle
            .bodies["feishu_hold_receipt"]
            .get("future_install", {})
            .get("staged_sha256"),
            code="production_cutover_feishu_sidecar_sha_invalid",
        ),
        "allowed_next_step": requested_step,
        "authorization_expires_at": authorization["expires_at"],
    }
    for field, expected_value in expected.items():
        if value.get(field) != expected_value:
            raise ProductionCutoverError("production_cutover_gate_binding_mismatch")
    for field in (
        "rollback_live_identity_sha256",
        "target_live_identity_sha256",
        "runtime_content_sha256",
        "workspace_runtime_sha256",
        "candidate_plist_set_sha256",
        "activation_contract_sha256",
        "feishu_sidecar_sha256",
    ):
        _require_sha256(value.get(field), code="production_cutover_gate_hash_invalid")
    gateway_order = value.get("gateway_aux_start_order")
    if (
        not isinstance(gateway_order, list)
        or len(gateway_order) != len(GATEWAY_AUX_LABELS)
        or set(gateway_order) != set(GATEWAY_AUX_LABELS)
    ):
        raise ProductionCutoverError("production_cutover_gateway_aux_order_invalid")
    resident_order = value.get("resident_start_order")
    if (
        not isinstance(resident_order, list)
        or len(resident_order) != len(RESIDENT_LABELS)
        or set(resident_order) != set(RESIDENT_LABELS)
    ):
        raise ProductionCutoverError("production_cutover_resident_order_invalid")
    return dict(value)


def _build_plan_from_bundle(
    inputs: CutoverInputs,
    bundle: ArtifactBundle,
    *,
    gate_validator: GateValidator,
    machine_identity_sha256: str,
    now: datetime,
) -> Mapping[str, Any]:
    authorization = _validate_local_artifact_chain(
        bundle,
        lease_fingerprint=inputs.cutover_lease_fingerprint,
        machine_identity_sha256=machine_identity_sha256,
        now=now,
    )
    try:
        gate_raw = gate_validator(
            artifacts=bundle.bodies,
            artifact_sha256=bundle.sha256,
            cutover_lease_fingerprint=inputs.cutover_lease_fingerprint,
            cutover_authorization=authorization,
            requested_step="plan",
            live_identity_sha256=None,
            prior_step_receipt=None,
        )
    except ProductionCutoverError:
        raise
    except Exception as exc:
        raise ProductionCutoverError("production_cutover_gate_rejected") from exc
    if not isinstance(gate_raw, Mapping):
        raise ProductionCutoverError("production_cutover_gate_result_invalid")
    gate = _validate_gate_result(
        gate_raw,
        bundle=bundle,
        authorization=authorization,
        lease_fingerprint=inputs.cutover_lease_fingerprint,
        requested_step="plan",
    )
    return {
        "schema_version": PLAN_SCHEMA_VERSION,
        "mode": "plan_only",
        "production_effects_executed": False,
        "release_id": authorization["release_id"],
        "bindings": gate,
        "authorization_machine_identity_sha256": authorization[
            "machine_identity_sha256"
        ],
        "payload_bindings": _plan_payload_bindings(bundle),
        "action_set": list(CUTOVER_ACTION_SET),
        "action_set_sha256": _sha256_json(list(CUTOVER_ACTION_SET)),
        "steps": [
            {"name": name, "mutating": name in MUTATING_STEPS} for name in STEP_NAMES
        ],
        "services": list(SERVICE_LABELS),
        "gateway_aux_start_order": gate["gateway_aux_start_order"],
        "resident_start_order": gate["resident_start_order"],
        "writers_stopped_before_install": True,
        "rollback": {
            "automatic_on_failure": True,
            "snapshot_required": True,
            "old_runtime_delete_forbidden": True,
            "restore_runtime_environment_plists_and_service_state": True,
        },
        "execution_contract": {
            "cli_apply_supported": False,
            "programmatic_apply_requires_explicit_adapter": True,
            "programmatic_apply_requires_active_lease": True,
            "shell_strings_forbidden": True,
            "global_lock_held_for_all_steps": True,
        },
    }


def build_cutover_plan(
    inputs: CutoverInputs,
    *,
    gate_validator: GateValidator = _default_gate_validator,
    machine_identity_provider: MachineIdentityProvider = _default_machine_identity_sha256,
    now: datetime | None = None,
) -> Mapping[str, Any]:
    """Build a read-only deterministic plan; never inspect or mutate live state."""
    bundle = _load_artifacts(inputs)
    current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    machine_identity_sha256 = machine_identity_provider()
    return _build_plan_from_bundle(
        inputs,
        bundle,
        gate_validator=gate_validator,
        machine_identity_sha256=machine_identity_sha256,
        now=current,
    )


def _authorize_step(
    inputs: CutoverInputs,
    bundle: ArtifactBundle,
    *,
    plan: Mapping[str, Any],
    gate_validator: GateValidator,
    machine_identity_sha256: str,
    requested_step: str,
    live_identity_sha256: str,
    prior_step_receipt: Mapping[str, Any] | None,
    now: datetime,
) -> Mapping[str, Any]:
    authorization = _validate_local_artifact_chain(
        bundle,
        lease_fingerprint=inputs.cutover_lease_fingerprint,
        machine_identity_sha256=machine_identity_sha256,
        now=now,
    )
    try:
        raw = gate_validator(
            artifacts=bundle.bodies,
            artifact_sha256=bundle.sha256,
            cutover_lease_fingerprint=inputs.cutover_lease_fingerprint,
            cutover_authorization=authorization,
            requested_step=requested_step,
            live_identity_sha256=live_identity_sha256,
            prior_step_receipt=prior_step_receipt,
        )
    except ProductionCutoverError:
        raise
    except Exception as exc:
        raise ProductionCutoverError("production_cutover_gate_rejected") from exc
    if not isinstance(raw, Mapping):
        raise ProductionCutoverError("production_cutover_gate_result_invalid")
    validated = _validate_gate_result(
        raw,
        bundle=bundle,
        authorization=authorization,
        lease_fingerprint=inputs.cutover_lease_fingerprint,
        requested_step=requested_step,
    )
    stable = dict(validated)
    stable["allowed_next_step"] = "plan"
    if stable != plan["bindings"]:
        raise ProductionCutoverError("production_cutover_step_gate_drift")
    return validated


def validate_cutover_plan(
    inputs: CutoverInputs,
    plan: Mapping[str, Any],
    *,
    gate_validator: GateValidator = _default_gate_validator,
    adapter: CutoverSystemAdapter | None = None,
    machine_identity_provider: MachineIdentityProvider = _default_machine_identity_sha256,
    now: datetime | None = None,
) -> Mapping[str, Any]:
    expected = build_cutover_plan(
        inputs,
        gate_validator=gate_validator,
        machine_identity_provider=machine_identity_provider,
        now=now,
    )
    if plan != expected:
        raise ProductionCutoverError("production_cutover_plan_mismatch")
    observation = None
    if adapter is not None:
        observation = dict(adapter.observe_live_identity())
        if (
            _sha256_json(observation)
            != plan["bindings"]["expected_live_identity_sha256"]
        ):
            raise ProductionCutoverError("production_cutover_live_identity_drift")
    return {
        "schema_version": "pnc_rca_production_cutover_validation_v1",
        "ok": True,
        "plan_sha256": _sha256_json(plan),
        "live_observation_performed": observation is not None,
        "production_effects_executed": False,
    }


def _ensure_journal_root(path: Path) -> Path:
    selected = path.expanduser()
    if not selected.is_absolute():
        raise ProductionCutoverError("production_cutover_journal_path_not_absolute")
    selected = selected.absolute()
    try:
        info = selected.lstat()
    except OSError as exc:
        raise ProductionCutoverError("production_cutover_journal_unavailable") from exc
    if (
        stat.S_ISLNK(info.st_mode)
        or not stat.S_ISDIR(info.st_mode)
        or stat.S_IMODE(info.st_mode) != 0o700
        or info.st_uid != os.geteuid()
    ):
        raise ProductionCutoverError("production_cutover_journal_identity_invalid")
    resolved = selected.resolve()
    for forbidden in (
        CANONICAL_RUNTIME_ROOT,
        CANONICAL_WORKSPACE_ROOT,
        CANONICAL_LAUNCH_AGENTS_ROOT,
    ):
        try:
            resolved.relative_to(forbidden)
        except ValueError:
            pass
        else:
            raise ProductionCutoverError(
                "production_cutover_journal_live_path_forbidden"
            )
        try:
            forbidden.relative_to(resolved)
        except ValueError:
            pass
        else:
            raise ProductionCutoverError(
                "production_cutover_journal_live_path_forbidden"
            )
    if resolved == CANONICAL_ENV_PATH:
        raise ProductionCutoverError("production_cutover_journal_live_path_forbidden")
    return selected


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(
        path,
        os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
    )
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _publish_no_clobber(path: Path, body: Mapping[str, Any]) -> bool:
    raw = _canonical_json(body)
    if path.exists() or path.is_symlink():
        existing = _read_owned_json(
            path, artifact="production_cutover_journal_artifact"
        )
        if existing.raw != raw:
            raise ProductionCutoverError("production_cutover_journal_conflict")
        return False
    temporary = path.parent / f".{path.name}.{secrets.token_hex(8)}.tmp"
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
        view = memoryview(raw)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise ProductionCutoverError("production_cutover_journal_write_failed")
            view = view[written:]
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = -1
        try:
            os.link(temporary, path, follow_symlinks=False)
        except FileExistsError:
            existing = _read_owned_json(
                path, artifact="production_cutover_journal_artifact"
            )
            if existing.raw != raw:
                raise ProductionCutoverError("production_cutover_journal_conflict")
        _fsync_directory(path.parent)
        return True
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        with contextlib.suppress(FileNotFoundError):
            temporary.unlink()


def _ensure_nonce_ledger_root(path: Path) -> Path:
    selected = path.expanduser().absolute()
    if not selected.is_absolute():
        raise ProductionCutoverError("production_cutover_nonce_ledger_path_invalid")
    try:
        os.mkdir(selected, 0o700)
        _fsync_directory(selected.parent)
    except FileExistsError:
        pass
    except OSError as exc:
        raise ProductionCutoverError(
            "production_cutover_nonce_ledger_unavailable"
        ) from exc
    try:
        info = selected.lstat()
    except OSError as exc:
        raise ProductionCutoverError(
            "production_cutover_nonce_ledger_unavailable"
        ) from exc
    if (
        stat.S_ISLNK(info.st_mode)
        or not stat.S_ISDIR(info.st_mode)
        or stat.S_IMODE(info.st_mode) != 0o700
        or info.st_uid != os.geteuid()
    ):
        raise ProductionCutoverError("production_cutover_nonce_ledger_identity_invalid")
    return selected


def _consume_authorization_nonce(
    *,
    ledger_root: Path,
    journal_root: Path,
    plan: Mapping[str, Any],
    authorization: Mapping[str, Any],
) -> Mapping[str, Any]:
    root = _ensure_nonce_ledger_root(ledger_root)
    nonce = authorization.get("nonce")
    if not isinstance(nonce, str) or NONCE_RE.fullmatch(nonce) is None:
        raise ProductionCutoverError("production_cutover_authorization_nonce_invalid")
    key = hashlib.sha256(
        _canonical_json({
            "nonce": nonce,
            "machine_identity_sha256": authorization["machine_identity_sha256"],
        })
    ).hexdigest()
    body = {
        "schema_version": NONCE_CONSUMPTION_SCHEMA_VERSION,
        "release_id": plan["release_id"],
        "nonce_sha256": hashlib.sha256(nonce.encode("utf-8")).hexdigest(),
        "machine_identity_sha256": authorization["machine_identity_sha256"],
        "authorization_receipt_sha256": authorization["receipt_sha256"],
        "plan_sha256": _sha256_json(plan),
        "journal_root": str(journal_root.resolve()),
    }
    path = root / f"{key}.json"
    if path.exists() or path.is_symlink():
        observed = _read_owned_json(
            path, artifact="production_cutover_nonce_consumption"
        ).body
        if observed != body:
            raise ProductionCutoverError("production_cutover_authorization_replay")
        return observed
    _publish_no_clobber(path, body)
    observed = _read_owned_json(
        path, artifact="production_cutover_nonce_consumption"
    ).body
    if observed != body:
        raise ProductionCutoverError("production_cutover_nonce_consumption_conflict")
    return observed


class _Journal:
    def __init__(
        self,
        root: Path,
        *,
        plan: Mapping[str, Any],
        run_identity: Mapping[str, Any],
    ):
        self.root = _ensure_journal_root(root)
        self.plan = plan
        self.plan_sha256 = _sha256_json(plan)
        self.run_identity = dict(run_identity)
        self.steps = self.root / "steps"

    @contextlib.contextmanager
    def lock(self):
        path = self.root / ".cutover-executor.lock"
        descriptor = os.open(
            path,
            os.O_RDWR
            | os.O_CREAT
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
        try:
            info = os.fstat(descriptor)
            if (
                not stat.S_ISREG(info.st_mode)
                or stat.S_IMODE(info.st_mode) != 0o600
                or info.st_uid != os.geteuid()
                or info.st_nlink != 1
            ):
                raise ProductionCutoverError("production_cutover_journal_lock_invalid")
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise ProductionCutoverError(
                    "production_cutover_already_running"
                ) from exc
            if not self.steps.exists():
                os.mkdir(self.steps, 0o700)
                _fsync_directory(self.root)
            step_info = self.steps.lstat()
            if (
                stat.S_ISLNK(step_info.st_mode)
                or not stat.S_ISDIR(step_info.st_mode)
                or stat.S_IMODE(step_info.st_mode) != 0o700
                or step_info.st_uid != os.geteuid()
            ):
                raise ProductionCutoverError("production_cutover_step_root_invalid")
            if (
                self.run_identity.get("schema_version")
                != JOURNAL_IDENTITY_SCHEMA_VERSION
                or self.run_identity.get("release_id") != self.plan["release_id"]
                or self.run_identity.get("plan_sha256") != self.plan_sha256
            ):
                raise ProductionCutoverError("production_cutover_run_identity_invalid")
            _publish_no_clobber(self.root / "plan.json", self.plan)
            _publish_no_clobber(self.root / "run-identity.json", self.run_identity)
            yield self
        finally:
            with contextlib.suppress(OSError):
                fcntl.flock(descriptor, fcntl.LOCK_UN)
            os.close(descriptor)

    def intent_path(self, index: int, step: str) -> Path:
        return self.steps / f"{index:02d}-{step}.intent.json"

    def done_path(self, index: int, step: str) -> Path:
        return self.steps / f"{index:02d}-{step}.done.json"

    def read_done(self, index: int, step: str) -> Mapping[str, Any] | None:
        path = self.done_path(index, step)
        if not path.exists():
            return None
        return _read_owned_json(path, artifact="production_cutover_step_done").body

    def incomplete(self) -> tuple[int, str] | None:
        for index, step in enumerate(STEP_NAMES, 1):
            if (
                self.intent_path(index, step).exists()
                and not self.done_path(index, step).exists()
            ):
                return index, step
        return None


def _normalize_commands(value: Any) -> list[list[str]]:
    if not isinstance(value, list):
        raise ProductionCutoverError("production_cutover_step_commands_invalid")
    normalized: list[list[str]] = []
    for command in value:
        if (
            not isinstance(command, list)
            or not command
            or any(
                not isinstance(argument, str) or not argument for argument in command
            )
        ):
            raise ProductionCutoverError("production_cutover_step_commands_invalid")
        normalized.append(list(command))
    return normalized


def _payload_file_descriptor(
    observed: _StablePayloadFile,
    *,
    binding_sha256: str,
) -> Mapping[str, Any]:
    if observed.sha256 != binding_sha256:
        raise ProductionCutoverError("production_cutover_payload_hash_mismatch")
    return {
        "schema_version": PAYLOAD_DESCRIPTOR_SCHEMA_VERSION,
        "kind": "regular_file",
        "path": str(observed.path),
        "binding_sha256": binding_sha256,
        "physical_sha256": observed.sha256,
        "size_bytes": len(observed.raw),
        "identity": dict(observed.identity),
    }


def _regular_payload_descriptor(
    path: Path,
    *,
    artifact: str,
    binding_sha256: str,
) -> Mapping[str, Any]:
    return _payload_file_descriptor(
        _read_stable_payload_file(
            path,
            artifact=artifact,
            expected_mode=0o600,
        ),
        binding_sha256=binding_sha256,
    )


def _safe_runtime_relative(value: Any) -> str:
    if not isinstance(value, str) or not value or "\\" in value:
        raise ProductionCutoverError("production_cutover_runtime_payload_invalid")
    path = Path(value)
    if path.is_absolute() or ".." in path.parts or str(path) in {"", "."}:
        raise ProductionCutoverError("production_cutover_runtime_payload_invalid")
    return path.as_posix()


def _runtime_payload_descriptor(
    bundle: ArtifactBundle,
) -> Mapping[str, Any]:
    manifest = bundle.bodies["runtime_stage_manifest"]
    root = Path(str(manifest.get("staging_root") or "")).expanduser().absolute()
    content = manifest.get("content")
    if (
        not root.is_absolute()
        or root == CANONICAL_RUNTIME_ROOT
        or not isinstance(content, Mapping)
        or manifest.get("content_sha256") != _sha256_json(content)
    ):
        raise ProductionCutoverError("production_cutover_runtime_payload_invalid")
    source = content.get("source")
    plists = content.get("candidate_plists")
    venv = content.get("venv")
    if (
        not isinstance(source, Mapping)
        or not isinstance(source.get("runtime_files"), Mapping)
        or not isinstance(plists, Mapping)
        or set(plists) != set(CANDIDATE_PLISTS)
        or not isinstance(venv, Mapping)
        or not isinstance(venv.get("files"), Mapping)
    ):
        raise ProductionCutoverError("production_cutover_runtime_payload_invalid")
    expected: dict[str, Mapping[str, Any]] = {}
    for relative, descriptor in source["runtime_files"].items():
        expected[_safe_runtime_relative(relative)] = descriptor
    for filename, pair in plists.items():
        if not isinstance(pair, Mapping) or not isinstance(pair.get("staged"), Mapping):
            raise ProductionCutoverError("production_cutover_runtime_payload_invalid")
        expected[_safe_runtime_relative(filename)] = pair["staged"]
    for relative, descriptor in venv["files"].items():
        expected[f".venv/{_safe_runtime_relative(relative)}"] = descriptor

    try:
        root_before = root.lstat()
    except OSError as exc:
        raise ProductionCutoverError(
            "production_cutover_runtime_payload_unavailable"
        ) from exc
    if (
        stat.S_ISLNK(root_before.st_mode)
        or not stat.S_ISDIR(root_before.st_mode)
        or root_before.st_uid != os.geteuid()
        or stat.S_IMODE(root_before.st_mode) & 0o022
    ):
        raise ProductionCutoverError(
            "production_cutover_runtime_payload_identity_invalid"
        )
    physical: dict[str, Any] = {}
    for relative, raw_descriptor in sorted(expected.items()):
        if not isinstance(raw_descriptor, Mapping):
            raise ProductionCutoverError("production_cutover_runtime_payload_invalid")
        sha256 = _require_sha256(
            raw_descriptor.get("sha256"),
            code="production_cutover_runtime_payload_invalid",
        )
        raw_mode = raw_descriptor.get("mode")
        size = raw_descriptor.get("size_bytes")
        if (
            not isinstance(raw_mode, str)
            or re.fullmatch(r"0[467][0-7]{2}", raw_mode) is None
            or isinstance(size, bool)
            or not isinstance(size, int)
            or size < 0
        ):
            raise ProductionCutoverError("production_cutover_runtime_payload_invalid")
        observed = _read_stable_payload_file(
            root / relative,
            artifact="production_cutover_runtime_payload_file",
            expected_mode=int(raw_mode, 8),
            max_bytes=max(MAX_JSON_BYTES, size),
        )
        if observed.sha256 != sha256 or len(observed.raw) != size:
            raise ProductionCutoverError("production_cutover_runtime_payload_mismatch")
        physical[relative] = {
            "sha256": observed.sha256,
            "size_bytes": len(observed.raw),
            "identity": dict(observed.identity),
        }
    try:
        actual = {
            path.relative_to(root).as_posix()
            for path in root.rglob("*")
            if path.is_file()
            and path != bundle.artifacts["runtime_stage_manifest"].path
        }
        if any(path.is_symlink() for path in root.rglob("*")) or actual != set(
            expected
        ):
            raise ProductionCutoverError(
                "production_cutover_runtime_payload_layout_mismatch"
            )
        root_after = root.lstat()
    except OSError as exc:
        raise ProductionCutoverError(
            "production_cutover_runtime_payload_unstable"
        ) from exc
    if _stat_fields(root_before) != _stat_fields(root_after):
        raise ProductionCutoverError("production_cutover_runtime_payload_unstable")
    binding = _require_sha256(
        manifest.get("content_sha256"),
        code="production_cutover_runtime_payload_invalid",
    )
    return {
        "schema_version": PAYLOAD_DESCRIPTOR_SCHEMA_VERSION,
        "kind": "runtime_tree",
        "path": str(root),
        "binding_sha256": binding,
        "physical_sha256": _sha256_json(physical),
        "files": physical,
        "root_identity": _stat_fields(root_before),
    }


def _workspace_payload_descriptor(bundle: ArtifactBundle) -> Mapping[str, Any]:
    try:
        from gateway import pnc_rca_workspace_runtime as workspace_runtime

        root = bundle.artifacts["workspace_runtime_manifest"].path.parent
        identity = workspace_runtime.validate_staged_workspace_runtime(root)
    except Exception as exc:
        raise ProductionCutoverError(
            "production_cutover_workspace_payload_invalid"
        ) from exc
    if (
        identity.manifest_path != bundle.artifacts["workspace_runtime_manifest"].path
        or identity.manifest_sha256 != bundle.sha256["workspace_runtime_manifest"]
        or identity.closure_sha256
        != bundle.bodies["workspace_runtime_manifest"].get("closure_sha256")
    ):
        raise ProductionCutoverError("production_cutover_workspace_payload_mismatch")
    body = identity.to_dict()
    return {
        "schema_version": PAYLOAD_DESCRIPTOR_SCHEMA_VERSION,
        "kind": "workspace_tree",
        "path": str(root),
        "binding_sha256": identity.closure_sha256,
        "physical_sha256": _sha256_json(body),
        "identity": body,
    }


def _validate_candidate_payloads(bundle: ArtifactBundle) -> Mapping[str, Any]:
    bindings = _plan_payload_bindings(bundle)
    return {
        "candidate_environment": _regular_payload_descriptor(
            Path(bindings["candidate_environment"]["source_path"]),
            artifact="production_cutover_candidate_environment",
            binding_sha256=bindings["candidate_environment"]["sha256"],
        ),
        "active_release_binding": _regular_payload_descriptor(
            Path(bindings["active_release_binding"]["source_path"]),
            artifact="production_cutover_active_release_binding",
            binding_sha256=bindings["active_release_binding"]["sha256"],
        ),
        "feishu_sidecar": _regular_payload_descriptor(
            Path(bindings["feishu_sidecar"]["source_path"]),
            artifact="production_cutover_feishu_sidecar",
            binding_sha256=bindings["feishu_sidecar"]["sha256"],
        ),
        "runtime": _runtime_payload_descriptor(bundle),
        "workspace": _workspace_payload_descriptor(bundle),
    }


def _payloads_for_step(
    step: str,
    payloads: Mapping[str, Any],
) -> Mapping[str, Any]:
    names = {
        "install_feishu_sidecar": ("feishu_sidecar",),
        "install_runtime": ("runtime",),
        "install_workspace": ("workspace",),
        "install_environment": (
            "candidate_environment",
            "active_release_binding",
        ),
        "install_plists": ("runtime",),
    }.get(step, ())
    return {name: payloads[name] for name in names}


def _expected_commands_for_step(step: str, plan: Mapping[str, Any]) -> list[list[str]]:
    domain = f"gui/{os.geteuid()}"
    payloads = plan["payload_bindings"]
    adapter = CUTOVER_ADAPTER_EXECUTABLE
    if step == "stop_writers":
        return [[adapter, "stop-writers", *WRITER_LABELS]]
    if step == "install_feishu_sidecar":
        binding = payloads["feishu_sidecar"]
        return [
            [
                adapter,
                "install-owner-file",
                binding["source_path"],
                binding["canonical_path"],
                binding["sha256"],
            ]
        ]
    if step == "install_runtime":
        binding = payloads["runtime"]
        return [
            [
                adapter,
                "install-retained-tree",
                binding["staging_root"],
                binding["canonical_path"],
                binding["content_sha256"],
            ]
        ]
    if step == "install_workspace":
        binding = payloads["workspace"]
        return [
            [
                adapter,
                "install-retained-tree",
                binding["staging_root"],
                binding["canonical_path"],
                binding["closure_sha256"],
            ]
        ]
    if step == "install_environment":
        environment = payloads["candidate_environment"]
        active = payloads["active_release_binding"]
        return [
            [
                adapter,
                "install-owner-file",
                environment["source_path"],
                environment["canonical_path"],
                environment["sha256"],
            ],
            [
                adapter,
                "install-owner-file",
                active["source_path"],
                active["canonical_path"],
                active["sha256"],
            ],
        ]
    if step == "install_plists":
        runtime_root = payloads["runtime"]["staging_root"]
        return [
            [
                adapter,
                "install-owner-file",
                str(Path(runtime_root) / candidate),
                str(
                    CANONICAL_LAUNCH_AGENTS_ROOT
                    / candidate.replace(".candidate.plist", ".plist")
                ),
                payloads["runtime"]["candidate_plist_sha256"][candidate],
            ]
            for candidate in CANDIDATE_PLISTS
        ]
    if step == "start_gateway_aux":
        return [
            ["/bin/launchctl", "kickstart", "-k", f"{domain}/{label}"]
            for label in plan["gateway_aux_start_order"]
        ]
    if step == "start_residents":
        return [
            [
                "/bin/launchctl",
                "bootstrap",
                domain,
                str(CANONICAL_LAUNCH_AGENTS_ROOT / f"{label}.plist"),
            ]
            for label in plan["resident_start_order"]
        ]
    if step == "transition_bounded_activation":
        return [
            [
                adapter,
                "transition-bounded-activation",
                plan["bindings"]["activation_contract_sha256"],
            ]
        ]
    if step == "rollback":
        return [
            [
                adapter,
                "restore-exact-snapshot",
                plan["bindings"]["rollback_live_identity_sha256"],
                _sha256_json(plan),
            ]
        ]
    if step in {"snapshot_live", "verify_gateway_aux", "verify_services"}:
        return []
    raise ProductionCutoverError("production_cutover_command_step_unknown")


def _expected_start_commands(step: str, plan: Mapping[str, Any]) -> list[list[str]]:
    if step not in {"start_gateway_aux", "start_residents"}:
        return []
    return _expected_commands_for_step(step, plan)


def _validate_planned_commands(
    step: str,
    commands: Any,
    *,
    plan: Mapping[str, Any],
) -> list[list[str]]:
    normalized = _normalize_commands(commands)
    if normalized != _expected_commands_for_step(step, plan):
        raise ProductionCutoverError("production_cutover_command_not_allowlisted")
    return normalized


def _preflight_adapter_step(
    adapter: CutoverSystemAdapter,
    *,
    step: str,
    expected_identity_sha256: str,
    plan: Mapping[str, Any],
    payload_descriptors: Mapping[str, Any],
    lease_fingerprint: str,
    lease_token: str,
) -> list[list[str]]:
    try:
        raw = adapter.preflight_step(
            step,
            expected_identity_sha256=expected_identity_sha256,
            plan=plan,
            payload_descriptors=payload_descriptors,
            lease_fingerprint=lease_fingerprint,
            lease_token=lease_token,
        )
    except ProductionCutoverError:
        raise
    except Exception as exc:
        raise ProductionCutoverError(
            "production_cutover_adapter_preflight_failed"
        ) from exc
    if (
        not isinstance(raw, Mapping)
        or set(raw)
        != {
            "schema_version",
            "step",
            "expected_identity_sha256",
            "commands",
            "payload_descriptors",
            "lease_fingerprint",
        }
        or raw.get("schema_version") != COMMAND_PREFLIGHT_SCHEMA_VERSION
        or raw.get("step") != step
        or raw.get("expected_identity_sha256") != expected_identity_sha256
        or raw.get("payload_descriptors") != payload_descriptors
        or raw.get("lease_fingerprint") != lease_fingerprint
    ):
        raise ProductionCutoverError("production_cutover_adapter_preflight_invalid")
    return _validate_planned_commands(step, raw.get("commands"), plan=plan)


def _validate_step_result(
    value: Mapping[str, Any],
    *,
    step: str,
    expected_before: str,
    plan: Mapping[str, Any],
    planned_commands: Sequence[Sequence[str]] | None = None,
) -> Mapping[str, Any]:
    if set(value) != {
        "schema_version",
        "step",
        "before_identity_sha256",
        "after_identity_sha256",
        "commands",
        "old_runtime_retained",
        "snapshot",
        "services",
        "evidence",
        "started_labels",
    } or (
        value.get("schema_version") != STEP_RESULT_SCHEMA_VERSION
        or value.get("step") != step
        or value.get("before_identity_sha256") != expected_before
        or value.get("old_runtime_retained") is not True
    ):
        raise ProductionCutoverError("production_cutover_step_result_invalid")
    after = _require_sha256(
        value.get("after_identity_sha256"),
        code="production_cutover_step_identity_invalid",
    )
    commands = _validate_planned_commands(step, value.get("commands"), plan=plan)
    if planned_commands is not None and commands != [
        list(item) for item in planned_commands
    ]:
        raise ProductionCutoverError("production_cutover_executed_commands_mismatch")
    snapshot = value.get("snapshot")
    services = value.get("services")
    evidence = value.get("evidence")
    started_labels = value.get("started_labels")
    if not isinstance(evidence, Mapping) or not isinstance(started_labels, list):
        raise ProductionCutoverError("production_cutover_step_result_invalid")
    if step == "snapshot_live":
        if (
            after != expected_before
            or not isinstance(snapshot, Mapping)
            or set(snapshot)
            != {
                "schema_version",
                "snapshot_id",
                "before_live_identity_sha256",
                "rollback_target_identity_sha256",
                "components",
                "old_runtime_retained",
            }
            or snapshot.get("schema_version") != SNAPSHOT_SCHEMA_VERSION
            or snapshot.get("before_live_identity_sha256") != expected_before
            or snapshot.get("rollback_target_identity_sha256")
            != plan["bindings"]["rollback_live_identity_sha256"]
            or snapshot.get("old_runtime_retained") is not True
            or not isinstance(snapshot.get("snapshot_id"), str)
            or not snapshot["snapshot_id"]
            or commands
        ):
            raise ProductionCutoverError("production_cutover_snapshot_invalid")
        components = snapshot.get("components")
        if not isinstance(components, Mapping) or set(components) != {
            "runtime",
            "workspace",
            "environment",
            "plists",
            "services",
            "feishu_sidecar",
            "active_release_binding",
        }:
            raise ProductionCutoverError("production_cutover_snapshot_invalid")
        for descriptor in components.values():
            if (
                not isinstance(descriptor, Mapping)
                or set(descriptor) != {"sha256", "restore_ref"}
                or not isinstance(descriptor.get("restore_ref"), str)
                or not descriptor["restore_ref"]
            ):
                raise ProductionCutoverError("production_cutover_snapshot_invalid")
            _require_sha256(
                descriptor.get("sha256"),
                code="production_cutover_snapshot_invalid",
            )
    elif snapshot is not None:
        raise ProductionCutoverError("production_cutover_step_snapshot_unexpected")
    expected_services: tuple[str, ...] = ()
    if step == "verify_gateway_aux":
        expected_services = GATEWAY_AUX_LABELS
    elif step == "verify_services":
        expected_services = SERVICE_LABELS
    if expected_services:
        if after != expected_before or not isinstance(services, Mapping):
            raise ProductionCutoverError(
                "production_cutover_service_verification_invalid"
            )
        if set(services) != set(expected_services):
            raise ProductionCutoverError("production_cutover_service_set_invalid")
        for label in expected_services:
            service = services[label]
            if (
                not isinstance(service, Mapping)
                or set(service)
                != {
                    "pid",
                    "process_create_time",
                    "runtime_sha256",
                    "health_ok",
                }
                or isinstance(service.get("pid"), bool)
                or not isinstance(service.get("pid"), int)
                or service["pid"] <= 0
                or not isinstance(service.get("process_create_time"), (int, float))
                or service["process_create_time"] <= 0
                or service.get("runtime_sha256")
                != plan["bindings"]["runtime_content_sha256"]
                or service.get("health_ok") is not True
            ):
                raise ProductionCutoverError(
                    "production_cutover_service_verification_invalid"
                )
    elif services not in ({}, None):
        raise ProductionCutoverError("production_cutover_step_services_unexpected")
    expected_started: list[str] = []
    if step == "start_gateway_aux":
        expected_started = list(plan["gateway_aux_start_order"])
    elif step == "start_residents":
        expected_started = list(plan["resident_start_order"])
    if started_labels != expected_started:
        raise ProductionCutoverError("production_cutover_service_start_order_invalid")
    if expected_started and commands != _expected_start_commands(step, plan):
        raise ProductionCutoverError("production_cutover_service_commands_invalid")
    if step == "stop_writers":
        if evidence.get(
            "schema_version"
        ) != "pnc_rca_writer_stop_evidence_v1" or evidence.get("writer_labels") != list(
            WRITER_LABELS
        ):
            raise ProductionCutoverError(
                "production_cutover_writer_stop_evidence_invalid"
            )
        _require_sha256(
            evidence.get("receipt_sha256"),
            code="production_cutover_writer_stop_evidence_invalid",
        )
        if not Path(str(evidence.get("receipt_path") or "")).is_absolute():
            raise ProductionCutoverError(
                "production_cutover_writer_stop_evidence_invalid"
            )
    elif step == "transition_bounded_activation":
        if evidence.get("state") != "bounded_active":
            raise ProductionCutoverError("production_cutover_activation_not_bounded")
        _require_sha256(
            evidence.get("receipt_sha256"),
            code="production_cutover_activation_evidence_invalid",
        )
        if not Path(str(evidence.get("receipt_path") or "")).is_absolute():
            raise ProductionCutoverError(
                "production_cutover_activation_evidence_invalid"
            )
    elif step in {
        "install_feishu_sidecar",
        "install_runtime",
        "install_workspace",
        "install_environment",
        "install_plists",
    }:
        expected_install = {
            "install_feishu_sidecar": plan["bindings"]["feishu_sidecar_sha256"],
            "install_runtime": plan["bindings"]["runtime_content_sha256"],
            "install_workspace": plan["bindings"]["workspace_runtime_sha256"],
            "install_environment": plan["bindings"]["candidate_env_sha256"],
            "install_plists": plan["bindings"]["candidate_plist_set_sha256"],
        }[step]
        if (
            evidence
            != {
                "installed_sha256": expected_install,
                "post_install_verified": True,
            }
            and step != "install_environment"
        ):
            raise ProductionCutoverError("production_cutover_install_evidence_invalid")
        if step == "install_environment":
            environment = plan["payload_bindings"]["candidate_environment"]
            active = plan["payload_bindings"]["active_release_binding"]
            if evidence != {
                "installed_sha256": expected_install,
                "post_install_verified": True,
                "live_environment": {
                    "canonical_path": environment["canonical_path"],
                    "installed_sha256": environment["sha256"],
                    "mode": "0600",
                    "uid": os.geteuid(),
                    "nlink": 1,
                    "post_install_verified": True,
                },
                "active_release_binding": {
                    "canonical_path": active["canonical_path"],
                    "installed_sha256": active["sha256"],
                    "mode": "0600",
                    "uid": os.geteuid(),
                    "nlink": 1,
                    "post_install_verified": True,
                },
            }:
                raise ProductionCutoverError(
                    "production_cutover_install_evidence_invalid"
                )
    elif evidence:
        raise ProductionCutoverError("production_cutover_step_evidence_unexpected")
    return {
        **dict(value),
        "commands": commands,
        "after_identity_sha256": after,
    }


def _observe_identity(adapter: CutoverSystemAdapter) -> tuple[Mapping[str, Any], str]:
    observed = dict(adapter.observe_live_identity())
    return observed, _sha256_json(observed)


def _rollback(
    *,
    journal: _Journal,
    adapter: CutoverSystemAdapter,
    lease: CutoverLease,
    lease_token: str,
    plan: Mapping[str, Any],
    snapshot: Mapping[str, Any],
    current_identity_sha256: str,
    reason: str,
    command_template: Sequence[Sequence[str]],
) -> Mapping[str, Any]:
    intent_path = journal.root / "rollback.intent.json"
    done_path = journal.root / "rollback.done.json"
    expected = plan["bindings"]["rollback_live_identity_sha256"]
    try:
        planned_commands = _preflight_adapter_step(
            adapter,
            step="rollback",
            expected_identity_sha256=current_identity_sha256,
            plan=plan,
            payload_descriptors={},
            lease_fingerprint=lease.fingerprint,
            lease_token=lease_token,
        )
        if [list(item) for item in command_template] != planned_commands:
            raise ProductionCutoverError("production_cutover_rollback_command_drift")
        intent = {
            "schema_version": ROLLBACK_INTENT_SCHEMA_VERSION,
            "plan_sha256": journal.plan_sha256,
            "reason": reason,
            "snapshot_sha256": _sha256_json(snapshot),
            "before_identity_sha256": current_identity_sha256,
            "target_identity_sha256": expected,
            "planned_commands": planned_commands,
            "lease_fingerprint": lease.fingerprint,
            "lease_token_sha256": hashlib.sha256(
                lease_token.encode("utf-8")
            ).hexdigest(),
        }
        if intent_path.exists():
            existing_intent = _read_owned_json(
                intent_path, artifact="production_cutover_rollback_intent"
            ).body
            if (
                existing_intent.get("schema_version") != ROLLBACK_INTENT_SCHEMA_VERSION
                or existing_intent.get("plan_sha256") != journal.plan_sha256
                or existing_intent.get("snapshot_sha256") != _sha256_json(snapshot)
                or existing_intent.get("target_identity_sha256") != expected
                or existing_intent.get("planned_commands") != planned_commands
                or existing_intent.get("lease_fingerprint") != lease.fingerprint
                or existing_intent.get("lease_token_sha256")
                != intent["lease_token_sha256"]
            ):
                raise ProductionCutoverError(
                    "production_cutover_rollback_intent_conflict"
                )
            intent = dict(existing_intent)
        else:
            _publish_no_clobber(intent_path, intent)

        _live, observed_before = _observe_identity(adapter)
        if observed_before == expected:
            receipt = {
                "schema_version": ROLLBACK_SCHEMA_VERSION,
                "ok": True,
                "reason": intent["reason"],
                "plan_sha256": journal.plan_sha256,
                "before_identity_sha256": intent["before_identity_sha256"],
                "restored_identity_sha256": expected,
                "old_runtime_retained": True,
                "commands": planned_commands,
                "recovered_after_effect": True,
                "snapshot_sha256": _sha256_json(snapshot),
                "restored_components": sorted(snapshot["components"]),
            }
            _publish_no_clobber(done_path, receipt)
            _publish_no_clobber(journal.root / "rollback.json", receipt)
            return receipt
        lease.assert_active()
        raw = adapter.rollback(
            snapshot=snapshot,
            expected_identity_sha256=observed_before,
            plan=plan,
            planned_commands=planned_commands,
            lease_fingerprint=lease.fingerprint,
            lease_token=lease_token,
        )
        normalized = _validate_step_result(
            raw,
            step="rollback",
            expected_before=observed_before,
            plan=plan,
            planned_commands=planned_commands,
        )
        _observed, after = _observe_identity(adapter)
        if normalized["after_identity_sha256"] != expected or after != expected:
            raise ProductionCutoverError(
                "production_cutover_rollback_identity_mismatch"
            )
        receipt = {
            "schema_version": ROLLBACK_SCHEMA_VERSION,
            "ok": True,
            "reason": reason,
            "plan_sha256": journal.plan_sha256,
            "before_identity_sha256": intent["before_identity_sha256"],
            "restored_identity_sha256": expected,
            "old_runtime_retained": True,
            "commands": normalized["commands"],
            "recovered_after_effect": False,
            "snapshot_sha256": _sha256_json(snapshot),
            "restored_components": sorted(snapshot["components"]),
        }
        _publish_no_clobber(done_path, receipt)
        _publish_no_clobber(journal.root / "rollback.json", receipt)
        return receipt
    except Exception as exc:
        raise ProductionCutoverError("production_cutover_rollback_failed") from exc


def _forward_run_identity(
    plan: Mapping[str, Any],
    *,
    lease_identity: Mapping[str, Any],
) -> Mapping[str, Any]:
    return {
        "schema_version": JOURNAL_IDENTITY_SCHEMA_VERSION,
        "release_id": plan["release_id"],
        "plan_sha256": _sha256_json(plan),
        "authorization_receipt_sha256": plan["bindings"][
            "cutover_authorization_receipt_sha256"
        ],
        "expected_live_identity_sha256": plan["bindings"][
            "expected_live_identity_sha256"
        ],
        "target_live_identity_sha256": plan["bindings"]["target_live_identity_sha256"],
        "rollback_live_identity_sha256": plan["bindings"][
            "rollback_live_identity_sha256"
        ],
        "forward_lease": dict(lease_identity),
    }


def _load_historical_recovery_context(journal_root: Path) -> Mapping[str, Any]:
    root = _ensure_journal_root(journal_root)
    plan_owned = _read_owned_json(
        root / "plan.json", artifact="production_recovery_plan"
    )
    run_owned = _read_owned_json(
        root / "run-identity.json", artifact="production_recovery_run_identity"
    )
    plan = plan_owned.body
    run_identity = run_owned.body
    if (
        plan.get("schema_version") != PLAN_SCHEMA_VERSION
        or run_identity.get("schema_version") != JOURNAL_IDENTITY_SCHEMA_VERSION
        or run_identity.get("plan_sha256") != _sha256_json(plan)
        or run_identity.get("release_id") != plan.get("release_id")
        or run_identity.get("rollback_live_identity_sha256")
        != plan.get("bindings", {}).get("rollback_live_identity_sha256")
        or not isinstance(run_identity.get("forward_lease"), Mapping)
    ):
        raise ProductionCutoverError("production_recovery_history_invalid")
    snapshot_owned = _read_owned_json(
        root / "steps" / "01-snapshot_live.done.json",
        artifact="production_recovery_snapshot_done",
    )
    snapshot = snapshot_owned.body.get("result", {}).get("snapshot")
    if (
        snapshot_owned.body.get("schema_version") != STEP_RESULT_SCHEMA_VERSION
        or snapshot_owned.body.get("plan_sha256") != _sha256_json(plan)
        or not isinstance(snapshot, Mapping)
        or snapshot.get("schema_version") != SNAPSHOT_SCHEMA_VERSION
        or snapshot.get("rollback_target_identity_sha256")
        != run_identity["rollback_live_identity_sha256"]
        or set(snapshot.get("components", {}))
        != {
            "runtime",
            "workspace",
            "environment",
            "plists",
            "services",
            "feishu_sidecar",
            "active_release_binding",
        }
    ):
        raise ProductionCutoverError("production_recovery_snapshot_invalid")
    return {
        "root": root,
        "plan_owned": plan_owned,
        "run_owned": run_owned,
        "snapshot_owned": snapshot_owned,
        "plan": plan,
        "run_identity": run_identity,
        "snapshot": snapshot,
    }


def _validate_recovery_authorization(
    path: Path,
    *,
    context: Mapping[str, Any],
    lease_identity: Mapping[str, Any],
    machine_identity_sha256: str,
    now: datetime,
) -> Mapping[str, Any]:
    owned = _read_owned_json(path, artifact="production_recovery_authorization")
    body = owned.body
    if set(body) != {
        "schema_version",
        "release_id",
        "decision",
        "created_at",
        "expires_at",
        "nonce",
        "bindings",
        "identity",
    }:
        raise ProductionCutoverError("production_recovery_authorization_shape_invalid")
    identity = body.get("identity")
    if (
        body.get("schema_version") != RECOVERY_AUTHORIZATION_SCHEMA_VERSION
        or body.get("decision") != RECOVERY_AUTHORIZATION_DECISION
        or body.get("release_id") != context["plan"].get("release_id")
        or not isinstance(identity, Mapping)
        or set(identity)
        != {
            "schema_version",
            "uid",
            "username",
            "machine_identity_sha256",
        }
        or identity.get("schema_version") != AUTHORIZATION_IDENTITY_SCHEMA_VERSION
        or identity.get("uid") != os.geteuid()
        or not isinstance(identity.get("username"), str)
        or not identity["username"].strip()
        or identity.get("machine_identity_sha256") != machine_identity_sha256
    ):
        raise ProductionCutoverError(
            "production_recovery_authorization_identity_invalid"
        )
    nonce = body.get("nonce")
    if not isinstance(nonce, str) or NONCE_RE.fullmatch(nonce) is None:
        raise ProductionCutoverError("production_recovery_authorization_nonce_invalid")
    forward_lease = context["run_identity"]["forward_lease"]
    recovery_holder = lease_identity["holder"]
    forward_holder = forward_lease.get("holder")
    if (
        not isinstance(forward_holder, Mapping)
        or lease_identity["fingerprint"] == forward_lease.get("fingerprint")
        or lease_identity["token_sha256"] == forward_lease.get("token_sha256")
        or recovery_holder.get("pid") == forward_holder.get("pid")
    ):
        raise ProductionCutoverError("production_recovery_lease_not_new")
    expected_bindings = {
        "original_plan_sha256": _sha256_json(context["plan"]),
        "journal_root": str(context["root"].resolve()),
        "run_identity_sha256": context["run_owned"].sha256,
        "snapshot_sha256": _sha256_json(context["snapshot"]),
        "rollback_target_identity_sha256": context["run_identity"][
            "rollback_live_identity_sha256"
        ],
        "forward_lease_fingerprint": forward_lease["fingerprint"],
        "forward_lease_token_sha256": forward_lease["token_sha256"],
        "forward_holder_sha256": forward_lease["holder_sha256"],
        "recovery_lease_fingerprint": lease_identity["fingerprint"],
        "recovery_lease_token_sha256": lease_identity["token_sha256"],
        "recovery_holder_sha256": lease_identity["holder_sha256"],
        "recovery_pid": recovery_holder["pid"],
        "machine_identity_sha256": machine_identity_sha256,
    }
    if body.get("bindings") != expected_bindings:
        raise ProductionCutoverError(
            "production_recovery_authorization_binding_mismatch"
        )
    created = _parse_time(
        body.get("created_at"), code="production_recovery_authorization_time_invalid"
    )
    expires = _parse_time(
        body.get("expires_at"), code="production_recovery_authorization_time_invalid"
    )
    current = now.astimezone(timezone.utc)
    if (
        expires <= created
        or (expires - created).total_seconds()
        > MAX_RECOVERY_AUTHORIZATION_VALIDITY_SECONDS
        or created - current > timedelta(seconds=MAX_FUTURE_SKEW_SECONDS)
        or current >= expires
    ):
        raise ProductionCutoverError("production_recovery_authorization_expired")
    return {
        "release_id": body["release_id"],
        "receipt_sha256": owned.sha256,
        "nonce": nonce,
        "expires_at": expires.isoformat(),
        "bindings": expected_bindings,
        "machine_identity_sha256": machine_identity_sha256,
    }


def _consume_recovery_nonce(
    *,
    ledger_root: Path,
    context: Mapping[str, Any],
    authorization: Mapping[str, Any],
) -> None:
    root = _ensure_nonce_ledger_root(ledger_root)
    nonce = authorization["nonce"]
    key = hashlib.sha256(f"recovery:{nonce}".encode("utf-8")).hexdigest()
    body = {
        "schema_version": NONCE_CONSUMPTION_SCHEMA_VERSION,
        "purpose": "production_recovery",
        "nonce_sha256": hashlib.sha256(nonce.encode("utf-8")).hexdigest(),
        "authorization_receipt_sha256": authorization["receipt_sha256"],
        "original_plan_sha256": _sha256_json(context["plan"]),
        "journal_root": str(context["root"].resolve()),
        "recovery_lease_fingerprint": authorization["bindings"][
            "recovery_lease_fingerprint"
        ],
        "recovery_holder_sha256": authorization["bindings"]["recovery_holder_sha256"],
    }
    _publish_no_clobber(root / f"recovery-{key}.json", body)


def apply_cutover(
    inputs: CutoverInputs,
    *,
    lease: CutoverLease,
    adapter: CutoverSystemAdapter,
    gate_validator: GateValidator,
    now: datetime | None = None,
    clock: Callable[[], datetime] | None = None,
    machine_identity_provider: MachineIdentityProvider = _default_machine_identity_sha256,
    nonce_ledger_root: Path = CANONICAL_NONCE_LEDGER_ROOT,
) -> CutoverResult:
    """Execute an exact cutover through explicit injected production boundaries."""
    if lease is None or adapter is None or gate_validator is None:
        raise ProductionCutoverError("production_cutover_apply_dependencies_required")
    if now is not None:
        raise ProductionCutoverError("production_cutover_fixed_now_forbidden")
    lease_identity = _lease_execution_identity(lease)
    lease_token = lease.token

    def current_time() -> datetime:
        observed = clock() if clock is not None else datetime.now(timezone.utc)
        if observed.tzinfo is None or observed.utcoffset() is None:
            raise ProductionCutoverError("production_cutover_clock_invalid")
        return observed.astimezone(timezone.utc)

    machine_identity_sha256 = machine_identity_provider()

    def assert_machine_identity() -> None:
        if machine_identity_provider() != machine_identity_sha256:
            raise ProductionCutoverError("production_cutover_machine_identity_drift")

    started_at = current_time()
    initial_bundle = _load_artifacts(inputs)
    plan = _build_plan_from_bundle(
        inputs,
        initial_bundle,
        gate_validator=gate_validator,
        machine_identity_sha256=machine_identity_sha256,
        now=started_at,
    )
    if lease.fingerprint != inputs.cutover_lease_fingerprint:
        raise ProductionCutoverError("production_cutover_lease_fingerprint_mismatch")
    journal = _Journal(
        inputs.journal_root,
        plan=plan,
        run_identity=_forward_run_identity(plan, lease_identity=lease_identity),
    )
    with lease:
        lease.assert_active()
        with journal.lock():
            # Exact inputs are read and gate-validated again under the global lease.
            locked_bundle = _load_artifacts(inputs)
            locked_plan = _build_plan_from_bundle(
                inputs,
                locked_bundle,
                gate_validator=gate_validator,
                machine_identity_sha256=machine_identity_sha256,
                now=current_time(),
            )
            if locked_plan != plan or locked_bundle.sha256 != initial_bundle.sha256:
                raise ProductionCutoverError("production_cutover_input_drift")
            lease.assert_active()
            assert_machine_identity()
            locked_authorization = _validate_local_artifact_chain(
                locked_bundle,
                lease_fingerprint=inputs.cutover_lease_fingerprint,
                machine_identity_sha256=machine_identity_sha256,
                now=current_time(),
            )
            _consume_authorization_nonce(
                ledger_root=nonce_ledger_root,
                journal_root=journal.root,
                plan=plan,
                authorization=locked_authorization,
            )
            complete_path = journal.root / "complete.json"
            if complete_path.exists():
                complete = _read_owned_json(
                    complete_path, artifact="production_cutover_complete"
                ).body
                _observed, live_sha = _observe_identity(adapter)
                if (
                    complete.get("schema_version") != COMPLETE_SCHEMA_VERSION
                    or complete.get("plan_sha256") != journal.plan_sha256
                    or live_sha != plan["bindings"]["target_live_identity_sha256"]
                ):
                    raise ProductionCutoverError("production_cutover_complete_conflict")
                return CutoverResult("apply", complete, resumed=True)
            rollback_done_path = journal.root / "rollback.done.json"
            if rollback_done_path.exists():
                rollback_done = _read_owned_json(
                    rollback_done_path,
                    artifact="production_cutover_rollback_done",
                ).body
                _live, restored_identity = _observe_identity(adapter)
                if (
                    rollback_done.get("schema_version") != ROLLBACK_SCHEMA_VERSION
                    or rollback_done.get("ok") is not True
                    or rollback_done.get("plan_sha256") != journal.plan_sha256
                    or rollback_done.get("restored_identity_sha256")
                    != plan["bindings"]["rollback_live_identity_sha256"]
                    or rollback_done.get("restored_components")
                    != sorted({
                        "runtime",
                        "workspace",
                        "environment",
                        "plists",
                        "services",
                        "feishu_sidecar",
                        "active_release_binding",
                    })
                    or restored_identity
                    != plan["bindings"]["rollback_live_identity_sha256"]
                ):
                    raise ProductionCutoverError(
                        "production_cutover_rollback_done_conflict"
                    )
                _publish_no_clobber(journal.root / "rollback.json", rollback_done)
                raise ProductionCutoverError(
                    "production_cutover_recovery_authorization_required"
                )
            if (journal.root / "rollback.json").exists():
                raise ProductionCutoverError(
                    "production_cutover_recovery_authorization_required"
                )
            failure_path = journal.root / "failure.json"
            rollback_intent_path = journal.root / "rollback.intent.json"
            if failure_path.exists() or rollback_intent_path.exists():
                raise ProductionCutoverError(
                    "production_cutover_recovery_authorization_required"
                )
            incomplete = journal.incomplete()
            if incomplete is not None:
                if incomplete == (1, "snapshot_live"):
                    raise ProductionCutoverError(
                        "production_cutover_incomplete_snapshot_requires_new_authorization"
                    )
                raise ProductionCutoverError(
                    "production_cutover_recovery_authorization_required"
                )

            baseline_payloads = _validate_candidate_payloads(locked_bundle)
            command_templates = {
                step: _preflight_adapter_step(
                    adapter,
                    step=step,
                    expected_identity_sha256=plan["bindings"][
                        "expected_live_identity_sha256"
                    ],
                    plan=plan,
                    payload_descriptors=_payloads_for_step(step, baseline_payloads),
                    lease_fingerprint=lease.fingerprint,
                    lease_token=lease_token,
                )
                for step in STEP_NAMES
            }
            rollback_command_template = _preflight_adapter_step(
                adapter,
                step="rollback",
                expected_identity_sha256=plan["bindings"][
                    "expected_live_identity_sha256"
                ],
                plan=plan,
                payload_descriptors={},
                lease_fingerprint=lease.fingerprint,
                lease_token=lease_token,
            )

            expected_identity = plan["bindings"]["expected_live_identity_sha256"]
            snapshot: Mapping[str, Any] | None = None
            any_mutation_intended = False
            resumed = False
            prior_step_receipt: Mapping[str, Any] | None = None
            first_pending = 1
            for index, step in enumerate(STEP_NAMES, 1):
                existing = journal.read_done(index, step)
                if existing is None:
                    first_pending = index
                    if any(
                        journal.read_done(later_index, later_step) is not None
                        for later_index, later_step in enumerate(
                            STEP_NAMES[index:], index + 1
                        )
                    ):
                        raise ProductionCutoverError(
                            "production_cutover_step_journal_gap"
                        )
                    break
                if not journal.intent_path(index, step).exists() or (
                    existing.get("schema_version") != STEP_RESULT_SCHEMA_VERSION
                    or existing.get("step") != step
                    or existing.get("plan_sha256") != journal.plan_sha256
                ):
                    raise ProductionCutoverError(
                        "production_cutover_step_journal_invalid"
                    )
                intent_body = _read_owned_json(
                    journal.intent_path(index, step),
                    artifact="production_cutover_step_intent",
                ).body
                expected_step_payloads = _payloads_for_step(step, baseline_payloads)
                if (
                    intent_body.get("schema_version") != STEP_INTENT_SCHEMA_VERSION
                    or intent_body.get("plan_sha256") != journal.plan_sha256
                    or intent_body.get("index") != index
                    or intent_body.get("step") != step
                    or intent_body.get("planned_commands") != command_templates[step]
                    or intent_body.get("payload_descriptors") != expected_step_payloads
                    or intent_body.get("lease_fingerprint") != lease.fingerprint
                    or intent_body.get("lease_token_sha256")
                    != hashlib.sha256(lease_token.encode("utf-8")).hexdigest()
                ):
                    raise ProductionCutoverError(
                        "production_cutover_step_journal_invalid"
                    )
                normalized = _validate_step_result(
                    existing.get("result", {}),
                    step=step,
                    expected_before=expected_identity,
                    plan=plan,
                    planned_commands=command_templates[step],
                )
                expected_identity = normalized["after_identity_sha256"]
                if step == "snapshot_live":
                    snapshot = normalized["snapshot"]
                if step in MUTATING_STEPS:
                    any_mutation_intended = True
                prior_step_receipt = existing
                resumed = True
            else:
                first_pending = len(STEP_NAMES) + 1
            _observed, observed_identity = _observe_identity(adapter)
            if observed_identity != expected_identity:
                raise ProductionCutoverError("production_cutover_live_identity_drift")
            try:
                for index, step in enumerate(
                    STEP_NAMES[first_pending - 1 :], first_pending
                ):
                    lease.assert_active()
                    assert_machine_identity()
                    # Revalidate all exact input bytes and the live CAS immediately
                    # before every external action.
                    current_bundle = _load_artifacts(inputs)
                    if current_bundle.sha256 != locked_bundle.sha256:
                        raise ProductionCutoverError("production_cutover_input_drift")
                    _live, live_sha = _observe_identity(adapter)
                    if live_sha != expected_identity:
                        raise ProductionCutoverError(
                            "production_cutover_live_identity_drift"
                        )
                    _authorize_step(
                        inputs,
                        current_bundle,
                        plan=plan,
                        gate_validator=gate_validator,
                        machine_identity_sha256=machine_identity_sha256,
                        requested_step=step,
                        live_identity_sha256=live_sha,
                        prior_step_receipt=prior_step_receipt,
                        now=current_time(),
                    )
                    current_payloads = baseline_payloads
                    if _payloads_for_step(step, baseline_payloads):
                        current_payloads = _validate_candidate_payloads(current_bundle)
                        if current_payloads != baseline_payloads:
                            raise ProductionCutoverError(
                                "production_cutover_payload_drift"
                            )
                    step_payloads = _payloads_for_step(step, current_payloads)
                    planned_commands = _preflight_adapter_step(
                        adapter,
                        step=step,
                        expected_identity_sha256=expected_identity,
                        plan=plan,
                        payload_descriptors=step_payloads,
                        lease_fingerprint=lease.fingerprint,
                        lease_token=lease_token,
                    )
                    if planned_commands != command_templates[step]:
                        raise ProductionCutoverError(
                            "production_cutover_step_command_drift"
                        )
                    intent = {
                        "schema_version": STEP_INTENT_SCHEMA_VERSION,
                        "plan_sha256": journal.plan_sha256,
                        "index": index,
                        "step": step,
                        "mutating": step in MUTATING_STEPS,
                        "expected_identity_sha256": expected_identity,
                        "planned_commands": planned_commands,
                        "payload_descriptors": step_payloads,
                        "lease_fingerprint": lease.fingerprint,
                        "lease_token_sha256": hashlib.sha256(
                            lease_token.encode("utf-8")
                        ).hexdigest(),
                    }
                    _publish_no_clobber(journal.intent_path(index, step), intent)
                    if step in MUTATING_STEPS:
                        any_mutation_intended = True
                    lease.assert_active()
                    raw_result = adapter.execute_step(
                        step,
                        expected_identity_sha256=expected_identity,
                        plan=plan,
                        planned_commands=planned_commands,
                        payload_descriptors=step_payloads,
                        lease_fingerprint=lease.fingerprint,
                        lease_token=lease_token,
                    )
                    normalized = _validate_step_result(
                        raw_result,
                        step=step,
                        expected_before=expected_identity,
                        plan=plan,
                        planned_commands=planned_commands,
                    )
                    _after_body, observed_after = _observe_identity(adapter)
                    if observed_after != normalized["after_identity_sha256"]:
                        raise ProductionCutoverError(
                            "production_cutover_step_identity_mismatch"
                        )
                    done = {
                        "schema_version": STEP_RESULT_SCHEMA_VERSION,
                        "plan_sha256": journal.plan_sha256,
                        "index": index,
                        "step": step,
                        "result": normalized,
                    }
                    _publish_no_clobber(journal.done_path(index, step), done)
                    expected_identity = normalized["after_identity_sha256"]
                    if step == "snapshot_live":
                        snapshot = normalized["snapshot"]
                    prior_step_receipt = done
                if expected_identity != plan["bindings"]["target_live_identity_sha256"]:
                    raise ProductionCutoverError(
                        "production_cutover_target_identity_mismatch"
                    )
                lease.assert_active()
                assert_machine_identity()
                final_bundle = _load_artifacts(inputs)
                if final_bundle.sha256 != locked_bundle.sha256:
                    raise ProductionCutoverError("production_cutover_input_drift")
                if _validate_candidate_payloads(final_bundle) != baseline_payloads:
                    raise ProductionCutoverError("production_cutover_payload_drift")
                complete = {
                    "schema_version": COMPLETE_SCHEMA_VERSION,
                    "ok": True,
                    "release_id": plan["release_id"],
                    "plan_sha256": journal.plan_sha256,
                    "authorization_receipt_sha256": plan["bindings"][
                        "cutover_authorization_receipt_sha256"
                    ],
                    "final_live_identity_sha256": expected_identity,
                    "old_runtime_retained": True,
                    "service_count": len(SERVICE_LABELS),
                    "candidate_env_sha256": plan["bindings"]["candidate_env_sha256"],
                    "active_release_binding_path": plan["payload_bindings"][
                        "active_release_binding"
                    ]["canonical_path"],
                    "active_release_binding_sha256": plan["payload_bindings"][
                        "active_release_binding"
                    ]["sha256"],
                }
                _publish_no_clobber(complete_path, complete)
                return CutoverResult("apply", complete, resumed=resumed)
            except Exception as exc:
                failure = {
                    "schema_version": FAILURE_SCHEMA_VERSION,
                    "ok": False,
                    "plan_sha256": journal.plan_sha256,
                    "code": (
                        exc.code
                        if isinstance(exc, ProductionCutoverError)
                        else "production_cutover_external_step_failed"
                    ),
                    "rollback_required": any_mutation_intended,
                }
                _publish_no_clobber(journal.root / "failure.json", failure)
                if any_mutation_intended:
                    if snapshot is None:
                        raise ProductionCutoverError(
                            "production_cutover_snapshot_missing"
                        ) from exc
                    _live, rollback_from = _observe_identity(adapter)
                    _rollback(
                        journal=journal,
                        adapter=adapter,
                        lease=lease,
                        lease_token=lease_token,
                        plan=plan,
                        snapshot=snapshot,
                        current_identity_sha256=rollback_from,
                        reason=failure["code"],
                        command_template=rollback_command_template,
                    )
                    raise ProductionCutoverError(
                        "production_cutover_apply_failed_rolled_back"
                    ) from exc
                raise


def recover_cutover(
    journal_root: Path,
    *,
    recovery_authorization_receipt: Path,
    lease: CutoverLease,
    adapter: CutoverSystemAdapter,
    clock: Callable[[], datetime] | None = None,
    machine_identity_provider: MachineIdentityProvider = _default_machine_identity_sha256,
    nonce_ledger_root: Path = CANONICAL_NONCE_LEDGER_ROOT,
) -> CutoverResult:
    """Restore only the historical snapshot under a new recovery authority."""
    if lease is None or adapter is None:
        raise ProductionCutoverError("production_recovery_dependencies_required")

    def current_time() -> datetime:
        observed = clock() if clock is not None else datetime.now(timezone.utc)
        if observed.tzinfo is None or observed.utcoffset() is None:
            raise ProductionCutoverError("production_recovery_clock_invalid")
        return observed.astimezone(timezone.utc)

    machine_identity_sha256 = machine_identity_provider()
    lease_identity = _lease_execution_identity(lease)
    initial = _load_historical_recovery_context(journal_root)
    authorization = _validate_recovery_authorization(
        recovery_authorization_receipt,
        context=initial,
        lease_identity=lease_identity,
        machine_identity_sha256=machine_identity_sha256,
        now=current_time(),
    )
    journal = _Journal(
        initial["root"],
        plan=initial["plan"],
        run_identity=initial["run_identity"],
    )
    with lease:
        lease.assert_active()
        with journal.lock():
            locked = _load_historical_recovery_context(journal_root)
            if (
                locked["plan_owned"].raw != initial["plan_owned"].raw
                or locked["run_owned"].raw != initial["run_owned"].raw
                or locked["snapshot_owned"].raw != initial["snapshot_owned"].raw
            ):
                raise ProductionCutoverError("production_recovery_history_drift")
            lease.assert_active()
            if machine_identity_provider() != machine_identity_sha256:
                raise ProductionCutoverError(
                    "production_recovery_machine_identity_drift"
                )
            locked_authorization = _validate_recovery_authorization(
                recovery_authorization_receipt,
                context=locked,
                lease_identity=lease_identity,
                machine_identity_sha256=machine_identity_sha256,
                now=current_time(),
            )
            if locked_authorization != authorization:
                raise ProductionCutoverError("production_recovery_authorization_drift")
            if (journal.root / "complete.json").exists():
                raise ProductionCutoverError("production_recovery_forward_complete")
            incomplete = journal.incomplete()
            mutation_intended = any(
                journal.intent_path(index, step).exists()
                for index, step in enumerate(STEP_NAMES, 1)
                if step in MUTATING_STEPS
            )
            if (
                not any(
                    path.exists()
                    for path in (
                        journal.root / "failure.json",
                        journal.root / "rollback.intent.json",
                    )
                )
                and incomplete is None
                and not mutation_intended
            ):
                raise ProductionCutoverError("production_recovery_not_required")
            _consume_recovery_nonce(
                ledger_root=nonce_ledger_root,
                context=locked,
                authorization=authorization,
            )

            recovery_root = journal.root / "recovery"
            try:
                os.mkdir(recovery_root, 0o700)
                _fsync_directory(journal.root)
            except FileExistsError:
                pass
            info = recovery_root.lstat()
            if (
                stat.S_ISLNK(info.st_mode)
                or not stat.S_ISDIR(info.st_mode)
                or stat.S_IMODE(info.st_mode) != 0o700
                or info.st_uid != os.geteuid()
            ):
                raise ProductionCutoverError("production_recovery_journal_invalid")
            attempt = authorization["receipt_sha256"]
            intent_path = recovery_root / f"{attempt}.intent.json"
            done_path = recovery_root / f"{attempt}.done.json"
            target = locked["run_identity"]["rollback_live_identity_sha256"]
            if done_path.exists():
                done = _read_owned_json(
                    done_path, artifact="production_recovery_done"
                ).body
                _live, current = _observe_identity(adapter)
                if (
                    done.get("schema_version") != RECOVERY_DONE_SCHEMA_VERSION
                    or done.get("ok") is not True
                    or done.get("recovery_authorization_receipt_sha256")
                    != authorization["receipt_sha256"]
                    or done.get("original_plan_sha256") != _sha256_json(locked["plan"])
                    or done.get("snapshot_sha256") != _sha256_json(locked["snapshot"])
                    or done.get("recovery_lease") != lease_identity
                    or done.get("restored_identity_sha256") != target
                    or done.get("forward_steps_executed") is not False
                    or current != target
                ):
                    raise ProductionCutoverError("production_recovery_done_conflict")
                return CutoverResult("recover", done, resumed=True)
            _live, before = _observe_identity(adapter)
            planned_commands = _preflight_adapter_step(
                adapter,
                step="rollback",
                expected_identity_sha256=before,
                plan=locked["plan"],
                payload_descriptors={},
                lease_fingerprint=lease.fingerprint,
                lease_token=lease.token,
            )
            intent = {
                "schema_version": RECOVERY_INTENT_SCHEMA_VERSION,
                "recovery_authorization_receipt_sha256": authorization[
                    "receipt_sha256"
                ],
                "original_plan_sha256": _sha256_json(locked["plan"]),
                "run_identity_sha256": locked["run_owned"].sha256,
                "snapshot_sha256": _sha256_json(locked["snapshot"]),
                "rollback_target_identity_sha256": locked["run_identity"][
                    "rollback_live_identity_sha256"
                ],
                "recovery_lease": dict(lease_identity),
                "before_identity_sha256": before,
                "planned_commands": planned_commands,
                "forward_steps_forbidden": True,
            }
            intent_created = _publish_no_clobber(intent_path, intent)
            executed = False
            if before != target:
                lease.assert_active()
                raw = adapter.rollback(
                    snapshot=locked["snapshot"],
                    expected_identity_sha256=before,
                    plan=locked["plan"],
                    planned_commands=planned_commands,
                    lease_fingerprint=lease.fingerprint,
                    lease_token=lease.token,
                )
                normalized = _validate_step_result(
                    raw,
                    step="rollback",
                    expected_before=before,
                    plan=locked["plan"],
                    planned_commands=planned_commands,
                )
                if normalized["after_identity_sha256"] != target:
                    raise ProductionCutoverError(
                        "production_recovery_rollback_identity_mismatch"
                    )
                executed = True
            _observed, after = _observe_identity(adapter)
            if after != target:
                raise ProductionCutoverError(
                    "production_recovery_rollback_identity_mismatch"
                )
            rollback_done_path = journal.root / "rollback.done.json"
            if rollback_done_path.exists():
                rollback_receipt = _read_owned_json(
                    rollback_done_path,
                    artifact="production_recovery_existing_rollback_done",
                ).body
                if (
                    rollback_receipt.get("schema_version") != ROLLBACK_SCHEMA_VERSION
                    or rollback_receipt.get("ok") is not True
                    or rollback_receipt.get("plan_sha256")
                    != _sha256_json(locked["plan"])
                    or rollback_receipt.get("restored_identity_sha256") != target
                    or rollback_receipt.get("snapshot_sha256")
                    != _sha256_json(locked["snapshot"])
                ):
                    raise ProductionCutoverError(
                        "production_recovery_existing_rollback_conflict"
                    )
            else:
                rollback_receipt = {
                    "schema_version": ROLLBACK_SCHEMA_VERSION,
                    "ok": True,
                    "reason": "separately_authorized_recovery",
                    "plan_sha256": _sha256_json(locked["plan"]),
                    "before_identity_sha256": before,
                    "restored_identity_sha256": target,
                    "old_runtime_retained": True,
                    "commands": planned_commands,
                    "recovered_after_effect": not executed,
                    "snapshot_sha256": _sha256_json(locked["snapshot"]),
                    "restored_components": sorted(locked["snapshot"]["components"]),
                    "recovery_authorization_receipt_sha256": authorization[
                        "receipt_sha256"
                    ],
                    "recovery_lease_fingerprint": lease.fingerprint,
                }
                _publish_no_clobber(rollback_done_path, rollback_receipt)
            _publish_no_clobber(journal.root / "rollback.json", rollback_receipt)
            done = {
                "schema_version": RECOVERY_DONE_SCHEMA_VERSION,
                "ok": True,
                "recovery_authorization_receipt_sha256": authorization[
                    "receipt_sha256"
                ],
                "original_plan_sha256": _sha256_json(locked["plan"]),
                "snapshot_sha256": _sha256_json(locked["snapshot"]),
                "recovery_lease": dict(lease_identity),
                "restored_identity_sha256": target,
                "rollback_executed": executed,
                "forward_steps_executed": False,
            }
            _publish_no_clobber(done_path, done)
            return CutoverResult("recover", done, resumed=not intent_created)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=("plan", "validate"))
    for field in ARTIFACT_FIELDS:
        parser.add_argument(f"--{field.replace('_', '-')}", type=Path, required=True)
    parser.add_argument("--cutover-lease-fingerprint", required=True)
    parser.add_argument("--journal-root", type=Path, required=True)
    parser.add_argument("--plan-file", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    try:
        args = _parser().parse_args(argv)
        inputs = CutoverInputs(
            **{field: getattr(args, field) for field in ARTIFACT_FIELDS},
            cutover_lease_fingerprint=args.cutover_lease_fingerprint,
            journal_root=args.journal_root,
        )
        plan = build_cutover_plan(inputs)
        if args.phase == "validate":
            if args.plan_file is None:
                raise ProductionCutoverError("production_cutover_plan_file_required")
            supplied = _read_owned_json(
                args.plan_file, artifact="production_cutover_plan"
            ).body
            body = validate_cutover_plan(inputs, supplied)
        else:
            body = plan
        print(_canonical_json(body).decode("utf-8"), end="")
        return 0
    except ProductionCutoverError as exc:
        print(
            _canonical_json({"ok": False, "code": exc.code}).decode("utf-8"),
            end="",
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
