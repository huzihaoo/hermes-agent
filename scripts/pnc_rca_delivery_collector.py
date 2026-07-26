#!/usr/bin/env python3
"""Collect VM terminal truth into durable, send-free RCA delivery records."""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path, PurePosixPath
import signal
import socket
import sqlite3
import subprocess
import sys
import textwrap
import threading
import time
from typing import Any, Callable, Mapping

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from dotenv import load_dotenv

from gateway.pnc_rca_admission import (
    RCA_KAFKA_TRIGGER_KINDS,
    RCA_MANUAL_TRIGGER_KINDS,
    build_rca_admission,
    validate_rca_admission,
    validate_rca_trigger_context,
)
from gateway import pnc_rca_capacity_transition as capacity_transition
from gateway.pnc_rca_capacity_runtime import (
    CapacityRuntimePaths,
    load_capacity_hmac_key,
)
from gateway.pnc_rca_capacity_sample_evidence import (
    CapacitySampleEvidenceError,
    TERMINAL_HMAC_ENV,
    build_capacity_sample,
    ensure_owner_only_lock_file,
    host_success_receipt_path,
    producer_activation_path,
    read_and_validate_producer_activation,
    read_remote_vm_terminal_receipt,
    validate_host_success_receipt,
    write_owner_only_create_once,
)
from gateway.pnc_rca_control_store import RcaControlStore, RecordConflictError
from gateway.pnc_rca_delivery_contract import (
    TERMINAL_FALLBACK_CONTRACT_SCHEMA_VERSION,
    TERMINAL_DELIVERY_OUTCOMES,
    DeliveryContractError,
    VerifiedDelivery,
    canonical_artifact_root,
    verify_delivery_bundle,
)
from gateway.pnc_rca_delivery_quarantine_baseline import (
    disabled_quarantine_baseline_status,
    quarantine_baseline_settings,
    read_quarantine_baseline_status,
)
from gateway.pnc_rca_delivery_store import (
    DeliveryRecordConflictError,
    ExecutionWatchClaim,
    RcaDeliveryStore,
    StaleDeliveryWatchLeaseError,
)
from gateway.pnc_rca_kafka_contract import NORMALIZED_EVENT_SCHEMA_VERSION
from gateway.pnc_rca_policy_config import (
    W3SnapshotAuthority,
    w3_snapshot_read_config_from_env,
)
from gateway.pnc_rca_quality_oracle import BANNED_PUBLIC_PHRASES
from gateway.pnc_rca_runtime_identity import (
    MAX_HEALTH_FUTURE_SKEW_SECONDS,
    RCA_DELIVERY_COLLECTOR_LOADED_DEPENDENCIES,
    build_runtime_identity,
    runtime_identity_is_valid,
)
from gateway.pnc_rca_snapshot import (
    AdmissionSnapshotExecutionBundle,
    snapshot_execution_inputs,
    validate_snapshot_execution_bundle,
)
from gateway.pnc_rca_write_fence import (
    ExternalWriteFenceError,
    canonical_write_fence_sha256,
    validate_write_fence,
    validate_write_fence_source_binding,
    write_fence_binding,
)
from hermes_constants import get_hermes_home
from scripts.pnc_foxglove_delivery import canonical_viz_mcap_path
from scripts import pnc_fault_taxonomy


ENV_PREFIX = "HERMES_RCA_DELIVERY_COLLECTOR_"
HEALTH_SCHEMA_VERSION = "pnc_rca_delivery_collector_health_v2"
SERVICE_LABEL = "local.pnc.rca-delivery-collector"
DEPENDENCY_PROBE_REFRESH_SECONDS = 30
SUBMISSION_OUTBOX_SCHEMA_VERSION = "pnc_rca_submission_outbox_v2"
REMOTE_CSS_PARSER_DISTRIBUTION = "tinycss2"
REMOTE_CSS_PARSER_VERSION = "1.2.1"
REMOTE_CSS_WEBENCODINGS_VERSION = "0.5.1"
REMOTE_CSS_RUNTIME_CHECK_SCHEMA = "rca_delivery_runtime_check_v1"
REMOTE_CSS_RUNTIME_CHECKER_PATH = (
    "/home/mini/.hermes/worker-state/check_rca_delivery_runtime.py"
)
REMOTE_CSS_RUNTIME_CHECKER_SHA256 = (
    "8997fa0740f1397e9187124249f18cd38ed93d5bf0f2bce51a59a76583eba0c5"
)
REMOTE_CSS_RUNTIME_REQUIREMENTS_PATH = (
    "/home/mini/.hermes/worker-state/requirements-rca-delivery.txt"
)
REMOTE_CSS_RUNTIME_REQUIREMENTS_SHA256 = (
    "5c38f8fa928701507b5b38e5ed15495d1529c77ef8a8ad6d3de38145f0dc213e"
)
REMOTE_CSS_RUNTIME_PYTHON = "/usr/bin/python3"
DEFAULT_SSH_MINI_AGENT = str(Path.home() / ".local" / "bin" / "ssh-mini-agent")
MAX_ARTIFACT_READ_TIMEOUT_SECONDS = 110
ARTIFACT_READ_LEASE_MARGIN_SECONDS = 15
MAX_HEALTH_HEARTBEAT_INTERVAL_SECONDS = 15.0
MAX_FAILURE_RECEIPT_BYTES = 256 * 1024
FAILURE_RECEIPT_SCHEMA_VERSION = "g1q3_rca_service_result_v2"
INFRA_REMEDIATION_SCHEMA_VERSION = "pnc_rca_infra_remediation_receipt_v1"
MAX_INFRA_REMEDIATION_SECONDS = 10
_EVENTUAL_ARTIFACT_CODES = frozenset({
    "delivery_contract_missing",
    "delivery_manifest_missing",
    "report_data_missing",
    "artifact_missing",
    "html_dependency_missing",
    "html_dependency_changed_during_read",
    "required_html_artifact_missing",
})
_RETRYABLE_INFRASTRUCTURE_ARTIFACT_CODES = frozenset({
    "html_css_parser_dependency_missing",
    "html_css_parser_version_mismatch",
    "viz_publication_missing",
    "viz_publication_path_invalid",
})
_RUNNING_STATES = frozenset({
    "pending",
    "submitted",
    "queued",
    "claimed",
    "running",
    "in_progress",
})
_COMPLETED_STATES = frozenset({"completed", "done"})
_FAILED_TERMINAL_STATES = frozenset({
    "failed",
    "blocked",
    "abandoned",
    "cancelled",
    "canceled",
})
_PUBLIC_TERMINAL_BLOCKER_CODES = {
    "need_keyframe": "need_keyframe",
    "need_key_frame": "need_keyframe",
    "required_input": "required_input",
    "missing_required_input": "required_input",
}
_PUBLIC_TERMINAL_ERROR_CODES = frozenset({
    "artifact_hash_mismatch",
    "artifact_reader_response_invalid",
    "delivery_record_conflict",
    "submission_admission_invalid",
    "submission_outbox_contract_invalid",
    "submission_receipt_identity_mismatch",
    "submission_watch_identity_mismatch",
    "terminal_artifact_grace_exceeded",
})
_PUBLIC_TERMINAL_FALLBACK_CODE = "taxonomy_gap:missing_terminal_error_code"


StatusReader = Callable[[str], Mapping[str, Any]]
ArtifactBundleReader = Callable[[ExecutionWatchClaim], Mapping[str, Any]]
FailureReceiptReader = Callable[[ExecutionWatchClaim], Mapping[str, Any]]
InfraRemediationRunner = Callable[
    [ExecutionWatchClaim, Mapping[str, Any], Mapping[str, Any], int],
    Mapping[str, Any],
]
TerminalReceiptReader = Callable[[str, str], bytes]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_iso(value: datetime | None = None) -> str:
    current = value or _utc_now()
    if current.tzinfo is None:
        raise ValueError("datetime values must be timezone-aware")
    return current.astimezone(timezone.utc).isoformat()


def _heartbeat_interval_seconds(max_age_seconds: int) -> float:
    return max(
        1.0,
        min(MAX_HEALTH_HEARTBEAT_INTERVAL_SECONDS, max_age_seconds / 3),
    )


def _utc_datetime(value: Any, *, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"{field} is invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise RuntimeError(f"{field} is invalid")
    return parsed.astimezone(timezone.utc)


def _work_window(
    claim: ExecutionWatchClaim,
    now: datetime,
) -> tuple[datetime, datetime, float]:
    started = _utc_datetime(claim.work_started_at, field="work_started_at")
    if now.tzinfo is None or now.utcoffset() is None:
        raise RuntimeError("current time is invalid")
    current = now.astimezone(timezone.utc)
    deadline = started + timedelta(seconds=pnc_fault_taxonomy.TERMINAL_FALLBACK_SECONDS)
    return started, deadline, (current - started).total_seconds()


def default_infra_remediation_runner(
    claim: ExecutionWatchClaim,
    blocker: Mapping[str, Any],
    remediation: Mapping[str, Any],
    timeout_seconds: int,
) -> Mapping[str, Any]:
    """Fail closed when no post-terminal same-task resume primitive exists.

    The active pipeline already performs its bounded in-process remediation
    before declaring terminal.  The host must not replay a raw stage command or
    pretend polling is remediation; it records this held result exactly once.
    """

    return {
        "schema_version": INFRA_REMEDIATION_SCHEMA_VERSION,
        "success": False,
        "status": "held",
        "submission_key": claim.submission_key,
        "business_key": claim.business_key,
        "generation": claim.generation,
        "task_id": claim.task_id,
        "operation": str(remediation.get("op") or "") or "unavailable",
        "blocker_kind": pnc_fault_taxonomy.blocker_kind(blocker),
        "resumed_same_task": False,
        "external_writes": False,
        "timeout_seconds": timeout_seconds,
        "error_code": "infra_remediation_primitive_unavailable",
    }


def _validated_remediation_result(
    value: Mapping[str, Any],
    *,
    claim: ExecutionWatchClaim,
    operation: str,
) -> tuple[dict[str, Any], bool]:
    result = dict(value) if isinstance(value, Mapping) else {}
    expected_keys = {
        "schema_version",
        "success",
        "status",
        "submission_key",
        "business_key",
        "generation",
        "task_id",
        "operation",
        "blocker_kind",
        "resumed_same_task",
        "external_writes",
        "timeout_seconds",
        "error_code",
    }
    if (
        set(result) != expected_keys
        or result.get("schema_version") != INFRA_REMEDIATION_SCHEMA_VERSION
        or result.get("submission_key") != claim.submission_key
        or result.get("business_key") != claim.business_key
        or result.get("generation") != claim.generation
        or result.get("task_id") != claim.task_id
        or result.get("operation") != operation
        or result.get("external_writes") is not False
        or type(result.get("timeout_seconds")) is not int
        or not 1 <= int(result["timeout_seconds"]) <= MAX_INFRA_REMEDIATION_SECONDS
        or type(result.get("success")) is not bool
        or type(result.get("resumed_same_task")) is not bool
        or result.get("status") not in {"succeeded", "held", "failed"}
        or not isinstance(result.get("error_code"), str)
    ):
        raise RuntimeError("infra_remediation_receipt_invalid")
    succeeded = (
        result["success"] is True
        and result["status"] == "succeeded"
        and result["resumed_same_task"] is True
        and result["error_code"] == ""
    )
    if result["success"] is not succeeded:
        raise RuntimeError("infra_remediation_receipt_invalid")
    return result, succeeded


class _PeriodicHeartbeat:
    def __init__(self, callback: Callable[[], None], *, interval_seconds: float):
        self._callback = callback
        self._interval_seconds = float(interval_seconds)
        self._stop = threading.Event()
        self._error: BaseException | None = None
        self._thread = threading.Thread(
            target=self._run,
            name=f"{SERVICE_LABEL}-heartbeat",
            daemon=True,
        )

    def _run(self) -> None:
        while not self._stop.wait(self._interval_seconds):
            try:
                self._callback()
            except BaseException as exc:  # pragma: no cover - surfaced on join
                self._error = exc
                self._stop.set()

    def __enter__(self) -> "_PeriodicHeartbeat":
        self._thread.start()
        return self

    def __exit__(self, exc_type, _exc, _traceback) -> None:
        self._stop.set()
        self._thread.join(timeout=max(1.0, self._interval_seconds + 1.0))
        if exc_type is None and self._thread.is_alive():
            raise RuntimeError("delivery_collector_heartbeat_stop_timeout")
        if exc_type is None and self._error is not None:
            raise RuntimeError("delivery_collector_heartbeat_failed") from self._error


def _eventual_artifact_error(code: str) -> bool:
    value = str(code or "")
    return value in _EVENTUAL_ARTIFACT_CODES or value.startswith((
        "artifact_missing_",
        "html_dependency_missing_",
    ))


def _boolean(env: Mapping[str, str], name: str, default: bool = False) -> bool:
    value = str(env.get(name, "true" if default else "false")).strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be true or false")


def _strict_boolean(env: Mapping[str, str], name: str, default: bool = False) -> bool:
    value = str(env.get(name, "true" if default else "false")).strip().lower()
    if value == "true":
        return True
    if value == "false":
        return False
    raise ValueError(f"{name} must be exactly true or false")


def _integer(
    env: Mapping[str, str],
    name: str,
    default: int,
    *,
    minimum: int = 1,
) -> int:
    try:
        value = int(str(env.get(name, default)).strip())
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if value < minimum:
        raise ValueError(f"{name} must be at least {minimum}")
    return value


def expected_remote_css_runtime_dependency() -> dict[str, str]:
    return {
        "schema_version": REMOTE_CSS_RUNTIME_CHECK_SCHEMA,
        "distribution": REMOTE_CSS_PARSER_DISTRIBUTION,
        "version": REMOTE_CSS_PARSER_VERSION,
        "webencodings_version": REMOTE_CSS_WEBENCODINGS_VERSION,
        "python_executable": REMOTE_CSS_RUNTIME_PYTHON,
        "checker_path": REMOTE_CSS_RUNTIME_CHECKER_PATH,
        "checker_sha256": REMOTE_CSS_RUNTIME_CHECKER_SHA256,
        "requirements_path": REMOTE_CSS_RUNTIME_REQUIREMENTS_PATH,
        "requirements_sha256": REMOTE_CSS_RUNTIME_REQUIREMENTS_SHA256,
    }


@dataclass(frozen=True)
class CollectorConfig:
    enabled: bool
    control_db_path: Path
    health_path: Path
    poll_interval_seconds: int
    running_poll_seconds: int
    max_poll_seconds: int
    lease_seconds: int
    batch_size: int
    backfill_batch_size: int
    health_max_age_seconds: int
    ssh_mini_agent: str
    artifact_read_timeout_seconds: int
    terminal_artifact_grace_seconds: int
    quarantine_baseline_path: Path
    quarantine_baseline_sha256: str
    quarantine_release_id: str
    quarantine_bootstrap_epoch_id: str
    quarantine_active_release_binding_path: Path
    quarantine_live_env_path: Path
    capacity_sample_enabled: bool = False
    capacity_sample_batch_size: int = 20
    capacity_sample_lock_timeout_seconds: int = 5
    capacity_terminal_receipt_timeout_seconds: int = 15
    w3_snapshot_read_mode: str = "legacy"
    w3_snapshot_authority: W3SnapshotAuthority | None = None

    @classmethod
    def from_env(
        cls,
        env: Mapping[str, str] | None = None,
        *,
        hermes_home: str | Path | None = None,
    ) -> "CollectorConfig":
        source = os.environ if env is None else env
        home = Path(hermes_home or get_hermes_home()).expanduser()
        enabled = _boolean(source, f"{ENV_PREFIX}ENABLED", False)
        capacity_sample_enabled = _strict_boolean(
            source, f"{ENV_PREFIX}CAPACITY_SAMPLE_ENABLED", False
        )
        if capacity_sample_enabled and not enabled:
            raise ValueError(
                f"{ENV_PREFIX}CAPACITY_SAMPLE_ENABLED requires collector ENABLED"
            )
        poll = _integer(source, f"{ENV_PREFIX}POLL_INTERVAL_SECONDS", 5)
        running_poll = _integer(source, f"{ENV_PREFIX}RUNNING_POLL_SECONDS", 20)
        max_poll = _integer(source, f"{ENV_PREFIX}MAX_POLL_SECONDS", 300)
        if max_poll < running_poll:
            raise ValueError(
                f"{ENV_PREFIX}MAX_POLL_SECONDS must be >= RUNNING_POLL_SECONDS"
            )
        agent = str(
            source.get(f"{ENV_PREFIX}SSH_MINI_AGENT", DEFAULT_SSH_MINI_AGENT)
        ).strip()
        if not agent:
            raise ValueError(f"{ENV_PREFIX}SSH_MINI_AGENT is required")
        artifact_timeout = _integer(
            source,
            f"{ENV_PREFIX}ARTIFACT_READ_TIMEOUT_SECONDS",
            MAX_ARTIFACT_READ_TIMEOUT_SECONDS,
        )
        if artifact_timeout > MAX_ARTIFACT_READ_TIMEOUT_SECONDS:
            raise ValueError(
                f"{ENV_PREFIX}ARTIFACT_READ_TIMEOUT_SECONDS must be at most "
                f"{MAX_ARTIFACT_READ_TIMEOUT_SECONDS}"
            )
        lease_seconds = _integer(source, f"{ENV_PREFIX}LEASE_SECONDS", 180, minimum=30)
        if lease_seconds <= artifact_timeout + ARTIFACT_READ_LEASE_MARGIN_SECONDS:
            raise ValueError(
                f"{ENV_PREFIX}LEASE_SECONDS must exceed "
                "ARTIFACT_READ_TIMEOUT_SECONDS plus the lease margin"
            )
        capacity_batch = _integer(
            source,
            f"{ENV_PREFIX}CAPACITY_SAMPLE_BATCH_SIZE",
            20,
            minimum=1,
        )
        if capacity_batch > 100:
            raise ValueError(
                f"{ENV_PREFIX}CAPACITY_SAMPLE_BATCH_SIZE must be at most 100"
            )
        terminal_receipt_timeout = _integer(
            source,
            f"{ENV_PREFIX}CAPACITY_TERMINAL_RECEIPT_TIMEOUT_SECONDS",
            15,
            minimum=1,
        )
        if terminal_receipt_timeout > 30:
            raise ValueError(
                f"{ENV_PREFIX}CAPACITY_TERMINAL_RECEIPT_TIMEOUT_SECONDS "
                "must be at most 30"
            )
        control_db_path = Path(
            source.get(
                f"{ENV_PREFIX}CONTROL_DB_PATH",
                home
                / "runtime"
                / "pnc_agent"
                / "feishu_issue_kafka_rca"
                / "control.sqlite3",
            )
        ).expanduser()
        quarantine = quarantine_baseline_settings(
            source,
            hermes_home=home,
            control_db_path=control_db_path,
        )
        w3_snapshot_read_mode, w3_snapshot_authority = (
            w3_snapshot_read_config_from_env(source)
        )
        return cls(
            enabled=enabled,
            control_db_path=control_db_path,
            health_path=Path(
                source.get(
                    f"{ENV_PREFIX}HEALTH_PATH",
                    home
                    / "runtime"
                    / "pnc_agent"
                    / "feishu_issue_kafka_rca"
                    / "delivery_collector_health.json",
                )
            ).expanduser(),
            poll_interval_seconds=poll,
            running_poll_seconds=running_poll,
            max_poll_seconds=max_poll,
            lease_seconds=lease_seconds,
            batch_size=_integer(source, f"{ENV_PREFIX}BATCH_SIZE", 20),
            backfill_batch_size=_integer(
                source, f"{ENV_PREFIX}BACKFILL_BATCH_SIZE", 1000
            ),
            health_max_age_seconds=_integer(
                source, f"{ENV_PREFIX}HEALTH_MAX_AGE_SECONDS", 60
            ),
            ssh_mini_agent=agent,
            artifact_read_timeout_seconds=artifact_timeout,
            terminal_artifact_grace_seconds=_integer(
                source, f"{ENV_PREFIX}TERMINAL_ARTIFACT_GRACE_SECONDS", 900
            ),
            quarantine_baseline_path=quarantine.baseline_path,
            quarantine_baseline_sha256=quarantine.baseline_sha256,
            quarantine_release_id=quarantine.release_id,
            quarantine_bootstrap_epoch_id=quarantine.bootstrap_epoch_id,
            quarantine_active_release_binding_path=(
                quarantine.active_release_binding_path
            ),
            quarantine_live_env_path=quarantine.live_env_path,
            capacity_sample_enabled=capacity_sample_enabled,
            capacity_sample_batch_size=capacity_batch,
            capacity_sample_lock_timeout_seconds=_integer(
                source,
                f"{ENV_PREFIX}CAPACITY_SAMPLE_LOCK_TIMEOUT_SECONDS",
                5,
                minimum=1,
            ),
            capacity_terminal_receipt_timeout_seconds=terminal_receipt_timeout,
            w3_snapshot_read_mode=w3_snapshot_read_mode,
            w3_snapshot_authority=w3_snapshot_authority,
        )

    def public_dict(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "control_db_path": str(self.control_db_path),
            "health_path": str(self.health_path),
            "poll_interval_seconds": self.poll_interval_seconds,
            "running_poll_seconds": self.running_poll_seconds,
            "max_poll_seconds": self.max_poll_seconds,
            "lease_seconds": self.lease_seconds,
            "batch_size": self.batch_size,
            "backfill_batch_size": self.backfill_batch_size,
            "health_max_age_seconds": self.health_max_age_seconds,
            "ssh_mini_agent": self.ssh_mini_agent,
            "artifact_read_timeout_seconds": self.artifact_read_timeout_seconds,
            "terminal_artifact_grace_seconds": self.terminal_artifact_grace_seconds,
            "quarantine_baseline_path": str(self.quarantine_baseline_path),
            "quarantine_baseline_sha256": self.quarantine_baseline_sha256,
            "quarantine_release_id": self.quarantine_release_id,
            "quarantine_bootstrap_epoch_id": self.quarantine_bootstrap_epoch_id,
            "quarantine_active_release_binding_path": str(
                self.quarantine_active_release_binding_path
            ),
            "quarantine_live_env_path": str(self.quarantine_live_env_path),
            "capacity_sample_enabled": self.capacity_sample_enabled,
            "capacity_sample_batch_size": self.capacity_sample_batch_size,
            "capacity_sample_lock_timeout_seconds": (
                self.capacity_sample_lock_timeout_seconds
            ),
            "capacity_terminal_receipt_timeout_seconds": (
                self.capacity_terminal_receipt_timeout_seconds
            ),
            "remote_css_parser": {
                **expected_remote_css_runtime_dependency(),
            },
            "external_writes": False,
            "w3_snapshot_read": (
                {
                    "mode": self.w3_snapshot_read_mode,
                    **self.w3_snapshot_authority.to_public_dict(),
                }
                if self.w3_snapshot_authority is not None
                else {"enabled": False, "mode": self.w3_snapshot_read_mode}
            ),
        }


class ArtifactBundleReadError(RuntimeError):
    def __init__(self, code: str, detail: str = "", *, permanent: bool = False):
        self.code = str(code or "artifact_bundle_unavailable")[:120]
        self.detail = str(detail or self.code)[:1000]
        self.permanent = bool(permanent)
        super().__init__(self.detail)


class FailureReceiptReadError(RuntimeError):
    def __init__(self, code: str, detail: str = ""):
        self.code = str(code or "failure_receipt_unavailable")[:120]
        self.detail = str(detail or self.code)[:1000]
        super().__init__(self.detail)


@dataclass
class CollectorStats:
    loops: int = 0
    watches_created: int = 0
    claimed: int = 0
    running: int = 0
    delivery_created: int = 0
    delivery_deduped: int = 0
    terminal_failed: int = 0
    quarantined: int = 0
    retried: int = 0
    idle: int = 0
    stale_lease: int = 0
    failure_holds: int = 0
    remediation_attempted: int = 0
    remediation_succeeded: int = 0
    remediation_held: int = 0
    internal_backlog: int = 0
    internal_alert: int = 0
    taxonomy_gaps: int = 0
    terminal_fallbacks: int = 0
    capacity_scanned: int = 0
    capacity_eligible: int = 0
    capacity_appended: int = 0
    capacity_rejected: int = 0
    capacity_frozen: int = 0
    capacity_last_error: str = ""


@dataclass(frozen=True)
class CollectOutcome:
    status: str
    submission_key: str = ""
    delivery_id: str = ""
    effect_key: str = ""
    error_code: str = ""
    next_poll_at: str | None = None
    created: bool | None = None


def default_status_reader(task_id: str) -> Mapping[str, Any]:
    """Read canonical shared-state truth without starting a completion process."""
    from tools.vm_task_tool import vm_task_status

    return vm_task_status(task_id, include_markdown=False)


def _remote_failure_receipt_script(claim: ExecutionWatchClaim) -> str:
    root = canonical_artifact_root(claim.submission_key).rstrip("/")
    receipt_path = f"{root}/rca_service_result.json"
    return textwrap.dedent(
        f"""
        import json
        import os
        import stat

        ROOT = {root!r}
        PATH = {receipt_path!r}
        EXPECTED_TASK_ID = {claim.task_id!r}
        EXPECTED_SUBMISSION_KEY = {claim.submission_key!r}
        MAX_BYTES = {MAX_FAILURE_RECEIPT_BYTES}

        def finish(value):
            print(json.dumps(value, ensure_ascii=False, sort_keys=True))
            raise SystemExit(0)

        try:
            current = '/'
            for part in ROOT.strip('/').split('/'):
                current = os.path.join(current, part)
                info = os.lstat(current)
                if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
                    raise RuntimeError('failure_receipt_parent_invalid')
            flags = os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0)
            fd = os.open(PATH, flags)
            try:
                info = os.fstat(fd)
                if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                    raise RuntimeError('failure_receipt_file_invalid')
                if info.st_size <= 0 or info.st_size > MAX_BYTES:
                    raise RuntimeError('failure_receipt_size_invalid')
                raw = os.read(fd, MAX_BYTES + 1)
            finally:
                os.close(fd)
            if len(raw) > MAX_BYTES:
                raise RuntimeError('failure_receipt_size_invalid')
            receipt = json.loads(raw.decode('utf-8'))
            if not isinstance(receipt, dict):
                raise RuntimeError('failure_receipt_shape_invalid')
            blocker = receipt.get('blocker')
            if (
                receipt.get('schema_version') != {FAILURE_RECEIPT_SCHEMA_VERSION!r}
                or receipt.get('task_id') != EXPECTED_TASK_ID
                or os.path.normpath(str(receipt.get('output_dir') or '')) != ROOT
                or EXPECTED_TASK_ID != EXPECTED_SUBMISSION_KEY
                or not isinstance(blocker, dict)
                or not str(blocker.get('kind') or '').strip()
            ):
                raise RuntimeError('failure_receipt_identity_invalid')
            if len(json.dumps(blocker, ensure_ascii=False).encode('utf-8')) > 32768:
                raise RuntimeError('failure_receipt_blocker_too_large')
            finish({{
                'ok': True,
                'schema_version': receipt['schema_version'],
                'task_id': receipt['task_id'],
                'status': str(receipt.get('status') or ''),
                'pipeline_status': str(receipt.get('pipeline_status') or ''),
                'pipeline_stage': str(receipt.get('pipeline_stage') or ''),
                'blocker': blocker,
            }})
        except FileNotFoundError:
            finish({{'ok': False, 'error_code': 'failure_receipt_missing'}})
        except (UnicodeError, ValueError):
            finish({{'ok': False, 'error_code': 'failure_receipt_json_invalid'}})
        except RuntimeError as exc:
            finish({{'ok': False, 'error_code': str(exc)}})
        """
    ).strip()


def default_failure_receipt_reader(
    claim: ExecutionWatchClaim,
    *,
    ssh_mini_agent: str = DEFAULT_SSH_MINI_AGENT,
    timeout_seconds: int = 30,
) -> Mapping[str, Any]:
    """Read the identity-bound VM service receipt without materializing input."""

    try:
        proc = subprocess.run(
            [ssh_mini_agent, "run_py_json"],
            input=_remote_failure_receipt_script(claim),
            text=True,
            capture_output=True,
            timeout=timeout_seconds,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise FailureReceiptReadError(
            "failure_receipt_reader_unavailable", type(exc).__name__
        ) from exc
    if proc.returncode != 0:
        raise FailureReceiptReadError(
            "failure_receipt_reader_unavailable",
            (proc.stderr or proc.stdout or f"ssh-mini-agent rc={proc.returncode}")[
                -1000:
            ],
        )
    try:
        payload = json.loads(proc.stdout or "{}")
    except json.JSONDecodeError as exc:
        raise FailureReceiptReadError("failure_receipt_response_invalid") from exc
    if not isinstance(payload, dict):
        raise FailureReceiptReadError("failure_receipt_response_invalid")
    if payload.get("ok") is not True:
        code = str(payload.get("error_code") or "failure_receipt_unavailable")
        raise FailureReceiptReadError(code)
    return payload


def probe_remote_css_parser(
    ssh_mini_agent: str,
    *,
    timeout_seconds: int = 15,
    worker_root: str | None = None,
) -> dict[str, str]:
    """Run the hash-pinned, read-only VM parser runtime checker."""
    selected_root = PurePosixPath(
        worker_root or str(PurePosixPath(REMOTE_CSS_RUNTIME_CHECKER_PATH).parent)
    )
    if (
        not selected_root.is_absolute()
        or ".." in selected_root.parts
        or selected_root == PurePosixPath("/")
    ):
        raise ArtifactBundleReadError(
            "html_css_parser_probe_root_invalid",
            permanent=True,
        )
    checker_path = str(
        selected_root / PurePosixPath(REMOTE_CSS_RUNTIME_CHECKER_PATH).name
    )
    requirements_path = str(
        selected_root / PurePosixPath(REMOTE_CSS_RUNTIME_REQUIREMENTS_PATH).name
    )
    probe = textwrap.dedent(
        f"""
        set -euo pipefail
        checker={checker_path!r}
        requirements={requirements_path!r}
        test -f "$checker" && test ! -L "$checker"
        test -f "$requirements" && test ! -L "$requirements"
        test "$(/usr/bin/sha256sum "$checker" | /usr/bin/awk '{{print $1}}')" = {REMOTE_CSS_RUNTIME_CHECKER_SHA256!r}
        test "$(/usr/bin/sha256sum "$requirements" | /usr/bin/awk '{{print $1}}')" = {REMOTE_CSS_RUNTIME_REQUIREMENTS_SHA256!r}
        exec {REMOTE_CSS_RUNTIME_PYTHON} "$checker" \
          --requirements "$requirements" \
          --expected-python {REMOTE_CSS_RUNTIME_PYTHON} \
          --json
        """
    ).strip()
    try:
        process = subprocess.run(
            [ssh_mini_agent, "run_bash_json"],
            input=probe,
            text=True,
            capture_output=True,
            timeout=timeout_seconds,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise ArtifactBundleReadError(
            "html_css_parser_probe_unavailable",
            type(exc).__name__,
            permanent=True,
        ) from exc
    try:
        payload = json.loads(process.stdout or "{}")
    except json.JSONDecodeError as exc:
        raise ArtifactBundleReadError(
            "html_css_parser_probe_invalid",
            "VM CSS parser probe returned invalid JSON",
            permanent=True,
        ) from exc
    expected_versions = {
        "tinycss2": REMOTE_CSS_PARSER_VERSION,
        "webencodings": REMOTE_CSS_WEBENCODINGS_VERSION,
    }
    expected_semantics = {
        "escaped_url_tokenized": True,
        "numeric_values_tokenized": True,
        "webencodings_utf8_lookup": True,
    }
    requirements = payload.get("requirements") if isinstance(payload, dict) else None
    python = payload.get("python") if isinstance(payload, dict) else None
    valid = (
        process.returncode == 0
        and isinstance(payload, dict)
        and payload.get("schema_version") == REMOTE_CSS_RUNTIME_CHECK_SCHEMA
        and payload.get("ok") is True
        and payload.get("mutates_state") is False
        and payload.get("errors") == []
        and payload.get("runtime_versions") == expected_versions
        and payload.get("semantic_checks") == expected_semantics
        and python
        == {
            "expected_executable": REMOTE_CSS_RUNTIME_PYTHON,
            "actual_executable": REMOTE_CSS_RUNTIME_PYTHON,
            "same_file": True,
        }
        and requirements
        == {
            "path": requirements_path,
            "sha256": REMOTE_CSS_RUNTIME_REQUIREMENTS_SHA256,
            "pins": expected_versions,
        }
    )
    if not valid:
        raise ArtifactBundleReadError(
            "html_css_parser_probe_failed",
            str(payload.get("errors") if isinstance(payload, dict) else "invalid"),
            permanent=True,
        )
    return expected_remote_css_runtime_dependency()


def _remote_bundle_script(submission_key: str) -> str:
    root = canonical_artifact_root(submission_key)
    formal_viz_path = canonical_viz_mcap_path(submission_key)
    if not formal_viz_path:
        raise ArtifactBundleReadError("viz_publication_path_invalid", permanent=True)
    formal_viz_root = str(PurePosixPath(formal_viz_path).parent)
    return textwrap.dedent(
        f"""
        import hashlib
        import html
        import json
        import os
        import posixpath
        import stat
        from html.parser import HTMLParser
        from importlib import metadata
        from urllib.parse import unquote, urlsplit

        try:
            import tinycss2
        except Exception:
            tinycss2 = None

        ROOT = {root!r}
        MAX_JSON_BYTES = 8 * 1024 * 1024
        MAX_ARTIFACTS = 512
        MAX_FILE_BYTES = 256 * 1024 * 1024
        MAX_VIZ_BYTES = 64 * 1024 * 1024 * 1024
        MAX_TOTAL_BYTES = 512 * 1024 * 1024
        MAX_HTML_BYTES = 32 * 1024 * 1024
        MAX_TEXT_FILE_BYTES = 32 * 1024 * 1024
        MAX_TEXT_TOTAL_BYTES = 64 * 1024 * 1024
        CSS_PARSER_DISTRIBUTION = {REMOTE_CSS_PARSER_DISTRIBUTION!r}
        CSS_PARSER_VERSION = {REMOTE_CSS_PARSER_VERSION!r}
        FORMAL_VIZ_ROOT = {formal_viz_root!r}
        BANNED_PUBLIC_PHRASES = {tuple(BANNED_PUBLIC_PHRASES)!r}

        def finish(value):
            print(json.dumps(value, ensure_ascii=False, sort_keys=True))

        def reject_banned_public_phrase(text):
            decoded = html.unescape(str(text or ''))
            if any(phrase in decoded for phrase in BANNED_PUBLIC_PHRASES):
                raise RuntimeError('public_artifact_banned_phrase')

        def symlink_free(path, anchor=None):
            anchor = anchor or root_norm
            current = anchor
            try:
                if stat.S_ISLNK(os.lstat(current).st_mode):
                    return False
            except FileNotFoundError:
                return False
            relative = posixpath.relpath(path, anchor)
            for part in relative.split('/'):
                current = posixpath.join(current, part)
                try:
                    if stat.S_ISLNK(os.lstat(current).st_mode):
                        return False
                except FileNotFoundError:
                    return False
            return True

        def open_regular(path, missing_code, max_bytes, anchor=None):
            try:
                before = os.lstat(path)
            except FileNotFoundError:
                raise RuntimeError(missing_code)
            if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
                raise RuntimeError(missing_code + '_not_regular')
            if not symlink_free(path, anchor):
                raise RuntimeError(missing_code + '_not_regular')
            try:
                fd = os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0))
            except (FileNotFoundError, OSError):
                raise RuntimeError(missing_code + '_open_failed')
            after = os.fstat(fd)
            if (
                not stat.S_ISREG(after.st_mode)
                or before.st_dev != after.st_dev
                or before.st_ino != after.st_ino
            ):
                os.close(fd)
                raise RuntimeError(missing_code + '_changed_during_read')
            if after.st_size <= 0 or after.st_size > max_bytes:
                os.close(fd)
                raise RuntimeError(missing_code + '_size_invalid')
            return fd, after

        def read_json(path, missing_code, anchor=None):
            fd, _info = open_regular(path, missing_code, MAX_JSON_BYTES, anchor)
            with os.fdopen(fd, 'rb') as handle:
                raw = handle.read(MAX_JSON_BYTES + 1)
            try:
                value = json.loads(raw.decode('utf-8'))
            except Exception:
                raise RuntimeError(missing_code.replace('_missing', '') + '_json_invalid')
            if not isinstance(value, dict):
                raise RuntimeError(missing_code.replace('_missing', '') + '_json_invalid')
            return value

        def public_report_projection(report_data):
            # Extract only decision-bearing RCA fields for Feishu projection.
            summary = report_data.get('summary') if isinstance(report_data.get('summary'), dict) else {{}}
            receipt = report_data.get('rca_receipt') if isinstance(report_data.get('rca_receipt'), dict) else {{}}
            responsibility = report_data.get('responsibility') if isinstance(report_data.get('responsibility'), dict) else {{}}
            if not responsibility and isinstance(receipt.get('responsibility'), dict):
                responsibility = receipt.get('responsibility')
            attribution = receipt.get('attribution_expression') if isinstance(receipt.get('attribution_expression'), dict) else {{}}
            slots = attribution.get('slots') if isinstance(attribution.get('slots'), dict) else {{}}
            candidate = (
                responsibility.get('candidate')
                or responsibility.get('owner')
                or report_data.get('candidate_responsibility')
                or report_data.get('responsibility_candidate')
                or slots.get('responsibility_candidate')
            )
            causal = report_data.get('causal_chain') if isinstance(report_data.get('causal_chain'), dict) else {{}}
            if not causal and isinstance(receipt.get('causal_chain'), dict):
                causal = receipt.get('causal_chain')
            narrative = causal.get('narrative') if isinstance(causal.get('narrative'), list) else []
            narrative = [
                {{'role': str(item.get('role') or ''), 'text': str(item.get('text') or '')[:1400]}}
                for item in narrative[:8] if isinstance(item, dict) and (item.get('role') or item.get('text'))
            ]
            hypotheses = causal.get('hypotheses') if isinstance(causal.get('hypotheses'), list) else []
            hypotheses = [
                {{k: item.get(k) for k in ('narrative', 'text', 'summary') if item.get(k)}}
                for item in hypotheses[:8] if isinstance(item, dict)
            ]
            evidence = report_data.get('evidence_summary') if isinstance(report_data.get('evidence_summary'), dict) else {{}}
            refs = evidence.get('refs') if isinstance(evidence.get('refs'), list) else []
            compact_refs = []
            for item in refs[:8]:
                if isinstance(item, dict):
                    compact_refs.append({{k: item.get(k) for k in ('evidence_ref', 'summary', 'field', 'check', 'fit_source') if item.get(k)}})
                elif item:
                    compact_refs.append(str(item)[:400])
            terminal = report_data.get('terminal_diagnostic') if isinstance(report_data.get('terminal_diagnostic'), dict) else {{}}
            return {{
                'summary': {{k: summary.get(k) for k in ('short_conclusion', 'l0', 'status', 'rca_pattern', 'rca_domain', 'high_confidence_boundary') if summary.get(k)}},
                'responsibility': {{k: responsibility.get(k) for k in ('candidate', 'owner', 'status', 'boundary', 'missing_evidence') if responsibility.get(k)}},
                'causal_chain': {{'narrative': narrative, 'hypotheses': hypotheses}},
                'evidence_summary': {{'refs': compact_refs, 'missing_evidence': [str(x)[:400] for x in (evidence.get('missing_evidence') or [])[:20]]}},
                'evidence_boundary': [str(x)[:500] for x in (report_data.get('evidence_boundary') or [])[:8]],
                'user_action': report_data.get('user_action') if isinstance(report_data.get('user_action'), dict) else {{}},
                'terminal_diagnostic': {{k: terminal.get(k) for k in ('blocker_kind', 'attribution_status', 'stage') if terminal.get(k)}},
                'candidate': str(candidate or '')[:500],
            }}

        def read_text_artifact(path, expected):
            fd, info = open_regular(path, 'html_dependency_missing', MAX_TEXT_FILE_BYTES)
            with os.fdopen(fd, 'rb') as handle:
                raw = handle.read(MAX_TEXT_FILE_BYTES + 1)
            if info.st_size != expected['size']:
                raise RuntimeError('html_dependency_changed_during_read')
            if hashlib.sha256(raw).hexdigest() != expected['sha256']:
                raise RuntimeError('html_dependency_changed_during_read')
            try:
                return raw.decode('utf-8'), len(raw)
            except UnicodeDecodeError:
                raise RuntimeError('html_dependency_text_invalid')

        def dependency_kind(path, media_type):
            lowered = path.lower()
            media = str(media_type or '').split(';', 1)[0].strip().lower()
            if media in ('text/html', 'application/xhtml+xml') or lowered.endswith(('.html', '.htm')):
                return 'html'
            if media == 'text/css' or lowered.endswith('.css'):
                return 'css'
            if (
                'javascript' in media
                or 'ecmascript' in media
                or lowered.endswith(('.js', '.mjs', '.cjs'))
            ):
                return 'javascript'
            return ''

        def require_css_parser():
            if tinycss2 is None:
                raise RuntimeError('html_css_parser_dependency_missing')
            try:
                installed = metadata.version(CSS_PARSER_DISTRIBUTION)
            except Exception:
                raise RuntimeError('html_css_parser_dependency_missing')
            if installed != CSS_PARSER_VERSION:
                raise RuntimeError('html_css_parser_version_mismatch')

        def css_token_refs(tokens):
            refs = []
            for token in tokens or ():
                token_type = str(getattr(token, 'type', '') or '').lower()
                if token_type == 'error':
                    raise RuntimeError('html_css_syntax_invalid')
                if token_type == 'url':
                    refs.append((str(token.value), ''))
                    continue
                if token_type == 'function':
                    function_name = str(
                        getattr(token, 'lower_name', None)
                        or getattr(token, 'name', '')
                    ).lower()
                    if function_name in (
                        'image',
                        'image-set',
                        '-webkit-image-set',
                        'cross-fade',
                        '-webkit-cross-fade',
                        'src',
                        'paint',
                        'element',
                        '-moz-element',
                    ):
                        raise RuntimeError('html_css_dynamic_resource_unsupported')
                    arguments = list(getattr(token, 'arguments', ()) or ())
                    if function_name == 'url':
                        meaningful = [
                            item
                            for item in arguments
                            if str(getattr(item, 'type', '') or '')
                            not in ('whitespace', 'comment')
                        ]
                        if len(meaningful) != 1 or meaningful[0].type != 'string':
                            raise RuntimeError('html_css_dynamic_resource_unsupported')
                        refs.append((str(meaningful[0].value), ''))
                    else:
                        refs.extend(css_token_refs(arguments))
                    continue
                if token_type == 'at-rule':
                    prelude = list(getattr(token, 'prelude', ()) or ())
                    if str(getattr(token, 'lower_at_keyword', '') or '') == 'import':
                        meaningful = [
                            item
                            for item in prelude
                            if str(getattr(item, 'type', '') or '')
                            not in ('whitespace', 'comment')
                        ]
                        if not meaningful:
                            raise RuntimeError('html_css_syntax_invalid')
                        first = meaningful[0]
                        if first.type == 'string':
                            refs.append((str(first.value), 'css'))
                        elif first.type == 'url':
                            refs.append((str(first.value), 'css'))
                        elif first.type == 'function':
                            function_name = str(
                                getattr(first, 'lower_name', None)
                                or getattr(first, 'name', '')
                            ).lower()
                            arguments = [
                                item
                                for item in list(
                                    getattr(first, 'arguments', ()) or ()
                                )
                                if str(getattr(item, 'type', '') or '')
                                not in ('whitespace', 'comment')
                            ]
                            if (
                                function_name != 'url'
                                or len(arguments) != 1
                                or arguments[0].type != 'string'
                            ):
                                raise RuntimeError(
                                    'html_css_dynamic_resource_unsupported'
                                )
                            refs.append((str(arguments[0].value), 'css'))
                        else:
                            raise RuntimeError('html_css_dynamic_resource_unsupported')
                        prelude = [item for item in prelude if item is not first]
                    refs.extend(css_token_refs(prelude))
                    refs.extend(css_token_refs(getattr(token, 'content', ()) or ()))
                    continue
                for field in ('prelude', 'content', 'value'):
                    nested = getattr(token, field, None)
                    if isinstance(nested, (list, tuple)):
                        refs.extend(css_token_refs(nested))
            return refs

        def css_refs(text, mode='stylesheet'):
            require_css_parser()
            try:
                if mode == 'declarations':
                    tokens = tinycss2.parse_declaration_list(
                        text,
                        skip_comments=False,
                        skip_whitespace=False,
                    )
                elif mode == 'component':
                    tokens = tinycss2.parse_component_value_list(
                        text,
                        skip_comments=False,
                    )
                else:
                    tokens = tinycss2.parse_stylesheet(
                        text,
                        skip_comments=False,
                        skip_whitespace=False,
                    )
            except Exception:
                raise RuntimeError('html_css_syntax_invalid')
            return css_token_refs(tokens)

        def html_refs(text, depth=0):
            if depth > 8:
                raise RuntimeError('html_srcdoc_nesting_too_deep')
            lowered_markup = text.lower()
            if '<!--' in lowered_markup:
                raise RuntimeError('html_comments_unsupported')
            if '<?' in lowered_markup:
                raise RuntimeError('html_processing_instruction_unsupported')
            if '<!' in lowered_markup.replace('<!doctype html>', ''):
                raise RuntimeError('html_declaration_unsupported')

            class DependencyParser(HTMLParser):
                def __init__(self):
                    super().__init__(convert_charrefs=True)
                    self.refs = []
                    self.capture_tag = ''
                    self.capture_data = []
                    self.svg_depth = 0

                def handle_starttag(self, tag, attrs):
                    tag = str(tag or '').lower()
                    attribute_map = {{}}
                    for raw_name, raw_value in attrs:
                        name = str(raw_name or '').lower()
                        if name in attribute_map:
                            raise RuntimeError('html_duplicate_attribute_unsupported')
                        attribute_map[name] = '' if raw_value is None else str(raw_value)
                    in_svg = self.svg_depth > 0 or tag == 'svg'
                    if tag in ('iframe', 'frame', 'object', 'embed', 'applet'):
                        raise RuntimeError('html_active_content_unsupported')
                    if in_svg and tag in (
                        'animate',
                        'animatecolor',
                        'animatemotion',
                        'animatetransform',
                        'set',
                    ):
                        raise RuntimeError('html_active_content_unsupported')
                    if tag == 'script':
                        script_type = attribute_map.get('type', '').strip().lower()
                        if script_type not in ('application/json', 'application/ld+json'):
                            raise RuntimeError('html_script_execution_unsupported')
                    if tag == 'form' or any(
                        name in attribute_map
                        for name in ('action', 'formaction', 'ping')
                    ):
                        raise RuntimeError('html_active_navigation_unsupported')
                    if any(name.startswith('on') for name in attribute_map):
                        raise RuntimeError('html_script_execution_unsupported')
                    for name in (
                        'href',
                        'xlink:href',
                        'src',
                        'poster',
                        'background',
                        'manifest',
                    ):
                        if name not in attribute_map:
                            continue
                        value = attribute_map[name].strip()
                        value = value.replace(chr(9), '').replace(chr(10), '')
                        value = value.replace(chr(13), '')
                        scheme = urlsplit(value).scheme.lower()
                        if scheme in ('javascript', 'vbscript'):
                            raise RuntimeError('html_script_execution_unsupported')
                        if scheme == 'data':
                            raise RuntimeError(
                                'html_embedded_data_dependency_unsupported'
                            )
                        if tag in ('a', 'area') and scheme not in ('', 'http', 'https'):
                            raise RuntimeError('html_navigation_scheme_unsupported')
                    if tag == 'base' and 'href' in attribute_map:
                        raise RuntimeError('html_base_url_unsupported')
                    if (
                        tag == 'meta'
                        and attribute_map.get('http-equiv', '').strip().lower() == 'refresh'
                    ):
                        raise RuntimeError('html_dynamic_dependency_unsupported')
                    for name in ('src', 'poster', 'background', 'manifest'):
                        if name in attribute_map:
                            self.refs.append((attribute_map[name], ''))
                    if tag == 'link' and 'href' in attribute_map:
                        rel = {{
                            value.lower()
                            for value in attribute_map.get('rel', '').split()
                        }}
                        expected_kind = 'css' if 'stylesheet' in rel else ''
                        self.refs.append((attribute_map['href'], expected_kind))
                    if tag in ('a', 'area') and 'href' in attribute_map:
                        href = attribute_map['href'].strip()
                        normalized_href = href.replace(chr(9), '').replace(chr(10), '')
                        normalized_href = normalized_href.replace(chr(13), '')
                        if (
                            normalized_href
                            and not normalized_href.startswith('#')
                            and not urlsplit(normalized_href).scheme
                        ):
                            self.refs.append((href, ''))
                    if in_svg and tag != 'a':
                        for name in ('href', 'xlink:href'):
                            if name in attribute_map:
                                self.refs.append((attribute_map[name], ''))
                    for name in ('srcset', 'imagesrcset'):
                        if name not in attribute_map:
                            continue
                        srcset = attribute_map[name]
                        for candidate in srcset.split(','):
                            fields = candidate.strip().split()
                            if not fields or len(fields) > 3:
                                raise RuntimeError('html_srcset_syntax_unsupported')
                            self.refs.append((fields[0], ''))
                    if 'style' in attribute_map:
                        self.refs.extend(
                            css_refs(attribute_map['style'], mode='declarations')
                        )
                    if in_svg:
                        for name, value in attribute_map.items():
                            if name not in ('style', 'href', 'xlink:href'):
                                self.refs.extend(css_refs(value, mode='component'))
                    if tag == 'style':
                        self.capture_tag = tag
                        self.capture_data = []
                    if tag == 'svg':
                        self.svg_depth += 1

                def handle_startendtag(self, tag, attrs):
                    self.handle_starttag(tag, attrs)
                    self.handle_endtag(tag)

                def handle_endtag(self, tag):
                    tag = str(tag or '').lower()
                    if tag == self.capture_tag:
                        self.refs.extend(css_refs(''.join(self.capture_data)))
                        self.capture_tag = ''
                        self.capture_data = []
                    if tag == 'svg' and self.svg_depth:
                        self.svg_depth -= 1

                def handle_data(self, data):
                    if self.capture_tag == 'style':
                        self.capture_data.append(data)

                def handle_comment(self, _data):
                    raise RuntimeError('html_comments_unsupported')

                def handle_decl(self, declaration):
                    if str(declaration or '').strip().lower() != 'doctype html':
                        raise RuntimeError('html_declaration_unsupported')

                def unknown_decl(self, _data):
                    raise RuntimeError('html_declaration_unsupported')

                def handle_pi(self, _data):
                    raise RuntimeError('html_processing_instruction_unsupported')

            parser = DependencyParser()
            try:
                parser.feed(text)
                parser.close()
                if parser.capture_tag:
                    raise RuntimeError('html_markup_invalid')
            except RuntimeError:
                raise
            except Exception:
                raise RuntimeError('html_markup_invalid')
            return parser.refs

        def resolve_ref(source_path, raw_ref):
            ref = html.unescape(str(raw_ref or '').strip())
            if not ref or ref.startswith('#'):
                return None
            if ref.lower().startswith('data:'):
                raise RuntimeError('html_embedded_data_dependency_unsupported')
            if '\\\\' in ref:
                raise RuntimeError('artifact_path_outside_root')
            parsed = urlsplit(ref)
            if parsed.scheme or parsed.netloc or ref.startswith('/'):
                raise RuntimeError('html_external_dependency_unsupported')
            parsed_path = unquote(parsed.path)
            if not parsed_path:
                return None
            dep_path = posixpath.normpath(
                posixpath.join(posixpath.dirname(source_path), parsed_path)
            )
            if posixpath.commonpath((root_norm, dep_path)) != root_norm:
                raise RuntimeError('artifact_path_outside_root')
            if dep_path.lower().endswith('.mcap'):
                raise RuntimeError('html_delivery_mcap_forbidden')
            return dep_path

        try:
            root_norm = posixpath.normpath(ROOT)
            contract = read_json(ROOT + 'delivery_contract.json', 'delivery_contract_missing')
            manifest = read_json(ROOT + 'delivery_manifest.json', 'delivery_manifest_missing')
            report_data = read_json(ROOT + 'report_data.json', 'report_data_missing')
            reject_banned_public_phrase(
                json.dumps(report_data, ensure_ascii=False, sort_keys=True)
            )
            contract = dict(contract)
            contract['public_result'] = public_report_projection(report_data)
            contract_artifacts = contract.get('artifacts')
            if not isinstance(contract_artifacts, dict):
                raise RuntimeError('viz_publication_missing')
            viz_publication = contract_artifacts.get('viz_publication')
            if viz_publication is None:
                viz_publication = {{}}
            if not isinstance(viz_publication, dict):
                raise RuntimeError('viz_publication_missing')
            rows = manifest.get('artifacts')
            if not isinstance(rows, list) or not rows or len(rows) > MAX_ARTIFACTS:
                raise RuntimeError('delivery_manifest_artifacts_invalid')
            observed = []
            if viz_publication:
                viz_path = str(viz_publication.get('path') or '')
                viz_manifest_path = str(viz_publication.get('manifest_path') or '')
                submission_key = str(viz_publication.get('submission_key') or '')
                expected_viz_path = posixpath.join(
                    FORMAL_VIZ_ROOT, submission_key + '.viz.mcap'
                )
                expected_viz_manifest_path = posixpath.join(
                    FORMAL_VIZ_ROOT, submission_key + '.viz.manifest.json',
                )
                if (
                    not submission_key
                    or viz_path != expected_viz_path
                    or viz_manifest_path != expected_viz_manifest_path
                    or posixpath.commonpath((FORMAL_VIZ_ROOT, viz_path)) != FORMAL_VIZ_ROOT
                ):
                    raise RuntimeError('viz_publication_path_invalid')
                viz_fd, viz_info = open_regular(
                    viz_path, 'viz_publication_missing', MAX_VIZ_BYTES, FORMAL_VIZ_ROOT
                )
                os.close(viz_fd)
                viz_manifest_fd, viz_manifest_info = open_regular(
                    viz_manifest_path,
                    'viz_publication_manifest_missing',
                    MAX_JSON_BYTES,
                    FORMAL_VIZ_ROOT,
                )
                with os.fdopen(viz_manifest_fd, 'rb') as handle:
                    viz_manifest_raw = handle.read(MAX_JSON_BYTES + 1)
                try:
                    viz_manifest = json.loads(viz_manifest_raw.decode('utf-8'))
                except Exception:
                    raise RuntimeError('viz_publication_manifest_json_invalid')
                if not isinstance(viz_manifest, dict) or any(
                    viz_manifest.get(key) != viz_publication.get(key)
                    for key in (
                        'schema_version', 'status', 'submission_key', 'path',
                        'size', 'sha256', 'source_path', 'source_sha256',
                        'published_at',
                    )
                ):
                    raise RuntimeError('viz_publication_manifest_mismatch')
                if viz_info.st_size != viz_publication.get('size'):
                    raise RuntimeError('viz_publication_size_mismatch')
                observed.extend([
                    {{
                        'path': viz_path,
                        'size': viz_info.st_size,
                        'sha256': str(viz_publication.get('sha256') or ''),
                        'is_file': True,
                        'is_symlink': False,
                        'parents_symlink_free': True,
                        'sha256_attested_by_manifest': True,
                    }},
                    {{
                        'path': viz_manifest_path,
                        'size': viz_manifest_info.st_size,
                        'sha256': hashlib.sha256(viz_manifest_raw).hexdigest(),
                        'is_file': True,
                        'is_symlink': False,
                        'parents_symlink_free': True,
                    }},
                ])
            artifact_meta = {{}}
            total = 0
            html_path = ''
            for row in rows:
                if not isinstance(row, dict):
                    raise RuntimeError('delivery_manifest_artifacts_invalid')
                raw_path = str(row.get('path') or '')
                if not raw_path or '..' in raw_path.split('/') or '\\x00' in raw_path:
                    raise RuntimeError('artifact_path_invalid')
                role = str(row.get('role') or '').strip().lower()
                media_type = str(row.get('media_type') or '').strip().lower()
                if (
                    raw_path.lower().endswith('.mcap')
                    or role in ('mcap', 'viz_mcap', 'visualization_mcap')
                    or 'mcap' in media_type
                ):
                    raise RuntimeError('html_delivery_mcap_forbidden')
                if (
                    raw_path.lower().endswith(('.svg', '.svgz', '.xhtml', '.xml'))
                    or media_type.split(';', 1)[0]
                    in (
                        'image/svg+xml',
                        'application/svg+xml',
                        'application/xhtml+xml',
                        'application/xml',
                        'text/xml',
                    )
                ):
                    raise RuntimeError('html_external_active_document_unsupported')
                path = posixpath.normpath(
                    raw_path if raw_path.startswith('/') else posixpath.join(ROOT, raw_path)
                )
                if posixpath.commonpath((root_norm, path)) != root_norm or path == root_norm:
                    raise RuntimeError('artifact_path_outside_root')
                fd, info = open_regular(path, 'artifact_missing', MAX_FILE_BYTES)
                is_symlink = False
                is_file = True
                total += info.st_size
                if total > MAX_TOTAL_BYTES:
                    os.close(fd)
                    raise RuntimeError('artifact_bundle_too_large')
                digest = hashlib.sha256()
                with os.fdopen(fd, 'rb') as handle:
                    for chunk in iter(lambda: handle.read(1024 * 1024), b''):
                        digest.update(chunk)
                observed.append({{
                    'path': path,
                    'size': info.st_size,
                    'sha256': digest.hexdigest(),
                    'is_file': is_file,
                    'is_symlink': is_symlink,
                    'parents_symlink_free': symlink_free(path),
                }})
                if path in artifact_meta:
                    raise RuntimeError('delivery_manifest_duplicate_artifact')
                artifact_meta[path] = {{
                    'media_type': media_type,
                    'size': info.st_size,
                    'sha256': digest.hexdigest(),
                }}
                if role == 'index_html':
                    html_path = path
            if not html_path:
                raise RuntimeError('required_html_artifact_missing')
            dependencies = []
            dependency_set = set()
            visited = set()
            queue = [(html_path, 'html')]
            text_total = 0
            while queue:
                source_path, expected_kind = queue.pop(0)
                source_meta = artifact_meta.get(source_path)
                if source_meta is None:
                    raise RuntimeError('html_dependency_not_manifested')
                kind = expected_kind or dependency_kind(
                    source_path,
                    source_meta['media_type'],
                )
                if not kind:
                    continue
                visit_key = (source_path, kind)
                if visit_key in visited:
                    continue
                visited.add(visit_key)
                text, text_size = read_text_artifact(source_path, source_meta)
                reject_banned_public_phrase(text)
                text_total += text_size
                if text_total > MAX_TEXT_TOTAL_BYTES:
                    raise RuntimeError('html_dependency_text_total_too_large')
                if kind == 'html':
                    refs = html_refs(text)
                elif kind == 'css':
                    refs = css_refs(text)
                else:
                    raise RuntimeError('html_script_execution_unsupported')
                for raw_ref, dependency_expected_kind in refs:
                    dep_path = resolve_ref(source_path, raw_ref)
                    if dep_path is None:
                        continue
                    if dep_path not in artifact_meta:
                        raise RuntimeError('html_dependency_not_manifested')
                    if dep_path not in dependency_set:
                        dependency_set.add(dep_path)
                        dependencies.append(dep_path)
                    if (dep_path, dependency_expected_kind) not in visited:
                        queue.append((dep_path, dependency_expected_kind))
            finish({{
                'ok': True,
                'delivery_contract': contract,
                'delivery_manifest': manifest,
                'observed_files': observed,
                'html_dependencies': dependencies,
            }})
        except RuntimeError as exc:
            code = str(exc)
            finish({{'ok': False, 'error_code': code, 'error': code}})
        """
    ).strip()


def default_artifact_bundle_reader(
    claim: ExecutionWatchClaim,
    *,
    ssh_mini_agent: str = DEFAULT_SSH_MINI_AGENT,
    timeout_seconds: int = 120,
) -> Mapping[str, Any]:
    """Hash one canonical VM bundle through a bounded, read-only agent script."""
    script = _remote_bundle_script(claim.submission_key)
    try:
        proc = subprocess.run(
            [ssh_mini_agent, "run_py_json"],
            input=script,
            text=True,
            capture_output=True,
            timeout=timeout_seconds,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise ArtifactBundleReadError(
            "artifact_reader_unavailable", type(exc).__name__
        ) from exc
    if proc.returncode != 0:
        raise ArtifactBundleReadError(
            "artifact_reader_unavailable",
            (proc.stderr or proc.stdout or f"ssh-mini-agent rc={proc.returncode}")[
                -1000:
            ],
        )
    try:
        payload = json.loads(proc.stdout or "{}")
    except json.JSONDecodeError as exc:
        raise ArtifactBundleReadError(
            "artifact_reader_response_invalid", "ssh-mini-agent returned invalid JSON"
        ) from exc
    if not isinstance(payload, dict):
        raise ArtifactBundleReadError("artifact_reader_response_invalid")
    if payload.get("ok") is not True:
        code = str(payload.get("error_code") or "artifact_bundle_unavailable")
        permanent = code not in {
            "artifact_reader_unavailable",
            "artifact_bundle_unavailable",
        } | _RETRYABLE_INFRASTRUCTURE_ARTIFACT_CODES and not _eventual_artifact_error(
            code
        )
        raise ArtifactBundleReadError(
            code, str(payload.get("error") or code), permanent=permanent
        )
    return payload


def _w3_execution_binding(
    snapshot_bundle: AdmissionSnapshotExecutionBundle,
) -> dict[str, str]:
    bundle = validate_snapshot_execution_bundle(snapshot_bundle)
    binding = {
        "schema_version": bundle.schema_version,
        "bundle_sha256": bundle.bundle_sha256,
        "snapshot_authority_sha256": bundle.snapshot_authority_sha256,
        "snapshot_id": bundle.snapshot.snapshot_id,
        "snapshot_sha256": bundle.snapshot.snapshot_sha256,
        "request_sha256": bundle.snapshot.request_sha256,
        "creator_source_envelope_sha256": (
            bundle.creator_source_envelope.source_envelope_sha256
        ),
    }
    binding.update(write_fence_binding(bundle.snapshot))
    return binding


def _snapshot_submission_admission(
    claim: ExecutionWatchClaim,
    snapshot_bundle: AdmissionSnapshotExecutionBundle,
):
    try:
        bundle = validate_snapshot_execution_bundle(snapshot_bundle)
        admission, _context = snapshot_execution_inputs(bundle)
    except Exception as exc:
        raise DeliveryContractError("w3_execution_snapshot_invalid") from exc
    envelope = bundle.creator_source_envelope
    refs = admission.source_refs
    if (
        admission.submission_key != claim.submission_key
        or admission.business_key != claim.business_key
        or admission.generation != claim.generation
        or refs.project_key != claim.project_key
        or refs.work_item_type_key != claim.work_item_type_key
        or refs.work_item_id != claim.work_item_id
        or claim.task_id != claim.submission_key
        or claim.origin_source_id != envelope.source_id
        or claim.trigger_origin_source_id != envelope.source_id
    ):
        raise DeliveryContractError("w3_execution_snapshot_identity_mismatch")
    result = claim.submission_result
    if (
        result.get("success") is not True
        or str(result.get("submission_key") or "").strip() != claim.submission_key
        or str(result.get("task_id") or "").strip() != claim.task_id
        or result.get("w3_execution_snapshot") != _w3_execution_binding(bundle)
    ):
        raise DeliveryContractError("w3_execution_snapshot_receipt_mismatch")
    return admission


def _submission_admission(
    claim: ExecutionWatchClaim,
    snapshot_bundle: AdmissionSnapshotExecutionBundle | None = None,
):
    if snapshot_bundle is not None:
        return _snapshot_submission_admission(claim, snapshot_bundle)
    payload = claim.submission_payload
    if payload.get("schema_version") != SUBMISSION_OUTBOX_SCHEMA_VERSION:
        raise DeliveryContractError("submission_outbox_contract_invalid")
    try:
        admission = validate_rca_admission(payload.get("admission") or {})
        trigger_context = validate_rca_trigger_context(
            payload.get("trigger_context") or {}
        )
    except Exception as exc:
        raise DeliveryContractError("submission_outbox_contract_invalid") from exc
    refs = admission.source_refs
    base_keys = {
        "schema_version",
        "business_key",
        "submission_key",
        "creation_rule_version",
        "generation",
        "origin_source_id",
        "admission",
        "trigger_context",
    }
    source_kind = trigger_context.source_kind
    if source_kind == "kafka_workflow_event":
        expected_keys = base_keys | {
            "source_event_id",
            "topic",
            "partition",
            "offset",
            "normalized_event",
        }
        normalized = payload.get("normalized_event")
        if not isinstance(normalized, Mapping):
            raise DeliveryContractError("submission_outbox_contract_invalid")
        expected_trigger_kind = (
            "issue_created" if claim.generation == 1 else "kafka_retrigger"
        )
        try:
            expected_admission = build_rca_admission(
                project_key=trigger_context.project_key,
                project_simple_name=trigger_context.project_simple_name,
                work_item_type_key=trigger_context.work_item_type_key,
                work_item_id=trigger_context.work_item_id,
                rule_version=trigger_context.creation_rule_version,
                trigger_kind=expected_trigger_kind,
                generation=claim.generation,
                topic=payload.get("topic", ""),
                partition=payload.get("partition"),
                offset=payload.get("offset"),
            )
        except Exception as exc:
            raise DeliveryContractError("submission_outbox_contract_invalid") from exc
        event_uid = (
            f"{payload.get('topic')}:{payload.get('partition')}:{payload.get('offset')}"
        )
        normalized_identity = {
            "schema_version": normalized.get("schema_version"),
            "creation_rule_version": normalized.get("creation_rule_version"),
            "project_key": normalized.get("project_key"),
            "project_simple_name": normalized.get("project_simple_name"),
            "work_item_type_key": normalized.get("work_item_type_key"),
            "work_item_id": normalized.get("work_item_id"),
            "issue_url": normalized.get("issue_url"),
            "title": normalized.get("title"),
        }
        trigger_identity = trigger_context.to_dict()
        trigger_identity.pop("source_kind")
        trigger_identity["schema_version"] = NORMALIZED_EVENT_SCHEMA_VERSION
        if (
            payload.get("source_event_id") != event_uid
            or normalized_identity != trigger_identity
            or admission.trigger_kind not in RCA_KAFKA_TRIGGER_KINDS
        ):
            raise DeliveryContractError("submission_outbox_contract_invalid")
    elif source_kind == "feishu_group_manual":
        expected_keys = base_keys
        expected_trigger_kind = (
            "manual_issue_request" if claim.generation == 1 else "manual_retrigger"
        )
        try:
            expected_admission = build_rca_admission(
                project_key=trigger_context.project_key,
                project_simple_name=trigger_context.project_simple_name,
                work_item_type_key=trigger_context.work_item_type_key,
                work_item_id=trigger_context.work_item_id,
                rule_version=trigger_context.creation_rule_version,
                trigger_kind=expected_trigger_kind,
                generation=claim.generation,
            )
        except Exception as exc:
            raise DeliveryContractError("submission_outbox_contract_invalid") from exc
        if refs.topic != "" or refs.partition is not None or refs.offset is not None:
            raise DeliveryContractError("submission_outbox_contract_invalid")
        if admission.trigger_kind not in RCA_MANUAL_TRIGGER_KINDS:
            raise DeliveryContractError("submission_outbox_contract_invalid")
    else:
        raise DeliveryContractError("submission_outbox_contract_invalid")

    origin_source_id = str(payload.get("origin_source_id") or "").strip()
    if (
        set(payload) != expected_keys
        or not origin_source_id
        or origin_source_id != claim.origin_source_id
        or origin_source_id != claim.trigger_origin_source_id
        or admission != expected_admission
        or admission.trigger_kind != expected_trigger_kind
        or admission.submission_key != claim.submission_key
        or admission.business_key != claim.business_key
        or admission.generation != claim.generation
        or payload.get("submission_key") != claim.submission_key
        or payload.get("business_key") != claim.business_key
        or payload.get("generation") != claim.generation
        or payload.get("creation_rule_version") != refs.rule_version
        or refs.project_key != claim.project_key
        or refs.work_item_type_key != claim.work_item_type_key
        or refs.work_item_id != claim.work_item_id
    ):
        raise DeliveryContractError("submission_watch_identity_mismatch")
    result = claim.submission_result
    if (
        result.get("success") is not True
        or str(result.get("submission_key") or "").strip() != claim.submission_key
        or str(result.get("task_id") or "").strip() != claim.task_id
    ):
        raise DeliveryContractError("submission_receipt_identity_mismatch")
    return admission


def _validate_w3_task_status(
    status: Mapping[str, Any],
    snapshot_bundle: AdmissionSnapshotExecutionBundle,
) -> None:
    binding = _w3_execution_binding(snapshot_bundle)
    meta = status.get("meta")
    meta = meta if isinstance(meta, Mapping) else {}
    expected_meta = {
        "rca_w3_snapshot_bundle_sha256": binding["bundle_sha256"],
        "rca_w3_snapshot_authority_sha256": binding[
            "snapshot_authority_sha256"
        ],
        "rca_w3_snapshot_sha256": binding["snapshot_sha256"],
        "rca_w3_request_sha256": binding["request_sha256"],
        "rca_w3_creator_source_envelope_sha256": binding[
            "creator_source_envelope_sha256"
        ],
    }
    fence = binding.get("write_fence")
    if isinstance(fence, Mapping):
        expected_meta.update(
            {
                "rca_w3_write_fence_id": str(fence.get("fence_id") or ""),
                "rca_w3_write_fence_sha256": canonical_write_fence_sha256(fence),
            }
        )
    if any(meta.get(name) != value for name, value in expected_meta.items()):
        raise DeliveryContractError("w3_execution_snapshot_task_mismatch")


def _validate_w3_delivery_bundle(
    delivery_bundle: Mapping[str, Any],
    snapshot_bundle: AdmissionSnapshotExecutionBundle,
) -> None:
    expected = _w3_execution_binding(snapshot_bundle)
    for field in ("delivery_contract", "delivery_manifest"):
        value = delivery_bundle.get(field)
        value = value if isinstance(value, Mapping) else {}
        if value.get("w3_execution_snapshot") != expected:
            raise DeliveryContractError("w3_execution_snapshot_delivery_mismatch")


def _status_state(status: Mapping[str, Any]) -> str:
    return (
        str(status.get("state") or status.get("dispatch_queue") or "").strip().lower()
    )


def _terminal_failure(
    status: Mapping[str, Any],
    state: str,
    *,
    failure_receipt: Mapping[str, Any] | None = None,
) -> tuple[
    pnc_fault_taxonomy.FailureDecision,
    str,
    dict[str, Any],
    dict[str, Any],
]:
    status_blocker = (
        dict(status.get("blocker"))
        if isinstance(status.get("blocker"), Mapping)
        else {}
    )
    receipt_blocker = (
        dict(failure_receipt.get("blocker"))
        if isinstance(failure_receipt, Mapping)
        and isinstance(failure_receipt.get("blocker"), Mapping)
        else {}
    )
    blocker = receipt_blocker or status_blocker
    source = "rca_service_result" if receipt_blocker else "vm_status"
    source_conflict = bool(
        receipt_blocker
        and status_blocker
        and pnc_fault_taxonomy.blocker_kind(receipt_blocker)
        != pnc_fault_taxonomy.blocker_kind(status_blocker)
    )
    if source_conflict:
        blocker = {
            **receipt_blocker,
            "fault_class": "receipt_status_blocker_conflict",
        }
    decision = pnc_fault_taxonomy.decide_failure(blocker)
    detail = str(
        blocker.get("message")
        or blocker.get("detail")
        or blocker.get("reason")
        or status.get("summary")
        or status.get("error")
        or state
    )
    projection = {
        **decision.as_dict(),
        "observed_state": state,
        "source": source,
        "source_conflict": source_conflict,
    }
    if isinstance(failure_receipt, Mapping):
        projection["receipt"] = {
            key: failure_receipt.get(key)
            for key in (
                "schema_version",
                "task_id",
                "status",
                "pipeline_status",
                "pipeline_stage",
            )
        }
    return decision, detail, projection, blocker


def _observed_failure(
    code: str,
    *,
    detail: str,
    state: str,
    source: str,
    retryable: bool | None = None,
) -> tuple[
    pnc_fault_taxonomy.FailureDecision,
    dict[str, Any],
    dict[str, Any],
]:
    blocker: dict[str, Any] = {
        "kind": str(code or "").strip().lower(),
        "message": str(detail or code)[:1000],
    }
    if retryable is not None:
        blocker["retryable"] = retryable
    decision = pnc_fault_taxonomy.decide_failure(blocker)
    projection = {
        **decision.as_dict(),
        "observed_state": state,
        "source": source,
        "source_conflict": False,
    }
    return decision, blocker, projection


def _durable_failure_decision(
    route: Mapping[str, Any],
) -> pnc_fault_taxonomy.FailureDecision:
    payload = route.get("route_payload")
    data = payload.get("decision") if isinstance(payload, Mapping) else None
    if not isinstance(data, Mapping):
        raise RuntimeError("durable_failure_route_decision_invalid")
    try:
        decision = pnc_fault_taxonomy.FailureDecision(
            raw_code=str(data["raw_code"]),
            terminal_error_code=str(data["terminal_error_code"]),
            lane=str(data["lane"]),
            internal_route=str(data["internal_route"]),
            known=data["known"],
            retryable=data["retryable"],
            external_comment_policy=str(data["external_comment_policy"]),
            terminal_fallback_seconds=data["terminal_fallback_seconds"],
            audit=dict(data["audit"]),
            contract_errors=tuple(data["contract_errors"]),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise RuntimeError("durable_failure_route_decision_invalid") from exc
    expected_route = {
        pnc_fault_taxonomy.INFRA_SELF_HEALABLE: (
            pnc_fault_taxonomy.INFRA_REMEDIATION_HOLD,
            "suppress_until_terminal_fallback",
            True,
        ),
        pnc_fault_taxonomy.NEEDS_HUMAN_INPUT: (
            pnc_fault_taxonomy.INTERNAL_BACKLOG,
            "honest_non_attribution_only",
            False,
        ),
        pnc_fault_taxonomy.HARD_DEFECT: (
            pnc_fault_taxonomy.INTERNAL_ALERT,
            "honest_non_attribution_only",
            False,
        ),
    }.get(decision.lane)
    if (
        not isinstance(data.get("audit"), Mapping)
        or not isinstance(data.get("contract_errors"), list)
        or type(decision.known) is not bool
        or type(decision.retryable) is not bool
        or type(decision.terminal_fallback_seconds) is not int
        or decision.terminal_fallback_seconds
        != pnc_fault_taxonomy.TERMINAL_FALLBACK_SECONDS
        or expected_route is None
        or decision.internal_route != expected_route[0]
        or decision.external_comment_policy != expected_route[1]
        or decision.retryable is not expected_route[2]
        or decision.internal_route
        not in {
            pnc_fault_taxonomy.INFRA_REMEDIATION_HOLD,
            pnc_fault_taxonomy.INTERNAL_BACKLOG,
            pnc_fault_taxonomy.INTERNAL_ALERT,
        }
        or decision.terminal_error_code != route.get("terminal_error_code")
        or decision.lane != route.get("lane")
        or decision.internal_route != route.get("route_kind")
    ):
        raise RuntimeError("durable_failure_route_decision_invalid")
    return decision


def _public_terminal_error_code(value: Any) -> str:
    candidate = str(value or "").strip().lower()
    known_blockers = (
        pnc_fault_taxonomy.INFRA_SELF_HEALABLE_KINDS
        | pnc_fault_taxonomy.NEEDS_HUMAN_INPUT_KINDS
        | pnc_fault_taxonomy.HARD_DEFECT_KINDS
        | frozenset(_PUBLIC_TERMINAL_BLOCKER_CODES.values())
    )
    taxonomy_gap = candidate.startswith("taxonomy_gap:") and all(
        char.islower() or char.isdigit() or char in "_.-:" for char in candidate
    )
    if (
        candidate in _PUBLIC_TERMINAL_ERROR_CODES
        or candidate in known_blockers
        or taxonomy_gap
    ):
        return candidate
    return _PUBLIC_TERMINAL_FALLBACK_CODE


class DeliveryCollector:
    def __init__(
        self,
        *,
        store: RcaDeliveryStore,
        config: CollectorConfig,
        status_reader: StatusReader = default_status_reader,
        artifact_bundle_reader: ArtifactBundleReader | None = None,
        failure_receipt_reader: FailureReceiptReader | None = None,
        infra_remediation_runner: InfraRemediationRunner | None = None,
        terminal_receipt_reader: TerminalReceiptReader | None = None,
        capacity_control_store: RcaControlStore | None = None,
        now: Callable[[], datetime] = _utc_now,
        lease_owner: str | None = None,
    ):
        self.store = store
        self.config = config
        self.status_reader = status_reader
        self.artifact_bundle_reader = artifact_bundle_reader or (
            lambda claim: default_artifact_bundle_reader(
                claim,
                ssh_mini_agent=config.ssh_mini_agent,
                timeout_seconds=config.artifact_read_timeout_seconds,
            )
        )
        self.failure_receipt_reader = failure_receipt_reader or (
            lambda claim: default_failure_receipt_reader(
                claim,
                ssh_mini_agent=config.ssh_mini_agent,
                timeout_seconds=min(config.artifact_read_timeout_seconds, 30),
            )
        )
        self.infra_remediation_runner = (
            infra_remediation_runner or default_infra_remediation_runner
        )
        self.now = now
        self.lease_owner = lease_owner or (
            f"rca-delivery-collector:{socket.gethostname()}:{os.getpid()}"
        )
        self.stats = CollectorStats()
        self.runtime_identity: Mapping[str, Any] | None = None
        self.capacity_control_store = capacity_control_store
        self.terminal_receipt_reader = terminal_receipt_reader or (
            lambda task_id, attempt_id: read_remote_vm_terminal_receipt(
                ssh_mini_agent=self.config.ssh_mini_agent,
                task_id=task_id,
                attempt_id=attempt_id,
                timeout_seconds=(self.config.capacity_terminal_receipt_timeout_seconds),
            )
        )
        self.capacity_last_error = ""
        self.capacity_last_outcome = "disabled"

    def backfill(self) -> int:
        inserted = self.store.backfill_completed_submissions(
            limit=self.config.backfill_batch_size,
            now=self.now(),
            activation_required=False,
        )
        self.stats.watches_created += inserted
        return inserted

    def _next_poll(self, attempt: int, *, running: bool) -> datetime:
        base = self.config.running_poll_seconds
        seconds = (
            base
            if running
            else min(
                self.config.max_poll_seconds,
                base * (2 ** min(max(attempt - 1, 0), 4)),
            )
        )
        return self.now() + timedelta(seconds=seconds)

    def _retry(
        self,
        claim: ExecutionWatchClaim,
        *,
        status: dict[str, Any],
        observed_state: str,
        error_code: str,
        error_detail: str,
        running: bool = False,
        not_after: datetime | None = None,
    ) -> CollectOutcome:
        next_poll = self._next_poll(claim.poll_attempt, running=running)
        if not_after is not None:
            next_poll = min(next_poll, not_after.astimezone(timezone.utc))
        self.store.reschedule_watch(
            submission_key=claim.submission_key,
            lease_token=claim.lease_token,
            observed_state=observed_state,
            status=status,
            next_poll_at=next_poll,
            error_code=error_code,
            error_detail=error_detail,
            now=self.now(),
        )
        if running:
            self.stats.running += 1
            outcome = "running"
        else:
            self.stats.retried += 1
            outcome = "retry_wait"
        return CollectOutcome(
            status=outcome,
            submission_key=claim.submission_key,
            error_code=error_code,
            next_poll_at=_utc_iso(next_poll),
        )

    @staticmethod
    def _failure_route_owner(lane: str) -> str:
        return {
            pnc_fault_taxonomy.INFRA_SELF_HEALABLE: "rca-infra",
            pnc_fault_taxonomy.NEEDS_HUMAN_INPUT: "rca-triage",
            pnc_fault_taxonomy.HARD_DEFECT: "rca-engineering",
        }[lane]

    def _handle_failure_until_deadline(
        self,
        claim: ExecutionWatchClaim,
        *,
        status: dict[str, Any],
        decision: pnc_fault_taxonomy.FailureDecision,
        blocker: Mapping[str, Any],
        detail: str,
        taxonomy: Mapping[str, Any],
    ) -> CollectOutcome:
        now = self.now()
        started, deadline, elapsed_seconds = _work_window(claim, now)
        owner = self._failure_route_owner(decision.lane)
        remediation = pnc_fault_taxonomy.remediation_for(blocker) or {}
        enriched = dict(status)
        projection = dict(taxonomy)
        projection["work_window"] = {
            "work_started_at": _utc_iso(started),
            "deadline_at": _utc_iso(deadline),
            "elapsed_seconds": max(0, int(elapsed_seconds)),
        }
        enriched["failure_taxonomy"] = projection
        route = self.store.upsert_failure_route(
            claim=claim,
            terminal_error_code=decision.terminal_error_code,
            lane=decision.lane,
            route_kind=decision.internal_route,
            owner=owner,
            work_started_at=_utc_iso(started),
            deadline_at=_utc_iso(deadline),
            audit={
                "schema_version": "pnc_rca_failure_route_audit_v1",
                "taxonomy_audit": decision.audit,
                "contract_errors": list(decision.contract_errors),
                "source": projection.get("source", "host_observation"),
                "receipt": projection.get("receipt", {}),
            },
            route_payload={
                "schema_version": "pnc_rca_failure_route_payload_v1",
                "decision": decision.as_dict(),
                "remediation": remediation,
                "blocker": dict(blocker),
            },
            now=now,
        )
        projection["durable_route"] = {
            "route_key": route.route_key,
            "owner": route.owner,
            "status": route.status,
            "created": route.created,
            "remediation_attempt_count": route.remediation_attempt_count,
        }
        if route.created:
            if decision.internal_route == pnc_fault_taxonomy.INTERNAL_BACKLOG:
                self.stats.internal_backlog += 1
            elif decision.internal_route == pnc_fault_taxonomy.INTERNAL_ALERT:
                self.stats.internal_alert += 1
            if not decision.known:
                self.stats.taxonomy_gaps += 1

        deadline_reached = elapsed_seconds >= decision.terminal_fallback_seconds
        if (
            not deadline_reached
            and decision.internal_route == pnc_fault_taxonomy.INFRA_REMEDIATION_HOLD
            and self.store.claim_failure_remediation(
                claim=claim,
                route_key=route.route_key,
                now=now,
            )
        ):
            self.stats.remediation_attempted += 1
            operation = str(remediation.get("op") or "") or "unavailable"
            try:
                raw_result = self.infra_remediation_runner(
                    claim,
                    blocker,
                    remediation,
                    MAX_INFRA_REMEDIATION_SECONDS,
                )
                result, succeeded = _validated_remediation_result(
                    raw_result,
                    claim=claim,
                    operation=operation,
                )
            except Exception as exc:
                result = {
                    "schema_version": INFRA_REMEDIATION_SCHEMA_VERSION,
                    "success": False,
                    "status": "failed",
                    "submission_key": claim.submission_key,
                    "business_key": claim.business_key,
                    "generation": claim.generation,
                    "task_id": claim.task_id,
                    "operation": operation,
                    "blocker_kind": pnc_fault_taxonomy.blocker_kind(blocker),
                    "resumed_same_task": False,
                    "external_writes": False,
                    "timeout_seconds": MAX_INFRA_REMEDIATION_SECONDS,
                    "error_code": (
                        f"infra_remediation_runner_failed:{type(exc).__name__}"
                    )[:120],
                }
                succeeded = False
            now = self.now()
            self.store.finish_failure_remediation(
                claim=claim,
                route_key=route.route_key,
                succeeded=succeeded,
                result=result,
                now=now,
            )
            started, deadline, elapsed_seconds = _work_window(claim, now)
            deadline_reached = elapsed_seconds >= decision.terminal_fallback_seconds
            projection["work_window"] = {
                "work_started_at": _utc_iso(started),
                "deadline_at": _utc_iso(deadline),
                "elapsed_seconds": max(0, int(elapsed_seconds)),
            }
            projection["durable_route"]["status"] = (
                "remediation_succeeded" if succeeded else "remediation_held"
            )
            projection["durable_route"]["remediation_attempt_count"] = 1
            projection["durable_route"]["remediation_result"] = result
            if succeeded:
                self.stats.remediation_succeeded += 1
            else:
                self.stats.remediation_held += 1

        enriched["failure_taxonomy"] = projection
        if not deadline_reached:
            next_poll = min(
                self._next_poll(claim.poll_attempt, running=False),
                deadline,
            )
            self.store.reschedule_failure_route(
                claim=claim,
                route_key=route.route_key,
                next_retry_at=next_poll,
                now=now,
            )
            self.store.reschedule_watch(
                submission_key=claim.submission_key,
                lease_token=claim.lease_token,
                observed_state=claim.state,
                status=enriched,
                next_poll_at=next_poll,
                error_code=decision.terminal_error_code,
                error_detail=detail,
                now=now,
            )
            self.stats.failure_holds += 1
            return CollectOutcome(
                status="failure_hold",
                submission_key=claim.submission_key,
                error_code=decision.terminal_error_code,
                next_poll_at=_utc_iso(next_poll),
            )

        fallback = {
            "schema_version": TERMINAL_FALLBACK_CONTRACT_SCHEMA_VERSION,
            "work_started_at": _utc_iso(started),
            "deadline_at": _utc_iso(deadline),
            "elapsed_seconds": max(0, int(elapsed_seconds)),
            "confidence_tier": "low",
            "terminal_class": "honest_non_attribution",
            "route_key": route.route_key,
            "route_kind": decision.internal_route,
            "route_owner": owner,
        }
        projection["terminal_fallback"] = fallback
        enriched["failure_taxonomy"] = projection
        self.stats.terminal_fallbacks += 1
        return self._durable_terminal_outcome(
            claim,
            status=enriched,
            outcome="terminal_failed",
            terminal_state="failed",
            error_code=decision.terminal_error_code,
            error_detail=detail,
            terminal_fallback=fallback,
        )

    def _durable_terminal_outcome(
        self,
        claim: ExecutionWatchClaim,
        *,
        status: dict[str, Any],
        outcome: str,
        terminal_state: str,
        error_code: str,
        error_detail: str,
        terminal_fallback: Mapping[str, Any] | None = None,
    ) -> CollectOutcome:
        safe_outcome = (
            outcome if outcome in TERMINAL_DELIVERY_OUTCOMES else "quarantined"
        )
        safe_state = (
            terminal_state
            if terminal_state in _FAILED_TERMINAL_STATES | {"quarantined"}
            else "quarantined"
        )
        safe_error_code = _public_terminal_error_code(error_code)
        try:
            result = self.store.create_terminal_delivery(
                claim=claim,
                status=status,
                outcome=safe_outcome,
                terminal_state=safe_state,
                error_code=safe_error_code,
                error_detail=error_detail,
                terminal_fallback=terminal_fallback,
                runtime_identity=self.runtime_identity,
                now=self.now(),
                activation_required=False,
            )
        except StaleDeliveryWatchLeaseError:
            self.stats.stale_lease += 1
            return CollectOutcome(
                status="lease_lost",
                submission_key=claim.submission_key,
                error_code="stale_delivery_watch_lease",
            )
        except DeliveryRecordConflictError as exc:
            return self._retry(
                claim,
                status=status,
                observed_state=claim.state,
                error_code="delivery_record_conflict",
                error_detail=f"delivery_record_conflict: {exc}",
            )
        if safe_outcome == "terminal_failed":
            self.stats.terminal_failed += 1
        else:
            self.stats.quarantined += 1
        if result.created:
            self.stats.delivery_created += 1
        else:
            self.stats.delivery_deduped += 1
        return CollectOutcome(
            status=safe_outcome,
            submission_key=claim.submission_key,
            delivery_id=result.delivery_id,
            effect_key=result.effect_key,
            error_code=safe_error_code,
            created=result.created,
        )

    def _handle_observed_failure(
        self,
        claim: ExecutionWatchClaim,
        *,
        status: dict[str, Any],
        code: str,
        detail: str,
        state: str,
        source: str,
    ) -> CollectOutcome:
        decision, blocker, taxonomy = _observed_failure(
            code,
            detail=detail,
            state=state,
            source=source,
        )
        return self._handle_failure_until_deadline(
            claim,
            status=status,
            decision=decision,
            blocker=blocker,
            detail=detail,
            taxonomy=taxonomy,
        )

    def _quarantine_w3_snapshot_failure(
        self,
        claim: ExecutionWatchClaim,
        *,
        code: str,
        detail: str,
    ) -> CollectOutcome:
        status = {
            "schema_version": "pnc_rca_w3_snapshot_quarantine_v1",
            "external_writes": False,
            "error_code": code,
        }
        try:
            self.store.quarantine_watch(
                submission_key=claim.submission_key,
                lease_token=claim.lease_token,
                status=status,
                error_code=code,
                error_detail=detail,
                now=self.now(),
            )
        except StaleDeliveryWatchLeaseError:
            self.stats.stale_lease += 1
            return CollectOutcome(
                status="lease_lost",
                submission_key=claim.submission_key,
                error_code="stale_delivery_watch_lease",
            )
        self.stats.quarantined += 1
        self.stats.internal_alert += 1
        return CollectOutcome(
            status="quarantined",
            submission_key=claim.submission_key,
            error_code=code,
        )

    def _deadline_outcome(
        self,
        claim: ExecutionWatchClaim,
        *,
        status: dict[str, Any],
        state: str,
        source: str,
    ) -> CollectOutcome | None:
        now = self.now()
        _started, _deadline, elapsed_seconds = _work_window(claim, now)
        if elapsed_seconds < pnc_fault_taxonomy.TERMINAL_FALLBACK_SECONDS:
            return None
        route = self.store.failure_route_for_deadline(claim=claim, now=now)
        if route is not None:
            try:
                decision = _durable_failure_decision(route)
            except RuntimeError:
                pass
            else:
                blocker: dict[str, Any] = {
                    "kind": decision.raw_code,
                    "retryable": decision.retryable,
                }
                if decision.audit:
                    blocker["audit"] = decision.audit
                taxonomy = {
                    **decision.as_dict(),
                    "observed_state": state,
                    "source": "durable_failure_route_deadline",
                    "source_conflict": False,
                    "deadline_observation_source": source,
                    "resumed_route_key": route.get("route_key"),
                }
                return self._handle_failure_until_deadline(
                    claim,
                    status=status,
                    decision=decision,
                    blocker=blocker,
                    detail=(
                        f"{decision.terminal_error_code} remained unresolved at "
                        "the RCA work deadline"
                    ),
                    taxonomy=taxonomy,
                )
        return self._handle_observed_failure(
            claim,
            status=status,
            code="rca_work_deadline_exceeded",
            detail="RCA work did not produce a deliverable result within 30 minutes",
            state=state,
            source=source,
        )

    def collect_one(self) -> CollectOutcome:
        self.stats.loops += 1
        if not self.config.enabled:
            return CollectOutcome(status="disabled")
        claim = self.store.claim_due_watch(
            lease_owner=self.lease_owner,
            lease_seconds=self.config.lease_seconds,
            now=self.now(),
            activation_required=False,
        )
        if claim is None:
            self.stats.idle += 1
            return CollectOutcome(status="idle")
        self.stats.claimed += 1
        status: dict[str, Any] = {}
        snapshot_bundle: AdmissionSnapshotExecutionBundle | None = None
        admission = None
        if self.config.w3_snapshot_read_mode == "snapshot_required":
            try:
                snapshot_bundle = self._control_store().read_w3_execution_snapshot(
                    claim.submission_key,
                    snapshot_authority=self.config.w3_snapshot_authority,
                    required=True,
                )
                if not isinstance(
                    snapshot_bundle,
                    AdmissionSnapshotExecutionBundle,
                ):
                    raise RecordConflictError("w3_execution_snapshot_missing")
                admission = _submission_admission(claim, snapshot_bundle)
            except Exception as exc:
                code = (
                    exc.code
                    if isinstance(exc, DeliveryContractError)
                    else str(exc) or "w3_execution_snapshot_invalid"
                )
                if code not in {
                    "w3_execution_snapshot_missing",
                    "w3_execution_snapshot_authority_mismatch",
                    "w3_execution_snapshot_invalid",
                    "w3_execution_snapshot_identity_mismatch",
                    "w3_execution_snapshot_receipt_mismatch",
                }:
                    code = "w3_execution_snapshot_invalid"
                return self._quarantine_w3_snapshot_failure(
                    claim,
                    code=code,
                    detail=f"{code}: {type(exc).__name__}",
                )
        deadline_outcome = self._deadline_outcome(
            claim,
            status=status,
            state=claim.state,
            source="before_admission",
        )
        if deadline_outcome is not None:
            return deadline_outcome
        if admission is None:
            try:
                admission = _submission_admission(claim)
            except Exception as exc:
                deadline_outcome = self._deadline_outcome(
                    claim,
                    status=status,
                    state=claim.state,
                    source="submission_admission",
                )
                if deadline_outcome is not None:
                    return deadline_outcome
                code = (
                    exc.code
                    if isinstance(exc, DeliveryContractError)
                    else "submission_admission_invalid"
                )
                return self._handle_observed_failure(
                    claim,
                    status=status,
                    code=code,
                    detail=f"{code}: {exc}",
                    state=claim.state,
                    source="submission_admission",
                )
        deadline_outcome = self._deadline_outcome(
            claim,
            status=status,
            state=claim.state,
            source="after_admission",
        )
        if deadline_outcome is not None:
            return deadline_outcome

        try:
            raw_status = self.status_reader(claim.task_id)
            if not isinstance(raw_status, Mapping):
                raise TypeError("status_reader must return an object")
            status = dict(raw_status)
            if snapshot_bundle is not None and status.get("success") is True:
                _validate_w3_task_status(status, snapshot_bundle)
        except Exception as exc:
            if snapshot_bundle is not None and isinstance(
                exc,
                DeliveryContractError,
            ):
                return self._quarantine_w3_snapshot_failure(
                    claim,
                    code=exc.code,
                    detail=exc.detail,
                )
            deadline_outcome = self._deadline_outcome(
                claim,
                status=status,
                state=claim.state,
                source="vm_status_reader",
            )
            if deadline_outcome is not None:
                return deadline_outcome
            return self._handle_observed_failure(
                claim,
                status=status,
                code="vm_status_reader_unavailable",
                detail=type(exc).__name__,
                state=claim.state,
                source="vm_status_reader",
            )

        state = _status_state(status)
        deadline_outcome = self._deadline_outcome(
            claim,
            status=status,
            state=state,
            source="after_vm_status_read",
        )
        if deadline_outcome is not None:
            return deadline_outcome
        if status.get("success") is not True:
            code = (
                "vm_status_missing" if state == "missing" else "vm_status_unavailable"
            )
            return self._handle_observed_failure(
                claim,
                status=status,
                code=code,
                detail=str(status.get("error") or code),
                state=state,
                source="vm_status",
            )
        if state in _RUNNING_STATES:
            _started, deadline, _elapsed_seconds = _work_window(claim, self.now())
            return self._retry(
                claim,
                status=status,
                observed_state=state,
                error_code="",
                error_detail="",
                running=True,
                not_after=deadline,
            )
        if state in _FAILED_TERMINAL_STATES:
            failure_receipt: Mapping[str, Any] | None = None
            try:
                failure_receipt = self.failure_receipt_reader(claim)
            except FailureReceiptReadError as exc:
                return self._handle_observed_failure(
                    claim,
                    status=status,
                    code=exc.code,
                    detail=exc.detail,
                    state=state,
                    source="failure_receipt_reader",
                )
            except Exception as exc:
                return self._handle_observed_failure(
                    claim,
                    status=status,
                    code="failure_receipt_reader_unavailable",
                    detail=type(exc).__name__,
                    state=state,
                    source="failure_receipt_reader",
                )
            decision, detail, taxonomy, blocker = _terminal_failure(
                status,
                state,
                failure_receipt=failure_receipt,
            )
            return self._handle_failure_until_deadline(
                claim,
                status=status,
                decision=decision,
                blocker=blocker,
                detail=detail,
                taxonomy=taxonomy,
            )
        if state not in _COMPLETED_STATES:
            return self._handle_observed_failure(
                claim,
                status=status,
                code="vm_status_unknown",
                detail=f"unrecognized VM state: {state or 'missing'}",
                state=state,
                source="vm_status",
            )

        try:
            bundle = self.artifact_bundle_reader(claim)
            if not isinstance(bundle, Mapping):
                raise ArtifactBundleReadError("artifact_reader_response_invalid")
            w3_binding = None
            if snapshot_bundle is not None:
                _validate_w3_delivery_bundle(bundle, snapshot_bundle)
                w3_binding = _w3_execution_binding(snapshot_bundle)
                fence = w3_binding.get("write_fence")
                if not isinstance(fence, Mapping):
                    raise DeliveryContractError("external_write_fence_missing")
                try:
                    source_targets = validate_write_fence_source_binding(
                        fence,
                        snapshot=snapshot_bundle.snapshot,
                        source_envelope=(
                            snapshot_bundle.creator_source_envelope
                        ),
                    )
                    live = self._control_store().validate_external_write_fence_binding(
                        fence
                    )
                    if any(
                        live.get(name) != source_targets.get(name)
                        for name in (
                            "issue_target",
                            "thread_target",
                            "chat_id",
                            "target_set_sha256",
                        )
                    ):
                        raise ExternalWriteFenceError(
                            "external_write_fence_target_mismatch"
                        )
                    validate_write_fence(
                        fence,
                        snapshot=snapshot_bundle.snapshot,
                        expected_epoch_id=live["epoch_id"],
                        expected_ledger_id=live["ledger_id"],
                        expected_business_key=claim.business_key,
                        expected_submission_key=claim.submission_key,
                        expected_generation=claim.generation,
                        expected_target_set_sha256=source_targets[
                            "target_set_sha256"
                        ],
                        now=self.now(),
                    )
                except ExternalWriteFenceError as exc:
                    raise DeliveryContractError(exc.code, exc.detail) from exc
                except Exception as exc:
                    raise DeliveryContractError(
                        "external_write_fence_epoch_not_current",
                        type(exc).__name__,
                    ) from exc
            delivery: VerifiedDelivery = verify_delivery_bundle(
                admission=admission,
                delivery_contract=bundle.get("delivery_contract") or {},
                delivery_manifest=bundle.get("delivery_manifest") or {},
                observed_files=bundle.get("observed_files") or [],
                html_dependencies=bundle.get("html_dependencies") or [],
                w3_execution_binding=w3_binding,
            )
        except ArtifactBundleReadError as exc:
            deadline_outcome = self._deadline_outcome(
                claim,
                status=status,
                state=state,
                source="artifact_bundle_reader",
            )
            if deadline_outcome is not None:
                return deadline_outcome
            return self._handle_observed_failure(
                claim,
                status=status,
                code=exc.code,
                detail=f"{exc.code}: {exc.detail}",
                state=state,
                source="artifact_bundle_reader",
            )
        except DeliveryContractError as exc:
            if snapshot_bundle is not None and exc.code.startswith(
                "w3_execution_snapshot_"
            ):
                return self._quarantine_w3_snapshot_failure(
                    claim,
                    code=exc.code,
                    detail=exc.detail,
                )
            deadline_outcome = self._deadline_outcome(
                claim,
                status=status,
                state=state,
                source="delivery_contract_verifier",
            )
            if deadline_outcome is not None:
                return deadline_outcome
            return self._handle_observed_failure(
                claim,
                status=status,
                code=exc.code,
                detail=f"{exc.code}: {exc.detail}",
                state=state,
                source="delivery_contract_verifier",
            )
        except Exception as exc:
            deadline_outcome = self._deadline_outcome(
                claim,
                status=status,
                state=state,
                source="delivery_contract_verifier",
            )
            if deadline_outcome is not None:
                return deadline_outcome
            return self._handle_observed_failure(
                claim,
                status=status,
                code="artifact_verifier_unavailable",
                detail=type(exc).__name__,
                state=state,
                source="delivery_contract_verifier",
            )
        deadline_outcome = self._deadline_outcome(
            claim,
            status=status,
            state=state,
            source="after_artifact_verification",
        )
        if deadline_outcome is not None:
            return deadline_outcome
        deadline_outcome = self._deadline_outcome(
            claim,
            status=status,
            state=state,
            source="before_delivery_create",
        )
        if deadline_outcome is not None:
            return deadline_outcome
        try:
            result = self.store.create_delivery(
                claim=claim,
                delivery=delivery,
                status=status,
                runtime_identity=self.runtime_identity,
                now=self.now(),
                activation_required=False,
            )
        except StaleDeliveryWatchLeaseError:
            self.stats.stale_lease += 1
            return CollectOutcome(
                status="lease_lost",
                submission_key=claim.submission_key,
                error_code="stale_delivery_watch_lease",
            )
        except DeliveryRecordConflictError as exc:
            return self._handle_observed_failure(
                claim,
                status=status,
                code="delivery_record_conflict",
                detail=str(exc),
                state=state,
                source="delivery_store",
            )
        if result.created:
            self.stats.delivery_created += 1
        else:
            self.stats.delivery_deduped += 1
        return CollectOutcome(
            status="delivery_created" if result.created else "delivery_deduped",
            submission_key=claim.submission_key,
            delivery_id=result.delivery_id,
            effect_key=result.effect_key,
            created=result.created,
        )

    def collect_batch(self) -> list[CollectOutcome]:
        self.backfill()
        outcomes: list[CollectOutcome] = []
        for _ in range(self.config.batch_size):
            outcome = self.collect_one()
            outcomes.append(outcome)
            if outcome.status in {"disabled", "idle"}:
                break
        try:
            self.collect_capacity_samples()
        except Exception as exc:
            self.stats.capacity_rejected += 1
            code = getattr(exc, "code", "rca_capacity_sample_collection_failed")
            self.capacity_last_error = str(code)[:120]
            self.stats.capacity_last_error = self.capacity_last_error
            self.capacity_last_outcome = "rejected"
            raise
        return outcomes

    def _control_store(self) -> RcaControlStore:
        if self.capacity_control_store is None:
            self.capacity_control_store = RcaControlStore(
                self.config.control_db_path, require_current=True
            )
        return self.capacity_control_store

    def _capacity_store(self) -> RcaControlStore:
        return self._control_store()

    def _capacity_ledger_identities(
        self, paths: CapacityRuntimePaths, *, hmac_key: bytes
    ) -> set[tuple[str, str]]:
        try:
            paths.sample_ledger.lstat()
        except FileNotFoundError:
            return set()
        ledger = capacity_transition.read_sample_ledger(
            paths.sample_ledger,
            hmac_key=hmac_key,
            timeout_seconds=self.config.capacity_sample_lock_timeout_seconds,
        )
        return {
            (str(sample["task_id"]), str(sample["attempt_id"]))
            for sample in ledger.samples
        }

    def collect_capacity_samples(self) -> None:
        if not self.config.capacity_sample_enabled:
            self.capacity_last_outcome = "disabled"
            self.capacity_last_error = ""
            self.stats.capacity_last_error = ""
            return
        paths = CapacityRuntimePaths.from_control_db(self.config.control_db_path)
        key = load_capacity_hmac_key()
        raw_terminal_key = os.environ.get(TERMINAL_HMAC_ENV, "").strip()
        terminal_key = (
            load_capacity_hmac_key(raw_terminal_key) if raw_terminal_key else None
        )
        control = self._capacity_store()
        self.capacity_last_error = ""
        self.stats.capacity_last_error = ""
        ensure_owner_only_lock_file(paths.global_lock)
        with capacity_transition.capacity_flock(
            paths.global_lock,
            exclusive=False,
            timeout_seconds=self.config.capacity_sample_lock_timeout_seconds,
        ):
            state = control.capacity_transition_state()
            if state is None:
                raise CapacitySampleEvidenceError(
                    "rca_capacity_persisted_state_missing"
                )
            if state.get("state") == capacity_transition.STEADY_ACTIVE:
                self.stats.capacity_frozen += 1
                self.capacity_last_outcome = "frozen"
                return
            if state.get("state") != capacity_transition.BOOTSTRAP_PRODUCTION:
                raise CapacitySampleEvidenceError(
                    "rca_capacity_sample_producer_not_bootstrap"
                )
            activation, activation_sha = read_and_validate_producer_activation(
                producer_activation_path(paths.state_root),
                hmac_key=key,
                expected_release_id=str(state.get("release_id") or ""),
                expected_bootstrap_epoch_id=str(state.get("bootstrap_epoch_id") or ""),
            )
            excluded = self._capacity_ledger_identities(paths, hmac_key=key)
        activated_at = datetime.fromisoformat(
            str(activation["activated_at"]).replace("Z", "+00:00")
        ).astimezone(timezone.utc)
        snapshots = self.store.capacity_sample_candidates(
            activated_at=activated_at,
            limit=self.config.capacity_sample_batch_size,
            excluded_task_attempts=excluded,
        )
        self.stats.capacity_scanned += len(snapshots)
        if not snapshots:
            self.capacity_last_outcome = "idle"
            return
        for snapshot in snapshots:
            payload = snapshot.payload
            task_id = str(payload.get("task_id") or "")
            attempt_id = str(payload.get("attempt_id") or "")
            try:
                if not task_id or not attempt_id:
                    raise CapacitySampleEvidenceError(
                        "rca_capacity_delivery_snapshot_identity_invalid"
                    )
                terminal_raw = self.terminal_receipt_reader(task_id, attempt_id)
                observed_at = datetime.fromisoformat(
                    str(payload.get("snapshot_at") or "").replace("Z", "+00:00")
                ).astimezone(timezone.utc)
                built = build_capacity_sample(
                    snapshot=payload,
                    delivery_snapshot_sha256=snapshot.snapshot_sha256,
                    task_meta=payload.get("task_meta") or {},
                    vm_terminal_raw=terminal_raw,
                    producer_activation=activation,
                    producer_activation_receipt_sha256=activation_sha,
                    admission_hmac_key=key,
                    terminal_hmac_key=terminal_key,
                    observed_at=observed_at,
                )
                self.stats.capacity_eligible += 1
                with capacity_transition.capacity_flock(
                    paths.global_lock,
                    exclusive=True,
                    timeout_seconds=(self.config.capacity_sample_lock_timeout_seconds),
                ):
                    current_state = control.capacity_transition_state()
                    if current_state is None:
                        raise CapacitySampleEvidenceError(
                            "rca_capacity_persisted_state_missing"
                        )
                    if current_state.get("state") == capacity_transition.STEADY_ACTIVE:
                        self.stats.capacity_frozen += 1
                        self.capacity_last_outcome = "frozen"
                        return
                    if (
                        current_state.get("state")
                        != capacity_transition.BOOTSTRAP_PRODUCTION
                        or current_state.get("release_id") != state.get("release_id")
                        or current_state.get("bootstrap_epoch_id")
                        != state.get("bootstrap_epoch_id")
                    ):
                        raise CapacitySampleEvidenceError(
                            "rca_capacity_sample_producer_state_changed"
                        )
                    current_activation, current_activation_sha = (
                        read_and_validate_producer_activation(
                            producer_activation_path(paths.state_root),
                            hmac_key=key,
                            expected_release_id=str(
                                current_state.get("release_id") or ""
                            ),
                            expected_bootstrap_epoch_id=str(
                                current_state.get("bootstrap_epoch_id") or ""
                            ),
                        )
                    )
                    if (
                        current_activation != activation
                        or current_activation_sha != activation_sha
                    ):
                        raise CapacitySampleEvidenceError(
                            "rca_capacity_producer_receipt_changed"
                        )
                    excluded = self._capacity_ledger_identities(paths, hmac_key=key)
                    if (task_id, attempt_id) in excluded:
                        self.capacity_last_outcome = "deduped"
                        continue
                    host_path = host_success_receipt_path(
                        paths.state_root,
                        task_id=task_id,
                        attempt_id=attempt_id,
                    )
                    host_sha = write_owner_only_create_once(
                        host_path, built.host_success_receipt
                    )
                    if host_sha != built.host_success_receipt_sha256:
                        raise CapacitySampleEvidenceError(
                            "rca_capacity_host_receipt_publish_mismatch"
                        )
                    validate_host_success_receipt(
                        built.host_success_receipt, hmac_key=key
                    )
                    capacity_transition.append_capacity_sample(
                        paths.sample_ledger,
                        built.sample,
                        hmac_key=key,
                        persisted_state_loader=control.capacity_transition_state,
                        timeout_seconds=(
                            self.config.capacity_sample_lock_timeout_seconds
                        ),
                    )
                    excluded.add((task_id, attempt_id))
                    self.stats.capacity_appended += 1
                    self.capacity_last_outcome = "appended"
            except Exception as exc:
                self.stats.capacity_rejected += 1
                code = getattr(exc, "code", "rca_capacity_sample_collection_failed")
                self.capacity_last_error = str(code)[:120]
                self.stats.capacity_last_error = self.capacity_last_error
                self.capacity_last_outcome = "rejected"

    def dry_run_once(self) -> dict[str, Any]:
        rows = self.store.preview_unwatched_completed(
            limit=self.config.backfill_batch_size,
            activation_required=False,
        )
        previews: list[dict[str, Any]] = []
        for row in rows[: self.config.batch_size]:
            task_id = str(row.get("submission_key") or "")
            try:
                raw = self.status_reader(task_id)
                status = dict(raw) if isinstance(raw, Mapping) else {}
                error = ""
            except Exception as exc:
                status = {}
                error = type(exc).__name__
            previews.append({
                "submission_key": task_id,
                "business_key": row.get("business_key"),
                "generation": row.get("generation"),
                "work_item_id": row.get("work_item_id"),
                "vm_state": _status_state(status),
                "status_success": status.get("success") is True,
                "error": error or status.get("error") or "",
            })
        return {
            "ok": True,
            "dry_run": True,
            "external_writes": False,
            "candidate_count": len(rows),
            "rows": previews,
        }


class HealthReporter:
    def __init__(
        self,
        config: CollectorConfig,
        store: RcaDeliveryStore,
        *,
        remote_css_probe: Callable[..., Mapping[str, Any]] | None = None,
    ):
        self.config = config
        self.store = store
        self.started_at = _utc_iso()
        self.runtime_identity = build_runtime_identity(
            service_label=SERVICE_LABEL,
            script_path=Path(__file__),
            public_config=config.public_dict(),
            loaded_dependencies=RCA_DELIVERY_COLLECTOR_LOADED_DEPENDENCIES,
        )
        self._remote_css_probe = remote_css_probe or probe_remote_css_parser
        self._remote_css_parser_receipt: dict[str, Any] = {
            "status": "disabled",
            "observed_at": self.started_at,
        }
        self._remote_css_parser_observed_at: datetime | None = None
        self._remote_css_parser_last_probe_at: datetime | None = None
        self._remote_css_parser_error = ""
        if self.config.enabled:
            self._refresh_remote_css_parser_receipt(force=True)

    @property
    def dependencies_ready(self) -> bool:
        return not self.config.enabled or (
            not self._remote_css_parser_error
            and self._remote_css_parser_observed_at is not None
        )

    @property
    def dependency_error(self) -> str:
        return self._remote_css_parser_error

    def _refresh_remote_css_parser_receipt(self, *, force: bool = False) -> bool:
        if not self.config.enabled:
            return True
        now = _utc_now()
        if (
            not force
            and self._remote_css_parser_last_probe_at is not None
            and (now - self._remote_css_parser_last_probe_at).total_seconds()
            < DEPENDENCY_PROBE_REFRESH_SECONDS
        ):
            return self.dependencies_ready
        self._remote_css_parser_last_probe_at = now
        try:
            probe = dict(
                self._remote_css_probe(
                    self.config.ssh_mini_agent,
                    timeout_seconds=min(self.config.artifact_read_timeout_seconds, 15),
                )
            )
            expected = expected_remote_css_runtime_dependency()
            if probe != expected:
                raise ArtifactBundleReadError(
                    "html_css_parser_probe_invalid",
                    "VM CSS parser probe receipt does not match the pinned dependency",
                    permanent=True,
                )
        except (ArtifactBundleReadError, OSError, subprocess.SubprocessError) as exc:
            code = getattr(exc, "code", "html_css_parser_probe_unavailable")
            self._remote_css_parser_error = str(code)[:120]
            if self._remote_css_parser_observed_at is None:
                self._remote_css_parser_receipt = {
                    "status": "unavailable",
                    "observed_at": _utc_iso(now),
                }
            return False
        self._remote_css_parser_observed_at = now
        self._remote_css_parser_error = ""
        self._remote_css_parser_receipt = {
            **expected,
            "observed_at": _utc_iso(now),
        }
        return True

    def write(
        self,
        *,
        state: str,
        stats: CollectorStats,
        last_outcome: CollectOutcome | None = None,
        error: str = "",
        refresh_dependencies: bool = True,
    ) -> None:
        if refresh_dependencies:
            self._refresh_remote_css_parser_receipt()
        store_health = self.store.health(
            activation_required=False,
            quarantine_baseline_path=self.config.quarantine_baseline_path,
            expected_quarantine_baseline_sha256=(
                self.config.quarantine_baseline_sha256
            ),
            quarantine_release_id=self.config.quarantine_release_id,
            quarantine_bootstrap_epoch_id=(self.config.quarantine_bootstrap_epoch_id),
            quarantine_active_release_binding_path=(
                self.config.quarantine_active_release_binding_path
            ),
            quarantine_live_env_path=self.config.quarantine_live_env_path,
        )
        dependency_error = self.dependency_error
        payload = {
            "schema_version": HEALTH_SCHEMA_VERSION,
            "state": state,
            "healthy": (
                state in {"running", "idle", "disabled"}
                and not error
                and not dependency_error
                and self.dependencies_ready
                and (not self.config.enabled or store_health.get("ok") is True)
            ),
            "enabled": self.config.enabled,
            "external_writes": False,
            "started_at": self.started_at,
            "updated_at": _utc_iso(),
            "runtime_identity": self.runtime_identity.to_dict(),
            "config": self.config.public_dict(),
            "dependencies": {
                "remote_css_parser": dict(self._remote_css_parser_receipt),
            },
            "dependency_error": dependency_error,
            "stats": asdict(stats),
            "capacity_samples": {
                "enabled": self.config.capacity_sample_enabled,
                "observation_healthy": (
                    not self.config.capacity_sample_enabled
                    or not stats.capacity_last_error
                ),
                "blocks_delivery_health": False,
                "scanned": stats.capacity_scanned,
                "eligible": stats.capacity_eligible,
                "appended": stats.capacity_appended,
                "rejected": stats.capacity_rejected,
                "frozen": stats.capacity_frozen,
                "last_error": stats.capacity_last_error,
            },
            "store": store_health,
            "last_outcome": asdict(last_outcome) if last_outcome else None,
            "error": str(error or "")[:1000],
        }
        path = self.config.health_path
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
        tmp.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        tmp.replace(path)


def run_collector_loop(
    collector: DeliveryCollector,
    *,
    once: bool = False,
    sleep: Callable[[float], None] = time.sleep,
    remote_css_probe: Callable[..., Mapping[str, Any]] | None = None,
) -> int:
    reporter = HealthReporter(
        collector.config,
        collector.store,
        remote_css_probe=remote_css_probe,
    )
    collector.runtime_identity = reporter.runtime_identity.to_dict()
    if not collector.config.enabled:
        reporter.write(state="disabled", stats=collector.stats)
        return 0
    stop = False

    def request_stop(_signum, _frame):
        nonlocal stop
        stop = True

    previous: dict[int, Any] = {}
    if not once:
        for signum in (signal.SIGINT, signal.SIGTERM):
            previous[signum] = signal.signal(signum, request_stop)
    last: CollectOutcome | None = None
    try:
        while not stop:
            if not reporter._refresh_remote_css_parser_receipt():
                reporter.write(
                    state="error",
                    stats=collector.stats,
                    last_outcome=last,
                    error=reporter.dependency_error,
                    refresh_dependencies=False,
                )
                if once:
                    return 2
                sleep(collector.config.poll_interval_seconds)
                continue
            try:
                reporter.write(
                    state="running",
                    stats=collector.stats,
                    last_outcome=last,
                    refresh_dependencies=False,
                )
                with _PeriodicHeartbeat(
                    lambda: reporter.write(
                        state="running",
                        stats=collector.stats,
                        last_outcome=last,
                        refresh_dependencies=False,
                    ),
                    interval_seconds=_heartbeat_interval_seconds(
                        collector.config.health_max_age_seconds
                    ),
                ):
                    outcomes = collector.collect_batch()
                last = outcomes[-1] if outcomes else None
                reporter.write(
                    state="idle" if last and last.status == "idle" else "running",
                    stats=collector.stats,
                    last_outcome=last,
                )
            except Exception as exc:
                reporter.write(
                    state="error",
                    stats=collector.stats,
                    last_outcome=last,
                    error=f"{type(exc).__name__}: {exc}",
                )
                if once:
                    return 2
            if once:
                return 0
            sleep(collector.config.poll_interval_seconds)
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)
    reporter.write(state="stopped", stats=collector.stats, last_outcome=last)
    return 0


def read_health(
    path: Path,
    *,
    max_age_seconds: int,
    now: datetime | None = None,
) -> tuple[bool, dict[str, Any]]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return False, {"error": f"health_unreadable: {type(exc).__name__}"}
    if (
        not isinstance(payload, dict)
        or payload.get("schema_version") != HEALTH_SCHEMA_VERSION
    ):
        return False, {"error": "health_schema_invalid"}
    config = payload.get("config")
    if not isinstance(config, Mapping) or not runtime_identity_is_valid(
        payload.get("runtime_identity"),
        service_label=SERVICE_LABEL,
        public_config=config,
    ):
        return False, {**payload, "error": "health_runtime_identity_invalid"}
    try:
        updated = datetime.fromisoformat(
            str(payload.get("updated_at") or "").replace("Z", "+00:00")
        )
        if updated.tzinfo is None or updated.utcoffset() is None:
            raise ValueError("health timestamp must be timezone-aware")
        observed_at = now or _utc_now()
        if observed_at.tzinfo is None or observed_at.utcoffset() is None:
            raise ValueError("health observation timestamp must be timezone-aware")
        age = (
            observed_at.astimezone(timezone.utc) - updated.astimezone(timezone.utc)
        ).total_seconds()
    except (TypeError, ValueError):
        return False, {**payload, "error": "health_timestamp_invalid"}
    fresh = -MAX_HEALTH_FUTURE_SKEW_SECONDS <= age <= max_age_seconds
    result = {**payload, "age_seconds": age}
    if age < -MAX_HEALTH_FUTURE_SKEW_SECONDS:
        result["error"] = "heartbeat_from_future"
    elif age > max_age_seconds:
        result["error"] = "heartbeat_stale"
    return payload.get("healthy") is True and fresh, result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Collect RCA VM terminal truth into a durable delivery outbox"
    )
    parser.add_argument("--check-config", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--health", action="store_true")
    parser.add_argument("--health-max-age-seconds", type=int)
    parser.add_argument("--check-config-worker-root")
    return parser


def load_collector_environment(env_file: str | Path | None = None) -> Path:
    path = Path(
        env_file
        or os.environ.get(f"{ENV_PREFIX}ENV_FILE")
        or Path(get_hermes_home()) / ".env"
    ).expanduser()
    load_dotenv(path, override=False, interpolate=False)
    return path


def main(argv: list[str] | None = None) -> int:
    load_collector_environment()
    args = _parser().parse_args(argv)
    try:
        config = CollectorConfig.from_env()
    except ValueError as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False))
        return 2
    if args.check_config:
        quarantine_baseline = (
            read_quarantine_baseline_status(
                config.control_db_path,
                baseline_path=config.quarantine_baseline_path,
                expected_sha256=config.quarantine_baseline_sha256,
                expected_release_id=config.quarantine_release_id,
                bootstrap_epoch_id=config.quarantine_bootstrap_epoch_id,
                active_release_binding_path=(
                    config.quarantine_active_release_binding_path
                ),
                live_env_path=config.quarantine_live_env_path,
            )
            if config.enabled
            else disabled_quarantine_baseline_status(
                baseline_path=config.quarantine_baseline_path,
                expected_sha256=config.quarantine_baseline_sha256,
            )
        )
        try:
            remote_css_parser = probe_remote_css_parser(
                config.ssh_mini_agent,
                timeout_seconds=min(config.artifact_read_timeout_seconds, 15),
                worker_root=args.check_config_worker_root,
            )
        except ArtifactBundleReadError as exc:
            print(
                json.dumps(
                    {"ok": False, "error": exc.code, "detail": exc.detail},
                    ensure_ascii=False,
                )
            )
            return 2
        print(
            json.dumps(
                {
                    "ok": quarantine_baseline["ready"],
                    "config": config.public_dict(),
                    "dependencies": {"remote_css_parser": remote_css_parser},
                    "quarantine_baseline": quarantine_baseline,
                },
                ensure_ascii=False,
            )
        )
        return 0 if quarantine_baseline["ready"] else 2
    if args.health:
        healthy, payload = read_health(
            config.health_path,
            max_age_seconds=(
                args.health_max_age_seconds or config.health_max_age_seconds
            ),
        )
        print(json.dumps(payload, ensure_ascii=False))
        return 0 if healthy else 2
    try:
        store = RcaDeliveryStore(config.control_db_path, require_current=True)
    except (OSError, RuntimeError, sqlite3.Error) as exc:
        print(
            json.dumps(
                {"ok": False, "error": f"delivery_store_unavailable: {exc}"},
                ensure_ascii=False,
            )
        )
        return 2
    collector = DeliveryCollector(store=store, config=config)
    if args.dry_run:
        print(json.dumps(collector.dry_run_once(), ensure_ascii=False))
        return 0
    return run_collector_loop(collector, once=args.once)


if __name__ == "__main__":
    raise SystemExit(main())
