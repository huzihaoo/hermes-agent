#!/usr/bin/env python3
"""Dispatch durable Kafka RCA outbox rows outside the Kafka poll loop."""

from __future__ import annotations

import argparse
from contextlib import nullcontext
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
import hmac
import json
import logging
import os
from pathlib import Path
import re
import signal
import socket
import subprocess
import sys
import threading
import time
from typing import Any, Callable, Mapping

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from dotenv import load_dotenv

from gateway.pnc_rca_admission import (
    RcaAdmission,
    validate_rca_admission,
    validate_rca_trigger_context,
)
from gateway.pnc_rca_control_store import (
    OutboxClaim,
    RcaControlStore,
    StaleOutboxLeaseError,
)
from gateway.pnc_rca_capacity_runtime import CapacityRuntimeResolver
from gateway.pnc_rca_delivery_store import (
    DeliveryBackpressureSnapshot,
    RcaDeliveryStore,
    validate_delivery_outcome_slo,
)
from gateway.pnc_rca_data_access import (
    RemoteDataAccessError,
    validate_remote_data_access,
)
from gateway.pnc_rca_derived_capacity_reservation import (
    DEFAULT_BOUNDARY_TIMEOUT_SECONDS as DERIVED_RESERVATION_TIMEOUT_SECONDS,
    DerivedCapacityReservationDecision,
    DerivedCapacityReservationError,
    DerivedCapacityReservationRequest,
    abort_precreate_derived_capacity,
    canonical_data_access_sha256,
    reserve_derived_capacity,
    validate_derived_capacity_precreate_abort_receipt,
    validate_derived_capacity_reservation_receipt,
)
from gateway.pnc_rca_kafka_contract import NORMALIZED_EVENT_SCHEMA_VERSION
from gateway.pnc_rca_prod_bootstrap import (
    ACTIVE_RELEASE_BINDING_NAME,
    EPOCH_ID_RE as BOOTSTRAP_EPOCH_ID_RE,
    RcaBootstrapAuthorizationError,
    load_active_release_binding,
    load_bootstrap_authorization,
)
from gateway.pnc_rca_runtime_identity import (
    MAX_HEALTH_FUTURE_SKEW_SECONDS,
    RCA_OUTBOX_DISPATCHER_LOADED_DEPENDENCIES,
    build_runtime_identity,
    canonical_json_sha256,
    runtime_identity_is_valid,
)
from gateway.pnc_rca_schema import (
    RcaExecutionRequest,
    RcaIssueContext,
    SourceQuality,
    build_execution_request,
    issue_context_from_compact_text,
    validate_issue_context_fields,
)
from gateway.pnc_rca_workspace_runtime import (
    WORKSPACE_RUNTIME_FILES,
    WORKSPACE_RUNTIME_IDENTITY_SCHEMA_VERSION,
    WorkspaceRuntimeError,
    WorkspaceRuntimeIdentity,
    validate_workspace_runtime,
)
from hermes_constants import get_hermes_home


ENV_PREFIX = "HERMES_RCA_OUTBOX_"
PROD_CAPACITY_MODE_ENV = "HERMES_RCA_PROD_CAPACITY_MODE"
PROD_RELEASE_ID_ENV = "HERMES_RCA_PROD_RELEASE_ID"
PROD_BOOTSTRAP_EPOCH_ID_ENV = "HERMES_RCA_PROD_BOOTSTRAP_EPOCH_ID"
DISPATCHER_HEALTH_SCHEMA_VERSION = "pnc_rca_outbox_dispatcher_health_v2"
SERVICE_LABEL = "local.pnc.rca-outbox-dispatcher"
OUTBOX_PAYLOAD_SCHEMA_VERSION = "pnc_rca_submission_outbox_v2"
SUPPORTED_OUTBOX_PAYLOAD_SCHEMA_VERSIONS = frozenset(
    {"pnc_rca_submission_outbox_v1", OUTBOX_PAYLOAD_SCHEMA_VERSION}
)
SERVICE_CAPABILITY = "submit_g1q3_rca_issue_intake"
SERVICE_OPERATION = "g1q3_rca_issue_intake"
DEFAULT_SERVICE_ID = "root_cause_analysis_agent"
STORAGE_ADMISSION_SCHEMA_VERSION = "g1q3_rca_storage_admission_v2"
DERIVED_CAPACITY_SUMMARY_SCHEMA_VERSION = "pnc_rca_derived_capacity_admission_v2"
DERIVED_CAPACITY_SCOPE = "derived_artifact_and_cache"
DERIVED_CAPACITY_REQUEST_SCOPE = "this_capacity_admission_only"
REMOTE_DATA_ACCESS_MODE = "remote_read"
DEFAULT_SSH_MINI_AGENT = str(Path.home() / ".local" / "bin" / "ssh-mini-agent")
REMOTE_STORAGE_ADMISSION_MODULE = (
    "/home/mini/data3/yj-evaluation-server/api/g1q3_rca/storage_admission.py"
)
VM_ARTIFACT_PREFIX = "/mnt/tmp/"
CIFS_ARTIFACT_PREFIX = (
    "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/"
)
RETRY_DELAYS_SECONDS = (0, 2, 5, 10, 20, 40, 120, 300, 900, 3600)
MIN_LEASE_SECONDS = 180
MAX_SUBMIT_BOUNDARY_SECONDS = 120
LEASE_BOUNDARY_MARGIN_SECONDS = 30
DEFAULT_STORAGE_EXPECTED_ARTIFACT_CACHE_BYTES = 1_000_000_000
DEFAULT_INPUT_WAIT_MAX_AGE_SECONDS = 900
MIN_INPUT_WAIT_MAX_AGE_SECONDS = 60
MAX_INPUT_WAIT_MAX_AGE_SECONDS = 3_600
HEALTH_HEARTBEAT_INTERVAL_SECONDS = 10.0
PROD_RELEASE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{7,127}$")
_FORBIDDEN_MDI_COMMAND_PATTERN = re.compile(
    r"\bmdi\s+(?:download|refresh2?|clip|event)\b", re.IGNORECASE
)

logger = logging.getLogger(__name__)

_SERVICE_ERROR_CODES = frozenset({
    "vm_task_service_capability_denied",
    "vm_task_service_operation_denied",
    "vm_task_service_permission_denied",
    "vm_task_permission_policy_unavailable",
    "vm_task_service_admission_invalid",
    "vm_task_service_request_invalid",
    "vm_task_service_request_identity_mismatch",
    "vm_task_service_existing_identity_conflict",
    "vm_task_service_reconcile_missing",
    "vm_task_service_reservation_invalid",
    "vm_task_service_reservation_reconcile_mismatch",
    "vm_task_service_reservation_not_admitted",
    "vm_task_service_workspace_runtime_drift",
    "vm_task_service_workspace_runtime_invalid",
})

_GLOBAL_CIRCUIT_ERROR_CODES = frozenset({
    "dispatcher_dependency_unavailable",
    "dispatcher_bootstrap_authorization_invalid",
    "dispatcher_submit_contract_invalid",
    "derived_capacity_reservation_abort_precreate_response_invalid",
    "derived_capacity_reservation_abort_precreate_unavailable",
    "derived_capacity_reservation_abort_precreate_wrapper_invalid",
    "derived_capacity_reservation_response_invalid",
    "derived_capacity_reservation_unavailable",
    "derived_capacity_reservation_wrapper_invalid",
    "storage_admission_schema_invalid",
    "storage_admission_response_invalid",
    "storage_admission_wrapper_invalid",
    "vm_task_service_admission_invalid",
    "vm_task_service_capability_denied",
    "vm_task_service_operation_denied",
    "vm_task_service_permission_denied",
    "vm_task_permission_policy_unavailable",
    "vm_task_service_workspace_runtime_drift",
    "vm_task_service_workspace_runtime_invalid",
})

_PER_CASE_QUARANTINE_ERROR_CODES = frozenset({
    "dispatcher_enrichment_contract_invalid",
    "dispatcher_enrichment_identity_mismatch",
    "dispatcher_execution_request_build_failed",
    "dispatcher_derived_capacity_reservation_invalid",
    "dispatcher_outbox_contract_invalid",
    "dispatcher_outbox_identity_mismatch",
    "dispatcher_remote_data_access_invalid",
    "dispatcher_submit_identity_mismatch",
    "derived_capacity_reservation_contract_invalid",
    "derived_capacity_reservation_identity_mismatch",
    "derived_capacity_reservation_request_invalid",
    "derived_capacity_reservation_abort_precreate_identity_mismatch",
    "derived_capacity_reservation_abort_precreate_invalid",
    "derived_capacity_reservation_schema_invalid",
    "derived_capacity_reservation_status_invalid",
    "vm_task_service_existing_identity_conflict",
    "vm_task_service_reconcile_missing",
    "vm_task_service_request_identity_mismatch",
    "vm_task_service_request_invalid",
    "vm_task_service_reservation_invalid",
    "vm_task_service_reservation_not_admitted",
    "vm_task_service_reservation_reconcile_mismatch",
})

_RETRYABLE_BOUNDARY_ERROR_CODES = frozenset({
    "derived_capacity_reservation_call_failed",
    "derived_capacity_reservation_abort_precreate_call_failed",
    "derived_capacity_reservation_abort_precreate_timeout",
    "derived_capacity_reservation_timeout",
    "storage_admission_call_failed",
    "storage_admission_timeout",
})

_DEFINITIVE_PRECREATE_ERROR_CODES = frozenset({
    "vm_task_service_admission_invalid",
    "vm_task_service_capability_denied",
    "vm_task_service_operation_denied",
    "vm_task_service_permission_denied",
    "vm_task_service_request_identity_mismatch",
    "vm_task_service_request_invalid",
    "vm_task_service_reservation_invalid",
    "vm_task_service_reservation_not_admitted",
    "vm_task_service_reservation_reconcile_mismatch",
    "vm_task_permission_policy_unavailable",
})

EnrichFunc = Callable[[Mapping[str, Any]], RcaIssueContext]
SubmitFunc = Callable[[RcaAdmission, RcaExecutionRequest], Mapping[str, Any]]


@dataclass(frozen=True)
class StorageAdmissionRequest:
    requested_cases: int
    assumed_cases_per_day: int
    expected_artifact_cache_bytes: int
    reserve_percent: int
    timeout_seconds: int


StorageAdmissionFunc = Callable[[StorageAdmissionRequest], Mapping[str, Any]]
DerivedCapacityReservationFunc = Callable[
    [DerivedCapacityReservationRequest], DerivedCapacityReservationDecision
]
DerivedCapacityPrecreateAbortFunc = Callable[
    [DerivedCapacityReservationRequest, Mapping[str, Any]],
    Mapping[str, Any],
]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_iso(value: datetime | None = None) -> str:
    current = value or _utc_now()
    if current.tzinfo is None:
        raise ValueError("datetime values must be timezone-aware")
    return current.astimezone(timezone.utc).isoformat()


def _boolean(env: Mapping[str, str], name: str, default: bool = False) -> bool:
    value = str(env.get(name, "true" if default else "false")).strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be true or false")


def _strict_boolean(
    env: Mapping[str, str], name: str, default: bool = False
) -> bool:
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


def canonical_artifact_paths(submission_key: str) -> tuple[str, str]:
    key = str(submission_key or "").strip()
    if not key or "/" in key or "\\" in key or key in {".", ".."}:
        raise ValueError("submission_key is not safe for an artifact namespace")
    return f"{VM_ARTIFACT_PREFIX}{key}/", f"{CIFS_ARTIFACT_PREFIX}{key}/"


def retry_delay_seconds(attempt: int) -> int:
    """Return the delay after a failed claim attempt.

    Attempt one was available at t+0. Its first retry is t+2, followed by
    5/10/20/40 seconds and 2/5/15/60 minutes (60 minutes thereafter).
    """
    index = max(1, int(attempt))
    return RETRY_DELAYS_SECONDS[min(index, len(RETRY_DELAYS_SECONDS) - 1)]


class EnrichmentNotReady(RuntimeError):
    def __init__(self, code: str, detail: str = ""):
        super().__init__(detail or code)
        self.code = str(code or "enrichment_not_ready")
        self.detail = str(detail or code)


class DispatchCircuitError(RuntimeError):
    def __init__(self, code: str, detail: str = ""):
        super().__init__(detail or code)
        self.code = str(code or "dispatcher_system_error")
        self.detail = str(detail or code)


class PermanentDispatchError(RuntimeError):
    def __init__(self, code: str, detail: str = ""):
        super().__init__(detail or code)
        self.code = str(code or "dispatch_permanent_error")
        self.detail = str(detail or code)


@dataclass(frozen=True)
class DispatcherConfig:
    dispatch_enabled: bool
    activation_required: bool
    control_db_path: Path
    delivery_db_path: Path
    health_path: Path
    service_id: str
    lease_seconds: int
    max_age_seconds: int
    input_wait_max_age_seconds: int
    poll_interval_seconds: int
    circuit_poll_interval_seconds: int
    batch_size: int
    data_access_mode: str
    allow_feishu_writeback: bool
    group_response_cap: str
    translate_baseline: str
    translate_contract_path: str
    storage_admission_enabled: bool
    storage_reservation_enabled: bool
    derived_capacity_reservation_enabled: bool
    delivery_backpressure_enabled: bool
    delivery_high_watermark: int
    delivery_resume_watermark: int
    storage_concurrency_reserve_cases: int
    storage_cases_per_day: int
    storage_expected_artifact_cache_bytes: int
    storage_reserve_percent: int
    storage_timeout_seconds: int
    derived_capacity_reservation_timeout_seconds: int
    capacity_mode: str
    release_id: str
    bootstrap_epoch_id: str
    active_release_binding_path: Path
    live_env_path: Path

    @classmethod
    def from_env(
        cls,
        env: Mapping[str, str] | None = None,
        *,
        hermes_home: str | Path | None = None,
    ) -> "DispatcherConfig":
        source = os.environ if env is None else env
        home = Path(hermes_home or get_hermes_home()).expanduser()
        service_id = str(
            source.get(f"{ENV_PREFIX}SERVICE_ID", DEFAULT_SERVICE_ID)
        ).strip()
        if service_id != DEFAULT_SERVICE_ID:
            raise ValueError(
                f"{ENV_PREFIX}SERVICE_ID must be exactly {DEFAULT_SERVICE_ID}"
            )
        group_response_cap = str(
            source.get(f"{ENV_PREFIX}GROUP_RESPONSE_CAP", "L1")
        ).strip()
        if group_response_cap not in {"L0", "L1"}:
            raise ValueError(f"{ENV_PREFIX}GROUP_RESPONSE_CAP must be L0 or L1")
        dispatch_enabled = _boolean(source, f"{ENV_PREFIX}DISPATCH_ENABLED", False)
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
        storage_admission_enabled = _boolean(
            source, f"{ENV_PREFIX}STORAGE_ADMISSION_ENABLED", False
        )
        storage_reservation_enabled = _boolean(
            source, f"{ENV_PREFIX}STORAGE_RESERVATION_ENABLED", False
        )
        if storage_reservation_enabled:
            raise ValueError(
                f"{ENV_PREFIX}STORAGE_RESERVATION_ENABLED must be false; the "
                "legacy atomic reservation is bound to an MDI download command"
            )
        derived_capacity_reservation_enabled = _boolean(
            source,
            f"{ENV_PREFIX}DERIVED_CAPACITY_RESERVATION_ENABLED",
            False,
        )
        delivery_backpressure_enabled = _boolean(
            source, f"{ENV_PREFIX}DELIVERY_BACKPRESSURE_ENABLED", False
        )
        if dispatch_enabled and (
            not storage_admission_enabled
            or not derived_capacity_reservation_enabled
            or not delivery_backpressure_enabled
        ):
            raise ValueError(
                f"{ENV_PREFIX}STORAGE_ADMISSION_ENABLED and "
                f"{ENV_PREFIX}DERIVED_CAPACITY_RESERVATION_ENABLED and "
                f"{ENV_PREFIX}DELIVERY_BACKPRESSURE_ENABLED must all be true "
                f"when {ENV_PREFIX}DISPATCH_ENABLED is true"
            )
        data_access_mode = str(
            source.get(f"{ENV_PREFIX}DATA_ACCESS_MODE", REMOTE_DATA_ACCESS_MODE)
        ).strip()
        if data_access_mode != REMOTE_DATA_ACCESS_MODE:
            raise ValueError(
                f"{ENV_PREFIX}DATA_ACCESS_MODE must be exactly "
                f"{REMOTE_DATA_ACCESS_MODE}"
            )
        allow_download_name = f"{ENV_PREFIX}ALLOW_DOWNLOAD"
        if allow_download_name in source and (
            str(source[allow_download_name]).strip().lower() != "false"
        ):
            raise ValueError(
                f"{allow_download_name} must be absent or exactly false in "
                f"{REMOTE_DATA_ACCESS_MODE} mode"
            )
        legacy_input_budget = f"{ENV_PREFIX}STORAGE_EXPECTED_INPUT_BYTES"
        if legacy_input_budget in source:
            raise ValueError(
                f"{legacy_input_budget} is unsupported for remote-read RCA; use "
                f"{ENV_PREFIX}STORAGE_EXPECTED_ARTIFACT_CACHE_BYTES"
            )
        delivery_db_path = Path(
            source.get(f"{ENV_PREFIX}DELIVERY_DB_PATH", control_db_path)
        ).expanduser()
        if dispatch_enabled and delivery_db_path != control_db_path:
            raise ValueError(
                f"{ENV_PREFIX}DELIVERY_DB_PATH must equal "
                f"{ENV_PREFIX}CONTROL_DB_PATH while dispatch is enabled"
            )
        delivery_high_watermark = _integer(
            source, f"{ENV_PREFIX}DELIVERY_HIGH_WATERMARK", 100
        )
        delivery_resume_watermark = _integer(
            source,
            f"{ENV_PREFIX}DELIVERY_RESUME_WATERMARK",
            50,
            minimum=0,
        )
        if delivery_resume_watermark >= delivery_high_watermark:
            raise ValueError(
                f"{ENV_PREFIX}DELIVERY_HIGH_WATERMARK must be greater than "
                f"{ENV_PREFIX}DELIVERY_RESUME_WATERMARK"
            )
        storage_reserve_percent = _integer(
            source, f"{ENV_PREFIX}STORAGE_RESERVE_PERCENT", 30
        )
        if storage_reserve_percent >= 100:
            raise ValueError(f"{ENV_PREFIX}STORAGE_RESERVE_PERCENT must be below 100")
        derived_reservation_timeout = _integer(
            source,
            f"{ENV_PREFIX}DERIVED_CAPACITY_RESERVATION_TIMEOUT_SECONDS",
            DERIVED_RESERVATION_TIMEOUT_SECONDS,
        )
        if derived_reservation_timeout > DERIVED_RESERVATION_TIMEOUT_SECONDS:
            raise ValueError(
                f"{ENV_PREFIX}DERIVED_CAPACITY_RESERVATION_TIMEOUT_SECONDS must "
                f"be at most {DERIVED_RESERVATION_TIMEOUT_SECONDS}"
            )
        input_wait_max_age_seconds = _integer(
            source,
            f"{ENV_PREFIX}INPUT_WAIT_MAX_AGE_SECONDS",
            DEFAULT_INPUT_WAIT_MAX_AGE_SECONDS,
            minimum=MIN_INPUT_WAIT_MAX_AGE_SECONDS,
        )
        if input_wait_max_age_seconds > MAX_INPUT_WAIT_MAX_AGE_SECONDS:
            raise ValueError(
                f"{ENV_PREFIX}INPUT_WAIT_MAX_AGE_SECONDS must be at most "
                f"{MAX_INPUT_WAIT_MAX_AGE_SECONDS}"
            )
        capacity_mode = str(source.get(PROD_CAPACITY_MODE_ENV, "steady")).strip()
        if capacity_mode not in {"steady", "bootstrap"}:
            raise ValueError(
                f"{PROD_CAPACITY_MODE_ENV} must be exactly steady or bootstrap"
            )
        release_id = str(source.get(PROD_RELEASE_ID_ENV, "")).strip()
        bootstrap_epoch_id = str(
            source.get(PROD_BOOTSTRAP_EPOCH_ID_ENV, "")
        ).strip()
        if bool(release_id) != bool(bootstrap_epoch_id):
            raise ValueError(
                f"{PROD_RELEASE_ID_ENV} and {PROD_BOOTSTRAP_EPOCH_ID_ENV} "
                "must be configured together"
            )
        if release_id and (
            PROD_RELEASE_ID_RE.fullmatch(release_id) is None
            or BOOTSTRAP_EPOCH_ID_RE.fullmatch(bootstrap_epoch_id) is None
        ):
            raise ValueError("RCA production release or bootstrap epoch id is invalid")
        if capacity_mode == "bootstrap" and not release_id:
            raise ValueError(
                "bootstrap RCA production capacity mode requires release and epoch ids"
            )
        config = cls(
            dispatch_enabled=dispatch_enabled,
            activation_required=_strict_boolean(
                source,
                f"{ENV_PREFIX}ACTIVATION_REQUIRED",
                False,
            ),
            control_db_path=control_db_path,
            delivery_db_path=delivery_db_path,
            health_path=Path(
                source.get(
                    f"{ENV_PREFIX}HEALTH_PATH",
                    home
                    / "runtime"
                    / "pnc_agent"
                    / "feishu_issue_kafka_rca"
                    / "outbox_dispatcher_health.json",
                )
            ).expanduser(),
            service_id=service_id,
            lease_seconds=_integer(
                source,
                f"{ENV_PREFIX}LEASE_SECONDS",
                MIN_LEASE_SECONDS,
                minimum=MIN_LEASE_SECONDS,
            ),
            max_age_seconds=_integer(
                source, f"{ENV_PREFIX}MAX_AGE_SECONDS", 86_400, minimum=60
            ),
            input_wait_max_age_seconds=input_wait_max_age_seconds,
            poll_interval_seconds=_integer(
                source, f"{ENV_PREFIX}POLL_INTERVAL_SECONDS", 2
            ),
            circuit_poll_interval_seconds=_integer(
                source, f"{ENV_PREFIX}CIRCUIT_POLL_INTERVAL_SECONDS", 30
            ),
            batch_size=_integer(source, f"{ENV_PREFIX}BATCH_SIZE", 10),
            data_access_mode=data_access_mode,
            allow_feishu_writeback=_boolean(
                source, f"{ENV_PREFIX}ALLOW_FEISHU_WRITEBACK", False
            ),
            group_response_cap=group_response_cap,
            translate_baseline=str(
                source.get(f"{ENV_PREFIX}TRANSLATE_BASELINE", "production")
            ).strip()
            or "production",
            translate_contract_path=str(
                source.get(f"{ENV_PREFIX}TRANSLATE_CONTRACT_PATH", "")
            ).strip(),
            storage_admission_enabled=storage_admission_enabled,
            storage_reservation_enabled=storage_reservation_enabled,
            derived_capacity_reservation_enabled=(derived_capacity_reservation_enabled),
            delivery_backpressure_enabled=delivery_backpressure_enabled,
            delivery_high_watermark=delivery_high_watermark,
            delivery_resume_watermark=delivery_resume_watermark,
            storage_concurrency_reserve_cases=_integer(
                source,
                f"{ENV_PREFIX}STORAGE_CONCURRENCY_RESERVE_CASES",
                4,
            ),
            storage_cases_per_day=_integer(
                source, f"{ENV_PREFIX}STORAGE_CASES_PER_DAY", 200
            ),
            storage_expected_artifact_cache_bytes=_integer(
                source,
                f"{ENV_PREFIX}STORAGE_EXPECTED_ARTIFACT_CACHE_BYTES",
                DEFAULT_STORAGE_EXPECTED_ARTIFACT_CACHE_BYTES,
            ),
            storage_reserve_percent=storage_reserve_percent,
            storage_timeout_seconds=_integer(
                source, f"{ENV_PREFIX}STORAGE_TIMEOUT_SECONDS", 45
            ),
            derived_capacity_reservation_timeout_seconds=(derived_reservation_timeout),
            capacity_mode=capacity_mode,
            release_id=release_id,
            bootstrap_epoch_id=bootstrap_epoch_id,
            active_release_binding_path=(
                control_db_path.parent / ACTIVE_RELEASE_BINDING_NAME
            ),
            live_env_path=home / ".env",
        )
        longest_boundary = max(
            config.storage_timeout_seconds,
            config.derived_capacity_reservation_timeout_seconds,
            MAX_SUBMIT_BOUNDARY_SECONDS,
        )
        if config.lease_seconds <= longest_boundary + LEASE_BOUNDARY_MARGIN_SECONDS:
            raise ValueError(
                f"{ENV_PREFIX}LEASE_SECONDS must exceed every external boundary "
                f"by more than {LEASE_BOUNDARY_MARGIN_SECONDS} seconds"
            )
        if config.input_wait_max_age_seconds > config.max_age_seconds:
            raise ValueError(
                f"{ENV_PREFIX}INPUT_WAIT_MAX_AGE_SECONDS must not exceed "
                f"{ENV_PREFIX}MAX_AGE_SECONDS"
            )
        return config

    def public_dict(self) -> dict[str, Any]:
        return {
            "dispatch_enabled": self.dispatch_enabled,
            "activation_required": self.activation_required,
            "control_db_path": str(self.control_db_path),
            "delivery_db_path": str(self.delivery_db_path),
            "health_path": str(self.health_path),
            "service_id": self.service_id,
            "service_capability": SERVICE_CAPABILITY,
            "service_operation": SERVICE_OPERATION,
            "lease_seconds": self.lease_seconds,
            "max_age_seconds": self.max_age_seconds,
            "input_wait_max_age_seconds": self.input_wait_max_age_seconds,
            "poll_interval_seconds": self.poll_interval_seconds,
            "circuit_poll_interval_seconds": self.circuit_poll_interval_seconds,
            "batch_size": self.batch_size,
            "data_access_mode": self.data_access_mode,
            "allow_download": False,
            "allow_feishu_writeback": self.allow_feishu_writeback,
            "group_response_cap": self.group_response_cap,
            "translate_baseline": self.translate_baseline,
            "translate_contract_path": self.translate_contract_path,
            "storage_admission_enabled": self.storage_admission_enabled,
            "storage_reservation_enabled": self.storage_reservation_enabled,
            "derived_capacity_reservation_enabled": (
                self.derived_capacity_reservation_enabled
            ),
            "delivery_backpressure_enabled": self.delivery_backpressure_enabled,
            "delivery_high_watermark": self.delivery_high_watermark,
            "delivery_resume_watermark": self.delivery_resume_watermark,
            "storage_concurrency_reserve_cases": (
                self.storage_concurrency_reserve_cases
            ),
            "storage_cases_per_day": self.storage_cases_per_day,
            "storage_capacity_scope": DERIVED_CAPACITY_SCOPE,
            "derived_capacity_atomic_reservation": (
                self.derived_capacity_reservation_enabled
            ),
            "storage_expected_artifact_cache_bytes": (
                self.storage_expected_artifact_cache_bytes
            ),
            "storage_reserve_percent": self.storage_reserve_percent,
            "storage_timeout_seconds": self.storage_timeout_seconds,
            "derived_capacity_reservation_timeout_seconds": (
                self.derived_capacity_reservation_timeout_seconds
            ),
            "capacity_mode": self.capacity_mode,
            "release_id": self.release_id,
            "bootstrap_epoch_id": self.bootstrap_epoch_id,
            "active_release_binding_path": str(self.active_release_binding_path),
            "live_env_path": str(self.live_env_path),
        }

    def runtime_public_dict(self) -> dict[str, Any]:
        return self.public_dict()

    @property
    def derived_capacity_atomic_reservation(self) -> bool:
        return self.derived_capacity_reservation_enabled


@dataclass
class DispatchStats:
    loops: int = 0
    claimed: int = 0
    completed: int = 0
    retried: int = 0
    quarantined: int = 0
    circuit_opened: int = 0
    idle: int = 0
    storage_admission_passed: int = 0
    storage_admission_blocked: int = 0
    storage_admission_errors: int = 0
    derived_capacity_reservation_admitted: int = 0
    derived_capacity_reservation_blocked: int = 0
    derived_capacity_reservation_errors: int = 0
    derived_capacity_precreate_aborted: int = 0
    derived_capacity_precreate_abort_errors: int = 0
    delivery_backpressure_checks: int = 0
    delivery_backpressure_blocked: int = 0
    delivery_backpressure_resumed: int = 0
    delivery_circuit_blocked: int = 0
    delivery_outcome_slo_blocked: int = 0
    delivery_backpressure_errors: int = 0
    lease_lost: int = 0


@dataclass(frozen=True)
class DispatchOutcome:
    status: str
    outbox_id: int | None = None
    submission_key: str = ""
    attempt: int = 0
    error_code: str = ""
    next_attempt_at: str | None = None
    deduped: bool = False
    downstream_unresolved_effects: int | None = None
    downstream_unresolved_work: int | None = None
    downstream_circuit_state: str = ""


def default_enrich_event(event: Mapping[str, Any]) -> RcaIssueContext:
    """Read one issue through the existing bounded host-side preread facade."""
    project_key = str(event.get("project_key") or "").strip()
    project_simple_name = str(event.get("project_simple_name") or "").strip()
    work_item_id = str(event.get("work_item_id") or "").strip()
    work_item_type = str(event.get("work_item_type_key") or "").strip()
    if not project_key or not work_item_id or not work_item_type:
        raise DispatchCircuitError(
            "dispatcher_event_identity_invalid",
            "normalized event is missing project/work-item identity",
        )

    from gateway.pnc_issue_context import fetch_g1q3_issue_context_result

    result = fetch_g1q3_issue_context_result(
        project_key=project_key,
        work_item_id=work_item_id,
    )
    if not result.context_text:
        blocker = result.blocker or {}
        code = str(blocker.get("kind") or "issue_enrichment_not_ready")
        detail = str(blocker.get("message") or code)
        if "unauthenticated" in code:
            raise DispatchCircuitError(code, detail)
        raise EnrichmentNotReady(code, detail)

    issue_url = str(event.get("issue_url") or "").strip()
    if not issue_url:
        if not project_simple_name:
            raise DispatchCircuitError(
                "dispatcher_event_identity_invalid",
                "normalized event is missing the Feishu project URL slug",
            )
        issue_url = (
            "https://project.feishu.cn/"
            f"{project_simple_name}/issue/detail/{work_item_id}"
        )
    source_quality: SourceQuality = result.source_quality
    context = issue_context_from_compact_text(
        project_key=project_key,
        work_item_id=work_item_id,
        url=issue_url,
        compact_text=result.context_text,
        source_quality=source_quality,
    )
    error_classes = sorted({
        str(item.get("error_class") or "").strip()
        for item in (result.errors or [])
        if isinstance(item, Mapping) and str(item.get("error_class") or "").strip()
    })
    read_provenance: dict[str, Any] = {
        "type": "host_issue_read_status",
        "status": result.status,
        "source": str(result.source or "unknown").strip(),
        "degraded": result.source == "mcp_auto_degraded",
    }
    if error_classes:
        read_provenance["error_classes"] = error_classes
    context = replace(
        context,
        work_item_type=work_item_type,
        media_refs=[*context.media_refs, read_provenance],
    )
    context, blocker = validate_issue_context_fields(context)
    if blocker:
        raise EnrichmentNotReady(
            str(blocker.get("kind") or "issue_fields_not_ready"),
            str(blocker.get("message") or "issue fields are not ready"),
        )
    return context


def default_submit(
    admission: RcaAdmission,
    execution_request: RcaExecutionRequest,
    *,
    config: DispatcherConfig | None = None,
    capacity_runtime: CapacityRuntimeResolver | None = None,
) -> Mapping[str, Any]:
    """Invoke only the fixed, capability-scoped VM submission wrapper."""
    from tools.vm_task_tool import vm_task_submit_service

    reservation = execution_request.toolchain.get("derived_capacity_reservation", {})
    reconcile_only = (
        isinstance(reservation, Mapping) and reservation.get("status") == "released"
    )
    decision_context = (
        capacity_runtime.shared_decision()
        if capacity_runtime is not None
        else nullcontext(None)
    )
    with decision_context as capacity_decision:
        if (
            capacity_decision is not None
            and not (config is not None and not config.activation_required)
        ):
            if capacity_decision.get("ready") is not True:
                raise DispatchCircuitError(
                    "dispatcher_capacity_runtime_blocked",
                    str(
                        capacity_decision.get("reason_code")
                        or "rca_capacity_runtime_blocked"
                    ),
                )
            capacity_mode = str(capacity_decision.get("effective_mode") or "")
        else:
            capacity_mode = config.capacity_mode if config is not None else "steady"
        if capacity_mode not in {"bootstrap", "steady"}:
            raise DispatchCircuitError(
                "dispatcher_capacity_runtime_blocked",
                "rca_capacity_runtime_mode_invalid",
            )
        capacity_bindings: dict[str, str] = {}
        if capacity_mode == "bootstrap":
            if config is None:
                raise DispatchCircuitError(
                    "dispatcher_bootstrap_authorization_invalid",
                    "dispatcher_bootstrap_config_missing",
                )
            try:
                authorization = _load_bound_bootstrap_authorization(config)
            except RcaBootstrapAuthorizationError as exc:
                raise DispatchCircuitError(
                    "dispatcher_bootstrap_authorization_invalid", exc.code
                ) from exc
            active_release_binding = authorization[
                "active_release_binding_sha256"
            ]
            if capacity_decision is not None:
                runtime_active_release_binding = str(
                    capacity_decision.get("active_release_binding_sha256") or ""
                )
                if not hmac.compare_digest(
                    runtime_active_release_binding,
                    active_release_binding,
                ):
                    raise DispatchCircuitError(
                        "dispatcher_bootstrap_authorization_invalid",
                        "rca_bootstrap_active_release_binding_mismatch",
                    )
                active_release_binding = runtime_active_release_binding
            capacity_bindings = {
                "bootstrap_epoch_id": authorization["bootstrap_epoch_id"],
                "release_bom_sha256": authorization["release_bom_sha256"],
                "bootstrap_started_at": authorization["started_at"],
                "bootstrap_deadline": authorization["deadline"],
                "bootstrap_authorization_fingerprint": authorization[
                    "receipt_fingerprint"
                ],
                "active_release_binding_sha256": active_release_binding,
            }
        return vm_task_submit_service(
            service_id=DEFAULT_SERVICE_ID,
            capability=SERVICE_CAPABILITY,
            operation=SERVICE_OPERATION,
            admission=admission,
            execution_request=execution_request,
            reconcile_only=reconcile_only,
            capacity_mode=capacity_mode,
            **capacity_bindings,
        )


def _load_bound_bootstrap_authorization(
    config: DispatcherConfig,
) -> dict[str, Any]:
    binding = load_active_release_binding(
        path=config.active_release_binding_path,
        live_env_path=config.live_env_path,
        expected_release_id=config.release_id,
        expected_epoch_id=config.bootstrap_epoch_id,
    )
    authorization = load_bootstrap_authorization(
        expected_epoch_id=binding["bootstrap_epoch_id"],
        expected_release_bom_sha256=binding["release_bom_sha256"],
        expected_release_approval_id=binding["release_id"],
        expected_approval_evidence_sha256=binding[
            "approval_evidence_sha256"
        ],
    )
    if (
        authorization["authorization_receipt_sha256"]
        != binding["authorization_receipt_sha256"]
        or authorization["receipt_fingerprint"]
        != binding["authorization_fingerprint"]
    ):
        raise RcaBootstrapAuthorizationError(
            "rca_active_release_authorization_identity_mismatch"
        )
    return {
        **authorization,
        "active_release_binding_sha256": binding["binding_receipt_sha256"],
        "candidate_env_sha256": binding["candidate_env_sha256"],
    }


def _remote_storage_admission_script(request: StorageAdmissionRequest) -> str:
    """Build the fixed read-only VM evaluation program sent to run_py_json."""
    request_json = json.dumps(
        {
            "requested_cases": request.requested_cases,
            "assumed_cases_per_day": request.assumed_cases_per_day,
            "expected_artifact_cache_bytes": (request.expected_artifact_cache_bytes),
            "reserve_percent": request.reserve_percent,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return f"""
import importlib.util
import json
import sys
from decimal import Decimal

MODULE_PATH = {REMOTE_STORAGE_ADMISSION_MODULE!r}
REQUEST = json.loads({request_json!r})
spec = importlib.util.spec_from_file_location("g1q3_rca_storage_admission", MODULE_PATH)
if spec is None or spec.loader is None:
    raise RuntimeError("storage_admission_module_unloadable")
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)
result = module.evaluate_storage_admission(
    requested_cases=REQUEST["requested_cases"],
    assumed_cases_per_day=REQUEST["assumed_cases_per_day"],
    expected_derived_artifact_bytes=REQUEST["expected_artifact_cache_bytes"],
    reserve_ratio=Decimal(REQUEST["reserve_percent"]) / Decimal("100"),
)
print(json.dumps(result, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
""".strip()


def _exact_int(value: Any, name: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ValueError(f"{name} must be an integer >= {minimum}")
    return value


def _number(value: Any, name: str, *, minimum: float = 0.0) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be numeric")
    result = float(value)
    if result < minimum:
        raise ValueError(f"{name} must be >= {minimum}")
    return result


def _exact_float(value: Any, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, float):
        raise ValueError(f"{name} must be a JSON float")
    return value


def _storage_target_summary(
    value: Any,
    *,
    observed_at: str,
    expected_bytes_per_case: int,
    requested_cases: int,
    assumed_cases_per_day: int,
    reserve_percent: int,
) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError("target must be an object")
    prefix = "target"
    name = str(value.get("name") or "").strip()
    if name != "task_output" or value.get("path") != "/mnt/tmp":
        raise ValueError("target must be the fixed /mnt/tmp task output")
    if value.get("capacity_scope") != DERIVED_CAPACITY_SCOPE:
        raise ValueError("target capacity_scope mismatch")
    if value.get("observed_at") != observed_at:
        raise ValueError("target observed_at mismatch")
    if abs(_exact_float(value.get("multiplier"), f"{prefix}.multiplier") - 3.25) > 1e-9:
        raise ValueError("target multiplier mismatch")
    bytes_per_case = _exact_int(
        value.get("bytes_per_case"), f"{prefix}.bytes_per_case", minimum=1
    )
    required_bytes = _exact_int(
        value.get("required_bytes"), f"{prefix}.required_bytes", minimum=1
    )
    if (
        bytes_per_case != expected_bytes_per_case
        or required_bytes != expected_bytes_per_case * requested_cases
    ):
        raise ValueError("target derived-capacity formula mismatch")
    if not isinstance(value.get("ok"), bool):
        raise ValueError("target.ok must be boolean")
    blocker = value.get("blocker")
    if blocker is not None and not isinstance(blocker, str):
        raise ValueError("target.blocker must be null or string")
    if (value["ok"] is True and blocker is not None) or (
        value["ok"] is False and not blocker
    ):
        raise ValueError("target ok/blocker mismatch")

    base_fields = {
        "name",
        "path",
        "capacity_scope",
        "observed_at",
        "multiplier",
        "bytes_per_case",
        "required_bytes",
        "ok",
        "blocker",
    }
    observation_blockers = {
        "task_output_path_unavailable",
        "task_output_invalid_statvfs",
    }
    if blocker in observation_blockers:
        expected_fields = base_fields | {
            "observation_error",
            "total_bytes",
            "free_bytes",
            "available_bytes",
            "reserve_bytes",
            "admittable_bytes",
            "max_additional_cases",
            "days_horizon_at_assumed_cases_per_day",
        }
        if set(value) != expected_fields:
            raise ValueError("target observation-error shape mismatch")
        observation_error = value.get("observation_error")
        if not isinstance(observation_error, Mapping) or set(observation_error) != {
            "type",
            "message",
        }:
            raise ValueError("target observation_error shape mismatch")
        if not all(
            isinstance(observation_error.get(field), str)
            and observation_error.get(field)
            for field in ("type", "message")
        ):
            raise ValueError("target observation_error must contain text")
        if any(
            value.get(field) is not None
            for field in ("total_bytes", "free_bytes", "available_bytes", "reserve_bytes")
        ):
            raise ValueError("target failed observation must not claim capacity")
        if (
            value.get("admittable_bytes") != 0
            or value.get("max_additional_cases") != 0
            or value.get("days_horizon_at_assumed_cases_per_day") != 0.0
        ):
            raise ValueError("target failed observation must report zero capacity")
        max_additional_cases = 0
        horizon = 0.0
    else:
        expected_fields = base_fields | {
            "filesystem_block_size_bytes",
            "total_bytes",
            "free_bytes",
            "available_bytes",
            "reserve_bytes",
            "admittable_bytes",
            "projected_available_after_request_bytes",
            "headroom_after_request_bytes",
            "max_additional_cases",
            "days_horizon_at_assumed_cases_per_day",
        }
        if set(value) != expected_fields:
            raise ValueError("target statvfs shape mismatch")
        block_size = _exact_int(
            value.get("filesystem_block_size_bytes"),
            f"{prefix}.filesystem_block_size_bytes",
            minimum=1,
        )
        total_bytes = _exact_int(
            value.get("total_bytes"), f"{prefix}.total_bytes", minimum=1
        )
        free_bytes = _exact_int(value.get("free_bytes"), f"{prefix}.free_bytes")
        available_bytes = _exact_int(
            value.get("available_bytes"), f"{prefix}.available_bytes"
        )
        reserve_bytes = _exact_int(
            value.get("reserve_bytes"), f"{prefix}.reserve_bytes"
        )
        admittable_bytes = _exact_int(
            value.get("admittable_bytes"), f"{prefix}.admittable_bytes"
        )
        projected_available = value.get("projected_available_after_request_bytes")
        headroom = value.get("headroom_after_request_bytes")
        if isinstance(projected_available, bool) or not isinstance(
            projected_available, int
        ):
            raise ValueError("target projected available must be an integer")
        if isinstance(headroom, bool) or not isinstance(headroom, int):
            raise ValueError("target headroom must be an integer")
        max_additional_cases = _exact_int(
            value.get("max_additional_cases"), f"{prefix}.max_additional_cases"
        )
        horizon = _exact_float(
            value.get("days_horizon_at_assumed_cases_per_day"),
            f"{prefix}.days_horizon_at_assumed_cases_per_day",
        )
        expected_reserve = (total_bytes * reserve_percent + 99) // 100
        expected_admittable = max(0, available_bytes - expected_reserve)
        expected_max_cases = expected_admittable // expected_bytes_per_case
        daily_bytes = expected_bytes_per_case * assumed_cases_per_day
        expected_horizon = (
            (expected_admittable * 1_000 // daily_bytes) / 1_000
            if expected_admittable
            else 0.0
        )
        if available_bytes <= expected_reserve:
            expected_blocker = "task_output_below_reserve_watermark"
        elif required_bytes > expected_admittable:
            expected_blocker = "task_output_insufficient_capacity"
        else:
            expected_blocker = None
        formula_matches = (
            total_bytes >= free_bytes >= available_bytes
            and all(
                amount % block_size == 0
                for amount in (total_bytes, free_bytes, available_bytes)
            )
            and reserve_bytes == expected_reserve
            and admittable_bytes == expected_admittable
            and projected_available == available_bytes - required_bytes
            and headroom == expected_admittable - required_bytes
            and max_additional_cases == expected_max_cases
            and horizon == expected_horizon
            and blocker == expected_blocker
            and value["ok"] is (expected_blocker is None)
        )
        if not formula_matches:
            raise ValueError("target statvfs capacity formula mismatch")
    return {
        "name": name,
        "capacity_scope": DERIVED_CAPACITY_SCOPE,
        "multiplier": 3.25,
        "bytes_per_case": bytes_per_case,
        "required_bytes": required_bytes,
        "ok": value["ok"],
        "blocker": blocker,
        "max_additional_cases": max_additional_cases,
        "days_horizon_at_assumed_cases_per_day": horizon,
    }


def validate_storage_admission(
    value: Mapping[str, Any],
    request: StorageAdmissionRequest,
) -> dict[str, Any]:
    """Validate VM capacity evidence and return a redacted request-safe summary."""
    if not isinstance(value, Mapping):
        raise ValueError("storage admission response must be an object")
    if set(value) != {
        "schema_version",
        "ok",
        "status",
        "observed_at",
        "capacity_scope",
        "policy",
        "required_bytes_total",
        "max_additional_cases",
        "days_horizon_at_assumed_cases_per_day",
        "blockers",
        "target",
        "side_effects",
    }:
        raise ValueError("storage admission response shape mismatch")
    if value.get("schema_version") != STORAGE_ADMISSION_SCHEMA_VERSION:
        raise ValueError("storage admission schema mismatch")
    if value.get("capacity_scope") != DERIVED_CAPACITY_SCOPE:
        raise ValueError("storage admission capacity_scope mismatch")
    status = str(value.get("status") or "").strip()
    if status not in {"pass", "blocked"}:
        raise ValueError("storage admission status must be pass or blocked")
    expected_ok = status == "pass"
    if value.get("ok") is not expected_ok:
        raise ValueError("storage admission ok/status mismatch")
    blockers = value.get("blockers")
    if not isinstance(blockers, list) or not all(
        isinstance(item, str) and item for item in blockers
    ):
        raise ValueError("storage admission blockers must be a list of codes")
    if (status == "pass" and blockers) or (status == "blocked" and not blockers):
        raise ValueError("storage admission blocker/status mismatch")
    if value.get("side_effects") != "none_read_only_statvfs":
        raise ValueError("storage admission did not attest read-only statvfs")
    observed_at = str(value.get("observed_at") or "").strip()
    try:
        observed = datetime.fromisoformat(observed_at)
    except ValueError as exc:
        raise ValueError("storage admission observed_at is invalid") from exc
    if observed.tzinfo is None:
        raise ValueError("storage admission observed_at must be timezone-aware")

    policy = value.get("policy")
    if not isinstance(policy, Mapping):
        raise ValueError("storage admission policy must be an object")
    if set(policy) != {
        "requested_cases",
        "concurrency_reserve_cases",
        "requested_cases_scope",
        "assumed_cases_per_day",
        "assumed_cases_per_day_scope",
        "expected_derived_artifact_bytes_per_case",
        "input_materialization_bytes_per_case",
        "input_materialization",
        "input_unit",
        "gb_definition_bytes",
        "reserve_ratio",
        "reserve_percent",
        "task_output_multiplier",
        "logical_budget_multipliers",
        "logical_budget_bytes_per_case",
    }:
        raise ValueError("storage admission policy shape mismatch")
    requested_cases = _exact_int(
        policy.get("requested_cases"), "policy.requested_cases", minimum=1
    )
    concurrency_reserve = _exact_int(
        policy.get("concurrency_reserve_cases"),
        "policy.concurrency_reserve_cases",
        minimum=1,
    )
    assumed_cases_per_day = _exact_int(
        policy.get("assumed_cases_per_day"),
        "policy.assumed_cases_per_day",
        minimum=1,
    )
    if policy.get("requested_cases_scope") != (
        "this_admission_capacity_reservation_only"
    ):
        raise ValueError("storage admission requested_cases_scope mismatch")
    if policy.get("assumed_cases_per_day_scope") != ("days_horizon_calculation_only"):
        raise ValueError("storage admission assumed_cases_per_day_scope mismatch")
    expected_artifact_cache_bytes = _exact_int(
        policy.get("expected_derived_artifact_bytes_per_case"),
        "policy.expected_derived_artifact_bytes_per_case",
        minimum=1,
    )
    if policy.get("input_materialization_bytes_per_case") != 0:
        raise ValueError("storage admission input materialization must be zero")
    if policy.get("input_materialization") != "forbidden":
        raise ValueError("storage admission input materialization must be forbidden")
    if policy.get("input_unit") != "bytes":
        raise ValueError("storage admission input_unit mismatch")
    if (
        _exact_int(
            policy.get("gb_definition_bytes"),
            "policy.gb_definition_bytes",
            minimum=1,
        )
        != 1_000_000_000
    ):
        raise ValueError("storage admission GB definition mismatch")
    reserve_ratio = _exact_float(policy.get("reserve_ratio"), "policy.reserve_ratio")
    reserve_percent = _exact_float(
        policy.get("reserve_percent"), "policy.reserve_percent"
    )
    if abs(
        _exact_float(policy.get("task_output_multiplier"), "policy.task_output_multiplier")
        - 3.25
    ) > 1e-9:
        raise ValueError("storage admission task_output_multiplier mismatch")
    logical_multipliers = policy.get("logical_budget_multipliers")
    if not isinstance(logical_multipliers, Mapping) or set(logical_multipliers) != {
        "derived_cache",
        "derived_artifacts_and_publisher",
        "total",
    }:
        raise ValueError("storage admission logical multiplier shape mismatch")
    for key, expected in {
        "derived_cache": 1.0,
        "derived_artifacts_and_publisher": 2.25,
        "total": 3.25,
    }.items():
        if abs(
            _exact_float(logical_multipliers.get(key), f"policy.logical_budget_multipliers.{key}")
            - expected
        ) > 1e-9:
            raise ValueError("storage admission logical multiplier mismatch")
    logical_bytes = policy.get("logical_budget_bytes_per_case")
    if not isinstance(logical_bytes, Mapping) or set(logical_bytes) != {
        "derived_cache",
        "derived_artifacts_and_publisher",
        "total",
    }:
        raise ValueError("storage admission logical byte budget shape mismatch")
    expected_cache = expected_artifact_cache_bytes
    expected_artifacts_and_publisher = (
        expected_artifact_cache_bytes * 9 + 3
    ) // 4
    expected_bytes_per_case = expected_cache + expected_artifacts_and_publisher
    if {
        key: _exact_int(logical_bytes.get(key), f"policy.logical_budget_bytes_per_case.{key}", minimum=1)
        for key in logical_bytes
    } != {
        "derived_cache": expected_cache,
        "derived_artifacts_and_publisher": expected_artifacts_and_publisher,
        "total": expected_bytes_per_case,
    }:
        raise ValueError("storage admission logical byte budget mismatch")
    if (
        requested_cases != request.requested_cases
        or concurrency_reserve != request.requested_cases
        or assumed_cases_per_day != request.assumed_cases_per_day
        or expected_artifact_cache_bytes != request.expected_artifact_cache_bytes
        or abs(reserve_ratio - (request.reserve_percent / 100.0)) > 1e-9
        or abs(reserve_percent - float(request.reserve_percent)) > 1e-9
    ):
        raise ValueError("storage admission policy does not match the host request")

    required_bytes_total = _exact_int(
        value.get("required_bytes_total"), "required_bytes_total", minimum=1
    )
    max_additional_cases = _exact_int(
        value.get("max_additional_cases"), "max_additional_cases"
    )
    horizon = _exact_float(
        value.get("days_horizon_at_assumed_cases_per_day"),
        "days_horizon_at_assumed_cases_per_day",
    )
    target = _storage_target_summary(
        value.get("target"),
        observed_at=observed_at,
        expected_bytes_per_case=expected_bytes_per_case,
        requested_cases=requested_cases,
        assumed_cases_per_day=assumed_cases_per_day,
        reserve_percent=request.reserve_percent,
    )
    if (status == "pass" and not target["ok"]) or (
        status == "blocked" and target["ok"]
    ):
        raise ValueError("storage admission target/status mismatch")
    expected_blockers = [] if target["ok"] else [target["blocker"]]
    if blockers != expected_blockers:
        raise ValueError("storage admission target blocker mismatch")
    if required_bytes_total != target["required_bytes"]:
        raise ValueError("storage admission required_bytes_total mismatch")
    if max_additional_cases != target["max_additional_cases"]:
        raise ValueError("storage admission max_additional_cases mismatch")
    if (status == "pass" and max_additional_cases < request.requested_cases) or (
        status == "blocked" and max_additional_cases >= request.requested_cases
    ):
        raise ValueError("storage admission capacity/status mismatch")
    if horizon != target["days_horizon_at_assumed_cases_per_day"]:
        raise ValueError("storage admission days horizon mismatch")
    return {
        "schema_version": DERIVED_CAPACITY_SUMMARY_SCHEMA_VERSION,
        "status": status,
        "observed_at": observed_at,
        "capacity_scope": DERIVED_CAPACITY_SCOPE,
        "atomic_reservation": False,
        "input_materialization_bytes_per_case": 0,
        "input_materialization": "forbidden",
        "policy": {
            "requested_cases": requested_cases,
            "concurrency_reserve_cases": concurrency_reserve,
            "requested_cases_scope": DERIVED_CAPACITY_REQUEST_SCOPE,
            "assumed_cases_per_day": assumed_cases_per_day,
            "assumed_cases_per_day_scope": ("days_horizon_calculation_only"),
            "expected_derived_artifact_bytes_per_case": (
                expected_artifact_cache_bytes
            ),
            "reserve_percent": reserve_percent,
            "task_output_multiplier": 3.25,
        },
        "required_bytes_total": required_bytes_total,
        "max_additional_cases": max_additional_cases,
        "days_horizon_at_assumed_cases_per_day": horizon,
        "target": target,
    }


def default_storage_admission(
    request: StorageAdmissionRequest,
) -> Mapping[str, Any]:
    """Evaluate VM storage using only the governed absolute run_py_json wrapper."""
    wrapper = Path(DEFAULT_SSH_MINI_AGENT)
    if not wrapper.is_absolute():
        raise DispatchCircuitError(
            "storage_admission_wrapper_invalid", "ssh-mini-agent path is not absolute"
        )
    environment = os.environ.copy()
    environment["SSH_MINI_AGENT_TIMEOUT"] = str(request.timeout_seconds)
    try:
        proc = subprocess.run(
            [str(wrapper), "run_py_json"],
            input=_remote_storage_admission_script(request),
            text=True,
            capture_output=True,
            timeout=request.timeout_seconds,
            check=False,
            env=environment,
        )
    except subprocess.TimeoutExpired as exc:
        raise DispatchCircuitError(
            "storage_admission_timeout", "storage admission wrapper timed out"
        ) from exc
    except OSError as exc:
        raise DispatchCircuitError(
            "storage_admission_call_failed", type(exc).__name__
        ) from exc
    if proc.returncode != 0:
        raise DispatchCircuitError(
            "storage_admission_call_failed",
            f"ssh-mini-agent returned rc={proc.returncode}",
        )
    try:
        payload = json.loads(proc.stdout or "")
    except json.JSONDecodeError as exc:
        raise DispatchCircuitError(
            "storage_admission_response_invalid",
            "ssh-mini-agent returned non-JSON storage evidence",
        ) from exc
    if not isinstance(payload, Mapping):
        raise DispatchCircuitError(
            "storage_admission_response_invalid",
            "ssh-mini-agent storage evidence is not an object",
        )
    try:
        validate_storage_admission(payload, request)
    except ValueError as exc:
        raise DispatchCircuitError(
            "storage_admission_schema_invalid", str(exc)
        ) from exc
    return payload


def _validated_claim_contract(
    claim: OutboxClaim,
) -> tuple[RcaAdmission, dict[str, Any]]:
    payload = claim.payload
    payload_schema = str(payload.get("schema_version") or "")
    if payload_schema not in SUPPORTED_OUTBOX_PAYLOAD_SCHEMA_VERSIONS:
        raise DispatchCircuitError(
            "dispatcher_outbox_contract_invalid", "unsupported outbox schema"
        )
    if claim.action != "submit_rca_issue_intake":
        raise DispatchCircuitError(
            "dispatcher_outbox_action_invalid", "unsupported outbox action"
        )
    try:
        admission = validate_rca_admission(payload.get("admission") or {})
    except Exception as exc:
        raise DispatchCircuitError(
            "dispatcher_outbox_contract_invalid",
            f"invalid durable admission contract: {type(exc).__name__}",
        ) from exc
    event = (
        payload.get("trigger_context")
        if payload_schema == OUTBOX_PAYLOAD_SCHEMA_VERSION
        else payload.get("normalized_event")
    )
    if not isinstance(event, dict):
        raise DispatchCircuitError(
            "dispatcher_outbox_contract_invalid", "normalized_event must be an object"
        )
    if payload_schema == OUTBOX_PAYLOAD_SCHEMA_VERSION:
        try:
            event = validate_rca_trigger_context(event).to_dict()
        except Exception as exc:
            raise DispatchCircuitError(
                "dispatcher_outbox_contract_invalid",
                f"invalid trigger context: {type(exc).__name__}",
            ) from exc
    elif event.get("schema_version") != NORMALIZED_EVENT_SCHEMA_VERSION:
        raise DispatchCircuitError(
            "dispatcher_outbox_contract_invalid",
            "unsupported normalized event schema",
        )
    refs = admission.source_refs
    outer_identity: dict[str, Any] = {
        "business_key": claim.business_key,
        "submission_key": claim.submission_key,
        "creation_rule_version": claim.creation_rule_version,
        "generation": claim.generation,
    }
    if payload_schema == OUTBOX_PAYLOAD_SCHEMA_VERSION:
        if not claim.origin_source_id:
            raise DispatchCircuitError(
                "dispatcher_outbox_identity_mismatch",
                "source-neutral outbox is missing origin_source_id",
            )
        outer_identity["origin_source_id"] = claim.origin_source_id
    if payload_schema == "pnc_rca_submission_outbox_v1":
        outer_identity.update(
            {
                "source_event_id": claim.source_event_id,
                "topic": claim.source_topic,
                "partition": claim.source_partition,
                "offset": claim.source_offset,
            }
        )
    for key, expected in outer_identity.items():
        if payload.get(key) != expected:
            raise DispatchCircuitError(
                "dispatcher_outbox_identity_mismatch",
                f"outbox payload {key} does not match durable columns",
            )
    if (
        admission.business_key != claim.business_key
        or admission.submission_key != claim.submission_key
        or admission.generation != claim.generation
        or refs.rule_version != claim.creation_rule_version
        or str(event.get("project_key") or "").strip() != refs.project_key
        or (
            payload_schema == OUTBOX_PAYLOAD_SCHEMA_VERSION
            and str(event.get("project_simple_name") or "").strip()
            != refs.project_simple_name
        )
        or str(event.get("work_item_type_key") or "").strip() != refs.work_item_type_key
        or str(event.get("work_item_id") or "").strip() != refs.work_item_id
        or str(event.get("creation_rule_version") or "").strip() != refs.rule_version
    ):
        raise DispatchCircuitError(
            "dispatcher_outbox_identity_mismatch",
            "admission, normalized event, and durable outbox identity disagree",
        )
    source_kind = str(event.get("source_kind") or "")
    if payload_schema == OUTBOX_PAYLOAD_SCHEMA_VERSION:
        if source_kind == "kafka_workflow_event":
            if (
                not refs.topic
                or refs.partition is None
                or refs.offset is None
                or refs.topic != claim.source_topic
                or refs.partition != claim.source_partition
                or refs.offset != claim.source_offset
            ):
                raise DispatchCircuitError(
                    "dispatcher_outbox_identity_mismatch",
                    "Kafka trigger coordinates disagree with durable lineage",
                )
        elif source_kind == "feishu_group_manual":
            if (
                refs.topic
                or refs.partition is not None
                or refs.offset is not None
                or any(
                    item is not None
                    for item in (
                        claim.source_event_id,
                        claim.source_topic,
                        claim.source_partition,
                        claim.source_offset,
                    )
                )
            ):
                raise DispatchCircuitError(
                    "dispatcher_outbox_identity_mismatch",
                    "manual trigger must not carry Kafka coordinates",
                )
    else:
        if (
            refs.topic != claim.source_topic
            or refs.partition != claim.source_partition
            or refs.offset != claim.source_offset
        ):
            raise DispatchCircuitError(
                "dispatcher_outbox_identity_mismatch",
                "legacy Kafka coordinates disagree with durable lineage",
            )
    return admission, event


def _contains_mapping_key(value: Any, forbidden_key: str) -> bool:
    if isinstance(value, Mapping):
        return any(
            str(key).lower() == forbidden_key.lower()
            or _contains_mapping_key(item, forbidden_key)
            for key, item in value.items()
        )
    if isinstance(value, (list, tuple)):
        return any(_contains_mapping_key(item, forbidden_key) for item in value)
    return False


def _contains_enabled_flag(value: Any, forbidden_keys: frozenset[str]) -> bool:
    if isinstance(value, Mapping):
        return any(
            (str(key).lower() in forbidden_keys and item is True)
            or _contains_enabled_flag(item, forbidden_keys)
            for key, item in value.items()
        )
    if isinstance(value, (list, tuple)):
        return any(_contains_enabled_flag(item, forbidden_keys) for item in value)
    return False


def _contains_mdi_command(value: Any) -> bool:
    if isinstance(value, str):
        return _FORBIDDEN_MDI_COMMAND_PATTERN.search(value) is not None
    if isinstance(value, Mapping):
        return any(_contains_mdi_command(item) for item in value.values())
    if isinstance(value, (list, tuple)):
        return any(_contains_mdi_command(item) for item in value)
    return False


def _validate_remote_read_execution_request(
    request: RcaExecutionRequest,
    *,
    require_derived_reservation: bool = False,
) -> None:
    request_body = asdict(request)
    data = request.data if isinstance(request.data, Mapping) else {}
    policy = (
        request.execution_policy
        if isinstance(request.execution_policy, Mapping)
        else {}
    )
    toolchain = request.toolchain if isinstance(request.toolchain, Mapping) else {}
    if _contains_mapping_key(request_body, "pdcl_download_cmd"):
        raise DispatchCircuitError(
            "dispatcher_remote_data_access_invalid",
            "remote-read execution request must not carry a legacy MDI field",
        )
    if _contains_mdi_command(request_body):
        raise DispatchCircuitError(
            "dispatcher_remote_data_access_invalid",
            "remote-read execution request must not carry an MDI command",
        )
    if _contains_mapping_key(request_body, "storage_reservation"):
        raise DispatchCircuitError(
            "dispatcher_remote_data_access_invalid",
            "remote-read execution request must not authorize legacy reservation",
        )
    if _contains_enabled_flag(
        request_body, frozenset({"allow_download", "mdi_download_allowed"})
    ):
        raise DispatchCircuitError(
            "dispatcher_remote_data_access_invalid",
            "remote-read execution request must not authorize MDI download",
        )
    derived_reservation = toolchain.get("derived_capacity_reservation")
    derived_capacity = (
        derived_reservation.get("capacity")
        if isinstance(derived_reservation, Mapping)
        else None
    )
    if require_derived_reservation and (
        not isinstance(derived_reservation, Mapping)
        or derived_reservation.get("schema_version")
        != "g1q3_rca_derived_capacity_reservation_v1"
        or derived_reservation.get("status") not in {"reserved", "active", "released"}
        or not isinstance(derived_reservation.get("fence"), int)
        or not isinstance(derived_capacity, Mapping)
        or derived_capacity.get("atomic_reservation") is not True
    ):
        raise DispatchCircuitError(
            "dispatcher_derived_capacity_reservation_invalid",
            "execution request is missing an admitted atomic reservation receipt",
        )
    access = data.get("data_access")
    try:
        validate_remote_data_access(access)
    except RemoteDataAccessError as exc:
        raise DispatchCircuitError(
            "dispatcher_remote_data_access_invalid",
            f"remote data access contract rejected: {exc.code}",
        ) from exc
    expected_policy = {
        "mode": REMOTE_DATA_ACCESS_MODE,
        "data_access_mode": REMOTE_DATA_ACCESS_MODE,
        "allow_download": False,
        "input_materialization": "forbidden",
        "derived_artifacts_allowed": True,
    }
    if any(policy.get(key) != value for key, value in expected_policy.items()):
        raise DispatchCircuitError(
            "dispatcher_remote_data_access_invalid",
            "remote-read execution policy is invalid",
        )


def build_dispatch_execution_request(
    *,
    claim: OutboxClaim,
    admission: RcaAdmission,
    issue_context: RcaIssueContext,
    config: DispatcherConfig,
    storage_admission_summary: Mapping[str, Any],
) -> RcaExecutionRequest:
    """Build a source-neutral request bound to the canonical submission paths."""
    refs = admission.source_refs
    if (
        issue_context.project_key != refs.project_key
        or issue_context.work_item_id != refs.work_item_id
        or issue_context.work_item_type != refs.work_item_type_key
    ):
        raise DispatchCircuitError(
            "dispatcher_enrichment_identity_mismatch",
            "enriched issue identity does not match admission",
        )
    artifact_root, artifact_cifs_root = canonical_artifact_paths(
        admission.submission_key
    )
    request = build_execution_request(
        request_kind="issue_intake",
        task_id=admission.submission_key,
        issue_context=issue_context,
        artifact_root=artifact_root,
        artifact_cifs_root=artifact_cifs_root,
        allow_download=False,
        allow_feishu_writeback=config.allow_feishu_writeback,
        group_response_cap=config.group_response_cap,
        translate_baseline=config.translate_baseline,
        translate_contract_path=config.translate_contract_path,
        toolchain={
            "intake_dispatcher": DISPATCHER_HEALTH_SCHEMA_VERSION,
            "storage_admission": dict(storage_admission_summary),
        },
    )
    trigger_context = claim.payload.get("trigger_context")
    trigger_context = trigger_context if isinstance(trigger_context, Mapping) else {}
    source_kind = str(
        trigger_context.get("source_kind") or "kafka_workflow_event"
    ).strip()
    durable_source_refs: dict[str, Any] = {
        "task_id": admission.submission_key,
        "source_kind": source_kind,
        "rule_version": claim.creation_rule_version,
        "generation": claim.generation,
        "business_key": admission.business_key,
        "submission_key": admission.submission_key,
        "origin_source_id": claim.origin_source_id,
    }
    if claim.source_event_id is not None:
        durable_source_refs["source_event_id"] = claim.source_event_id
    if claim.source_topic is not None:
        durable_source_refs["topic"] = claim.source_topic
    if claim.source_partition is not None:
        durable_source_refs["partition"] = claim.source_partition
    if claim.source_offset is not None:
        durable_source_refs["offset"] = claim.source_offset
    request = replace(
        request,
        source_refs=durable_source_refs,
    )
    _validate_remote_read_execution_request(request)
    return request


def build_dispatch_derived_capacity_reservation_request(
    *,
    admission: RcaAdmission,
    request: RcaExecutionRequest,
    config: DispatcherConfig,
) -> DerivedCapacityReservationRequest:
    data = request.data if isinstance(request.data, Mapping) else {}
    data_access = data.get("data_access")
    if not isinstance(data_access, Mapping):
        raise DispatchCircuitError(
            "dispatcher_derived_capacity_reservation_invalid",
            "execution request is missing canonical data_access",
        )
    artifact_root = str(data.get("artifact_root") or "")
    try:
        return DerivedCapacityReservationRequest(
            submission_key=admission.submission_key,
            task_id=admission.submission_key,
            business_key=admission.business_key,
            data_access_sha256=canonical_data_access_sha256(data_access),
            artifact_root=artifact_root,
            expected_artifact_cache_bytes=(
                config.storage_expected_artifact_cache_bytes
            ),
            timeout_seconds=(config.derived_capacity_reservation_timeout_seconds),
        )
    except DerivedCapacityReservationError as exc:
        raise DispatchCircuitError(exc.code, exc.detail) from exc


def attach_derived_capacity_reservation(
    request: RcaExecutionRequest,
    decision: DerivedCapacityReservationDecision,
) -> RcaExecutionRequest:
    attached = replace(
        request,
        toolchain={
            **request.toolchain,
            "derived_capacity_reservation": dict(decision.receipt),
        },
    )
    _validate_remote_read_execution_request(attached, require_derived_reservation=True)
    return attached


def _submission_receipt(
    result: Mapping[str, Any],
    *,
    submission_key: str,
    capacity_admission_summary: Mapping[str, Any],
    derived_capacity_reservation_receipt: Mapping[str, Any],
) -> dict[str, Any]:
    task = result.get("task")
    task = task if isinstance(task, Mapping) else {}
    task_id = str(task.get("task_id") or task.get("id") or "").strip()
    if task_id != submission_key:
        raise DispatchCircuitError(
            "dispatcher_submit_identity_mismatch",
            "successful submit did not return the canonical submission task id",
        )
    return {
        "success": True,
        "submission_key": submission_key,
        "task_id": task_id,
        "task_state": str(task.get("state") or ""),
        "deduped": result.get("deduped") is True,
        "created": result.get("created"),
        "returncode": result.get("returncode"),
        "capacity_admission": {
            "schema_version": str(
                capacity_admission_summary.get("schema_version") or ""
            ),
            "capacity_scope": str(
                capacity_admission_summary.get("capacity_scope") or ""
            ),
            "atomic_reservation": False,
            "status": str(capacity_admission_summary.get("status") or ""),
            "observed_at": str(capacity_admission_summary.get("observed_at") or ""),
            "summary_sha256": canonical_json_sha256(dict(capacity_admission_summary)),
        },
        "derived_capacity_reservation": {
            "schema_version": str(
                derived_capacity_reservation_receipt.get("schema_version") or ""
            ),
            "atomic_reservation": True,
            "reservation_id": str(
                derived_capacity_reservation_receipt.get("reservation_id") or ""
            ),
            "fence": derived_capacity_reservation_receipt.get("fence"),
            "contract_sha256": str(
                derived_capacity_reservation_receipt.get("contract_sha256") or ""
            ),
            "status": str(derived_capacity_reservation_receipt.get("status") or ""),
            "receipt_sha256": canonical_json_sha256(
                dict(derived_capacity_reservation_receipt)
            ),
        },
    }


def _is_definitive_precreate_failure(result: Mapping[str, Any]) -> bool:
    if result.get("success") is not False:
        return False
    if result.get("retryable") is not False:
        return False
    if result.get("created") is False:
        return True
    error_code = str(result.get("error_code") or "").strip()
    return error_code in _DEFINITIVE_PRECREATE_ERROR_CODES


def _precreate_abort_audit(
    receipt: Mapping[str, Any],
) -> dict[str, Any]:
    audit = dict(receipt)
    audit["receipt_sha256"] = canonical_json_sha256(dict(receipt))
    return audit


def _detail_with_precreate_abort_audit(
    detail: str,
    abort_audit: Mapping[str, Any] | None,
) -> str:
    if not abort_audit:
        return detail
    serialized = json.dumps(dict(abort_audit), sort_keys=True, separators=(",", ":"))
    return f"{detail}; derived_capacity_precreate_abort={serialized}"


class OutboxDispatcher:
    def __init__(
        self,
        *,
        store: RcaControlStore,
        config: DispatcherConfig,
        enrich: EnrichFunc,
        storage_admission: StorageAdmissionFunc,
        submit: SubmitFunc,
        derived_capacity_reservation: DerivedCapacityReservationFunc | None = None,
        derived_capacity_abort_precreate: DerivedCapacityPrecreateAbortFunc = (
            abort_precreate_derived_capacity
        ),
        delivery_store: RcaDeliveryStore | None = None,
        now: Callable[[], datetime] = _utc_now,
        lease_owner: str | None = None,
    ):
        self.store = store
        self.config = config
        if config.lease_seconds < MIN_LEASE_SECONDS:
            raise ValueError("dispatcher lease is shorter than the production minimum")
        if not (
            MIN_INPUT_WAIT_MAX_AGE_SECONDS
            <= config.input_wait_max_age_seconds
            <= MAX_INPUT_WAIT_MAX_AGE_SECONDS
        ):
            raise ValueError("dispatcher input-wait retry horizon is out of bounds")
        if config.input_wait_max_age_seconds > config.max_age_seconds:
            raise ValueError(
                "dispatcher input-wait retry horizon exceeds the general horizon"
            )
        if config.storage_reservation_enabled:
            raise ValueError(
                "dispatcher legacy MDI-bound storage reservation must remain disabled"
            )
        if config.dispatch_enabled and (
            not config.storage_admission_enabled
            or not config.derived_capacity_reservation_enabled
            or not config.delivery_backpressure_enabled
        ):
            raise ValueError(
                "dispatch requires derived-capacity admission, atomic reservation, "
                "and delivery backpressure"
            )
        if config.dispatch_enabled and not callable(derived_capacity_reservation):
            raise ValueError(
                "dispatch requires a callable derived-capacity atomic reservation"
            )
        if config.dispatch_enabled and not callable(derived_capacity_abort_precreate):
            raise ValueError(
                "dispatch requires a callable derived-capacity pre-create abort boundary"
            )
        self.enrich = enrich
        self.storage_admission = storage_admission
        self.derived_capacity_reservation = derived_capacity_reservation
        self.derived_capacity_abort_precreate = derived_capacity_abort_precreate
        self.submit = submit
        self.delivery_store = delivery_store
        self.now = now
        self.lease_owner = lease_owner or (
            f"{config.service_id}:{socket.gethostname()}:{os.getpid()}"
        )
        self.stats = DispatchStats()
        self._delivery_backpressure_active = False
        self._last_delivery_snapshot: dict[str, Any] | None = None
        self._last_delivery_error: dict[str, str] | None = None
        self.runtime_identity: Mapping[str, Any] | None = None
        self.workspace_runtime_guard: Callable[[], DispatchOutcome | None] | None = None

    def dispatch_one(self) -> DispatchOutcome:
        self.stats.loops += 1
        if not self.config.dispatch_enabled:
            return DispatchOutcome(status="disabled")
        if self.workspace_runtime_guard is not None:
            guarded = self.workspace_runtime_guard()
            if guarded is not None:
                return guarded
        circuit = self.store.dispatcher_circuit()
        if circuit.is_open:
            return DispatchOutcome(
                status="circuit_open", error_code=circuit.reason_code
            )
        downstream = self._delivery_backpressure_outcome()
        if downstream is not None:
            return downstream
        current = self.now()
        claim = self.store.claim_outbox(
            lease_owner=self.lease_owner,
            lease_seconds=self.config.lease_seconds,
            max_age_seconds=self.config.max_age_seconds,
            activation_required=self.config.activation_required,
            now=current,
        )
        if claim is None:
            self.stats.idle += 1
            return DispatchOutcome(status="idle")
        self.stats.claimed += 1
        try:
            admission, event = _validated_claim_contract(claim)
            self._renew(claim)
            issue_context = self.enrich(event)
            if not isinstance(issue_context, RcaIssueContext):
                raise DispatchCircuitError(
                    "dispatcher_enrichment_contract_invalid",
                    "enrich(event) must return RcaIssueContext",
                )
            refs = admission.source_refs
            if (
                issue_context.project_key != refs.project_key
                or issue_context.work_item_id != refs.work_item_id
                or issue_context.work_item_type != refs.work_item_type_key
            ):
                raise DispatchCircuitError(
                    "dispatcher_enrichment_identity_mismatch",
                    "enriched issue identity does not match admission",
                )
            storage_request = StorageAdmissionRequest(
                requested_cases=self.config.storage_concurrency_reserve_cases,
                assumed_cases_per_day=self.config.storage_cases_per_day,
                expected_artifact_cache_bytes=(
                    self.config.storage_expected_artifact_cache_bytes
                ),
                reserve_percent=self.config.storage_reserve_percent,
                timeout_seconds=self.config.storage_timeout_seconds,
            )
            try:
                self._renew(claim)
                storage_response = self.storage_admission(storage_request)
                storage_summary = validate_storage_admission(
                    storage_response, storage_request
                )
            except DispatchCircuitError:
                self.stats.storage_admission_errors += 1
                raise
            except Exception as exc:
                self.stats.storage_admission_errors += 1
                raise DispatchCircuitError(
                    "storage_admission_schema_invalid"
                    if isinstance(exc, ValueError)
                    else "storage_admission_call_failed",
                    f"storage admission failed: {type(exc).__name__}",
                ) from exc
            if storage_summary["status"] == "blocked":
                self.stats.storage_admission_blocked += 1
                return self._retry(
                    claim,
                    "storage_admission_blocked",
                    "storage capacity admission is blocked",
                )
            self.stats.storage_admission_passed += 1
            try:
                request = build_dispatch_execution_request(
                    claim=claim,
                    admission=admission,
                    issue_context=issue_context,
                    config=self.config,
                    storage_admission_summary=storage_summary,
                )
            except DispatchCircuitError:
                raise
            except Exception as exc:
                raise DispatchCircuitError(
                    "dispatcher_execution_request_build_failed",
                    f"execution request build failed: {type(exc).__name__}",
                ) from exc
            reservation_request = build_dispatch_derived_capacity_reservation_request(
                admission=admission,
                request=request,
                config=self.config,
            )
            try:
                self._renew(claim)
                reservation_boundary = self.derived_capacity_reservation
                if not callable(reservation_boundary):
                    raise DerivedCapacityReservationError(
                        "derived_capacity_reservation_unavailable",
                        "derived-capacity reservation boundary is unavailable",
                    )
                reservation_decision = reservation_boundary(reservation_request)
                if not isinstance(
                    reservation_decision, DerivedCapacityReservationDecision
                ):
                    raise DerivedCapacityReservationError(
                        "derived_capacity_reservation_schema_invalid",
                        "reservation boundary returned an invalid decision",
                    )
                validated_reservation = validate_derived_capacity_reservation_receipt(
                    reservation_decision.receipt,
                    reservation_request,
                )
                if reservation_decision != validated_reservation:
                    raise DerivedCapacityReservationError(
                        "derived_capacity_reservation_schema_invalid",
                        "reservation decision does not match its receipt",
                    )
            except DerivedCapacityReservationError as exc:
                self.stats.derived_capacity_reservation_errors += 1
                raise DispatchCircuitError(exc.code, exc.detail) from exc
            except Exception as exc:
                self.stats.derived_capacity_reservation_errors += 1
                raise DispatchCircuitError(
                    "derived_capacity_reservation_call_failed",
                    f"derived-capacity reservation failed: {type(exc).__name__}",
                ) from exc
            if validated_reservation.blocked:
                self.stats.derived_capacity_reservation_blocked += 1
                return self._retry(
                    claim,
                    "derived_capacity_reservation_blocked",
                    "atomic derived-capacity reservation is waiting for capacity",
                )
            if (
                not validated_reservation.admitted
                and not validated_reservation.reconcile_only
            ):
                self.stats.derived_capacity_reservation_errors += 1
                raise DispatchCircuitError(
                    "derived_capacity_reservation_status_invalid",
                    "atomic derived-capacity reservation was not admitted",
                )
            if validated_reservation.admitted:
                self.stats.derived_capacity_reservation_admitted += 1
            request = attach_derived_capacity_reservation(
                request, validated_reservation
            )
            self._renew(claim)
            result = self.submit(admission, request)
            if not isinstance(result, Mapping):
                raise DispatchCircuitError(
                    "dispatcher_submit_contract_invalid",
                    "submit result must be an object",
                )
            abort_audit: dict[str, Any] | None = None
            if (
                validated_reservation.status == "reserved"
                and _is_definitive_precreate_failure(result)
            ):
                try:
                    self._renew(claim)
                    abort_boundary = self.derived_capacity_abort_precreate
                    if not callable(abort_boundary):
                        raise DerivedCapacityReservationError(
                            "derived_capacity_reservation_abort_precreate_unavailable",
                            "derived-capacity pre-create abort boundary is unavailable",
                        )
                    abort_receipt = abort_boundary(
                        reservation_request, validated_reservation.receipt
                    )
                    if not isinstance(abort_receipt, Mapping):
                        raise DerivedCapacityReservationError(
                            "derived_capacity_reservation_abort_precreate_invalid",
                            "pre-create abort boundary returned an invalid receipt",
                        )
                    validated_abort = (
                        validate_derived_capacity_precreate_abort_receipt(
                            abort_receipt,
                            reservation_request,
                            validated_reservation.receipt,
                        )
                    )
                    if dict(abort_receipt) != validated_abort:
                        raise DerivedCapacityReservationError(
                            "derived_capacity_reservation_abort_precreate_invalid",
                            "pre-create abort result does not match its receipt",
                        )
                    abort_audit = _precreate_abort_audit(validated_abort)
                    self.stats.derived_capacity_precreate_aborted += 1
                except DerivedCapacityReservationError as exc:
                    self.stats.derived_capacity_precreate_abort_errors += 1
                    raise DispatchCircuitError(exc.code, exc.detail) from exc
                except Exception as exc:
                    self.stats.derived_capacity_precreate_abort_errors += 1
                    raise DispatchCircuitError(
                        "derived_capacity_reservation_abort_precreate_call_failed",
                        f"derived-capacity pre-create abort failed: {type(exc).__name__}",
                    ) from exc
            if result.get("success") is True:
                receipt = _submission_receipt(
                    result,
                    submission_key=admission.submission_key,
                    capacity_admission_summary=storage_summary,
                    derived_capacity_reservation_receipt=(
                        validated_reservation.receipt
                    ),
                )
                self._renew(claim)
                self.store.complete_outbox(
                    outbox_id=claim.outbox_id,
                    lease_token=claim.lease_token,
                    result=receipt,
                    runtime_identity=self.runtime_identity,
                    now=self.now(),
                )
                self.stats.completed += 1
                return DispatchOutcome(
                    status="completed",
                    outbox_id=claim.outbox_id,
                    submission_key=claim.submission_key,
                    attempt=claim.attempt,
                    deduped=receipt["deduped"],
                )
            if result.get("success") is not False:
                raise DispatchCircuitError(
                    "dispatcher_submit_contract_invalid",
                    "submit result must carry a boolean success value",
                )
            error_code = str(
                result.get("error_code") or "vm_task_submit_failed"
            ).strip()
            detail = _detail_with_precreate_abort_audit(
                str(result.get("error") or error_code), abort_audit
            )
            if error_code in _SERVICE_ERROR_CODES:
                raise DispatchCircuitError(error_code, detail)
            if result.get("retryable") is True:
                return self._retry(claim, error_code, detail)
            return self._quarantine(claim, error_code, detail)
        except StaleOutboxLeaseError:
            return self._lease_lost(claim)
        except EnrichmentNotReady as exc:
            return self._retry(
                claim,
                exc.code,
                exc.detail,
                max_age_seconds=self.config.input_wait_max_age_seconds,
            )
        except PermanentDispatchError as exc:
            return self._quarantine(claim, exc.code, exc.detail)
        except DispatchCircuitError as exc:
            return self._handle_dispatch_error(claim, exc.code, exc.detail)
        except Exception as exc:
            if isinstance(exc, ImportError):
                return self._open_circuit(
                    claim,
                    "dispatcher_dependency_unavailable",
                    type(exc).__name__,
                )
            # An exception from an injected network boundary has an uncertain
            # outcome. Retry through the create-once wrapper, which reconciles
            # the canonical task id before any subsequent create.
            boundary = "enrichment_or_submit_exception"
            return self._retry(claim, boundary, type(exc).__name__)

    def _delivery_backpressure_outcome(self) -> DispatchOutcome | None:
        self.stats.delivery_backpressure_checks += 1
        try:
            if self.delivery_store is None:
                snapshot = RcaDeliveryStore.read_existing_backpressure_snapshot(
                    self.config.delivery_db_path,
                    now=self.now(),
                )
            else:
                snapshot = self.delivery_store.backpressure_snapshot(now=self.now())
            if not isinstance(snapshot, DeliveryBackpressureSnapshot):
                raise ValueError(
                    "delivery backpressure boundary returned an invalid contract"
                )
            outcome_slo_healthy = validate_delivery_outcome_slo(
                snapshot.outcome_slo,
                expected_observed_at=snapshot.observed_at,
            )
            counts = (
                snapshot.pending,
                snapshot.claimed,
                snapshot.retry_wait,
                snapshot.uncertain,
            )
            pipeline_counts = (
                snapshot.untracked_completed_submissions,
                snapshot.pending_watches,
                snapshot.running_watches,
            )
            if (
                snapshot.db_path != str(self.config.delivery_db_path)
                or any(value < 0 for value in counts)
                or any(value < 0 for value in pipeline_counts)
                or snapshot.unresolved_effects != sum(counts)
                or snapshot.unresolved_work
                != snapshot.unresolved_effects + sum(pipeline_counts)
                or snapshot.circuit.state not in {"closed", "open"}
            ):
                raise ValueError("delivery backpressure snapshot is inconsistent")
        except Exception as exc:
            self.stats.delivery_backpressure_errors += 1
            self._last_delivery_snapshot = None
            self._last_delivery_error = {
                "code": (
                    "delivery_backpressure_contract_invalid"
                    if isinstance(exc, ValueError) or "contract_invalid" in str(exc)
                    else "delivery_backpressure_unavailable"
                ),
                "detail": f"{type(exc).__name__}: {exc}",
            }
            return DispatchOutcome(
                status="downstream_error",
                error_code=self._last_delivery_error["code"],
            )

        self._last_delivery_snapshot = snapshot.public_dict()
        self._last_delivery_error = None
        if not outcome_slo_healthy:
            self._delivery_backpressure_active = True
            self.stats.delivery_backpressure_blocked += 1
            self.stats.delivery_outcome_slo_blocked += 1
            return DispatchOutcome(
                status="downstream_backpressure",
                error_code="delivery_outcome_slo_failed",
                downstream_unresolved_effects=snapshot.unresolved_effects,
                downstream_unresolved_work=snapshot.unresolved_work,
                downstream_circuit_state=snapshot.circuit.state,
            )
        if snapshot.circuit.is_open:
            self.stats.delivery_circuit_blocked += 1
            return DispatchOutcome(
                status="downstream_backpressure",
                error_code="delivery_dispatcher_circuit_open",
                downstream_unresolved_effects=snapshot.unresolved_effects,
                downstream_unresolved_work=snapshot.unresolved_work,
                downstream_circuit_state=snapshot.circuit.state,
            )

        unresolved = snapshot.unresolved_work
        if self._delivery_backpressure_active:
            if unresolved <= self.config.delivery_resume_watermark:
                self._delivery_backpressure_active = False
                self.stats.delivery_backpressure_resumed += 1
            else:
                self.stats.delivery_backpressure_blocked += 1
                return DispatchOutcome(
                    status="downstream_backpressure",
                    error_code="delivery_pending_above_resume_watermark",
                    downstream_unresolved_effects=snapshot.unresolved_effects,
                    downstream_unresolved_work=unresolved,
                    downstream_circuit_state=snapshot.circuit.state,
                )
        if unresolved >= self.config.delivery_high_watermark:
            self._delivery_backpressure_active = True
            self.stats.delivery_backpressure_blocked += 1
            return DispatchOutcome(
                status="downstream_backpressure",
                error_code="delivery_pending_high_watermark",
                downstream_unresolved_effects=snapshot.unresolved_effects,
                downstream_unresolved_work=unresolved,
                downstream_circuit_state=snapshot.circuit.state,
            )
        return None

    def delivery_backpressure_health(self) -> dict[str, Any]:
        return {
            "enabled": self.config.delivery_backpressure_enabled,
            "active": self._delivery_backpressure_active,
            "high_watermark": self.config.delivery_high_watermark,
            "resume_watermark": self.config.delivery_resume_watermark,
            "last_snapshot": self._last_delivery_snapshot,
            "last_error": self._last_delivery_error,
        }

    def _renew(self, claim: OutboxClaim) -> None:
        self.store.extend_outbox_lease(
            outbox_id=claim.outbox_id,
            lease_token=claim.lease_token,
            lease_owner=claim.lease_owner,
            lease_seconds=self.config.lease_seconds,
            activation_required=self.config.activation_required,
            now=self.now(),
        )

    def _lease_lost(self, claim: OutboxClaim) -> DispatchOutcome:
        self.stats.lease_lost += 1
        return DispatchOutcome(
            status="lease_lost",
            outbox_id=claim.outbox_id,
            submission_key=claim.submission_key,
            attempt=claim.attempt,
            error_code="stale_outbox_lease",
        )

    def _retry(
        self,
        claim: OutboxClaim,
        error_code: str,
        detail: str,
        *,
        max_age_seconds: int | None = None,
    ) -> DispatchOutcome:
        try:
            self._renew(claim)
            mutation = self.store.retry_outbox(
                outbox_id=claim.outbox_id,
                lease_token=claim.lease_token,
                error_code=error_code,
                error_detail=detail,
                delay_seconds=retry_delay_seconds(claim.attempt),
                max_age_seconds=(
                    self.config.max_age_seconds
                    if max_age_seconds is None
                    else max_age_seconds
                ),
                now=self.now(),
            )
        except StaleOutboxLeaseError:
            return self._lease_lost(claim)
        if mutation.status == "quarantined":
            self.stats.quarantined += 1
        else:
            self.stats.retried += 1
        return DispatchOutcome(
            status=mutation.status,
            outbox_id=claim.outbox_id,
            submission_key=claim.submission_key,
            attempt=claim.attempt,
            error_code=error_code,
            next_attempt_at=mutation.next_attempt_at,
        )

    def _handle_dispatch_error(
        self,
        claim: OutboxClaim,
        error_code: str,
        detail: str,
    ) -> DispatchOutcome:
        if error_code in _GLOBAL_CIRCUIT_ERROR_CODES:
            return self._open_circuit(claim, error_code, detail)
        if error_code in _PER_CASE_QUARANTINE_ERROR_CODES:
            return self._quarantine(claim, error_code, detail)
        if (
            error_code in _RETRYABLE_BOUNDARY_ERROR_CODES
            or error_code.endswith("_timeout")
            or error_code.endswith("_call_failed")
        ):
            return self._retry(claim, error_code, detail)
        # Unknown scoped failures are retried to preserve queue availability;
        # the durable retry horizon eventually quarantines a persistent poison row.
        return self._retry(claim, error_code, detail)

    def _quarantine(
        self,
        claim: OutboxClaim,
        error_code: str,
        detail: str,
    ) -> DispatchOutcome:
        try:
            self._renew(claim)
            self.store.quarantine_outbox(
                outbox_id=claim.outbox_id,
                lease_token=claim.lease_token,
                error_code=error_code,
                error_detail=detail,
                now=self.now(),
            )
        except StaleOutboxLeaseError:
            return self._lease_lost(claim)
        self.stats.quarantined += 1
        return DispatchOutcome(
            status="quarantined",
            outbox_id=claim.outbox_id,
            submission_key=claim.submission_key,
            attempt=claim.attempt,
            error_code=error_code,
        )

    def _open_circuit(
        self,
        claim: OutboxClaim,
        error_code: str,
        detail: str,
    ) -> DispatchOutcome:
        try:
            self._renew(claim)
            mutation = self.store.retry_outbox_and_open_circuit(
                outbox_id=claim.outbox_id,
                lease_token=claim.lease_token,
                error_code=error_code,
                error_detail=detail,
                delay_seconds=retry_delay_seconds(claim.attempt),
                max_age_seconds=self.config.max_age_seconds,
                now=self.now(),
            )
        except StaleOutboxLeaseError:
            return self._lease_lost(claim)
        self.stats.circuit_opened += 1
        if mutation.status == "quarantined":
            self.stats.quarantined += 1
        return DispatchOutcome(
            status="circuit_open",
            outbox_id=claim.outbox_id,
            submission_key=claim.submission_key,
            attempt=claim.attempt,
            error_code=error_code,
            next_attempt_at=mutation.next_attempt_at,
        )

    def dispatch_batch(self) -> list[DispatchOutcome]:
        outcomes: list[DispatchOutcome] = []
        for _ in range(self.config.batch_size):
            outcome = self.dispatch_one()
            outcomes.append(outcome)
            if outcome.status in {
                "disabled",
                "idle",
                "circuit_open",
                "downstream_backpressure",
                "downstream_error",
                "lease_lost",
                "capacity_authorization_unavailable",
            }:
                break
        return outcomes


class HealthReporter:
    def __init__(
        self,
        config: DispatcherConfig,
        store: RcaControlStore,
        *,
        delivery_backpressure_status: Callable[[], Mapping[str, Any]] | None = None,
        workspace_runtime_observer: Callable[
            [], WorkspaceRuntimeIdentity
        ] | None = None,
        bootstrap_authorization_observer: Callable[
            [], Mapping[str, Any]
        ] | None = None,
        capacity_runtime_observer: Callable[[], Mapping[str, Any]] | None = None,
        capacity_runtime: CapacityRuntimeResolver | None = None,
    ):
        self.config = config
        self.store = store
        self.delivery_backpressure_status = delivery_backpressure_status
        self.started_at = _utc_iso()
        self.public_config = config.runtime_public_dict()
        self._write_lock = threading.RLock()
        self._last_body: dict[str, Any] | None = None
        self._workspace_runtime_observer = (
            workspace_runtime_observer or validate_workspace_runtime
        )
        self._bootstrap_authorization_observer = (
            bootstrap_authorization_observer
            or (lambda: _load_bound_bootstrap_authorization(self.config))
        )
        self._capacity_runtime_observer = capacity_runtime_observer
        self._capacity_runtime = capacity_runtime
        self._bound_workspace_runtime: WorkspaceRuntimeIdentity | None = None
        self._workspace_runtime_startup_error = ""
        try:
            self._bound_workspace_runtime = self._observe_workspace_runtime()
        except WorkspaceRuntimeError as exc:
            self._workspace_runtime_startup_error = exc.code
        self.runtime_identity = build_runtime_identity(
            service_label=SERVICE_LABEL,
            script_path=Path(__file__),
            public_config=self.public_config,
            loaded_dependencies=RCA_OUTBOX_DISPATCHER_LOADED_DEPENDENCIES,
        )

    def _observe_workspace_runtime(self) -> WorkspaceRuntimeIdentity:
        try:
            identity = self._workspace_runtime_observer()
        except WorkspaceRuntimeError:
            raise
        except Exception as exc:
            raise WorkspaceRuntimeError(
                "rca_workspace_runtime_observer_failed"
            ) from exc
        if not isinstance(identity, WorkspaceRuntimeIdentity):
            raise WorkspaceRuntimeError("rca_workspace_runtime_identity_invalid")
        return identity

    def workspace_runtime_status(self) -> dict[str, Any]:
        required = self.config.dispatch_enabled or self.config.activation_required
        bound = self._bound_workspace_runtime
        try:
            observed = self._observe_workspace_runtime()
        except WorkspaceRuntimeError as exc:
            return {
                "required": required,
                "bound": bound is not None,
                "ready": False,
                "state": "unavailable",
                "error_code": exc.code,
                "startup_error_code": self._workspace_runtime_startup_error,
                "identity": bound.to_dict() if bound is not None else None,
            }
        if bound is None:
            return {
                "required": required,
                "bound": False,
                "ready": False,
                "state": "startup_unbound",
                "error_code": (
                    self._workspace_runtime_startup_error
                    or "rca_workspace_runtime_startup_unbound"
                ),
                "startup_error_code": self._workspace_runtime_startup_error,
                "identity": observed.to_dict(),
            }
        if observed != bound:
            return {
                "required": required,
                "bound": True,
                "ready": False,
                "state": "drifted",
                "error_code": "rca_workspace_runtime_identity_drift",
                "startup_error_code": "",
                "identity": bound.to_dict(),
                "observed_identity": observed.to_dict(),
            }
        return {
            "required": required,
            "bound": True,
            "ready": True,
            "state": "ready",
            "error_code": "",
            "startup_error_code": "",
            "identity": bound.to_dict(),
        }

    def dispatch_guard_outcome(self) -> DispatchOutcome | None:
        status = self.workspace_runtime_status()
        if status["required"] is True and status["ready"] is not True:
            return DispatchOutcome(
                status="workspace_runtime_unavailable",
                error_code=str(status["error_code"]),
            )
        capacity = self.capacity_admission_status()
        if capacity["required"] is True and capacity["ready"] is not True:
            self.store.open_dispatcher_circuit(
                reason_code="dispatcher_capacity_runtime_blocked",
                reason_detail=str(capacity["error_code"]),
            )
            return DispatchOutcome(
                status="capacity_authorization_unavailable",
                error_code=str(capacity["error_code"]),
            )
        return None

    def _capacity_status_from_runtime_decision(
        self, decision: Mapping[str, Any]
    ) -> dict[str, Any]:
        required = bool(
            decision.get("configured") is True
            and (self.config.dispatch_enabled or self.config.activation_required)
        )
        mode = str(decision.get("effective_mode") or "blocked")
        if decision.get("ready") is not True or mode not in {"bootstrap", "steady"}:
            return {
                "required": required,
                "ready": False,
                "state": str(decision.get("effective_state") or "STEADY_BLOCKED"),
                "error_code": str(
                    decision.get("reason_code") or "rca_capacity_runtime_blocked"
                ),
                "capacity_mode": "blocked",
                "authorization": None,
                "runtime": dict(decision),
            }
        if mode == "steady":
            return {
                "required": required,
                "ready": True,
                "state": str(decision.get("effective_state") or "STEADY_ACTIVE"),
                "error_code": "",
                "capacity_mode": "steady",
                "authorization": None,
                "runtime": dict(decision),
            }
        try:
            authorization = dict(self._bootstrap_authorization_observer())
            runtime_active_release_binding = str(
                decision.get("active_release_binding_sha256") or ""
            )
            if (
                authorization.get("authorization_ready") is not True
                or authorization.get("capacity_mode") != "bootstrap"
                or authorization.get("bootstrap_epoch_id")
                != self.config.bootstrap_epoch_id
                or authorization.get("release_approval_id") != self.config.release_id
                or any(
                    re.fullmatch(r"[0-9a-f]{64}", str(authorization.get(field) or ""))
                    is None
                    for field in (
                        "receipt_fingerprint",
                        "authorization_receipt_sha256",
                        "active_release_binding_sha256",
                    )
                )
                or not hmac.compare_digest(
                    runtime_active_release_binding,
                    str(authorization.get("active_release_binding_sha256") or ""),
                )
            ):
                raise RcaBootstrapAuthorizationError(
                    "rca_bootstrap_authorization_projection_invalid"
                )
        except RcaBootstrapAuthorizationError as exc:
            return {
                "required": required,
                "ready": False,
                "state": str(decision.get("effective_state") or ""),
                "error_code": exc.code,
                "capacity_mode": "bootstrap",
                "authorization": None,
                "runtime": dict(decision),
            }
        return {
            "required": required,
            "ready": True,
            "state": str(decision.get("effective_state") or ""),
            "error_code": "",
            "capacity_mode": "bootstrap",
            "authorization": {
                "bootstrap_epoch_id": authorization["bootstrap_epoch_id"],
                "started_at": authorization["started_at"],
                "deadline": authorization["deadline"],
                "receipt_fingerprint": authorization["receipt_fingerprint"],
                "authorization_receipt_sha256": authorization[
                    "authorization_receipt_sha256"
                ],
                "active_release_binding_sha256": authorization[
                    "active_release_binding_sha256"
                ],
                "candidate_env_sha256": authorization["candidate_env_sha256"],
                "release_bom_sha256": authorization["release_bom_sha256"],
                "release_approval_id": authorization["release_approval_id"],
                "approval_evidence_sha256": authorization[
                    "approval_evidence_sha256"
                ],
            },
            "runtime": dict(decision),
        }

    def capacity_admission_status(self) -> dict[str, Any]:
        if self._capacity_runtime is not None:
            with self._capacity_runtime.shared_decision() as decision:
                return self._capacity_status_from_runtime_decision(decision)
        if self._capacity_runtime_observer is not None:
            try:
                decision = dict(self._capacity_runtime_observer())
            except Exception:
                decision = {
                    "configured": bool(
                        self.config.release_id and self.config.bootstrap_epoch_id
                    ),
                    "ready": False,
                    "effective_state": "STEADY_BLOCKED",
                    "effective_mode": "blocked",
                    "reason_code": "rca_capacity_runtime_observation_failed",
                }
            return self._capacity_status_from_runtime_decision(decision)
        required = bool(
            self.config.capacity_mode == "bootstrap"
            and (self.config.dispatch_enabled or self.config.activation_required)
        )
        if self.config.capacity_mode == "steady":
            return {
                "required": False,
                "ready": True,
                "state": "steady",
                "error_code": "",
                "capacity_mode": "steady",
                "authorization": None,
            }
        try:
            authorization = dict(self._bootstrap_authorization_observer())
            if (
                authorization.get("authorization_ready") is not True
                or authorization.get("capacity_mode") != "bootstrap"
                or authorization.get("bootstrap_epoch_id")
                != self.config.bootstrap_epoch_id
                or authorization.get("release_approval_id")
                != self.config.release_id
                or re.fullmatch(
                    r"[0-9a-f]{64}",
                    str(authorization.get("receipt_fingerprint") or ""),
                )
                is None
                or re.fullmatch(
                    r"[0-9a-f]{64}",
                    str(authorization.get("authorization_receipt_sha256") or ""),
                )
                is None
                or re.fullmatch(
                    r"[0-9a-f]{64}",
                    str(authorization.get("active_release_binding_sha256") or ""),
                )
                is None
            ):
                raise RcaBootstrapAuthorizationError(
                    "rca_bootstrap_authorization_projection_invalid"
                )
        except RcaBootstrapAuthorizationError as exc:
            return {
                "required": required,
                "ready": False,
                "state": "unavailable",
                "error_code": exc.code,
                "capacity_mode": "bootstrap",
                "authorization": None,
            }
        return {
            "required": required,
            "ready": True,
            "state": "ready",
            "error_code": "",
            "capacity_mode": "bootstrap",
            "authorization": {
                "bootstrap_epoch_id": authorization["bootstrap_epoch_id"],
                "started_at": authorization["started_at"],
                "deadline": authorization["deadline"],
                "receipt_fingerprint": authorization["receipt_fingerprint"],
                "authorization_receipt_sha256": authorization[
                    "authorization_receipt_sha256"
                ],
                "active_release_binding_sha256": authorization[
                    "active_release_binding_sha256"
                ],
                "candidate_env_sha256": authorization["candidate_env_sha256"],
                "release_bom_sha256": authorization["release_bom_sha256"],
                "release_approval_id": authorization["release_approval_id"],
                "approval_evidence_sha256": authorization[
                    "approval_evidence_sha256"
                ],
            },
        }

    def write(
        self,
        *,
        state: str,
        stats: DispatchStats,
        last_outcome: DispatchOutcome | None = None,
    ) -> None:
        circuit = self.store.dispatcher_circuit()
        downstream = (
            dict(self.delivery_backpressure_status())
            if self.delivery_backpressure_status is not None
            else {
                "enabled": self.config.delivery_backpressure_enabled,
                "active": False,
                "high_watermark": self.config.delivery_high_watermark,
                "resume_watermark": self.config.delivery_resume_watermark,
                "last_snapshot": None,
                "last_error": None,
            }
        )
        store_health = self.store.health()
        workspace_runtime = self.workspace_runtime_status()
        capacity_admission = self.capacity_admission_status()
        healthy = (
            state
            not in {
                "error",
                "circuit_open",
                "downstream_backpressure",
                "downstream_error",
                "lease_lost",
            }
            and not circuit.is_open
            and store_health.get("ok") is True
            and (
                workspace_runtime["required"] is not True
                or workspace_runtime["ready"] is True
            )
            and (
                capacity_admission["required"] is not True
                or capacity_admission["ready"] is True
            )
        )
        ready_for_dispatch = bool(
            self.config.dispatch_enabled
            and healthy
            and workspace_runtime["ready"] is True
            and capacity_admission["ready"] is True
        )
        observed_at = _utc_iso()
        body = {
            "schema_version": DISPATCHER_HEALTH_SCHEMA_VERSION,
            "ok": healthy,
            "healthy": healthy,
            "enabled": self.config.dispatch_enabled,
            "activation_required": self.config.activation_required,
            "state": state,
            "started_at": self.started_at,
            "heartbeat_at": observed_at,
            "readiness_observed_at": observed_at,
            "readiness": {
                "state": state,
                "healthy": healthy,
                "ready_for_dispatch": ready_for_dispatch,
                "observed_at": observed_at,
            },
            "liveness": {
                "state": "reporting",
                "heartbeat_at": observed_at,
                "readiness_observed_at": observed_at,
            },
            "runtime_identity": self.runtime_identity.to_dict(),
            "workspace_runtime": workspace_runtime,
            "capacity_admission": capacity_admission,
            "config": self.public_config,
            "stats": asdict(stats),
            "last_outcome": asdict(last_outcome) if last_outcome else None,
            "delivery_backpressure": downstream,
            "store": store_health,
        }
        self._publish(body)

    def heartbeat(
        self,
        *,
        liveness_state: str,
        stats: DispatchStats,
        last_outcome: DispatchOutcome | None = None,
    ) -> bool:
        """Refresh liveness and fail readiness closed on workspace drift."""
        with self._write_lock:
            if self._last_body is None:
                return False
            observed_at = _utc_iso()
            body = dict(self._last_body)
            workspace_runtime = self.workspace_runtime_status()
            capacity_admission = self.capacity_admission_status()
            body["workspace_runtime"] = workspace_runtime
            body["capacity_admission"] = capacity_admission
            if (
                workspace_runtime["required"] is True
                and workspace_runtime["ready"] is not True
            ):
                body["ok"] = False
                body["healthy"] = False
                body["readiness_observed_at"] = observed_at
                body["readiness"] = {
                    "state": "workspace_runtime_unavailable",
                    "healthy": False,
                    "ready_for_dispatch": False,
                    "observed_at": observed_at,
                }
            elif (
                capacity_admission["required"] is True
                and capacity_admission["ready"] is not True
            ):
                body["ok"] = False
                body["healthy"] = False
                body["readiness_observed_at"] = observed_at
                body["readiness"] = {
                    "state": "capacity_authorization_unavailable",
                    "healthy": False,
                    "ready_for_dispatch": False,
                    "observed_at": observed_at,
                }
            body["heartbeat_at"] = observed_at
            body["liveness"] = {
                "state": str(liveness_state or "unknown"),
                "heartbeat_at": observed_at,
                "readiness_observed_at": body.get("readiness_observed_at"),
            }
            body["stats"] = asdict(stats)
            if last_outcome is not None:
                body["last_outcome"] = asdict(last_outcome)
            self._publish_locked(body)
            return True

    def _publish(self, body: Mapping[str, Any]) -> None:
        with self._write_lock:
            self._publish_locked(body)

    def _publish_locked(self, body: Mapping[str, Any]) -> None:
        serialized = dict(body)
        path = self.config.health_path
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
        temporary.write_text(
            json.dumps(serialized, ensure_ascii=False, indent=2, sort_keys=True)
            + "\n",
            encoding="utf-8",
        )
        os.chmod(temporary, 0o600)
        os.replace(temporary, path)
        self._last_body = serialized


def _health_runtime_identity_ok(payload: Mapping[str, Any]) -> bool:
    config = payload.get("config")
    return isinstance(config, Mapping) and runtime_identity_is_valid(
        payload.get("runtime_identity"),
        service_label=SERVICE_LABEL,
        public_config=config,
    )


def _health_workspace_runtime_ok(payload: Mapping[str, Any]) -> bool:
    status = payload.get("workspace_runtime")
    if not isinstance(status, Mapping):
        return False
    required = payload.get("enabled") is True or payload.get("activation_required") is True
    if status.get("required") is not required:
        return False
    if not required:
        return status.get("ready") in {True, False}
    identity = status.get("identity")
    if (
        status.get("bound") is not True
        or status.get("ready") is not True
        or status.get("state") != "ready"
        or not isinstance(identity, Mapping)
        or identity.get("schema_version")
        != WORKSPACE_RUNTIME_IDENTITY_SCHEMA_VERSION
        or re.fullmatch(r"[0-9a-f]{64}", str(identity.get("manifest_sha256") or ""))
        is None
        or re.fullmatch(r"[0-9a-f]{64}", str(identity.get("closure_sha256") or ""))
        is None
        or re.fullmatch(r"[0-9a-f]{40}", str(identity.get("source_commit") or ""))
        is None
    ):
        return False
    file_sha256 = identity.get("file_sha256")
    return isinstance(file_sha256, Mapping) and set(file_sha256) == set(
        WORKSPACE_RUNTIME_FILES
    ) and all(
        re.fullmatch(r"[0-9a-f]{64}", str(file_sha256[path] or "")) is not None
        for path in WORKSPACE_RUNTIME_FILES
    )


def _health_capacity_admission_ok(payload: Mapping[str, Any]) -> bool:
    status = payload.get("capacity_admission")
    config = payload.get("config")
    if not isinstance(status, Mapping) or not isinstance(config, Mapping):
        return False
    runtime_status = status.get("runtime")
    if isinstance(runtime_status, Mapping):
        configured = runtime_status.get("configured") is True
        mode = str(runtime_status.get("effective_mode") or "")
        state = str(runtime_status.get("effective_state") or "")
        lock = runtime_status.get("lock")
        if (
            runtime_status.get("schema_version")
            != "pnc_rca_capacity_runtime_decision_v1"
            or mode not in {"bootstrap", "steady"}
            or status.get("capacity_mode") != mode
            or status.get("ready") is not True
            or not isinstance(lock, Mapping)
            or (configured and lock.get("held") is not True)
            or lock.get("error_code") not in {"", None}
            or isinstance(runtime_status.get("generation"), bool)
            or not isinstance(runtime_status.get("generation"), int)
        ):
            return False
        required = bool(
            configured
            and (
                payload.get("enabled") is True
                or payload.get("activation_required") is True
            )
        )
        if status.get("required") is not required:
            return False
        if not configured:
            return bool(
                runtime_status.get("legacy_compatibility") is True
                and mode == "steady"
                and state == "STEADY_ACTIVE"
                and status.get("authorization") is None
            )
        ledger = runtime_status.get("ledger")
        if (
            not isinstance(ledger, Mapping)
            or re.fullmatch(r"[0-9a-f]{64}", str(ledger.get("sha256") or ""))
            is None
            or isinstance(ledger.get("sample_count"), bool)
            or not isinstance(ledger.get("sample_count"), int)
            or runtime_status.get("current_release_id") != config.get("release_id")
            or runtime_status.get("current_bootstrap_epoch_id")
            != config.get("bootstrap_epoch_id")
            or re.fullmatch(
                r"[0-9a-f]{64}",
                str(runtime_status.get("active_release_binding_sha256") or ""),
            )
            is None
        ):
            return False
        if mode == "steady":
            artifacts = runtime_status.get("artifacts")
            return bool(
                state == "STEADY_ACTIVE"
                and runtime_status.get("irreversible") is True
                and runtime_status.get("generation") >= 2
                and status.get("authorization") is None
                and isinstance(artifacts, Mapping)
                and all(
                    re.fullmatch(r"[0-9a-f]{64}", str(value or ""))
                    is not None
                    for value in artifacts.values()
                )
                and bool(runtime_status.get("ratchet_origin_release_id"))
                and bool(runtime_status.get("ratchet_origin_bootstrap_epoch_id"))
            )
        authorization = status.get("authorization")
        return bool(
            state in {"BOOTSTRAP_PRODUCTION", "STEADY_READY"}
            and runtime_status.get("generation") == 1
            and runtime_status.get("irreversible") is False
            and isinstance(authorization, Mapping)
            and authorization.get("bootstrap_epoch_id")
            == config.get("bootstrap_epoch_id")
            and authorization.get("release_approval_id") == config.get("release_id")
            and hmac.compare_digest(
                str(runtime_status.get("active_release_binding_sha256") or ""),
                str(authorization.get("active_release_binding_sha256") or ""),
            )
            and all(
                re.fullmatch(r"[0-9a-f]{64}", str(authorization.get(field) or ""))
                is not None
                for field in (
                    "receipt_fingerprint",
                    "authorization_receipt_sha256",
                    "active_release_binding_sha256",
                    "candidate_env_sha256",
                    "release_bom_sha256",
                    "approval_evidence_sha256",
                )
            )
        )
    mode = str(config.get("capacity_mode") or "")
    if mode not in {"steady", "bootstrap"} or status.get("capacity_mode") != mode:
        return False
    required = bool(
        mode == "bootstrap"
        and (
            payload.get("enabled") is True
            or payload.get("activation_required") is True
        )
    )
    if status.get("required") is not required:
        return False
    if mode == "steady":
        return (
            status.get("ready") is True
            and status.get("state") == "steady"
            and status.get("authorization") is None
        )
    if not required:
        return status.get("ready") in {True, False}
    authorization = status.get("authorization")
    return bool(
        status.get("ready") is True
        and status.get("state") == "ready"
        and isinstance(authorization, Mapping)
        and authorization.get("bootstrap_epoch_id")
        == config.get("bootstrap_epoch_id")
        and authorization.get("release_approval_id") == config.get("release_id")
        and all(
            re.fullmatch(r"[0-9a-f]{64}", str(authorization.get(field) or ""))
            is not None
            for field in (
                "receipt_fingerprint",
                "authorization_receipt_sha256",
                "active_release_binding_sha256",
                "candidate_env_sha256",
                "release_bom_sha256",
                "approval_evidence_sha256",
            )
        )
        and bool(str(authorization.get("release_approval_id") or "").strip())
    )


def read_health_status(
    config: DispatcherConfig,
    *,
    max_age_seconds: int = 60,
    now: datetime | None = None,
) -> dict[str, Any]:
    try:
        payload = json.loads(config.health_path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {
            "ok": False,
            "error": "health_unavailable",
            "message": str(exc),
            "health_path": str(config.health_path),
        }
    if not isinstance(payload, dict):
        return {
            "ok": False,
            "error": "health_payload_invalid",
            "health_path": str(config.health_path),
        }
    try:
        heartbeat = datetime.fromisoformat(
            str(payload["heartbeat_at"]).replace("Z", "+00:00")
        )
        if heartbeat.tzinfo is None or heartbeat.utcoffset() is None:
            raise ValueError("heartbeat timestamp must be timezone-aware")
        observed_at = now or _utc_now()
        if observed_at.tzinfo is None or observed_at.utcoffset() is None:
            raise ValueError("health observation timestamp must be timezone-aware")
        age = (
            observed_at.astimezone(timezone.utc) - heartbeat.astimezone(timezone.utc)
        ).total_seconds()
    except (KeyError, TypeError, ValueError) as exc:
        return {
            "ok": False,
            "error": "health_timestamp_invalid",
            "message": str(exc),
            "health_path": str(config.health_path),
        }
    fresh = -MAX_HEALTH_FUTURE_SKEW_SECONDS <= age <= max_age_seconds
    enabled = payload.get("enabled")
    state = str(payload.get("state") or "")
    mode_ok = (enabled is True and state != "disabled") or (
        enabled is False and state == "disabled"
    )
    store = payload.get("store")
    downstream = payload.get("delivery_backpressure")
    producer_ok = (
        payload.get("schema_version") == DISPATCHER_HEALTH_SCHEMA_VERSION
        and payload.get("ok") is True
        and payload.get("healthy") is True
        and mode_ok
        and isinstance(store, Mapping)
        and store.get("ok") is True
        and isinstance(downstream, Mapping)
        and _health_runtime_identity_ok(payload)
        and _health_workspace_runtime_ok(payload)
        and _health_capacity_admission_ok(payload)
    )
    result = dict(payload)
    result["ok"] = bool(fresh and producer_ok)
    result["health_check"] = {
        "fresh": fresh,
        "heartbeat_age_seconds": age,
        "max_age_seconds": max_age_seconds,
        "checked_at": _utc_iso(now),
    }
    if not producer_ok:
        result["health_check"]["reason"] = "dispatcher_reported_unhealthy"
    elif age < -MAX_HEALTH_FUTURE_SKEW_SECONDS:
        result["health_check"]["reason"] = "heartbeat_from_future"
    elif not fresh:
        result["health_check"]["reason"] = "heartbeat_stale"
    return result


def load_dispatcher_environment(env_file: str | Path | None = None) -> Path:
    path = Path(
        env_file
        or os.environ.get(f"{ENV_PREFIX}ENV_FILE")
        or get_hermes_home() / ".env"
    ).expanduser()
    load_dotenv(path, override=False, interpolate=False)
    return path


def run_dispatch_loop(
    dispatcher: OutboxDispatcher,
    health: HealthReporter,
    *,
    stop_requested: Callable[[], bool],
    sleep: Callable[[float], None] = time.sleep,
    heartbeat_interval_seconds: float = HEALTH_HEARTBEAT_INTERVAL_SECONDS,
) -> None:
    if (
        isinstance(heartbeat_interval_seconds, bool)
        or not 0 < float(heartbeat_interval_seconds) < float("inf")
    ):
        raise ValueError("heartbeat_interval_seconds must be a positive number")

    heartbeat_stop = threading.Event()
    processing = threading.Event()
    latest_outcome: list[DispatchOutcome | None] = [None]
    health.write(state="starting", stats=dispatcher.stats)

    def publish_heartbeat() -> None:
        while not heartbeat_stop.wait(float(heartbeat_interval_seconds)):
            try:
                health.heartbeat(
                    liveness_state=(
                        "processing" if processing.is_set() else "waiting"
                    ),
                    stats=dispatcher.stats,
                    last_outcome=latest_outcome[0],
                )
            except Exception:
                logger.exception("failed to publish RCA outbox liveness heartbeat")

    heartbeat_thread = threading.Thread(
        target=publish_heartbeat,
        name="pnc-rca-outbox-heartbeat",
        daemon=True,
    )
    heartbeat_thread.start()
    try:
        while not stop_requested():
            workspace_guard = health.dispatch_guard_outcome()
            if workspace_guard is not None:
                latest_outcome[0] = workspace_guard
                health.write(
                    state=workspace_guard.status,
                    stats=dispatcher.stats,
                    last_outcome=workspace_guard,
                )
                sleep(dispatcher.config.circuit_poll_interval_seconds)
                continue
            processing.set()
            try:
                outcomes = dispatcher.dispatch_batch()
            finally:
                processing.clear()
            last = outcomes[-1]
            latest_outcome[0] = last
            health.write(
                state=last.status,
                stats=dispatcher.stats,
                last_outcome=last,
            )
            if last.status in {
                "circuit_open",
                "downstream_backpressure",
                "downstream_error",
            }:
                sleep(dispatcher.config.circuit_poll_interval_seconds)
            elif last.status in {"idle", "disabled", "lease_lost"}:
                sleep(dispatcher.config.poll_interval_seconds)
    finally:
        heartbeat_stop.set()
        heartbeat_thread.join(timeout=float(heartbeat_interval_seconds) + 1.0)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", help="dotenv path; defaults to HERMES_HOME/.env")
    parser.add_argument("--check-config", action="store_true")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="list due rows without claiming, enriching, or submitting",
    )
    parser.add_argument("--health", action="store_true")
    parser.add_argument("--health-max-age-seconds", type=int, default=60)
    parser.add_argument("--once", action="store_true")
    parser.add_argument(
        "--promote-shadow-event",
        metavar="EVENT_UID",
        help="atomically promote one exact accepted shadow event and exit",
    )
    parser.add_argument("--operator", help="audited shadow-promotion operator")
    parser.add_argument("--reason", help="audited shadow-promotion reason")
    parser.add_argument(
        "--activation-epoch-id",
        help="exact current activation epoch required for governed promotion",
    )
    parser.add_argument(
        "--clear-circuit",
        action="store_true",
        help="explicitly close the persisted circuit and exit",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    try:
        load_dispatcher_environment(args.env_file)
        config = DispatcherConfig.from_env()
        if args.check_config:
            print(json.dumps({"ok": True, "config": config.public_dict()}, indent=2))
            return 0
        if args.health:
            result = read_health_status(
                config, max_age_seconds=args.health_max_age_seconds
            )
            print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
            return 0 if result.get("ok") is True else 2

        store = RcaControlStore(config.control_db_path, require_current=True)
        capacity_runtime = CapacityRuntimeResolver.from_environment(
            store=store,
            control_db_path=config.control_db_path,
            release_id=(config.release_id if config.activation_required else ""),
            bootstrap_epoch_id=(
                config.bootstrap_epoch_id if config.activation_required else ""
            ),
            initial_policy=(config.capacity_mode if config.activation_required else "steady"),
        )
        if args.promote_shadow_event:
            expected_epoch_id = str(args.activation_epoch_id or "").strip()
            activation_slot_kind = ""
            if config.activation_required:
                if not expected_epoch_id:
                    raise ValueError(
                        "--activation-epoch-id is required when activation is required"
                    )
                epoch = store.activation_epoch()
                if epoch is None or str(epoch["epoch_id"]) != expected_epoch_id:
                    raise ValueError("--activation-epoch-id is not current")
                if str(epoch["state"]) == "bounded_active":
                    activation_slot_kind = "kafka_success"
            promotion = store.promote_shadow_event(
                args.promote_shadow_event,
                operator=args.operator or "",
                reason=args.reason or "",
                expected_activation_epoch_id=expected_epoch_id,
                activation_required=config.activation_required,
                activation_slot_kind=activation_slot_kind,
            )
            print(
                json.dumps(
                    {"ok": True, "promotion": asdict(promotion)},
                    ensure_ascii=False,
                    indent=2,
                    sort_keys=True,
                )
            )
            return 0
        if args.clear_circuit:
            circuit = store.close_dispatcher_circuit()
            print(json.dumps({"ok": True, "circuit": asdict(circuit)}, indent=2))
            return 0
        if args.dry_run:
            rows = store.preview_dispatchable(
                limit=config.batch_size,
                activation_required=config.activation_required,
            )
            print(
                json.dumps(
                    {
                        "ok": True,
                        "dry_run": True,
                        "dispatch_enabled": config.dispatch_enabled,
                        "activation_required": config.activation_required,
                        "circuit": asdict(store.dispatcher_circuit()),
                        "due_count_in_sample": len(rows),
                        "rows": rows,
                    },
                    ensure_ascii=False,
                    indent=2,
                    sort_keys=True,
                )
            )
            return 0

        dispatcher = OutboxDispatcher(
            store=store,
            config=config,
            enrich=default_enrich_event,
            storage_admission=default_storage_admission,
            derived_capacity_reservation=reserve_derived_capacity,
            derived_capacity_abort_precreate=abort_precreate_derived_capacity,
            submit=lambda admission, execution_request: default_submit(
                admission,
                execution_request,
                config=config,
                capacity_runtime=capacity_runtime,
            ),
        )
        health = HealthReporter(
            config,
            store,
            delivery_backpressure_status=dispatcher.delivery_backpressure_health,
            capacity_runtime=capacity_runtime,
        )
        dispatcher.runtime_identity = health.runtime_identity.to_dict()
        dispatcher.workspace_runtime_guard = health.dispatch_guard_outcome
        workspace_guard = health.dispatch_guard_outcome()
        if workspace_guard is not None:
            health.write(
                state=workspace_guard.status,
                stats=dispatcher.stats,
                last_outcome=workspace_guard,
            )
            print(json.dumps(asdict(workspace_guard), indent=2))
            return 2
        if not config.dispatch_enabled:
            outcome = dispatcher.dispatch_one()
            health.write(state="disabled", stats=dispatcher.stats, last_outcome=outcome)
            print(json.dumps(asdict(outcome), indent=2))
            return 0
        if args.once:
            outcomes = dispatcher.dispatch_batch()
            last = outcomes[-1]
            health.write(state=last.status, stats=dispatcher.stats, last_outcome=last)
            print(json.dumps([asdict(item) for item in outcomes], indent=2))
            return (
                2
                if last.status
                in {
                    "circuit_open",
                    "downstream_backpressure",
                    "downstream_error",
                }
                else 0
            )

        stopping = False

        def request_stop(_signum: int, _frame: Any) -> None:
            nonlocal stopping
            stopping = True

        signal.signal(signal.SIGTERM, request_stop)
        signal.signal(signal.SIGINT, request_stop)
        run_dispatch_loop(
            dispatcher,
            health,
            stop_requested=lambda: stopping,
        )
        return 0
    except Exception as exc:
        print(
            json.dumps(
                {"ok": False, "error": type(exc).__name__, "message": str(exc)},
                ensure_ascii=False,
            ),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
