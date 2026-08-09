#!/usr/bin/env python3
"""Deliver durable RCA effects to exact Feishu issue, thread, or card targets."""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import socket
import sqlite3
import stat
import sys
import threading
import time
from typing import Any, Callable, Mapping
from urllib import error as urllib_error
from urllib.parse import parse_qs, urlsplit
from urllib import request as urllib_request

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from dotenv import load_dotenv

from gateway.pnc_rca_abstention_projection import (
    RcaEvidenceProjectionError,
    build_gate_a_identifier_binding,
    build_gate_a_public_result,
    validate_gate_a_projection,
)
from gateway.pnc_rca_delivery_contract import (
    DELIVERY_CARD_PATCH_EFFECT_KIND,
    DELIVERY_EFFECT_KIND,
    DELIVERY_EFFECT_KINDS,
    DELIVERY_EFFECT_SCHEMA_VERSION,
    DELIVERY_EFFECT_SCHEMA_VERSION_V3,
    DELIVERY_REPORT_LINK_KIND,
    DELIVERY_THREAD_EFFECT_KIND,
    TERMINAL_DELIVERY_EFFECT_SCHEMA_VERSION,
    TERMINAL_DELIVERY_EFFECT_SCHEMA_VERSION_V1,
    TERMINAL_DELIVERY_EFFECT_SCHEMA_VERSION_COMMENT_ONLY,
    TERMINAL_FALLBACK_DELIVERY_EFFECT_SCHEMA_VERSION,
    TERMINAL_FALLBACK_DELIVERY_EFFECT_SCHEMA_VERSION_COMMENT_ONLY,
    TERMINAL_DELIVERY_OUTCOMES,
    DeliveryContractError,
    MAX_DELIVERY_ARTIFACT_BYTES,
    MAX_DELIVERY_INDEX_HTML_BYTES,
    RCA_REPORT_FIELD_KEY,
    RCA_RESULT_FIELD_KEY,
    build_issue_comment_content,
    build_thread_reply_content,
    build_terminal_delivery,
    build_terminal_thread_reply_effect,
    canonical_issue_url,
    compute_delivery_effect_key,
    compute_delivery_effect_payload_sha256,
    delivery_effect_idempotency_uuid,
    delivery_effect_marker,
    render_public_rca_result_field,
    render_public_rca_result,
    delivery_oracle_contract,
    issue_focus_payload_sha256,
    validate_delivery_issue_focus,
    validate_card_patch_effect_payload,
    validate_report_asset_url,
    validate_report_url,
    validate_delivery_subscription_target,
    verify_persisted_artifact_inventory,
)
from gateway.pnc_rca_quality_oracle import (
    CANDIDATE_HYPOTHESIS,
    HONEST_NON_ATTRIBUTION,
    TierOracleConflict,
    evaluate_structural_tier,
    require_publishable,
)
from gateway.pnc_rca_delivery_quarantine_baseline import (
    disabled_quarantine_baseline_status,
    quarantine_baseline_settings,
    read_quarantine_baseline_status,
)
from gateway.pnc_rca_control_store import (
    RecordConflictError,
    RcaControlStore,
    _dispatcher_circuit_reset_fingerprint,
)
from gateway.pnc_rca_provider_fence import (
    RcaProviderWriteClaim,
    bound_provider_write_claim,
    build_manual_provider_write_claim,
    build_historical_epoch_provider_claim,
    build_profile_terminal_provider_claim,
    build_terminal_rerun_provider_claim,
    build_write_fence_provider_claim,
    current_provider_write_claim,
    revalidate_provider_write_claim,
)
from gateway.pnc_rca_delivery_store import (
    DELIVERY_CIRCUIT_RESET_META_PREFIX,
    DELIVERY_CIRCUIT_RESET_SCHEMA_VERSION,
    OUTBOX_PROFILE_TERMINAL_WRITE_ERROR_CODES,
    PRE_W3_EFFECT_DISPOSITION_COMMAND,
    PRE_W3_EFFECT_DISPOSITION_META_PREFIX,
    PRE_W3_EFFECT_DISPOSITION_SCHEMA_VERSION,
    DeliveryEffectClaim,
    DeliveryRecordConflictError,
    RcaDeliveryStore,
    StaleDeliveryEffectLeaseError,
    _pre_w3_disposition_sha256,
    _pre_w3_effect_disposition_after,
    _pre_w3_effect_disposition_fingerprint,
)
from gateway.pnc_rca_prod_bootstrap import load_active_release_binding
from gateway.pnc_rca_delivery_observability import (
    DeliveryObservationError,
    append_delivery_observation_verified,
    build_delivery_observation,
    default_observation_path,
    delivery_observation_file_lock,
    delivery_observation_id,
    delivery_observation_payload_sha256,
    ensure_delivery_observation_path,
    read_delivery_observation_receipt,
)
from gateway.pnc_rca_write_fence import (
    ExternalWriteFenceError,
    RESIDENT_EXTERNAL_WRITE_STATES,
    require_resident_activation_epoch,
    validate_write_fence,
)
from gateway.pnc_rca_conclusion_adjudication import (
    ConclusionAdjudicationError,
    identifies_adjudication_effect,
    validate_adjudication_effect_claim,
)
from gateway.pnc_rca_runtime_identity import (
    MAX_HEALTH_FUTURE_SKEW_SECONDS,
    RCA_DELIVERY_DISPATCHER_LOADED_DEPENDENCIES,
    build_runtime_identity,
    runtime_identity_is_valid,
)
from gateway.pnc_issue_context import (
    G1Q3_ADOPTION_FIELD_KEY,
    G1Q3_ADOPTION_MAX_OPERATION_PAGES,
    G1Q3_ADOPTION_MAX_WINDOW_MS,
    G1Q3_ADOPTION_VALUE_KEYS,
    G1Q3AdoptionReadError,
    normalize_g1q3_adoption_operation_page,
    validate_g1q3_adoption_window,
)
from hermes_constants import get_hermes_home
from scripts.pnc_foxglove_delivery import (
    G1Q3_RCA_FORMAL_VIZ_ROOT,
    canonical_viz_mcap_cifs_path,
    canonical_viz_mcap_path,
    validate_foxglove_url,
)
from scripts.pnc_rca_outbox_dispatcher import (
    _absolute_new_receipt_path as _absolute_new_circuit_reset_receipt_path,
    _create_immutable_file,
    _control_db_identity,
    _open_receipt_parent,
    _receipt_parent_identity,
    _validate_reset_audit_text as _validate_circuit_reset_text,
    _write_immutable_receipt as _write_immutable_circuit_reset_receipt,
)


ENV_PREFIX = "HERMES_RCA_DELIVERY_DISPATCHER_"
HEALTH_SCHEMA_VERSION = "pnc_rca_delivery_dispatcher_health_v2"
SERVICE_LABEL = "local.pnc.rca-delivery-dispatcher"
MAX_EFFECT_AGE_SECONDS = 86_400
MEEGLE_COMMENT_PAGE_TIMEOUT_SECONDS = 12
MAX_MEEGLE_COMMENT_PAGES = 5
MAX_MEEGLE_COMMENTS = 500
MAX_MEEGLE_OPERATION_PAGES = G1Q3_ADOPTION_MAX_OPERATION_PAGES
MAX_MEEGLE_OPERATION_WINDOW_MS = G1Q3_ADOPTION_MAX_WINDOW_MS
MAX_MEEGLE_OPERATION_WINDOWS = 64
MAX_EXTERNAL_BOUNDARY_TIMEOUT_SECONDS = MEEGLE_COMMENT_PAGE_TIMEOUT_SECONDS * (
    MAX_MEEGLE_COMMENT_PAGES + 1
)
LEASE_BOUNDARY_MARGIN_SECONDS = 15
EFFECT_LEASE_RENEW_INTERVAL_SECONDS = 10
MAX_EFFECT_LEASE_RENEW_INTERVAL_SECONDS = 15
MAX_HEALTH_HEARTBEAT_INTERVAL_SECONDS = 15.0
HTTP_VERIFY_READ_CHUNK_BYTES = 64 * 1024
RETRY_DELAYS_SECONDS = (2, 5, 10, 20, 40, 120, 300, 900, 3600)
UNCERTAIN_RECONCILIATION_POLL_SECONDS = 30
MAX_RECOVERY_WRITES = 2
_REMOTE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$")
_FEISHU_OPEN_ID_RE = re.compile(r"^ou_[A-Za-z0-9_-]{8,255}$")
_FEISHU_RENDERED_MENTION_RE = re.compile(r"@_user_[1-9][0-9]*(?![A-Za-z0-9_])")
_FEISHU_ISSUE_URL_RE = re.compile(
    r"^https://project\.feishu\.cn/([A-Za-z0-9._-]+)/issue/detail/([0-9]+)$"
)
_PROJECT_SIMPLE_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_VIZ_REPORT_STATUSES = frozenset({"report_ready"})
_CIRCUIT_CODES = frozenset({
    "feishu_auth_failed",
    "feishu_card_dependency_unavailable",
    "feishu_permission_denied",
    "meegle_dependency_unavailable",
    "feishu_thread_dependency_unavailable",
    "meegle_response_invalid",
    "delivery_boundary_contract_invalid",
    "report_http_auth_or_permission",
})
DELIVERY_CIRCUIT_RESET_RECOVERY_SCHEMA_VERSION = (
    "pnc_rca_delivery_circuit_reset_recovery_v1"
)
PRE_W3_EFFECT_DISPOSITION_RECOVERY_SCHEMA_VERSION = (
    "pnc_rca_pre_w3_effect_disposition_recovery_v1"
)
class DeliveryCircuitResetReceiptMaterializationError(RuntimeError):
    def __init__(self, *, reset_id: str, receipt_path: Path, cause: Exception):
        self.reset_id = str(reset_id)
        self.receipt_path = receipt_path
        self.meta_key = f"{DELIVERY_CIRCUIT_RESET_META_PREFIX}{self.reset_id}"
        self.cause = cause
        super().__init__(
            "delivery_circuit_reset_recovery_required:"
            f"reset_id={self.reset_id}:receipt={receipt_path}:cause={cause}"
        )


class PreW3EffectDispositionReceiptMaterializationError(RuntimeError):
    def __init__(self, *, disposition_id: str, receipt_path: Path, cause: Exception):
        self.disposition_id = str(disposition_id)
        self.receipt_path = receipt_path
        self.meta_key = (
            f"{PRE_W3_EFFECT_DISPOSITION_META_PREFIX}{self.disposition_id}"
        )
        self.cause = cause
        super().__init__(
            "pre_w3_effect_disposition_recovery_required:"
            f"disposition_id={self.disposition_id}:receipt={receipt_path}:cause={cause}"
        )


def _card_patch_exception_result(exc: Exception) -> dict[str, Any]:
    detail = f"{type(exc).__name__}: {exc}"
    lowered = detail.lower()
    if any(
        marker in lowered
        for marker in (
            "dependencies not installed",
            "is not configured",
            "modulenotfounderror",
            "importerror",
        )
    ):
        return {
            "success": False,
            "outcome_uncertain": False,
            "error_code": "feishu_card_dependency_unavailable",
            "error": detail,
        }
    if any(marker in lowered for marker in ("permission", "forbidden", "230006")):
        return {
            "success": False,
            "outcome_uncertain": False,
            "error_code": "feishu_permission_denied",
            "error": detail,
        }
    if any(
        marker in lowered
        for marker in (
            "access_token",
            "tenant_access_token",
            "unauthorized",
            "999916",
            "99991663",
        )
    ):
        return {
            "success": False,
            "outcome_uncertain": False,
            "error_code": "feishu_auth_failed",
            "error": detail,
        }
    return {
        "success": False,
        "outcome_uncertain": True,
        "error_code": "feishu_card_patch_outcome_unknown",
        "error": detail,
    }


ListComments = Callable[[str, str], Mapping[str, Any]]
AddComment = Callable[[str, str, str], Mapping[str, Any]]
GetFields = Callable[[str, str, tuple[str, ...]], Mapping[str, Any]]
UpdateFields = Callable[[str, str, tuple[tuple[str, str], ...]], Mapping[str, Any]]
ListThreadReplies = Callable[[str, str], Mapping[str, Any]]
AddThreadReply = Callable[[str, str, str, str], Mapping[str, Any]]
PatchTaskCard = Callable[..., Mapping[str, Any]]
ReportVerifier = Callable[[str, int, str], Mapping[str, Any]]
ProviderWriteClaim = RcaProviderWriteClaim
_bound_provider_write_guard = bound_provider_write_claim


def _require_provider_write_guard(
    operation: str,
    target: str,
    *,
    chat_id: str = "",
) -> dict[str, Any]:
    claim = current_provider_write_claim()
    project_key = ""
    work_item_id = ""
    thread_id = ""
    if operation in {"feishu_issue_comment", "feishu_issue_field_update"}:
        parts = str(target or "").split(":")
        if len(parts) != 3 or parts[0] != "feishu_project":
            raise ExternalWriteFenceError("external_write_fence_target_mismatch")
        project_key, work_item_id = parts[1:]
    elif operation == "feishu_thread_reply":
        thread_id = str(target or "").strip()
    return revalidate_provider_write_claim(
        claim,
        operation=operation,
        chat_id=str(chat_id or "").strip(),
        thread_id=thread_id,
        issue_project_key=project_key,
        issue_work_item_id=work_item_id,
    )


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_iso(value: datetime | None = None) -> str:
    current = value or _utc_now()
    if current.tzinfo is None or current.utcoffset() is None:
        raise ValueError("datetime values must be timezone-aware")
    return current.astimezone(timezone.utc).isoformat()


def _heartbeat_interval_seconds(max_age_seconds: int) -> float:
    return max(
        1.0,
        min(MAX_HEALTH_HEARTBEAT_INTERVAL_SECONDS, max_age_seconds / 3),
    )


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
            raise RuntimeError("delivery_dispatcher_heartbeat_stop_timeout")
        if exc_type is None and self._error is not None:
            raise RuntimeError("delivery_dispatcher_heartbeat_failed") from self._error


class _EffectLeaseKeeper:
    """Renew one fenced claim and serialize its final local mutation."""

    def __init__(
        self,
        renew: Callable[[], None],
        *,
        interval_seconds: float,
        thread_name: str,
        on_background_renewal: Callable[[], None],
        on_failure: Callable[[], None],
    ):
        interval = float(interval_seconds)
        if interval <= 0 or interval > MAX_EFFECT_LEASE_RENEW_INTERVAL_SECONDS:
            raise ValueError(
                "effect lease renewal interval must be greater than zero and at most "
                f"{MAX_EFFECT_LEASE_RENEW_INTERVAL_SECONDS} seconds"
            )
        self._renew = renew
        self._interval_seconds = interval
        self._on_background_renewal = on_background_renewal
        self._on_failure = on_failure
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._failure: BaseException | None = None
        self._failure_reported = False
        self._started = False
        self._thread = threading.Thread(
            target=self._run,
            name=thread_name,
            daemon=True,
        )

    @property
    def thread_name(self) -> str:
        return self._thread.name

    @property
    def thread_alive(self) -> bool:
        return self._thread.is_alive()

    def _remember_failure_locked(self, exc: BaseException) -> None:
        if self._failure is None:
            self._failure = exc
        if not self._failure_reported:
            self._failure_reported = True
            self._on_failure()
        self._stop.set()

    def _raise_if_failed_locked(self) -> None:
        if self._failure is not None:
            raise StaleDeliveryEffectLeaseError(
                "delivery effect lease keeper lost its fenced claim"
            ) from self._failure

    def _renew_locked(self, *, background: bool) -> None:
        self._raise_if_failed_locked()
        try:
            self._renew()
            if background:
                self._on_background_renewal()
        except BaseException as exc:
            self._remember_failure_locked(exc)
            self._raise_if_failed_locked()

    def _run(self) -> None:
        while not self._stop.wait(self._interval_seconds):
            with self._lock:
                if self._stop.is_set():
                    return
                try:
                    self._renew_locked(background=True)
                except StaleDeliveryEffectLeaseError:
                    return

    def start(self) -> None:
        if self._started:
            raise RuntimeError("delivery_effect_lease_keeper_already_started")
        self._started = True
        self._thread.start()

    def renew_now(self) -> None:
        with self._lock:
            self._renew_locked(background=False)

    def checkpoint(self) -> None:
        with self._lock:
            self._raise_if_failed_locked()

    def settle(self, mutation: Callable[[], Any]) -> Any:
        """Fence, stop future renewals, then release the claim under one lock."""
        with self._lock:
            self._renew_locked(background=False)
            self._stop.set()
            return mutation()

    def settle_without_renewal(self, mutation: Callable[[], Any]) -> Any:
        """Stop renewals and let the fenced mutation validate in its transaction."""
        with self._lock:
            self._raise_if_failed_locked()
            self._stop.set()
            return mutation()

    def stop(self) -> None:
        self._stop.set()
        if not self._started:
            return
        self._thread.join(timeout=max(1.0, self._interval_seconds + 6.0))
        if self._thread.is_alive():
            raise RuntimeError("delivery_effect_lease_keeper_stop_timeout")


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _circuit_reset_canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _circuit_reset_sha256(value: Any) -> str:
    return hashlib.sha256(
        _circuit_reset_canonical_json(value).encode("utf-8")
    ).hexdigest()


def _circuit_reset_config_binding(config: "DispatcherConfig") -> str:
    payload = config.public_dict()
    payload["inventory_pin"] = config.inventory_pin
    payload["observation_release_id"] = config.observation_release_id
    return _circuit_reset_sha256(payload)


def _circuit_reset_destination_binding(path: Path) -> dict[str, Any]:
    absolute = str(path.expanduser().absolute())
    parent = _receipt_parent_identity(path)
    return {
        "path_sha256": hashlib.sha256(absolute.encode("utf-8")).hexdigest(),
        "parent_device": int(parent["device"]),
        "parent_inode": int(parent["inode"]),
    }


def _bound_source_sha256(path: Path) -> str:
    lexical = path.expanduser().absolute()
    lexical_stat = lexical.lstat()
    if stat.S_ISLNK(lexical_stat.st_mode):
        raise ValueError("delivery_circuit_reset_tool_provenance_invalid")
    resolved = lexical.resolve(strict=True)
    if resolved != lexical:
        raise ValueError("delivery_circuit_reset_tool_provenance_invalid")
    descriptor = os.open(
        resolved,
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0),
    )
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
            or before.st_uid != os.getuid()
            or stat.S_IMODE(before.st_mode) & 0o022
            or before.st_dev != lexical_stat.st_dev
            or before.st_ino != lexical_stat.st_ino
        ):
            raise ValueError("delivery_circuit_reset_tool_provenance_invalid")
        digest = hashlib.sha256()
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
        after = os.fstat(descriptor)
        if (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
        ) != (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
        ):
            raise ValueError("delivery_circuit_reset_tool_provenance_changed")
        return digest.hexdigest()
    finally:
        os.close(descriptor)


def _circuit_reset_tool_provenance() -> dict[str, Any]:
    entrypoint = Path(__file__).resolve(strict=True)
    store_path = (REPO_ROOT / "gateway" / "pnc_rca_delivery_store.py").resolve(
        strict=True
    )
    helper_path = (REPO_ROOT / "scripts" / "pnc_rca_outbox_dispatcher.py").resolve(
        strict=True
    )
    control_path = (REPO_ROOT / "gateway" / "pnc_rca_control_store.py").resolve(
        strict=True
    )
    bootstrap_path = (REPO_ROOT / "gateway" / "pnc_rca_prod_bootstrap.py").resolve(
        strict=True
    )
    return {
        "entrypoint_path": str(entrypoint),
        "entrypoint_sha256": _bound_source_sha256(entrypoint),
        "delivery_store_path": str(store_path),
        "delivery_store_sha256": _bound_source_sha256(store_path),
        "receipt_helper_path": str(helper_path),
        "receipt_helper_sha256": _bound_source_sha256(helper_path),
        "control_store_path": str(control_path),
        "control_store_sha256": _bound_source_sha256(control_path),
        "bootstrap_path": str(bootstrap_path),
        "bootstrap_sha256": _bound_source_sha256(bootstrap_path),
    }


def _active_release_binding_snapshot(config: "DispatcherConfig") -> dict[str, Any]:
    if not config.quarantine_release_id or not config.quarantine_bootstrap_epoch_id:
        raise ValueError("delivery_circuit_reset_active_binding_config_missing")
    binding = load_active_release_binding(
        path=config.quarantine_active_release_binding_path,
        live_env_path=config.quarantine_live_env_path,
        expected_release_id=config.quarantine_release_id,
        expected_epoch_id=config.quarantine_bootstrap_epoch_id,
    )
    return {
        "path": str(config.quarantine_active_release_binding_path.absolute()),
        "sha256": binding["binding_receipt_sha256"],
        "release_id": binding["release_id"],
        "authority_sha256": binding["authority_sha256"],
        "authority_epoch_id": binding["authority_epoch_id"],
        "bootstrap_epoch_id": binding["bootstrap_epoch_id"],
    }


def _build_delivery_circuit_reset_receipt(
    *,
    config: "DispatcherConfig",
    effect_kind: str,
    operator: str,
    reason: str,
    before: Mapping[str, Any],
    recorded_at: str,
    receipt_path: Path | None,
    active_release_binding: Mapping[str, Any],
    tool_provenance: Mapping[str, Any],
) -> dict[str, Any]:
    before_state = json.loads(_circuit_reset_canonical_json(before))
    before_failure = before_state["permanent_failure"]
    last_failure_present = bool(before_failure["last_failure_present"])
    if receipt_path is None:
        raise ValueError("delivery_circuit_reset_receipt_required")
    destination_binding = _circuit_reset_destination_binding(receipt_path)
    after_circuit = {
        "state": "closed",
        "reason_code": "",
        "reason_detail": "",
        "opened_at": None,
        "updated_at": recorded_at,
    }
    after_failure = {
        "threshold": before_failure["threshold"],
        "consecutive_failures": 0,
        "last_failure": {},
        "last_failure_present": False,
    }
    db_identity = _control_db_identity(config.control_db_path)
    config_binding_sha256 = _circuit_reset_config_binding(config)
    plan_id = _circuit_reset_sha256(
        {
            "operator": operator,
            "reason": reason,
            "effect_kind": effect_kind,
            "before": before_state,
            "control_db_identity": db_identity,
            "config_binding_sha256": config_binding_sha256,
            "active_release_binding_sha256": active_release_binding["sha256"],
            "tool_provenance": dict(tool_provenance),
            "destination_binding": destination_binding,
        }
    )
    reset_seed = {
        "recorded_at": recorded_at,
        "operator": operator,
        "reason": reason,
        "effect_kind": effect_kind,
        "before": before_state,
        "control_db_identity": db_identity,
        "active_release_binding_sha256": active_release_binding["sha256"],
        "tool_provenance": dict(tool_provenance),
        "plan_id": plan_id,
    }
    reset_id = _circuit_reset_sha256(reset_seed)
    receipt: dict[str, Any] = {
        "schema_version": DELIVERY_CIRCUIT_RESET_SCHEMA_VERSION,
        "command": "clear-circuit",
        "reset_id": reset_id,
        "plan_id": plan_id,
        "before_state_sha256": _circuit_reset_sha256(before_state["circuit"]),
        "recorded_at": recorded_at,
        "operator": operator,
        "reason": reason,
        "circuit_scope": "delivery",
        "effect_kind": effect_kind,
        "control_db_identity": db_identity,
        "config_binding_sha256": config_binding_sha256,
        "destination_binding": destination_binding,
        "active_release_binding": dict(active_release_binding),
        "tool_provenance": dict(tool_provenance),
        "before": before_state["circuit"],
        "after": after_circuit,
        "pre_state": before_state["circuit"],
        "post_state": after_circuit,
        "permanent_failure_before": before_failure,
        "permanent_failure_after": after_failure,
        "effect_delta": {
            "external_writes": 0,
            "external_effects_triggered": False,
            "delivery_effect_rows": 0,
            "scope": "delivery_dispatcher_circuit_reset_command",
            "database_rows": {
                "circuit_updated": 1,
                "control_meta_inserted": 1,
                "permanent_failure_streak_upserted": 1,
                "permanent_failure_last_deleted": int(last_failure_present),
                "total": 3 + int(last_failure_present),
            },
        },
    }
    receipt["receipt_fingerprint"] = _dispatcher_circuit_reset_fingerprint(receipt)
    return receipt


def _build_delivery_circuit_reset_recovery_envelope(
    audit: Mapping[str, Any],
    receipt_path: Path,
) -> dict[str, Any]:
    destination = _circuit_reset_destination_binding(receipt_path)
    envelope: dict[str, Any] = {
        "schema_version": DELIVERY_CIRCUIT_RESET_RECOVERY_SCHEMA_VERSION,
        "command": "materialize-delivery-circuit-reset",
        "recovered": True,
        "source_reset_id": audit["reset_id"],
        "source_receipt_fingerprint": audit["receipt_fingerprint"],
        "planned_destination_binding": audit["destination_binding"],
        "materialized_destination": {
            "path": str(receipt_path),
            "binding": destination,
        },
        "materialized_at": _utc_now().isoformat(),
        "external_effects_triggered": False,
        "audit": dict(audit),
    }
    envelope["receipt_fingerprint"] = _circuit_reset_sha256(envelope)
    return envelope


def _pre_w3_effect_disposition_active_release_binding(
    config: "DispatcherConfig",
) -> dict[str, Any]:
    """Bind the active release while tolerating only an observed env mismatch.

    This weaker live-env assertion is confined to a no-provider, risk-reducing
    quarantine command.  Circuit reset continues to require exact env equality.
    """
    if not config.quarantine_release_id or not config.quarantine_bootstrap_epoch_id:
        raise ValueError("pre_w3_effect_disposition_active_binding_config_missing")
    binding = load_active_release_binding(
        path=config.quarantine_active_release_binding_path,
        live_env_path=config.quarantine_live_env_path,
        expected_release_id=config.quarantine_release_id,
        expected_epoch_id=config.quarantine_bootstrap_epoch_id,
        verify_live_env=False,
    )
    live_env_path = config.quarantine_live_env_path.expanduser().absolute()
    live_env_sha256 = _bound_source_sha256(live_env_path)
    candidate_env_sha256 = str(binding["candidate_env_sha256"])
    return {
        "path": str(config.quarantine_active_release_binding_path.absolute()),
        "sha256": str(binding["binding_receipt_sha256"]),
        "release_id": str(binding["release_id"]),
        "authority_sha256": str(binding["authority_sha256"]),
        "authority_epoch_id": str(binding["authority_epoch_id"]),
        "bootstrap_epoch_id": str(binding["bootstrap_epoch_id"]),
        "release_bom_sha256": str(binding["release_bom_sha256"]),
        "candidate_env_sha256": candidate_env_sha256,
        "live_env_path": str(live_env_path),
        "live_env_sha256": live_env_sha256,
        "live_env_matches_candidate": live_env_sha256 == candidate_env_sha256,
        "mismatch_policy": "observed_only_no_external_write",
    }


def _pre_w3_effect_disposition_backup_binding(
    path: Path,
    expected_sha256: str,
    *,
    source_path: Path,
    expected_snapshot: Mapping[str, Any],
    verify_source_logical_digest: bool = True,
) -> dict[str, Any]:
    selected = path.expanduser().absolute()
    source = source_path.expanduser().absolute()
    expected = str(expected_sha256 or "").strip().lower()
    if (
        not path.is_absolute()
        or not source_path.is_absolute()
        or _SHA256_RE.fullmatch(expected) is None
    ):
        raise ValueError("pre_w3_effect_disposition_backup_invalid")
    try:
        source_stat = source.lstat()
    except OSError as exc:
        raise ValueError("pre_w3_effect_disposition_backup_invalid") from exc
    if (
        stat.S_ISLNK(source_stat.st_mode)
        or not stat.S_ISREG(source_stat.st_mode)
        or source == selected
    ):
        raise ValueError("pre_w3_effect_disposition_backup_invalid")
    source_sha256 = _bound_source_sha256(source)
    observed_sha256 = _bound_source_sha256(selected)
    backup_db: sqlite3.Connection | None = None
    try:
        backup_db = sqlite3.connect(
            f"file:{selected}?mode=ro&immutable=1",
            uri=True,
            timeout=5,
        )
        backup_db.execute("PRAGMA query_only=ON")
        journal_mode = str(backup_db.execute("PRAGMA journal_mode").fetchone()[0])
        quick_check = str(backup_db.execute("PRAGMA quick_check").fetchone()[0])
        foreign_key_violations = backup_db.execute(
            "PRAGMA foreign_key_check"
        ).fetchall()
    except sqlite3.Error as exc:
        raise ValueError("pre_w3_effect_disposition_backup_invalid") from exc
    finally:
        if backup_db is not None:
            backup_db.close()
    descriptor = -1
    try:
        before = selected.lstat()
        descriptor = os.open(
            selected,
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0),
        )
        opened = os.fstat(descriptor)
        magic = os.read(descriptor, 16)
        after = os.fstat(descriptor)
        if (
            observed_sha256 != expected
            or magic != b"SQLite format 3\x00"
            or journal_mode != "delete"
            or quick_check != "ok"
            or foreign_key_violations
            or opened.st_size < 512
            or opened.st_size % 512 != 0
            or (opened.st_dev, opened.st_ino)
            == (source_stat.st_dev, source_stat.st_ino)
            or (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
            != (opened.st_dev, opened.st_ino, opened.st_size, opened.st_mtime_ns)
            or (opened.st_dev, opened.st_ino, opened.st_size, opened.st_mtime_ns)
            != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
        ):
            raise ValueError("pre_w3_effect_disposition_backup_invalid")
        backup_store = RcaDeliveryStore(
            selected,
            require_current=True,
            read_only=True,
            ensure_current_rows=False,
        )
        backup_snapshot = backup_store.pre_w3_effect_disposition_snapshot(
            effect_keys=expected_snapshot["effect_keys"],
        )
        expected_logical_digest = expected_snapshot["control_db_logical_digest"]
        backup_logical_digest = backup_store.control_db_logical_digest()
        source_logical_digest = expected_logical_digest
        if verify_source_logical_digest:
            source_store = RcaDeliveryStore(
                source,
                require_current=True,
                read_only=True,
                ensure_current_rows=False,
            )
            source_logical_digest = source_store.control_db_logical_digest()
        if (
            backup_snapshot != expected_snapshot
            or backup_logical_digest != expected_logical_digest
            or source_logical_digest != expected_logical_digest
        ):
            raise ValueError("pre_w3_effect_disposition_backup_snapshot_mismatch")
        return {
            "path": str(selected),
            "sha256": observed_sha256,
            "size_bytes": int(opened.st_size),
            "device": int(opened.st_dev),
            "inode": int(opened.st_ino),
            "mtime_ns": int(opened.st_mtime_ns),
            "source_path": str(source),
            "source_sha256": source_sha256,
            "source_device": int(source_stat.st_dev),
            "source_inode": int(source_stat.st_ino),
            "source_size_bytes": int(source_stat.st_size),
            "source_mtime_ns": int(source_stat.st_mtime_ns),
            "journal_mode": journal_mode,
            "quick_check": quick_check,
            "foreign_key_check": "ok",
            "snapshot_sha256": str(backup_snapshot["snapshot_sha256"]),
            "effect_set_sha256": str(backup_snapshot["effect_set_sha256"]),
            "logical_digest_sha256": str(backup_logical_digest["sha256"]),
        }
    except (OSError, RuntimeError, sqlite3.Error) as exc:
        raise ValueError("pre_w3_effect_disposition_backup_invalid") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _build_pre_w3_effect_disposition_receipt(
    *,
    config: "DispatcherConfig",
    snapshot: Mapping[str, Any],
    operator: str,
    reason: str,
    recorded_at: str,
    receipt_path: Path,
    backup_binding: Mapping[str, Any],
    active_release_binding: Mapping[str, Any],
    tool_provenance: Mapping[str, Any],
) -> dict[str, Any]:
    destination_binding = _circuit_reset_destination_binding(receipt_path)
    control_db_identity = _control_db_identity(config.control_db_path)
    config_binding_sha256 = _circuit_reset_config_binding(config)
    tool_provenance_sha256 = _pre_w3_disposition_sha256(tool_provenance)
    stable_db_identity = {
        key: control_db_identity[key]
        for key in ("path", "device", "inode")
    }
    stable_backup_binding = {
        key: backup_binding[key]
        for key in (
            "path",
            "sha256",
            "size_bytes",
            "device",
            "inode",
            "journal_mode",
            "quick_check",
            "foreign_key_check",
            "snapshot_sha256",
            "effect_set_sha256",
            "logical_digest_sha256",
            "source_path",
            "source_device",
            "source_inode",
        )
    }
    plan_id = _pre_w3_disposition_sha256({
        "command": PRE_W3_EFFECT_DISPOSITION_COMMAND,
        "operator": operator,
        "reason": reason,
        "effect_keys": snapshot["effect_keys"],
        "effect_set_sha256": snapshot["effect_set_sha256"],
        "before_snapshot_sha256": snapshot["snapshot_sha256"],
        "current_activation_sha256": snapshot["current_activation"]["sha256"],
        "circuit": snapshot["circuit"],
        "control_db_identity": stable_db_identity,
        "destination_binding": destination_binding,
        "backup_binding": stable_backup_binding,
        "active_release_binding": dict(active_release_binding),
        "config_binding_sha256": config_binding_sha256,
        "tool_provenance_sha256": tool_provenance_sha256,
    })
    disposition_id = _pre_w3_disposition_sha256({"plan_id": plan_id})
    after = _pre_w3_effect_disposition_after(
        snapshot,
        disposition_id=disposition_id,
        recorded_at=recorded_at,
    )
    count = len(snapshot["effect_keys"])
    receipt: dict[str, Any] = {
        "schema_version": PRE_W3_EFFECT_DISPOSITION_SCHEMA_VERSION,
        "command": PRE_W3_EFFECT_DISPOSITION_COMMAND,
        "disposition_id": disposition_id,
        "plan_id": plan_id,
        "recorded_at": recorded_at,
        "operator": operator,
        "reason": reason,
        "effect_kind": DELIVERY_EFFECT_KIND,
        "effect_keys": list(snapshot["effect_keys"]),
        "effect_set_sha256": snapshot["effect_set_sha256"],
        "before_snapshot_sha256": snapshot["snapshot_sha256"],
        "control_db_identity": control_db_identity,
        "destination_binding": destination_binding,
        "backup_binding": dict(backup_binding),
        "active_release_binding": dict(active_release_binding),
        "config_binding_sha256": config_binding_sha256,
        "tool_provenance": dict(tool_provenance),
        "tool_provenance_sha256": tool_provenance_sha256,
        "before": dict(snapshot),
        "after": after,
        "effect_delta": {
            "external_effects_triggered": False,
            "provider_calls": 0,
            "control_meta_inserted": 1,
            "attempt_audits_inserted": count,
            "effects_updated": count,
            "jobs_updated": count,
            "quarantine_audits_inserted": 2 * count,
            "total_database_rows": 1 + (5 * count),
        },
        "external_writes_performed": False,
        "provider_calls_performed": 0,
    }
    receipt["receipt_fingerprint"] = _pre_w3_effect_disposition_fingerprint(
        receipt
    )
    return receipt


def _build_pre_w3_effect_disposition_recovery_envelope(
    audit: Mapping[str, Any],
    receipt_path: Path,
) -> dict[str, Any]:
    destination = _circuit_reset_destination_binding(receipt_path)
    envelope: dict[str, Any] = {
        "schema_version": PRE_W3_EFFECT_DISPOSITION_RECOVERY_SCHEMA_VERSION,
        "command": "materialize-pre-w3-effect-disposition",
        "recovered": True,
        "source_disposition_id": audit["disposition_id"],
        "source_receipt_fingerprint": audit["receipt_fingerprint"],
        "planned_destination_binding": audit["destination_binding"],
        "materialized_destination": {
            "path": str(receipt_path),
            "binding": destination,
        },
        "materialized_at": _utc_now().isoformat(),
        "external_effects_triggered": False,
        "provider_calls_performed": 0,
        "audit": dict(audit),
    }
    envelope["receipt_fingerprint"] = _pre_w3_disposition_sha256(envelope)
    return envelope


def _existing_pre_w3_disposition_receipt_sha256(
    path: Path,
    audit: Mapping[str, Any],
) -> str | None:
    selected = path.expanduser()
    if not selected.is_absolute() or str(selected) != str(selected.absolute()):
        raise ValueError("pre_w3_effect_disposition_receipt_path_invalid")
    selected = selected.absolute()
    if _circuit_reset_destination_binding(selected) != audit["destination_binding"]:
        raise ValueError("pre_w3_effect_disposition_receipt_destination_changed")
    sidecar = Path(f"{selected}.sha256")
    try:
        observed = selected.lstat()
    except FileNotFoundError:
        if sidecar.exists():
            raise ValueError("pre_w3_effect_disposition_receipt_incomplete")
        return None
    if (
        stat.S_ISLNK(observed.st_mode)
        or not stat.S_ISREG(observed.st_mode)
        or observed.st_nlink != 1
        or observed.st_uid != os.getuid()
        or stat.S_IMODE(observed.st_mode) != 0o444
    ):
        raise ValueError("pre_w3_effect_disposition_receipt_invalid")
    expected_raw = _circuit_reset_canonical_json(audit).encode("utf-8")
    raw = selected.read_bytes()
    digest = hashlib.sha256(raw).hexdigest()
    if raw != expected_raw or _bound_source_sha256(selected) != digest:
        raise ValueError("pre_w3_effect_disposition_receipt_invalid")
    try:
        sidecar_stat = sidecar.lstat()
    except FileNotFoundError:
        # A DB commit can succeed immediately before the sidecar write.  The
        # audit and immutable main receipt are already exact at this point, so
        # repair only the missing sidecar and never rewrite either artifact.
        parent_path = selected.parent
        parent_fd, parent_identity = _open_receipt_parent(parent_path)
        try:
            expected_parent = {
                "device": int(audit["destination_binding"]["parent_device"]),
                "inode": int(audit["destination_binding"]["parent_inode"]),
            }
            if parent_identity != expected_parent:
                raise ValueError("pre_w3_effect_disposition_receipt_parent_changed")
            try:
                _create_immutable_file(
                    parent_path,
                    parent_fd,
                    sidecar.name,
                    f"{digest}  {selected.name}\n".encode("ascii"),
                    parent_identity,
                )
            except ValueError as exc:
                # Another retry may have won the no-clobber race.  The final
                # exact sidecar validation below remains authoritative.
                if "already_exists" not in str(exc):
                    raise
        finally:
            os.close(parent_fd)
        sidecar_stat = sidecar.lstat()
    if (
        stat.S_ISLNK(sidecar_stat.st_mode)
        or not stat.S_ISREG(sidecar_stat.st_mode)
        or sidecar_stat.st_nlink != 1
        or sidecar_stat.st_uid != os.getuid()
        or stat.S_IMODE(sidecar_stat.st_mode) != 0o444
        or sidecar.read_text(encoding="ascii") != f"{digest}  {selected.name}\n"
    ):
        raise ValueError("pre_w3_effect_disposition_receipt_invalid")
    return digest


def _stable_key(prefix: str, material: Mapping[str, Any]) -> str:
    digest = hashlib.sha256(_canonical_json(material).encode("utf-8")).hexdigest()
    return f"{prefix}-{digest}"


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
    env: Mapping[str, str], name: str, default: int, *, minimum: int = 1
) -> int:
    try:
        value = int(str(env.get(name, default)).strip())
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if value < minimum:
        raise ValueError(f"{name} must be at least {minimum}")
    return value


def retry_delay_seconds(attempt: int, retry_after: Any = None) -> int:
    index = min(max(int(attempt), 1) - 1, len(RETRY_DELAYS_SECONDS) - 1)
    scheduled = RETRY_DELAYS_SECONDS[index]
    try:
        requested = int(retry_after)
    except (TypeError, ValueError):
        requested = 0
    return max(scheduled, max(0, requested))


@dataclass(frozen=True)
class DispatcherConfig:
    enabled: bool
    activation_required: bool
    control_db_path: Path
    health_path: Path
    lease_seconds: int
    poll_interval_seconds: int
    circuit_poll_interval_seconds: int
    batch_size: int
    health_max_age_seconds: int
    report_http_timeout_seconds: int
    reconciliation_visibility_grace_seconds: int
    reconciliation_min_missing_reads: int
    recovery_write_interval_seconds: int
    quarantine_baseline_path: Path
    quarantine_baseline_sha256: str
    quarantine_release_id: str
    quarantine_bootstrap_epoch_id: str
    quarantine_active_release_binding_path: Path
    quarantine_live_env_path: Path
    observability_enabled: bool = True
    observability_path: Path = field(default_factory=default_observation_path)
    inventory_pin: str = ""
    observation_release_id: str = ""

    def __post_init__(self) -> None:
        minimum_lease = (
            max(
                self.report_http_timeout_seconds,
                MAX_EXTERNAL_BOUNDARY_TIMEOUT_SECONDS,
            )
            + LEASE_BOUNDARY_MARGIN_SECONDS
        )
        if self.lease_seconds <= minimum_lease:
            raise ValueError(
                f"{ENV_PREFIX}LEASE_SECONDS must exceed the maximum single "
                f"boundary timeout plus margin ({minimum_lease}s)"
            )
        if self.enabled:
            if not self.observability_enabled:
                raise ValueError(
                    f"{ENV_PREFIX}OBSERVABILITY_ENABLED must be true when enabled"
                )
            if not self.observability_path.is_absolute():
                raise ValueError(f"{ENV_PREFIX}OBSERVABILITY_PATH must be absolute")
            if _SHA256_RE.fullmatch(self.inventory_pin) is None:
                raise ValueError(
                    f"{ENV_PREFIX}INVENTORY_PIN must be a lowercase SHA-256"
                )
            if not self.observation_release_id:
                raise ValueError(
                    f"{ENV_PREFIX}OBSERVATION_RELEASE_ID is required when enabled"
                )

    @classmethod
    def from_env(
        cls,
        env: Mapping[str, str] | None = None,
        *,
        hermes_home: str | Path | None = None,
    ) -> "DispatcherConfig":
        source = os.environ if env is None else env
        home = Path(hermes_home or get_hermes_home()).expanduser()
        http_timeout = _integer(source, f"{ENV_PREFIX}REPORT_HTTP_TIMEOUT_SECONDS", 10)
        if http_timeout > 15:
            raise ValueError(
                f"{ENV_PREFIX}REPORT_HTTP_TIMEOUT_SECONDS must be at most 15"
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
        return cls(
            enabled=_boolean(source, f"{ENV_PREFIX}ENABLED", False),
            activation_required=_strict_boolean(
                source,
                f"{ENV_PREFIX}ACTIVATION_REQUIRED",
                False,
            ),
            control_db_path=control_db_path,
            health_path=Path(
                source.get(
                    f"{ENV_PREFIX}HEALTH_PATH",
                    home
                    / "runtime"
                    / "pnc_agent"
                    / "feishu_issue_kafka_rca"
                    / "delivery_dispatcher_health.json",
                )
            ).expanduser(),
            lease_seconds=_integer(
                source, f"{ENV_PREFIX}LEASE_SECONDS", 120, minimum=90
            ),
            poll_interval_seconds=_integer(
                source, f"{ENV_PREFIX}POLL_INTERVAL_SECONDS", 2
            ),
            circuit_poll_interval_seconds=_integer(
                source, f"{ENV_PREFIX}CIRCUIT_POLL_INTERVAL_SECONDS", 30
            ),
            batch_size=_integer(source, f"{ENV_PREFIX}BATCH_SIZE", 10),
            health_max_age_seconds=_integer(
                source, f"{ENV_PREFIX}HEALTH_MAX_AGE_SECONDS", 60
            ),
            report_http_timeout_seconds=http_timeout,
            reconciliation_visibility_grace_seconds=_integer(
                source,
                f"{ENV_PREFIX}RECONCILIATION_VISIBILITY_GRACE_SECONDS",
                120,
                minimum=30,
            ),
            reconciliation_min_missing_reads=_integer(
                source,
                f"{ENV_PREFIX}RECONCILIATION_MIN_MISSING_READS",
                3,
                minimum=2,
            ),
            recovery_write_interval_seconds=_integer(
                source,
                f"{ENV_PREFIX}RECOVERY_WRITE_INTERVAL_SECONDS",
                300,
                minimum=60,
            ),
            quarantine_baseline_path=quarantine.baseline_path,
            quarantine_baseline_sha256=quarantine.baseline_sha256,
            quarantine_release_id=quarantine.release_id,
            quarantine_bootstrap_epoch_id=quarantine.bootstrap_epoch_id,
            quarantine_active_release_binding_path=(
                quarantine.active_release_binding_path
            ),
            quarantine_live_env_path=quarantine.live_env_path,
            observability_enabled=_boolean(
                source, f"{ENV_PREFIX}OBSERVABILITY_ENABLED", True
            ),
            observability_path=Path(
                source.get(
                    f"{ENV_PREFIX}OBSERVABILITY_PATH",
                    default_observation_path(home),
                )
            ).expanduser(),
            inventory_pin=str(
                source.get(f"{ENV_PREFIX}INVENTORY_PIN", "") or ""
            ).strip(),
            observation_release_id=str(
                source.get(f"{ENV_PREFIX}OBSERVATION_RELEASE_ID", "") or ""
            ).strip(),
        )

    def public_dict(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "activation_required": self.activation_required,
            "control_db_path": str(self.control_db_path),
            "health_path": str(self.health_path),
            "lease_seconds": self.lease_seconds,
            "max_effect_age_seconds": MAX_EFFECT_AGE_SECONDS,
            "poll_interval_seconds": self.poll_interval_seconds,
            "circuit_poll_interval_seconds": self.circuit_poll_interval_seconds,
            "batch_size": self.batch_size,
            "health_max_age_seconds": self.health_max_age_seconds,
            "report_http_timeout_seconds": self.report_http_timeout_seconds,
            "reconciliation_visibility_grace_seconds": (
                self.reconciliation_visibility_grace_seconds
            ),
            "reconciliation_min_missing_reads": (self.reconciliation_min_missing_reads),
            "recovery_write_interval_seconds": (self.recovery_write_interval_seconds),
            "quarantine_baseline_path": str(self.quarantine_baseline_path),
            "quarantine_baseline_sha256": self.quarantine_baseline_sha256,
            "quarantine_release_id": self.quarantine_release_id,
            "quarantine_bootstrap_epoch_id": self.quarantine_bootstrap_epoch_id,
            "quarantine_active_release_binding_path": str(
                self.quarantine_active_release_binding_path
            ),
            "quarantine_live_env_path": str(self.quarantine_live_env_path),
            "observability_enabled": self.observability_enabled,
            "observability_path": str(self.observability_path),
            "inventory_pin_configured": bool(self.inventory_pin),
            "observation_release_id_configured": bool(self.observation_release_id),
            "max_recovery_writes": MAX_RECOVERY_WRITES,
            "lease_boundary_margin_seconds": LEASE_BOUNDARY_MARGIN_SECONDS,
            "effect_lease_keeper_enabled": True,
            "effect_lease_renew_interval_seconds": (
                EFFECT_LEASE_RENEW_INTERVAL_SECONDS
            ),
            "max_external_boundary_timeout_seconds": (
                MAX_EXTERNAL_BOUNDARY_TIMEOUT_SECONDS
            ),
            "allowed_effect_kind": DELIVERY_EFFECT_KIND,
            "allowed_effect_kinds": sorted(DELIVERY_EFFECT_KINDS),
            "external_writes": self.enabled,
        }


@dataclass
class DispatchStats:
    loops: int = 0
    claimed: int = 0
    delivered: int = 0
    reconciled: int = 0
    retried: int = 0
    uncertain: int = 0
    quarantined: int = 0
    circuit_opened: int = 0
    lease_extensions: int = 0
    lease_lost: int = 0
    effect_lease_keeper_started: int = 0
    effect_lease_keeper_stopped: int = 0
    effect_lease_keeper_renewals: int = 0
    effect_lease_keeper_failures: int = 0
    effect_lease_keeper_active: int = 0
    observability_written: int = 0
    observability_errors: int = 0
    observability_last_error: str = ""
    observability_current_error: str = ""
    idle: int = 0


@dataclass(frozen=True)
class DispatchOutcome:
    status: str
    effect_key: str = ""
    delivery_id: str = ""
    attempt: int = 0
    error_code: str = ""
    remote_id: str = ""
    next_attempt_at: str | None = None


@dataclass(frozen=True)
class ValidatedEffect:
    effect_kind: str
    marker: str
    content: str
    artifacts: tuple[tuple[str, str, int, str], ...]
    field_updates: tuple[tuple[str, str], ...] = ()
    chat_id: str = ""
    thread_id: str = ""
    idempotency_uuid: str = ""
    message_id: str = ""
    submission_key: str = ""
    render_hash: str = ""
    card_payload: Mapping[str, Any] | None = None


class _NoRedirect(urllib_request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        del req, fp, code, msg, headers, newurl
        return None


class _ReportDeadlineExceeded(TimeoutError):
    pass


def _remaining_deadline_seconds(
    deadline: float, monotonic: Callable[[], float]
) -> float:
    remaining = deadline - float(monotonic())
    if remaining <= 0:
        raise _ReportDeadlineExceeded("report verification deadline exceeded")
    return remaining


def _set_stream_read_timeout(response: Any, timeout_seconds: float) -> None:
    fp = getattr(response, "fp", None)
    raw = getattr(fp, "raw", None)
    candidates = (
        getattr(raw, "_sock", None),
        getattr(fp, "_sock", None),
        getattr(getattr(response, "raw", None), "_sock", None),
    )
    for candidate in candidates:
        setter = getattr(candidate, "settimeout", None)
        if setter is None:
            continue
        try:
            setter(timeout_seconds)
        except OSError:
            pass
        return


def _report_timeout_result() -> dict[str, Any]:
    return {
        "success": False,
        "error_code": "report_http_timeout",
        "error": "report verification exceeded its total deadline",
    }


def _retry_after(headers: Any) -> int | None:
    if headers is None:
        return None
    try:
        value = headers.get("Retry-After")
        return int(str(value).strip()) if value is not None else None
    except (AttributeError, TypeError, ValueError):
        return None


def _foxglove_publication_path(value: Any) -> str:
    text = str(value or "").strip()
    try:
        parsed = urlsplit(text)
        query = parse_qs(parsed.query, keep_blank_values=True)
    except ValueError:
        return ""
    paths = query.get("ds.mcapPath", [])
    if len(paths) != 1 or not validate_foxglove_url(text, paths[0]):
        return ""
    return paths[0]


def _verify_local_viz_mcap(
    path: str,
    expected_size: int,
    expected_sha256: str,
) -> dict[str, Any]:
    """Re-stat and hash the canonical publication immediately before writing."""

    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = -1
    try:
        root = Path(G1Q3_RCA_FORMAL_VIZ_ROOT)
        current = Path(path).parent
        if current != root and root not in current.parents:
            return {
                "success": False,
                "permanent": True,
                "error_code": "viz_mcap_path_invalid",
            }
        while True:
            parent_info = os.lstat(current)
            if stat.S_ISLNK(parent_info.st_mode) or not stat.S_ISDIR(
                parent_info.st_mode
            ):
                return {
                    "success": False,
                    "permanent": True,
                    "error_code": "viz_mcap_parent_invalid",
                }
            if current == root:
                break
            current = current.parent
        descriptor = os.open(path, flags)
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink < 1:
            return {
                "success": False,
                "permanent": True,
                "error_code": "viz_mcap_not_regular",
            }
        if info.st_size != expected_size:
            return {
                "success": False,
                "permanent": True,
                "error_code": "viz_mcap_size_mismatch",
                "content_length": info.st_size,
            }
        digest = hashlib.sha256()
        received = 0
        while True:
            chunk = os.read(descriptor, HTTP_VERIFY_READ_CHUNK_BYTES)
            if not chunk:
                break
            received += len(chunk)
            if received > expected_size:
                return {
                    "success": False,
                    "permanent": True,
                    "error_code": "viz_mcap_size_mismatch",
                    "content_length": received,
                }
            digest.update(chunk)
        actual_sha256 = digest.hexdigest()
        if received != expected_size:
            return {
                "success": False,
                "permanent": True,
                "error_code": "viz_mcap_size_mismatch",
                "content_length": received,
            }
        if actual_sha256 != expected_sha256:
            return {
                "success": False,
                "permanent": True,
                "error_code": "viz_mcap_hash_mismatch",
                "sha256": actual_sha256,
            }
        return {
            "success": True,
            "content_length": received,
            "sha256": actual_sha256,
        }
    except FileNotFoundError:
        return {"success": False, "error_code": "viz_mcap_missing"}
    except OSError as exc:
        return {
            "success": False,
            "error_code": "viz_mcap_stat_unavailable",
            "error": type(exc).__name__,
        }
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _verify_foxglove_publication(
    report_url: str,
    expected_size: int,
    expected_sha256: str,
    *,
    timeout_seconds: int,
    monotonic: Callable[[], float],
    opener: Any | None,
) -> Mapping[str, Any]:
    path = _foxglove_publication_path(report_url)
    if not path:
        return {
            "success": False,
            "permanent": True,
            "error_code": "foxglove_url_invalid",
        }
    local = _verify_local_viz_mcap(path, expected_size, expected_sha256)
    if local.get("success") is not True:
        if (
            local.get("error_code") == "viz_mcap_missing"
            and os.getenv("HERMES_RCA_OUTBOX_DATA_ACCESS_MODE") == "remote_read"
        ):
            return {
                "success": True,
                "status_code": 200,
                "content_length": expected_size,
                "sha256": expected_sha256,
                "viz_mcap_path": path,
                "renderer_probe": "upstream_sealed_remote_publication",
            }
        return local
    parsed = urlsplit(report_url)
    renderer_url = f"{parsed.scheme}://{parsed.netloc}/"
    deadline = float(monotonic()) + timeout_seconds
    http_opener = (
        opener if opener is not None else urllib_request.build_opener(_NoRedirect())
    )
    try:
        request = urllib_request.Request(renderer_url, method="HEAD")
        with http_opener.open(
            request,
            timeout=_remaining_deadline_seconds(deadline, monotonic),
        ) as response:
            status = int(response.getcode())
        if status != 200:
            return {
                "success": False,
                "error_code": "foxglove_renderer_not_ready",
                "status_code": status,
            }
        return {
            "success": True,
            "status_code": status,
            "content_length": local["content_length"],
            "sha256": local["sha256"],
            "viz_mcap_path": path,
            "renderer_probe": "spa_endpoint_only",
        }
    except (_ReportDeadlineExceeded, TimeoutError):
        return {
            "success": False,
            "error_code": "foxglove_renderer_timeout",
        }
    except urllib_error.HTTPError as exc:
        return {
            "success": False,
            "error_code": "foxglove_renderer_not_ready",
            "status_code": int(exc.code),
            "error": str(exc)[:500],
        }
    except (urllib_error.URLError, OSError) as exc:
        return {
            "success": False,
            "error_code": "foxglove_renderer_unavailable",
            "error": type(exc).__name__,
        }


def default_report_verifier(
    report_url: str,
    expected_size: int,
    expected_sha256: str,
    *,
    timeout_seconds: int = 10,
    monotonic: Callable[[], float] = time.monotonic,
    opener: Any | None = None,
) -> Mapping[str, Any]:
    """Verify a Foxglove publication, retaining sealed-HTML rollback support."""
    if (
        isinstance(expected_size, bool)
        or not isinstance(expected_size, int)
        or expected_size <= 0
        or expected_size > MAX_DELIVERY_ARTIFACT_BYTES
        or not _SHA256_RE.fullmatch(str(expected_sha256 or ""))
        or isinstance(timeout_seconds, bool)
        or not isinstance(timeout_seconds, int)
        or timeout_seconds <= 0
    ):
        return {
            "success": False,
            "permanent": True,
            "error_code": "report_http_expectation_invalid",
        }
    if _foxglove_publication_path(report_url):
        return _verify_foxglove_publication(
            report_url,
            expected_size,
            expected_sha256,
            timeout_seconds=timeout_seconds,
            monotonic=monotonic,
            opener=opener,
        )
    try:
        validate_report_asset_url(report_url)
    except DeliveryContractError as exc:
        return {
            "success": False,
            "permanent": True,
            "error_code": exc.code,
            "error": exc.detail,
        }
    deadline = float(monotonic()) + timeout_seconds
    http_opener = (
        opener if opener is not None else urllib_request.build_opener(_NoRedirect())
    )
    try:
        head = urllib_request.Request(report_url, method="HEAD")
        with http_opener.open(
            head,
            timeout=_remaining_deadline_seconds(deadline, monotonic),
        ) as response:
            status = int(response.getcode())
            head_length = response.headers.get("Content-Length")
        _remaining_deadline_seconds(deadline, monotonic)
        if status != 200:
            return {
                "success": False,
                "error_code": "report_http_not_ready",
                "status_code": status,
            }
        try:
            parsed_head_length = int(str(head_length))
        except (TypeError, ValueError):
            return {
                "success": False,
                "permanent": True,
                "error_code": "report_http_content_length_missing",
            }
        if parsed_head_length != expected_size:
            return {
                "success": False,
                "permanent": True,
                "error_code": "report_http_content_length_mismatch",
                "content_length": parsed_head_length,
            }

        get = urllib_request.Request(report_url, method="GET")
        with http_opener.open(
            get,
            timeout=_remaining_deadline_seconds(deadline, monotonic),
        ) as response:
            status = int(response.getcode())
            get_length = response.headers.get("Content-Length")
            if status != 200:
                return {
                    "success": False,
                    "error_code": "report_http_not_ready",
                    "status_code": status,
                }
            try:
                parsed_get_length = int(str(get_length))
            except (TypeError, ValueError):
                return {
                    "success": False,
                    "permanent": True,
                    "error_code": "report_http_content_length_missing",
                }
            if parsed_get_length != expected_size:
                return {
                    "success": False,
                    "permanent": True,
                    "error_code": "report_http_content_length_mismatch",
                    "content_length": parsed_get_length,
                }
            digest = hashlib.sha256()
            received = 0
            read_chunk = getattr(response, "read1", None) or response.read
            while True:
                remaining = _remaining_deadline_seconds(deadline, monotonic)
                _set_stream_read_timeout(response, remaining)
                chunk = read_chunk(
                    min(
                        HTTP_VERIFY_READ_CHUNK_BYTES,
                        expected_size + 1 - received,
                    )
                )
                _remaining_deadline_seconds(deadline, monotonic)
                if not chunk:
                    break
                received += len(chunk)
                if received > expected_size:
                    return {
                        "success": False,
                        "permanent": True,
                        "error_code": "report_http_body_too_large",
                    }
                digest.update(chunk)
        actual_sha = digest.hexdigest()
        if received != expected_size:
            return {
                "success": False,
                "permanent": True,
                "error_code": "report_http_body_size_mismatch",
                "content_length": received,
            }
        if actual_sha != expected_sha256:
            return {
                "success": False,
                "permanent": True,
                "error_code": "report_http_hash_mismatch",
                "sha256": actual_sha,
            }
        return {
            "success": True,
            "status_code": 200,
            "content_length": received,
            "sha256": actual_sha,
        }
    except _ReportDeadlineExceeded:
        return _report_timeout_result()
    except TimeoutError:
        return _report_timeout_result()
    except urllib_error.HTTPError as exc:
        code = int(exc.code)
        if code in {401, 403}:
            error_code = "report_http_auth_or_permission"
        elif code == 429:
            error_code = "report_http_rate_limited"
        else:
            error_code = "report_http_not_ready"
        return {
            "success": False,
            "error_code": error_code,
            "status_code": code,
            "retry_after_seconds": _retry_after(exc.headers),
            "error": str(exc)[:500],
        }
    except urllib_error.URLError as exc:
        if isinstance(getattr(exc, "reason", None), TimeoutError):
            return _report_timeout_result()
        return {
            "success": False,
            "error_code": "report_http_unavailable",
            "error": type(exc).__name__,
        }
    except OSError as exc:
        return {
            "success": False,
            "error_code": "report_http_unavailable",
            "error": type(exc).__name__,
        }


def _json_stdout(value: str) -> Any:
    text = str(value or "").strip()
    if not text:
        return {}
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {}


def _field_value_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, Mapping):
        for key in ("link", "url", "href", "text", "value"):
            if key in value:
                normalized = _field_value_text(value[key])
                if normalized:
                    return normalized
    return ""


def _unwrap_data(value: Any) -> Any:
    current = value
    for _ in range(4):
        if isinstance(current, Mapping) and isinstance(
            current.get("data"), (Mapping, list)
        ):
            current = current["data"]
        else:
            break
    return current


def _remote_id(value: Any) -> str:
    current = _unwrap_data(value)
    if not isinstance(current, Mapping):
        return ""
    for key in ("comment_id", "id"):
        candidate = str(current.get(key) or "").strip()
        if _REMOTE_ID_RE.fullmatch(candidate):
            return candidate
    comment = current.get("comment")
    if isinstance(comment, Mapping):
        return _remote_id(comment)
    return ""


def _comment_rows(value: Any) -> list[dict[str, str]] | None:
    current = _unwrap_data(value)
    rows: Any = None
    if isinstance(current, list):
        rows = current
    elif isinstance(current, Mapping):
        for key in ("comments", "items", "list", "results", "result"):
            if isinstance(current.get(key), list):
                rows = current[key]
                break
    if not isinstance(rows, list):
        return None
    normalized: list[dict[str, str]] = []
    for item in rows:
        if not isinstance(item, Mapping):
            return None
        remote_id = _remote_id(item)
        content: Any = ""
        for key in ("content", "text", "body", "comment"):
            if isinstance(item.get(key), str):
                content = item[key]
                break
        if not remote_id or not isinstance(content, str):
            return None
        normalized.append({"remote_id": remote_id, "content": content})
    return normalized


def _comment_page_has_more(
    value: Any,
    *,
    requested_page: int,
    row_count: int,
) -> bool | None:
    current = value
    observed: list[Any] = []
    pagination_rows: list[Mapping[str, Any]] = []
    for _ in range(4):
        if not isinstance(current, Mapping):
            break
        containers = [current]
        containers.extend(
            nested
            for key in ("meta", "pagination", "page_info")
            if isinstance((nested := current.get(key)), Mapping)
        )
        for container in containers:
            for key in ("has_more", "hasMore"):
                if key in container:
                    observed.append(container[key])
            pagination_keys = {"page_num", "page_size", "total", "total_pages"}
            if pagination_keys & set(container):
                if not pagination_keys <= set(container):
                    raise ValueError("comment pagination fields are incomplete")
                pagination_rows.append(container)
        nested_data = current.get("data")
        if not isinstance(nested_data, Mapping):
            break
        current = nested_data
    decisions: list[bool] = []
    if observed:
        if any(not isinstance(item, bool) for item in observed):
            raise ValueError("comment pagination has_more must be boolean")
        decisions.extend(bool(item) for item in observed)
    for pagination in pagination_rows:
        values = {
            key: pagination.get(key)
            for key in ("page_num", "page_size", "total", "total_pages")
        }
        if any(
            isinstance(item, bool) or not isinstance(item, int)
            for item in values.values()
        ):
            raise ValueError("comment pagination counters must be integers")
        page_num = int(values["page_num"])
        page_size = int(values["page_size"])
        total = int(values["total"])
        total_pages = int(values["total_pages"])
        if (
            page_num != requested_page
            or page_size < 1
            or total < 0
            or total_pages < 0
            or row_count < 0
            or row_count > page_size
        ):
            raise ValueError("comment pagination counters are inconsistent")
        expected_pages = (total + page_size - 1) // page_size if total else 0
        if total_pages != expected_pages:
            raise ValueError("comment pagination total_pages mismatch")
        if (total_pages == 0 and row_count) or (
            total_pages > 0 and requested_page > total_pages and row_count
        ):
            raise ValueError("comment page rows exceed declared total pages")
        decisions.append(requested_page < total_pages)
    if not decisions:
        return None
    if any(item is not decisions[0] for item in decisions[1:]):
        raise ValueError("comment pagination completion values conflict")
    return decisions[0]


def _error_payload(rc: int, stdout: str, stderr: str) -> dict[str, Any]:
    text = f"{stderr}\n{stdout}".strip()
    lowered = text.lower()
    payload = _json_stdout(stdout)
    retry_after: Any = None
    if isinstance(payload, Mapping):
        retry_after = payload.get("retry_after") or payload.get("retry_after_seconds")
    if retry_after is None:
        match = re.search(r"retry[-_ ]?after[^0-9]{0,8}([0-9]+)", lowered)
        retry_after = int(match.group(1)) if match else None
    if rc == 127:
        code = "meegle_dependency_unavailable"
    elif "work_item_id not found" in lowered:
        code = "feishu_work_item_not_found"
    elif any(
        token in lowered
        for token in ("unauth", "not logged", "login required", "token expired", "401")
    ):
        code = "feishu_auth_failed"
    elif any(token in lowered for token in ("permission denied", "forbidden", "403")):
        code = "feishu_permission_denied"
    elif "429" in lowered or "rate limit" in lowered or retry_after is not None:
        code = "feishu_rate_limited"
    else:
        code = "meegle_call_failed"
    result = {
        "success": False,
        "outcome_uncertain": False,
        "error_code": code,
        "error": text[:1000] or f"meegle rc={rc}",
        "retry_after_seconds": retry_after,
    }
    if code == "feishu_work_item_not_found":
        result["permanent"] = True
    return result


class MeegleIssueCommentAdapter:
    """Bounded Meegle adapter for RCA fields and issue comments."""

    _RCA_FIELD_METADATA = {
        RCA_RESULT_FIELD_KEY: ("归因结果", "text"),
        RCA_REPORT_FIELD_KEY: ("归因报告", "link"),
        G1Q3_ADOPTION_FIELD_KEY: ("是否采纳", "select"),
    }
    _READ_FIELD_KEY_SETS = frozenset({
        (RCA_RESULT_FIELD_KEY,),
        (RCA_RESULT_FIELD_KEY, RCA_REPORT_FIELD_KEY),
        (G1Q3_ADOPTION_FIELD_KEY,),
        (
            RCA_RESULT_FIELD_KEY,
            RCA_REPORT_FIELD_KEY,
            G1Q3_ADOPTION_FIELD_KEY,
        ),
    })

    def __init__(
        self, runner: Callable[[list[str]], tuple[int, str, str]] | None = None
    ):
        if runner is None:
            from gateway.pnc_issue_context import default_meegle_runner

            runner = default_meegle_runner
        self.runner = runner

    def list_comments(self, project_key: str, work_item_id: str) -> Mapping[str, Any]:
        comments: list[dict[str, str]] = []
        seen_remote_ids: set[str] = set()
        for page_num in range(1, MAX_MEEGLE_COMMENT_PAGES + 1):
            rc, out, err = self.runner([
                "comment",
                "list",
                "--project-key",
                str(project_key),
                "--work-item-id",
                str(work_item_id),
                "--page-num",
                str(page_num),
                "--format",
                "json",
            ])
            if rc != 0:
                return _error_payload(rc, out, err)
            payload = _json_stdout(out)
            page_comments = _comment_rows(payload)
            try:
                has_more = _comment_page_has_more(
                    payload,
                    requested_page=page_num,
                    row_count=len(page_comments or []),
                )
            except ValueError:
                page_comments = None
                has_more = None
            if page_comments is None:
                return {
                    "success": False,
                    "outcome_uncertain": False,
                    "error_code": "meegle_response_invalid",
                    "error": "comment page is missing strict ids/content or pagination",
                }
            if len(comments) + len(page_comments) > MAX_MEEGLE_COMMENTS:
                return {
                    "success": False,
                    "outcome_uncertain": False,
                    "permanent": True,
                    "error_code": "meegle_comment_limit_exceeded",
                    "error": "comment history exceeds the bounded reconciliation limit",
                }
            for comment in page_comments:
                remote_id = comment["remote_id"]
                if remote_id in seen_remote_ids:
                    return {
                        "success": False,
                        "outcome_uncertain": False,
                        "error_code": "meegle_response_invalid",
                        "error": "comment pagination repeated a remote comment id",
                    }
                seen_remote_ids.add(remote_id)
                comments.append(comment)
            if has_more is False:
                return {
                    "success": True,
                    "comments": comments,
                    "pages_read": page_num,
                }
            if not page_comments:
                if has_more is True:
                    return {
                        "success": False,
                        "outcome_uncertain": False,
                        "error_code": "meegle_response_invalid",
                        "error": "empty comment page claims more pages",
                    }
                return {
                    "success": True,
                    "comments": comments,
                    "pages_read": page_num,
                }
        return {
            "success": False,
            "outcome_uncertain": False,
            "permanent": True,
            "error_code": "meegle_comment_pagination_incomplete",
            "error": "comment history exceeds the bounded page reconciliation limit",
        }

    def get_fields(
        self,
        project_key: str,
        work_item_id: str,
        field_keys: tuple[str, ...],
    ) -> Mapping[str, Any]:
        if field_keys not in self._READ_FIELD_KEY_SETS:
            return {
                "success": False,
                "permanent": True,
                "error_code": "feishu_field_allowlist_invalid",
            }
        args = [
            "workitem",
            "get",
            "--project-key",
            str(project_key),
            "--work-item-id",
            str(work_item_id),
        ]
        for field_key in field_keys:
            args.extend(["--fields", field_key])
        args.extend(["--format", "json"])
        rc, out, err = self.runner(args)
        if rc != 0:
            return _error_payload(rc, out, err)
        payload = _json_stdout(out)
        if isinstance(payload, Mapping) and isinstance(payload.get("data"), Mapping):
            payload = payload["data"]
        if not isinstance(payload, Mapping):
            return {
                "success": False,
                "error_code": "meegle_response_invalid",
                "error": "work item field read must return an object",
            }
        rows = payload.get("work_item_fields")
        if rows is None:
            rows = payload.get("fields")
        normalized: dict[str, str] = {}
        if isinstance(rows, Mapping):
            for key in field_keys:
                if key in rows:
                    normalized[key] = _field_value_text(rows[key])
        elif isinstance(rows, list):
            for row in rows:
                if not isinstance(row, Mapping):
                    return {
                        "success": False,
                        "error_code": "meegle_response_invalid",
                        "error": "work item fields must contain objects",
                    }
                key = str(row.get("key") or "").strip()
                if key in field_keys:
                    normalized[key] = _field_value_text(row.get("value"))
        elif rows is not None:
            return {
                "success": False,
                "error_code": "meegle_response_invalid",
                "error": "work item field read is missing fields",
            }
        missing_keys = tuple(key for key in field_keys if key not in normalized)
        if missing_keys:
            attribute = payload.get("work_item_attribute")
            work_item_type = (
                attribute.get("work_item_type")
                if isinstance(attribute, Mapping)
                else None
            )
            work_item_type_key = (
                str(work_item_type.get("key") or "").strip()
                if isinstance(work_item_type, Mapping)
                else ""
            )
            if (
                not isinstance(attribute, Mapping)
                or str(attribute.get("work_item_id") or "") != str(work_item_id)
                or not work_item_type_key
            ):
                return {
                    "success": False,
                    "error_code": "meegle_response_invalid",
                    "error": "omitted-field response identity is missing or mismatched",
                }
            metadata_args = [
                "workitem",
                "meta-fields",
                "--project-key",
                str(project_key),
                "--work-item-type",
                work_item_type_key,
                "--page-num",
                "1",
            ]
            for field_key in field_keys:
                metadata_args.extend(["--field-keys", field_key])
            metadata_args.extend(["--format", "json"])
            rc, out, err = self.runner(metadata_args)
            if rc != 0:
                return _error_payload(rc, out, err)
            metadata = _json_stdout(out)
            if isinstance(metadata, Mapping) and isinstance(
                metadata.get("data"), Mapping
            ):
                metadata = metadata["data"]
            metadata_rows = (
                metadata.get("list") if isinstance(metadata, Mapping) else None
            )
            definitions = {}
            if isinstance(metadata_rows, list):
                definitions = {
                    str(row.get("field_key") or "").strip(): (
                        str(row.get("field_name") or "").strip(),
                        str(row.get("field_type") or "").strip(),
                    )
                    for row in metadata_rows
                    if isinstance(row, Mapping)
                    and str(row.get("field_key") or "").strip() in field_keys
                }
            expected_definitions = {
                key: self._RCA_FIELD_METADATA[key] for key in field_keys
            }
            if definitions != expected_definitions:
                return {
                    "success": False,
                    "permanent": True,
                    "error_code": "feishu_field_metadata_invalid",
                    "error": "configured attribution field metadata is missing or mismatched",
                }
            normalized.update({key: "" for key in missing_keys})
        return {"success": True, "fields": normalized}

    def _list_adoption_operation_window(
        self,
        project_key: str,
        work_item_id: str,
        *,
        start_ms: int,
        end_ms: int,
    ) -> Mapping[str, Any]:
        try:
            start_ms, end_ms = validate_g1q3_adoption_window(start_ms, end_ms)
        except G1Q3AdoptionReadError as exc:
            return {
                "success": False,
                "permanent": True,
                "error_code": exc.code,
                "error": exc.detail,
            }
        operations: list[dict[str, Any]] = []
        seen_tokens: set[str] = set()
        start_from = ""
        for page_num in range(1, MAX_MEEGLE_OPERATION_PAGES + 1):
            args = [
                "workitem",
                "list-op-records",
                "--project-key",
                str(project_key),
                "--work-item-id",
                str(work_item_id),
                "--op-record-module",
                "field_mod",
                "--operation-type",
                "modify",
                "--start",
                str(start_ms),
                "--end",
                str(end_ms),
            ]
            if start_from:
                args.extend(["--start-from", start_from])
            args.extend(["--format", "json"])
            rc, out, err = self.runner(args)
            if rc != 0:
                return _error_payload(rc, out, err)
            try:
                page, next_token = normalize_g1q3_adoption_operation_page(
                    _json_stdout(out)
                )
            except G1Q3AdoptionReadError as exc:
                return {
                    "success": False,
                    "permanent": True,
                    "error_code": exc.code,
                    "error": exc.detail,
                }
            if any(
                int(operation["operation_time"]) < start_ms
                or int(operation["operation_time"]) > end_ms
                for operation in page
            ):
                return {
                    "success": False,
                    "permanent": True,
                    "error_code": "g1q3_adoption_operation_out_of_window",
                    "error": "list-op-records returned an operation outside its query window",
                }
            operations.extend(page)
            if not next_token:
                return {
                    "success": True,
                    "operations": operations,
                    "pages_read": page_num,
                    "start_ms": start_ms,
                    "end_ms": end_ms,
                }
            if next_token in seen_tokens:
                return {
                    "success": False,
                    "permanent": True,
                    "error_code": "g1q3_adoption_pagination_cycle",
                    "error": "list-op-records repeated start_from",
                }
            seen_tokens.add(next_token)
            start_from = next_token
        return {
            "success": False,
            "permanent": True,
            "error_code": "g1q3_adoption_operation_page_limit",
            "error": "adoption operation history exceeds 20 pages in one window",
        }

    def read_adoption(
        self,
        project_key: str,
        work_item_id: str,
        *,
        start_ms: int,
        end_ms: int,
        require_current_match: bool = True,
    ) -> Mapping[str, Any]:
        """Read one generation's explicit adoption state without writing it."""

        if (
            isinstance(start_ms, bool)
            or isinstance(end_ms, bool)
            or not isinstance(start_ms, int)
            or not isinstance(end_ms, int)
            or start_ms < 0
            or end_ms < start_ms
        ):
            return {
                "success": False,
                "permanent": True,
                "error_code": "g1q3_adoption_window_invalid",
            }
        fields = self.get_fields(
            project_key,
            work_item_id,
            (G1Q3_ADOPTION_FIELD_KEY,),
        )
        if fields.get("success") is not True:
            return fields
        current_value = str(
            (fields.get("fields") or {}).get(G1Q3_ADOPTION_FIELD_KEY) or ""
        ).strip()
        if (
            require_current_match
            and current_value
            and current_value not in G1Q3_ADOPTION_VALUE_KEYS
        ):
            return {
                "success": False,
                "permanent": True,
                "error_code": "g1q3_adoption_value_invalid",
            }

        operations: list[dict[str, Any]] = []
        windows_read = 0
        cursor = start_ms
        while cursor <= end_ms:
            windows_read += 1
            if windows_read > MAX_MEEGLE_OPERATION_WINDOWS:
                return {
                    "success": False,
                    "permanent": True,
                    "error_code": "g1q3_adoption_window_limit",
                    "error": "adoption history exceeds 64 bounded windows",
                }
            window_end = min(end_ms, cursor + MAX_MEEGLE_OPERATION_WINDOW_MS)
            window = self._list_adoption_operation_window(
                project_key,
                work_item_id,
                start_ms=cursor,
                end_ms=window_end,
            )
            if window.get("success") is not True:
                return window
            operations.extend(dict(item) for item in window["operations"])
            cursor = window_end + 1

        identities: set[tuple[Any, ...]] = set()
        timestamps: set[int] = set()
        for operation in operations:
            identity = (
                operation.get("operation_time"),
                operation.get("operator"),
                operation.get("old"),
                operation.get("new"),
            )
            if identity in identities:
                return {
                    "success": False,
                    "permanent": True,
                    "error_code": "g1q3_adoption_operation_duplicate",
                }
            identities.add(identity)
            timestamp = int(operation["operation_time"])
            if timestamp in timestamps:
                return {
                    "success": False,
                    "permanent": True,
                    "error_code": "g1q3_adoption_operation_order_ambiguous",
                }
            timestamps.add(timestamp)
        operations.sort(
            key=lambda item: (
                int(item["operation_time"]),
                str(item["operator"]),
                str(item["new"]),
            )
        )

        result: dict[str, Any] = {
            "success": True,
            "source": "official_meegle_api",
            "scope": {
                "project_key": str(project_key),
                "work_item_id": str(work_item_id),
            },
            "field_key": G1Q3_ADOPTION_FIELD_KEY,
            "current_value": current_value,
            "operations": operations,
            "windows_read": windows_read,
            "start_ms": start_ms,
            "end_ms": end_ms,
        }
        if not operations:
            result.update({
                "status": "unreviewed",
                "reason": (
                    "generation_has_no_user_operation_record"
                    if not require_current_match
                    else "current_field_empty"
                    if not current_value
                    else "current_value_has_no_user_operation_record"
                ),
            })
            if require_current_match and current_value:
                result["ignored_default_value"] = current_value
            return result
        latest = operations[-1]
        if require_current_match and latest["new"] != current_value:
            return {
                **result,
                "success": False,
                "permanent": True,
                "error_code": "g1q3_adoption_current_operation_mismatch",
            }
        if latest["status"] == "unreviewed":
            result.update({
                "status": "unreviewed",
                "reason": "latest_user_operation_cleared_field",
                "operation": latest,
                "explicit": False,
            })
            return result
        result.update({
            "status": latest["status"],
            "operation": latest,
            "explicit": True,
        })
        return result

    def read_generation_adoption(
        self,
        project_key: str,
        work_item_id: str,
        *,
        generation: int,
        conclusion_time_ms: int,
        next_conclusion_time_ms: int | None = None,
        observed_at_ms: int | None = None,
    ) -> Mapping[str, Any]:
        """Bind the latest explicit response to one conclusion generation.

        Closed generations use ``[conclusion, next_conclusion)``.  The current
        generation uses ``[conclusion, observed_at]`` and additionally checks
        that the field's current value matches the latest operation.
        """

        if (
            isinstance(conclusion_time_ms, bool)
            or not isinstance(conclusion_time_ms, int)
            or conclusion_time_ms < 0
        ):
            return {
                "success": False,
                "permanent": True,
                "error_code": "g1q3_adoption_conclusion_time_invalid",
            }
        if (
            isinstance(generation, bool)
            or not isinstance(generation, int)
            or generation < 1
        ):
            return {
                "success": False,
                "permanent": True,
                "error_code": "g1q3_adoption_generation_invalid",
            }
        if next_conclusion_time_ms is not None:
            if (
                isinstance(next_conclusion_time_ms, bool)
                or not isinstance(next_conclusion_time_ms, int)
                or next_conclusion_time_ms <= conclusion_time_ms
            ):
                return {
                    "success": False,
                    "permanent": True,
                    "error_code": "g1q3_adoption_generation_window_invalid",
                }
            end_ms = next_conclusion_time_ms - 1
            require_current_match = False
        else:
            if (
                isinstance(observed_at_ms, bool)
                or not isinstance(observed_at_ms, int)
                or observed_at_ms < conclusion_time_ms
            ):
                return {
                    "success": False,
                    "permanent": True,
                    "error_code": "g1q3_adoption_observation_time_invalid",
                }
            end_ms = observed_at_ms
            require_current_match = True

        result = dict(
            self.read_adoption(
                project_key,
                work_item_id,
                start_ms=conclusion_time_ms,
                end_ms=end_ms,
                require_current_match=require_current_match,
            )
        )
        result.update({
            "generation": generation,
            "conclusion_time_ms": conclusion_time_ms,
            "next_conclusion_time_ms": next_conclusion_time_ms,
            "window_semantics": (
                "half_open_conclusion_to_next_conclusion"
                if next_conclusion_time_ms is not None
                else "closed_conclusion_to_observed_at"
            ),
        })
        return result

    def get_fields_and_comments(
        self,
        project_key: str,
        work_item_id: str,
        field_keys: tuple[str, ...] = (
            RCA_RESULT_FIELD_KEY,
            RCA_REPORT_FIELD_KEY,
        ),
    ) -> Mapping[str, Any]:
        """Read the exact RCA fields and every bounded comment body.

        Closeout callers need one official observation boundary. Returning a
        partial result would allow fields from one instant to be paired with a
        truncated or failed comment read, so either official read failure is
        returned unchanged and no success-shaped aggregate is emitted.
        """
        fields = self.get_fields(project_key, work_item_id, field_keys)
        if fields.get("success") is not True:
            return fields
        comments = self.list_comments(project_key, work_item_id)
        if comments.get("success") is not True:
            return comments
        return {
            "success": True,
            "source": "official_meegle_api",
            "scope": {
                "project_key": str(project_key),
                "work_item_id": str(work_item_id),
            },
            "fields": dict(fields["fields"]),
            "comments": [dict(item) for item in comments["comments"]],
            "pages_read": int(comments["pages_read"]),
        }

    def update_fields(
        self,
        project_key: str,
        work_item_id: str,
        field_updates: tuple[tuple[str, str], ...],
    ) -> Mapping[str, Any]:
        field_keys = tuple(key for key, _value in field_updates)
        if field_keys not in {
            (RCA_RESULT_FIELD_KEY,),
            (RCA_RESULT_FIELD_KEY, RCA_REPORT_FIELD_KEY),
        }:
            return {
                "success": False,
                "permanent": True,
                "error_code": "feishu_field_allowlist_invalid",
            }
        fields = [
            {"field_key": key, "field_value": value} for key, value in field_updates
        ]
        _require_provider_write_guard(
            "feishu_issue_field_update",
            f"feishu_project:{project_key}:{work_item_id}",
        )
        rc, out, err = self.runner([
            "workitem",
            "update",
            "--project-key",
            str(project_key),
            "--work-item-id",
            str(work_item_id),
            "--params",
            json.dumps({"fields": fields}, ensure_ascii=False, sort_keys=True),
            "--format",
            "json",
        ])
        if rc != 0:
            return _error_payload(rc, out, err)
        payload = _json_stdout(out)
        if payload is None:
            return {
                "success": False,
                "outcome_uncertain": True,
                "error_code": "meegle_response_invalid",
            }
        return {"success": True}

    def add_comment(
        self, project_key: str, work_item_id: str, content: str
    ) -> Mapping[str, Any]:
        _require_provider_write_guard(
            "feishu_issue_comment",
            f"feishu_project:{project_key}:{work_item_id}",
        )
        rc, out, err = self.runner([
            "comment",
            "add",
            "--project-key",
            str(project_key),
            "--work-item-id",
            str(work_item_id),
            "--content",
            str(content),
            "--format",
            "json",
        ])
        if rc != 0:
            return _error_payload(rc, out, err)
        remote_id = _remote_id(_json_stdout(out))
        if not remote_id:
            readback = self.list_comments(project_key, work_item_id)
            comments = readback.get("comments") if isinstance(readback, Mapping) else None
            marker = _delivery_marker_from_content(content)
            if isinstance(comments, list) and marker:
                matches = _confirmed_content_matches(comments, marker, content)
                if len(matches) == 1:
                    return {
                        "success": True,
                        "remote_id": matches[0]["remote_id"],
                        "confirmed_by": "comment_list_readback",
                    }
            return {
                "success": False,
                "outcome_uncertain": True,
                "error_code": "feishu_add_remote_id_missing",
                "error": "Meegle reported success without a strict comment id",
            }
        return {"success": True, "remote_id": remote_id}


class FeishuThreadReplyAdapter:
    """Outbound-only Feishu topic boundary with bounded marker reads."""

    def __init__(self, adapter: Any | None = None):
        self._adapter = adapter or self._build_adapter()

    @staticmethod
    def _build_adapter() -> Any:
        from gateway.config import Platform, load_gateway_config
        from gateway.platforms import feishu

        if not feishu.check_feishu_requirements():
            raise RuntimeError("Feishu dependencies are unavailable")
        config = load_gateway_config()
        platform = Platform("feishu")
        platform_config = config.platforms.get(platform)
        if platform_config is None or not platform_config.enabled:
            raise RuntimeError("Feishu platform is not configured")
        adapter = feishu.FeishuAdapter(platform_config)
        domain = (
            feishu.FEISHU_DOMAIN
            if getattr(adapter, "_domain_name", "feishu") != "lark"
            else feishu.LARK_DOMAIN
        )
        adapter._client = adapter._build_lark_client(domain)
        return adapter

    @staticmethod
    def _error_result(response: Any, default_code: str) -> dict[str, Any]:
        code = str(getattr(response, "code", "") or "")
        detail = str(getattr(response, "msg", "") or default_code)
        lowered = f"{code} {detail}".lower()
        if code in {"99991663", "99991664"} or "token" in lowered:
            error_code = "feishu_auth_failed"
        elif code in {"230027", "230073"} or any(
            value in lowered for value in ("permission", "forbidden", "invisible")
        ):
            error_code = "feishu_permission_denied"
        elif code == "429" or "rate" in lowered:
            error_code = "feishu_rate_limited"
        else:
            error_code = default_code
        return {
            "success": False,
            "outcome_uncertain": False,
            "error_code": error_code,
            "error": detail[:1000],
        }

    @staticmethod
    def _topic_anchor(thread_id: str) -> str:
        value = str(thread_id or "").strip()
        if not value.startswith("topic:"):
            raise ValueError("thread_id must be a topic root reference")
        anchor = value[len("topic:") :].strip()
        if not _REMOTE_ID_RE.fullmatch(anchor):
            raise ValueError("thread_id contains an invalid topic root")
        return anchor

    @staticmethod
    def _restore_single_text_mention(content: str, mentions: Any) -> str:
        """Restore one API-proven Feishu text mention for exact body comparison."""
        placeholders = _FEISHU_RENDERED_MENTION_RE.findall(content)
        if (
            len(placeholders) != 1
            or not isinstance(mentions, list)
            or len(mentions) != 1
            or not isinstance(mentions[0], Mapping)
        ):
            return content
        mention = mentions[0]
        key = str(mention.get("key") or "").strip()
        open_id = str(mention.get("id") or "").strip()
        if (
            key != placeholders[0]
            or str(mention.get("id_type") or "").strip() != "open_id"
            or _FEISHU_OPEN_ID_RE.fullmatch(open_id) is None
            or content.count(key) != 1
        ):
            return content
        return content.replace(key, f'<at user_id="{open_id}"></at>', 1)

    async def _resolve_thread_id(self, chat_id: str, anchor: str) -> Mapping[str, Any]:
        request = self._adapter._build_get_message_request(anchor)
        try:
            response = await asyncio.wait_for(
                asyncio.to_thread(
                    self._adapter._client.im.v1.message.get,
                    request,
                ),
                timeout=MEEGLE_COMMENT_PAGE_TIMEOUT_SECONDS,
            )
        except TimeoutError:
            return {
                "success": False,
                "error_code": "feishu_thread_read_timeout",
                "error": "thread root lookup exceeded its deadline",
            }
        except Exception as exc:
            return {
                "success": False,
                "error_code": "feishu_thread_read_unavailable",
                "error": type(exc).__name__,
            }
        if not self._adapter._response_succeeded(response):
            return self._error_result(response, "feishu_thread_read_failed")
        items = getattr(getattr(response, "data", None), "items", None) or []
        if len(items) != 1:
            return {
                "success": False,
                "permanent": True,
                "error_code": "feishu_thread_root_invalid",
                "error": "topic root lookup must return exactly one message",
            }
        root = items[0]
        if (
            str(getattr(root, "message_id", "") or "") != anchor
            or str(getattr(root, "chat_id", "") or "") != chat_id
        ):
            return {
                "success": False,
                "permanent": True,
                "error_code": "feishu_thread_root_identity_mismatch",
            }
        actual_thread_id = str(getattr(root, "thread_id", "") or "").strip()
        if not actual_thread_id:
            return {"success": True, "thread_id": ""}
        if not _REMOTE_ID_RE.fullmatch(actual_thread_id):
            return {
                "success": False,
                "permanent": True,
                "error_code": "feishu_thread_id_invalid",
            }
        return {"success": True, "thread_id": actual_thread_id}

    async def _list_replies(self, chat_id: str, thread_id: str) -> Mapping[str, Any]:
        from gateway.platforms.feishu import AccessTokenType, BaseRequest, HttpMethod

        anchor = self._topic_anchor(thread_id)
        resolved = await self._resolve_thread_id(chat_id, anchor)
        if resolved.get("success") is not True:
            return resolved
        actual_thread_id = str(resolved.get("thread_id") or "")
        if not actual_thread_id:
            return {"success": True, "comments": [], "pages_read": 0}
        comments: list[dict[str, str]] = []
        seen_ids: set[str] = set()
        page_token = ""
        for page_num in range(1, MAX_MEEGLE_COMMENT_PAGES + 1):
            queries = [
                ("container_id_type", "thread"),
                ("container_id", actual_thread_id),
                ("sort_type", "ByCreateTimeDesc"),
                ("page_size", "50"),
            ]
            if page_token:
                queries.append(("page_token", page_token))
            request = (
                BaseRequest
                .builder()
                .http_method(HttpMethod.GET)
                .uri("/open-apis/im/v1/messages")
                .queries(queries)
                .token_types({AccessTokenType.TENANT})
                .build()
            )
            try:
                response = await asyncio.wait_for(
                    asyncio.to_thread(self._adapter._client.request, request),
                    timeout=MEEGLE_COMMENT_PAGE_TIMEOUT_SECONDS,
                )
                raw = getattr(getattr(response, "raw", None), "content", None)
                payload = json.loads(raw) if raw else None
            except TimeoutError:
                return {
                    "success": False,
                    "error_code": "feishu_thread_read_timeout",
                    "error": "thread reply page exceeded its deadline",
                }
            except Exception as exc:
                return {
                    "success": False,
                    "error_code": "feishu_thread_read_unavailable",
                    "error": type(exc).__name__,
                }
            if not isinstance(payload, Mapping):
                return {
                    "success": False,
                    "error_code": "delivery_boundary_contract_invalid",
                }
            if payload.get("code") != 0:
                response_like = type(
                    "FeishuError",
                    (),
                    {"code": payload.get("code"), "msg": payload.get("msg")},
                )()
                return self._error_result(response_like, "feishu_thread_read_failed")
            data = payload.get("data")
            if not isinstance(data, Mapping) or not isinstance(data.get("items"), list):
                return {
                    "success": False,
                    "error_code": "delivery_boundary_contract_invalid",
                }
            for item in data["items"]:
                if not isinstance(item, Mapping):
                    return {
                        "success": False,
                        "error_code": "delivery_boundary_contract_invalid",
                    }
                remote_id = str(item.get("message_id") or "").strip()
                if not _REMOTE_ID_RE.fullmatch(remote_id) or remote_id in seen_ids:
                    return {
                        "success": False,
                        "error_code": "delivery_boundary_contract_invalid",
                    }
                seen_ids.add(remote_id)
                if remote_id == anchor:
                    continue
                if (
                    str(item.get("root_id") or "") != anchor
                    or str(item.get("thread_id") or "") != actual_thread_id
                    or str(item.get("msg_type") or "") != "text"
                ):
                    continue
                body = item.get("body")
                raw_content = body.get("content") if isinstance(body, Mapping) else None
                try:
                    content_value = json.loads(raw_content) if raw_content else None
                except (TypeError, json.JSONDecodeError):
                    content_value = None
                content = (
                    content_value.get("text")
                    if isinstance(content_value, Mapping)
                    else None
                )
                if not isinstance(content, str):
                    return {
                        "success": False,
                        "error_code": "delivery_boundary_contract_invalid",
                    }
                content = self._restore_single_text_mention(
                    content,
                    item.get("mentions"),
                )
                comments.append({"remote_id": remote_id, "content": content})
                if len(comments) > MAX_MEEGLE_COMMENTS:
                    return {
                        "success": False,
                        "permanent": True,
                        "error_code": "feishu_thread_message_limit_exceeded",
                    }
            has_more = data.get("has_more") is True
            page_token = str(data.get("page_token") or "").strip()
            if not has_more:
                return {
                    "success": True,
                    "comments": comments,
                    "pages_read": page_num,
                }
            if not page_token:
                return {
                    "success": False,
                    "error_code": "delivery_boundary_contract_invalid",
                }
        return {
            "success": False,
            "permanent": True,
            "error_code": "feishu_thread_pagination_incomplete",
        }

    def list_replies(self, chat_id: str, thread_id: str) -> Mapping[str, Any]:
        return asyncio.run(self._list_replies(chat_id, thread_id))

    async def _add_reply(
        self,
        chat_id: str,
        thread_id: str,
        content: str,
        idempotency_uuid: str,
    ) -> Mapping[str, Any]:
        _require_provider_write_guard(
            "feishu_thread_reply",
            thread_id,
            chat_id=chat_id,
        )
        provider_claim = current_provider_write_claim()

        try:
            result = await asyncio.wait_for(
                self._adapter.send(
                    chat_id,
                    content,
                    metadata={
                        "thread_id": thread_id,
                        "idempotency_uuid": idempotency_uuid,
                        "_pnc_rca_external_write_guard": provider_claim,
                        "_pnc_rca_external_write_operation": ("feishu_thread_reply"),
                    },
                ),
                timeout=MEEGLE_COMMENT_PAGE_TIMEOUT_SECONDS,
            )
        except TimeoutError:
            return {
                "success": False,
                "outcome_uncertain": True,
                "error_code": "feishu_thread_reply_timeout",
                "error": "thread reply exceeded its deadline",
            }
        except Exception as exc:
            return {
                "success": False,
                "outcome_uncertain": True,
                "error_code": "feishu_thread_reply_unavailable",
                "error": type(exc).__name__,
            }
        if not result.success:
            response_like = type(
                "FeishuError",
                (),
                {"code": "", "msg": result.error or "thread reply failed"},
            )()
            value = self._error_result(response_like, "feishu_thread_reply_failed")
            value["outcome_uncertain"] = True
            return value
        remote_id = str(result.message_id or "").strip()
        if not _REMOTE_ID_RE.fullmatch(remote_id):
            return {
                "success": False,
                "outcome_uncertain": True,
                "error_code": "feishu_thread_reply_remote_id_missing",
            }
        return {"success": True, "remote_id": remote_id}

    def add_reply(
        self,
        chat_id: str,
        thread_id: str,
        content: str,
        idempotency_uuid: str,
    ) -> Mapping[str, Any]:
        return asyncio.run(
            self._add_reply(chat_id, thread_id, content, idempotency_uuid)
        )


def _validate_card_patch_effect(claim: DeliveryEffectClaim) -> ValidatedEffect:
    if claim.outcome != "success":
        raise DeliveryContractError("delivery_card_patch_outcome_invalid")
    payload = validate_card_patch_effect_payload(claim.payload)
    expected = {
        "effect_key": claim.effect_key,
        "delivery_id": claim.delivery_id,
        "effect_kind": claim.effect_kind,
        "target_key": claim.target_key,
        "project_key": claim.project_key,
        "work_item_type_key": claim.work_item_type_key,
        "work_item_id": claim.work_item_id,
        "business_key": claim.business_key,
        "submission_key": claim.submission_key,
        "generation": claim.generation,
        "semantic_payload_sha256": claim.payload_sha256,
    }
    if any(payload.get(key) != value for key, value in expected.items()):
        raise DeliveryContractError("delivery_card_patch_effect_identity_invalid")
    card_payload = payload.get("card_payload")
    if not isinstance(card_payload, Mapping):
        raise DeliveryContractError("delivery_card_patch_payload_invalid")
    content = json.dumps(
        card_payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return ValidatedEffect(
        effect_kind=DELIVERY_CARD_PATCH_EFFECT_KIND,
        marker="",
        content=content,
        artifacts=(),
        chat_id=str(payload.get("chat_id") or ""),
        thread_id=str(payload.get("thread_id") or ""),
        idempotency_uuid=str(payload.get("idempotency_uuid") or ""),
        message_id=str(payload.get("message_id") or ""),
        submission_key=str(payload.get("submission_key") or ""),
        render_hash=str(payload.get("render_hash") or ""),
        card_payload=dict(card_payload),
    )


def _validate_effect(claim: DeliveryEffectClaim) -> ValidatedEffect:
    if claim.effect_kind not in DELIVERY_EFFECT_KINDS or claim.required is not True:
        raise DeliveryContractError(
            "delivery_effect_kind_unsupported",
            "dispatcher only accepts required RCA delivery effects",
        )
    if claim.effect_kind == DELIVERY_CARD_PATCH_EFFECT_KIND:
        return _validate_card_patch_effect(claim)
    if claim.outcome != "success":
        return _validate_terminal_effect(claim)
    payload = claim.payload
    if identifies_adjudication_effect(payload, target_key=claim.target_key):
        try:
            marker, content, field_updates = validate_adjudication_effect_claim(claim)
        except ConclusionAdjudicationError as exc:
            raise DeliveryContractError(str(exc)) from exc
        return ValidatedEffect(
            effect_kind=DELIVERY_EFFECT_KIND,
            marker=marker,
            content=content,
            artifacts=(),
            field_updates=field_updates,
        )
    raw_gate_a_projection = claim.contract.get("gate_a_projection")
    if raw_gate_a_projection is None:
        raise DeliveryContractError("delivery_gate_a_projection_required")
    try:
        gate_a_projection = validate_gate_a_projection(raw_gate_a_projection)
        if gate_a_projection["level"] == "L1_observation":
            gate_a_projection = validate_gate_a_projection(
                raw_gate_a_projection,
                identifier_binding=build_gate_a_identifier_binding(
                    claim.contract.get("consumer_capability")
                ),
            )
        expected_public_result = build_gate_a_public_result(gate_a_projection)
    except RcaEvidenceProjectionError as exc:
        raise DeliveryContractError("delivery_gate_a_projection_invalid") from exc
    if claim.contract.get("public_result") != expected_public_result:
        raise DeliveryContractError("delivery_gate_a_public_result_mismatch")
    schema_version = str(payload.get("schema_version") or "")
    if schema_version not in {
        DELIVERY_EFFECT_SCHEMA_VERSION_V3,
        DELIVERY_EFFECT_SCHEMA_VERSION,
    }:
        raise DeliveryContractError("delivery_effect_schema_unsupported")
    report_link_fields = {"report_link_kind"}
    expected_issue_url = str(claim.issue_url or "").strip()
    project_simple_name = str(payload.get("project_simple_name") or "").strip()
    issue_url_match = _FEISHU_ISSUE_URL_RE.fullmatch(expected_issue_url)
    canonical_issue_url = (
        f"https://project.feishu.cn/{project_simple_name}"
        f"/issue/detail/{claim.work_item_id}"
    )
    if (
        _PROJECT_SIMPLE_NAME_RE.fullmatch(project_simple_name) is None
        or issue_url_match is None
        or issue_url_match.group(1) != project_simple_name
        or issue_url_match.group(2) != claim.work_item_id
        or expected_issue_url != canonical_issue_url
    ):
        raise DeliveryContractError("delivery_issue_url_identity_mismatch")
    target: dict[str, Any]
    if claim.effect_kind == DELIVERY_EFFECT_KIND:
        target = {
            "schema_version": "pnc_rca_delivery_target_v1",
            "platform": "feishu_project",
            "project_key": claim.project_key,
            "work_item_type_key": claim.work_item_type_key,
            "work_item_id": claim.work_item_id,
            "output_cap": "L1",
        }
        exact_payload_keys = {
            "schema_version",
            "delivery_id",
            "effect_kind",
            "target_key",
            "project_key",
            "project_simple_name",
            "work_item_type_key",
            "work_item_id",
            "issue_url",
            "artifact_set_id",
            "report_url",
            "report_cifs_path",
            "report_status",
            "viz_mcap_vm",
            "foxglove_url",
            "requires_human_review",
            "conclusion",
            "effect_key",
            "semantic_payload_sha256",
            "marker",
            "comment_content",
            "field_updates",
            "terminal_class",
            "confidence_tier",
            "quality_oracle",
            "quality_oracle_sha256",
            *report_link_fields,
        }
        content_field = "comment_content"
    else:
        target = {
            "schema_version": "pnc_rca_delivery_target_v1",
            "platform": payload.get("platform"),
            "chat_id": payload.get("chat_id"),
            "thread_id": payload.get("thread_id"),
            "reply_anchor_message_id": payload.get("reply_anchor_message_id"),
            "source_message_id": payload.get("source_message_id"),
            "requester_id": payload.get("requester_id"),
            "reply_in_thread": payload.get("reply_in_thread"),
            "output_cap": payload.get("output_cap"),
        }
        exact_payload_keys = {
            "schema_version",
            "delivery_id",
            "effect_kind",
            "target_key",
            "project_key",
            "project_simple_name",
            "work_item_type_key",
            "work_item_id",
            "issue_url",
            "artifact_set_id",
            "report_url",
            "report_cifs_path",
            "report_status",
            "viz_mcap_vm",
            "foxglove_url",
            "requires_human_review",
            "conclusion",
            "platform",
            "chat_id",
            "thread_id",
            "reply_anchor_message_id",
            "source_message_id",
            "requester_id",
            "reply_in_thread",
            "output_cap",
            "effect_key",
            "semantic_payload_sha256",
            "marker",
            "idempotency_uuid",
            "message_content",
            "field_updates",
            "terminal_class",
            "confidence_tier",
            "quality_oracle",
            "quality_oracle_sha256",
            *report_link_fields,
        }
        content_field = "message_content"
    if schema_version == DELIVERY_EFFECT_SCHEMA_VERSION:
        exact_payload_keys.add("result_field_value")
        focus_effect = any(
            key in payload
            for key in ("issue_title", "issue_focus_sha256", "issue_focus_validation")
        )
        if focus_effect:
            exact_payload_keys.update(
                {"issue_title", "issue_focus_sha256", "issue_focus_validation"}
            )
    if set(payload) != exact_payload_keys:
        raise DeliveryContractError("delivery_effect_payload_shape_invalid")
    focus_validation = None
    focus_effect = schema_version == DELIVERY_EFFECT_SCHEMA_VERSION and any(
        key in payload
        for key in ("issue_title", "issue_focus_sha256", "issue_focus_validation")
    )
    contract_has_focus = (
        isinstance(claim.contract, Mapping)
        and claim.contract.get("issue_focus") is not None
    )
    if focus_effect:
        try:
            bound_title, focus_validation = validate_delivery_issue_focus(
                claim.contract,
                payload.get("issue_title"),
            )
        except DeliveryContractError as exc:
            raise DeliveryContractError("issue_focus_effect_binding_invalid") from exc
        raw_focus = claim.contract.get("issue_focus")
        if (
            payload.get("issue_title") != bound_title
            or not isinstance(raw_focus, Mapping)
            or payload.get("issue_focus_sha256") != issue_focus_payload_sha256(raw_focus)
            or payload.get("issue_focus_validation")
            != focus_validation.to_dict()
        ):
            raise DeliveryContractError("issue_focus_effect_binding_mismatch")
    elif contract_has_focus or str(claim.contract.get("schema_version") or "") == "g1q3_delivery_contract_v2":
        raise DeliveryContractError("issue_focus_effect_missing")
    validate_delivery_subscription_target(
        effect_kind=claim.effect_kind,
        target_key=claim.target_key,
        target=target,
        project_key=claim.project_key,
        work_item_type_key=claim.work_item_type_key,
        work_item_id=claim.work_item_id,
    )
    expected_identity = {
        "schema_version": schema_version,
        "delivery_id": claim.delivery_id,
        "effect_kind": claim.effect_kind,
        "target_key": claim.target_key,
        "project_key": claim.project_key,
        "work_item_type_key": claim.work_item_type_key,
        "work_item_id": claim.work_item_id,
        "artifact_set_id": claim.artifact_set_id,
        "report_url": claim.report_url,
        "issue_url": expected_issue_url,
    }
    for key, expected in expected_identity.items():
        if payload.get(key) != expected:
            raise DeliveryContractError(
                "delivery_effect_identity_mismatch", f"effect {key} is invalid"
            )
    manifest_submission_key = str(claim.manifest.get("submission_key") or "").strip()
    if not manifest_submission_key:
        raise DeliveryContractError("delivery_manifest_store_identity_mismatch")
    expected_viz_path = canonical_viz_mcap_path(manifest_submission_key)
    try:
        validate_report_url(
            claim.manifest.get("report_url"),
            submission_key=manifest_submission_key,
            artifact_set_id=claim.artifact_set_id,
        )
    except DeliveryContractError as exc:
        raise DeliveryContractError("delivery_effect_report_url_invalid") from exc
    has_viz_surface = bool(payload.get("viz_mcap_vm")) and bool(
        payload.get("foxglove_url")
    )
    if not has_viz_surface:
        raise DeliveryContractError("delivery_effect_foxglove_identity_mismatch")
    expected_report_cifs_path = canonical_viz_mcap_cifs_path(manifest_submission_key)
    if payload.get("report_cifs_path") != expected_report_cifs_path:
        raise DeliveryContractError("delivery_report_cifs_identity_mismatch")
    if (
        payload.get("viz_mcap_vm") != expected_viz_path
        or payload.get("foxglove_url") != claim.report_url
        or not validate_foxglove_url(
            payload.get("foxglove_url"), payload.get("viz_mcap_vm")
        )
    ):
        raise DeliveryContractError("delivery_effect_foxglove_identity_mismatch")
    if payload.get("report_link_kind") != DELIVERY_REPORT_LINK_KIND:
        raise DeliveryContractError("delivery_effect_report_link_kind_invalid")
    expected_human_review = payload.get("terminal_class") == CANDIDATE_HYPOTHESIS
    if (
        payload.get("requires_human_review") is not expected_human_review
        or payload.get("report_status") not in _VIZ_REPORT_STATUSES
    ):
        raise DeliveryContractError("delivery_effect_review_boundary_invalid")
    expected_report_link = claim.report_url
    if schema_version == DELIVERY_EFFECT_SCHEMA_VERSION:
        expected_result_field = render_public_rca_result_field(
            claim.contract,
            terminal_class=str(payload.get("terminal_class") or ""),
        )
        if payload.get("result_field_value") != expected_result_field:
            raise DeliveryContractError("delivery_result_field_projection_invalid")
    else:
        # v3 bound the result field directly to the comment conclusion.  Keep
        # that stored meaning intact instead of applying the v4 projection.
        expected_result_field = payload.get("conclusion")
    expected_field_updates = [
        {
            "field_key": RCA_RESULT_FIELD_KEY,
            "field_value": expected_result_field,
        },
        {
            "field_key": RCA_REPORT_FIELD_KEY,
            "field_value": expected_report_link,
        },
    ]
    if payload.get("field_updates") != expected_field_updates:
        raise DeliveryContractError("delivery_effect_field_updates_invalid")
    verified_artifacts = verify_persisted_artifact_inventory(
        manifest=claim.manifest,
        stored_artifacts=claim.artifacts,
        expected_artifact_set_id=claim.artifact_set_id,
    )
    semantic_sha = compute_delivery_effect_payload_sha256(payload, claim.effect_kind)
    if (
        semantic_sha != claim.payload_sha256
        or payload.get("semantic_payload_sha256") != semantic_sha
    ):
        raise DeliveryContractError("delivery_effect_payload_hash_mismatch")
    expected_effect_key = compute_delivery_effect_key(
        delivery_id=claim.delivery_id,
        effect_kind=claim.effect_kind,
        target_key=claim.target_key,
        semantic_payload_sha256=semantic_sha,
    )
    if (
        claim.effect_key != expected_effect_key
        or payload.get("effect_key") != expected_effect_key
    ):
        raise DeliveryContractError("delivery_effect_key_mismatch")
    marker = delivery_effect_marker(claim.effect_key, claim.artifact_set_id)
    content = str(payload.get(content_field) or "")
    if (
        payload.get("marker") != marker
        or not content
        or content.splitlines().count(marker) != 1
    ):
        raise DeliveryContractError("delivery_effect_marker_invalid")
    conclusion = payload.get("conclusion")
    if not isinstance(conclusion, str) or not conclusion:
        raise DeliveryContractError("delivery_effect_conclusion_invalid")
    if claim.effect_kind == DELIVERY_EFFECT_KIND:
        expected_content = build_issue_comment_content(
            marker=marker,
            work_item_id=claim.work_item_id,
            report_status=str(payload.get("report_status") or ""),
            conclusion=conclusion,
            report_url=claim.report_url,
            foxglove_url=str(payload.get("foxglove_url") or ""),
            report_cifs_path=expected_report_cifs_path,
            issue_url=expected_issue_url,
            terminal_class=str(payload.get("terminal_class") or ""),
        )
    else:
        expected_content = build_thread_reply_content(
            marker=marker,
            work_item_id=claim.work_item_id,
            report_status=str(payload.get("report_status") or ""),
            conclusion=conclusion,
            report_url=claim.report_url,
            foxglove_url=str(payload.get("foxglove_url") or ""),
            issue_url=expected_issue_url,
            requester_id=str(payload.get("requester_id") or ""),
            terminal_class=str(payload.get("terminal_class") or ""),
        )
    if content != expected_content:
        raise DeliveryContractError("delivery_effect_content_invalid")
    replay_contract = delivery_oracle_contract(
        claim.contract,
        focus_validation,
    )
    replayed_oracle = evaluate_structural_tier(
        replay_contract,
        publication_text=(
            f"{conclusion}\n{expected_result_field}\n{content}"
        ),
    )
    try:
        require_publishable(replayed_oracle)
    except TierOracleConflict as exc:
        raise DeliveryContractError(
            "classification_conflict",
            ",".join(exc.result.violations)
            or f"{exc.result.terminal_class}_not_publishable",
        ) from exc
    if (
        payload.get("terminal_class") != replayed_oracle.terminal_class
        or payload.get("confidence_tier") != replayed_oracle.confidence_tier
        or payload.get("quality_oracle") != replayed_oracle.as_dict()
        or payload.get("quality_oracle_sha256") != replayed_oracle.sha256()
    ):
        raise DeliveryContractError(
            "classification_conflict",
            "persisted quality oracle does not match replayed contract",
        )
    if focus_validation is not None:
        expected_focus_conclusion = render_public_rca_result(
            claim.contract,
            terminal_class=replayed_oracle.terminal_class,
        )
        if conclusion != expected_focus_conclusion:
            raise DeliveryContractError("issue_focus_conclusion_projection_invalid")
    if claim.effect_kind == DELIVERY_THREAD_EFFECT_KIND:
        expected_uuid = delivery_effect_idempotency_uuid(claim.effect_key)
        if payload.get("idempotency_uuid") != expected_uuid:
            raise DeliveryContractError("delivery_effect_idempotency_invalid")
    artifacts_by_role = {item.role: item for item in verified_artifacts}
    for role in ("index_html", "report_data"):
        artifact = artifacts_by_role.get(role)
        if artifact is None or artifact.required is not True:
            raise DeliveryContractError(f"delivery_{role}_identity_invalid")
    if not artifacts_by_role["index_html"].relative_path.lower().endswith(".html"):
        raise DeliveryContractError("required_html_artifact_invalid")
    if artifacts_by_role["index_html"].size > MAX_DELIVERY_INDEX_HTML_BYTES:
        raise DeliveryContractError("delivery_index_html_too_large")
    if not artifacts_by_role["report_data"].relative_path.lower().endswith(".json"):
        raise DeliveryContractError("required_report_data_artifact_invalid")
    html_validation = claim.manifest.get("html_validation")
    if (
        not isinstance(html_validation, Mapping)
        or html_validation.get("report_data_sha256")
        != artifacts_by_role["report_data"].sha256
    ):
        raise DeliveryContractError("html_validation_report_data_hash_mismatch")
    contract_artifacts = claim.contract.get("artifacts")
    publication = (
        contract_artifacts.get("viz_publication")
        if isinstance(contract_artifacts, Mapping)
        else None
    )
    publication_fields = {
        "schema_version",
        "status",
        "submission_key",
        "path",
        "size",
        "sha256",
        "manifest_path",
        "manifest_size",
        "manifest_sha256",
        "source_path",
        "source_sha256",
        "published_at",
    }
    if not isinstance(publication, Mapping) or set(publication) != publication_fields:
        raise DeliveryContractError("viz_publication_shape_invalid")
    publication_size = publication.get("size")
    publication_sha256 = str(publication.get("sha256") or "")
    if (
        publication.get("schema_version") != "g1q3_rca_viz_publication_v1"
        or publication.get("status") != "published"
        or publication.get("submission_key") != manifest_submission_key
        or publication.get("path") != expected_viz_path
        or isinstance(publication_size, bool)
        or not isinstance(publication_size, int)
        or publication_size <= 0
        or publication_size > MAX_DELIVERY_ARTIFACT_BYTES
        or _SHA256_RE.fullmatch(publication_sha256) is None
        or publication.get("source_sha256") != publication_sha256
    ):
        raise DeliveryContractError("viz_publication_identity_mismatch")
    # The public gate re-stats and hashes viz.mcap, then probes only the SPA
    # endpoint. A 200 response does not claim that CVEStudio parsed this file.
    artifact_requests = (
        (
            "viz_mcap",
            claim.report_url,
            publication_size,
            publication_sha256,
        ),
    )
    return ValidatedEffect(
        effect_kind=claim.effect_kind,
        marker=marker,
        content=content,
        artifacts=artifact_requests,
        field_updates=(
            (
                RCA_RESULT_FIELD_KEY,
                str(expected_result_field or ""),
            ),
            (RCA_REPORT_FIELD_KEY, str(expected_report_link or "")),
        )
        if claim.effect_kind == DELIVERY_EFFECT_KIND
        else (),
        chat_id=str(payload.get("chat_id") or ""),
        thread_id=str(payload.get("thread_id") or ""),
        idempotency_uuid=str(payload.get("idempotency_uuid") or ""),
    )


def _validate_terminal_effect(claim: DeliveryEffectClaim) -> ValidatedEffect:
    if claim.outcome not in TERMINAL_DELIVERY_OUTCOMES:
        raise DeliveryContractError("terminal_delivery_outcome_invalid")
    profile_terminal_public_target = (
        claim.effect_kind == DELIVERY_EFFECT_KIND
        and claim.terminal_error_code
        in OUTBOX_PROFILE_TERMINAL_WRITE_ERROR_CODES
        and "w3_execution_snapshot" not in claim.contract
    )
    if (
        claim.artifact_set_id != claim.outcome_key
        or not re.fullmatch(r"g1q3-rca-terminal-v1-[0-9a-f]{64}", claim.outcome_key)
        or (claim.issue_url and not profile_terminal_public_target)
        or claim.report_url
        or claim.manifest != {}
        or claim.artifacts != []
    ):
        raise DeliveryContractError("terminal_delivery_artifact_boundary_invalid")
    payload = claim.payload
    schema_version = str(payload.get("schema_version") or "")
    if schema_version == TERMINAL_DELIVERY_EFFECT_SCHEMA_VERSION_V1:
        diagnostic_code = ""
    elif schema_version in {
        TERMINAL_DELIVERY_EFFECT_SCHEMA_VERSION,
        TERMINAL_DELIVERY_EFFECT_SCHEMA_VERSION_COMMENT_ONLY,
    }:
        diagnostic_code = str(claim.contract.get("diagnostic_code") or "")
        diagnostic_detail = str(claim.contract.get("diagnostic_detail") or "")
    elif schema_version in {
        TERMINAL_FALLBACK_DELIVERY_EFFECT_SCHEMA_VERSION,
        TERMINAL_FALLBACK_DELIVERY_EFFECT_SCHEMA_VERSION_COMMENT_ONLY,
    }:
        diagnostic_code = ""
        diagnostic_detail = ""
    else:
        raise DeliveryContractError("terminal_delivery_schema_unsupported")
    if schema_version == TERMINAL_DELIVERY_EFFECT_SCHEMA_VERSION_V1:
        diagnostic_detail = ""
    primary = build_terminal_delivery(
        business_key=claim.business_key,
        submission_key=claim.submission_key,
        generation=claim.generation,
        project_key=claim.project_key,
        work_item_type_key=claim.work_item_type_key,
        work_item_id=claim.work_item_id,
        outcome=claim.outcome,
        terminal_state=claim.terminal_state,
        error_code=claim.terminal_error_code,
        diagnostic_code=diagnostic_code,
        diagnostic_detail=diagnostic_detail,
        terminal_fallback=(
            claim.contract.get("terminal_fallback")
            if schema_version
            in {
                TERMINAL_FALLBACK_DELIVERY_EFFECT_SCHEMA_VERSION,
                TERMINAL_FALLBACK_DELIVERY_EFFECT_SCHEMA_VERSION_COMMENT_ONLY,
            }
            else None
        ),
        schema_version=schema_version,
    )
    if profile_terminal_public_target:
        expected_issue_url = canonical_issue_url(
            claim.project_key,
            claim.work_item_id,
        )
        if (
            not claim.issue_url
            or claim.issue_url.rstrip("/") != expected_issue_url.rstrip("/")
        ):
            raise DeliveryContractError("delivery_issue_url_identity_mismatch")
    if claim.contract != primary.contract:
        raise DeliveryContractError("terminal_delivery_diagnostic_contract_invalid")
    if claim.effect_kind == DELIVERY_EFFECT_KIND:
        expected_key = primary.effect_key
        expected_sha = primary.semantic_payload_sha256
        expected_payload = primary.effect_payload
        content_field = "comment_content"
        target = {
            "schema_version": "pnc_rca_delivery_target_v1",
            "platform": "feishu_project",
            "project_key": claim.project_key,
            "work_item_type_key": claim.work_item_type_key,
            "work_item_id": claim.work_item_id,
            "output_cap": "L1",
        }
    else:
        target = {
            "schema_version": "pnc_rca_delivery_target_v1",
            "platform": payload.get("platform"),
            "chat_id": payload.get("chat_id"),
            "thread_id": payload.get("thread_id"),
            "reply_anchor_message_id": payload.get("reply_anchor_message_id"),
            "source_message_id": payload.get("source_message_id"),
            "requester_id": payload.get("requester_id"),
            "reply_in_thread": payload.get("reply_in_thread"),
            "output_cap": payload.get("output_cap"),
        }
        expected_key, expected_sha, expected_payload = (
            build_terminal_thread_reply_effect(
                issue_effect_payload=primary.effect_payload,
                target_key=claim.target_key,
                target=target,
            )
        )
        content_field = "message_content"
    validate_delivery_subscription_target(
        effect_kind=claim.effect_kind,
        target_key=claim.target_key,
        target=target,
        project_key=claim.project_key,
        work_item_type_key=claim.work_item_type_key,
        work_item_id=claim.work_item_id,
    )
    if (
        claim.delivery_id != primary.delivery_id
        or claim.outcome_key != primary.outcome_key
        or claim.effect_key != expected_key
        or claim.payload_sha256 != expected_sha
        or payload != expected_payload
    ):
        raise DeliveryContractError("terminal_delivery_effect_binding_invalid")
    content = str(payload.get(content_field) or "")
    marker = str(payload.get("marker") or "")
    if not content or content.splitlines().count(marker) != 1:
        raise DeliveryContractError("terminal_delivery_effect_marker_invalid")
    if schema_version in {
        TERMINAL_FALLBACK_DELIVERY_EFFECT_SCHEMA_VERSION,
        TERMINAL_FALLBACK_DELIVERY_EFFECT_SCHEMA_VERSION_COMMENT_ONLY,
    }:
        public_contract = claim.contract.get("public_contract")
        replayed_oracle = evaluate_structural_tier(
            public_contract if isinstance(public_contract, Mapping) else {},
            publication_text=f"{payload.get('conclusion') or ''}\n{content}",
        )
        try:
            require_publishable(replayed_oracle)
        except TierOracleConflict as exc:
            raise DeliveryContractError(
                "classification_conflict",
                ",".join(exc.result.violations)
                or f"{exc.result.terminal_class}_not_publishable",
            ) from exc
        if (
            replayed_oracle.schema_version != "pnc_rca_structural_tier_oracle_v2"
            or replayed_oracle.terminal_class != HONEST_NON_ATTRIBUTION
            or replayed_oracle.confidence_tier != "low"
            or payload.get("terminal_class") != replayed_oracle.terminal_class
            or payload.get("confidence_tier") != replayed_oracle.confidence_tier
            or payload.get("quality_oracle") != replayed_oracle.as_dict()
            or payload.get("quality_oracle_sha256") != replayed_oracle.sha256()
        ):
            raise DeliveryContractError(
                "classification_conflict",
                "terminal fallback quality oracle replay mismatch",
            )
    if claim.effect_kind == DELIVERY_THREAD_EFFECT_KIND:
        expected_uuid = delivery_effect_idempotency_uuid(claim.effect_key)
        if payload.get("idempotency_uuid") != expected_uuid:
            raise DeliveryContractError("delivery_effect_idempotency_invalid")
    field_updates: tuple[tuple[str, str], ...] = ()
    profile_terminal_comment_only = (
        claim.effect_kind == DELIVERY_EFFECT_KIND
        and claim.terminal_error_code
        in OUTBOX_PROFILE_TERMINAL_WRITE_ERROR_CODES
        and "w3_execution_snapshot" not in claim.contract
    )
    if (
        claim.effect_kind == DELIVERY_EFFECT_KIND
        and not profile_terminal_comment_only
        and schema_version in {
            TERMINAL_DELIVERY_EFFECT_SCHEMA_VERSION,
            TERMINAL_FALLBACK_DELIVERY_EFFECT_SCHEMA_VERSION,
        }
    ):
        field_updates = (
            (
                RCA_RESULT_FIELD_KEY,
                (
                    str(primary.effect_payload.get("conclusion") or "")
                    if schema_version
                    == TERMINAL_FALLBACK_DELIVERY_EFFECT_SCHEMA_VERSION
                    else primary.diagnostic_result
                ),
            ),
        )
    if schema_version in {
        TERMINAL_DELIVERY_EFFECT_SCHEMA_VERSION_COMMENT_ONLY,
        TERMINAL_FALLBACK_DELIVERY_EFFECT_SCHEMA_VERSION_COMMENT_ONLY,
    } and payload.get("field_updates") != []:
        raise DeliveryContractError("terminal_delivery_field_write_forbidden")
    return ValidatedEffect(
        effect_kind=claim.effect_kind,
        marker=marker,
        content=content,
        artifacts=(),
        field_updates=field_updates,
        chat_id=str(payload.get("chat_id") or ""),
        thread_id=str(payload.get("thread_id") or ""),
        idempotency_uuid=str(payload.get("idempotency_uuid") or ""),
    )


def _strict_comments(result: Any) -> tuple[list[dict[str, str]] | None, dict[str, Any]]:
    if not isinstance(result, Mapping):
        return None, {
            "success": False,
            "error_code": "delivery_boundary_contract_invalid",
            "error": "list_comments must return an object",
        }
    payload = dict(result)
    if payload.get("success") is not True:
        if payload.get("success") is not False:
            payload = {
                "success": False,
                "error_code": "delivery_boundary_contract_invalid",
                "error": "list_comments success must be boolean",
            }
        return None, payload
    comments = payload.get("comments")
    if not isinstance(comments, list):
        return None, {
            "success": False,
            "error_code": "delivery_boundary_contract_invalid",
            "error": "list_comments response must contain comments",
        }
    normalized: list[dict[str, str]] = []
    for item in comments:
        if not isinstance(item, Mapping):
            return None, {
                "success": False,
                "error_code": "delivery_boundary_contract_invalid",
            }
        remote_id = str(item.get("remote_id") or "").strip()
        content = item.get("content")
        if not _REMOTE_ID_RE.fullmatch(remote_id) or not isinstance(content, str):
            return None, {
                "success": False,
                "error_code": "delivery_boundary_contract_invalid",
                "error": "comment ids/content must be strict strings",
            }
        normalized.append({"remote_id": remote_id, "content": content})
    return normalized, payload


def _strict_field_values(
    result: Any,
    expected_keys: tuple[str, ...],
) -> tuple[dict[str, str] | None, dict[str, Any]]:
    if not isinstance(result, Mapping):
        return None, {
            "success": False,
            "error_code": "delivery_boundary_contract_invalid",
            "error": "get_fields must return an object",
        }
    payload = dict(result)
    if payload.get("success") is not True:
        if payload.get("success") is not False:
            payload = {
                "success": False,
                "error_code": "delivery_boundary_contract_invalid",
                "error": "get_fields success must be boolean",
            }
        return None, payload
    fields = payload.get("fields")
    if not isinstance(fields, Mapping):
        return None, {
            "success": False,
            "error_code": "delivery_boundary_contract_invalid",
            "error": "get_fields response must contain a fields object",
        }
    normalized: dict[str, str] = {}
    for key, value in fields.items():
        key_text = str(key)
        if key_text not in expected_keys or not isinstance(value, str):
            return None, {
                "success": False,
                "error_code": "delivery_boundary_contract_invalid",
                "error": "field read returned an unexpected key or value type",
            }
        normalized[key_text] = value.strip()
    return normalized, payload


def _field_updates_match(
    fields: Mapping[str, str], updates: tuple[tuple[str, str], ...]
) -> bool:
    return all(fields.get(key) == value for key, value in updates)


_NON_CAUSAL_RESULT_MARKERS = (
    "暂无法判断",
    "自动RCA未归因",
    "自动 RCA 未归因",
    "未形成归因",
    "未生成可确认",
    "无法形成确认归因",
    "事件在当前生产数据源中不存在",
    "问题数据事件在当前生产数据源中不存在",
    "无法建立可确认的因果链",
    "问题现象描述不足",
    "当前问题域暂不在",
    "证据不足以",
)


def _is_causal_result(value: str) -> bool:
    """Return whether a published result contains a concrete responsibility."""
    text = str(value or "").strip()
    if not text or any(marker in text for marker in _NON_CAUSAL_RESULT_MARKERS):
        return False
    for line in text.splitlines():
        if not line.startswith("责任模块："):
            continue
        responsibility = line.split("：", 1)[1].strip().rstrip("。")
        return bool(responsibility and responsibility not in {"暂无法判断", "未知"})
    return False


def _quality_regression_guard(
    before_fields: Mapping[str, str],
    updates: tuple[tuple[str, str], ...],
) -> bool:
    """Whether this issue-field update would replace a causal result with a non-causal one."""
    update_map = dict(updates)
    existing = str(before_fields.get(RCA_RESULT_FIELD_KEY) or "")
    proposed = str(update_map.get(RCA_RESULT_FIELD_KEY) or "")
    return _is_causal_result(existing) and not _is_causal_result(proposed)


def _marker_matches(
    comments: list[dict[str, str]], marker: str
) -> list[dict[str, str]]:
    variants = {marker}
    remote_marker = marker
    if marker.startswith("[") and marker.endswith("]"):
        # Meegle preserves the marker text but strips Markdown link brackets.
        remote_marker = marker[1:-1]
        variants.add(remote_marker)
    compact_variants = {variant.replace(" ", "") for variant in variants}
    return [
        item
        for item in comments
        if any(
            line in variants or line.replace(" ", "") in compact_variants
            for line in item["content"].splitlines()
        )
    ]


def _delivery_marker_from_content(content: str) -> str:
    markers = [
        line
        for line in str(content).splitlines()
        if (
            line.startswith("[RCA_DELIVERY:")
            or line.startswith("[RCA_TERMINAL:")
        )
        and line.endswith("]")
    ]
    return markers[0] if len(markers) == 1 else ""


def _canonical_remote_content(content: str, marker: str) -> str | None:
    lines = [line for line in content.splitlines() if line != ""]
    if not lines:
        return None
    remote_marker = (
        marker[1:-1] if marker.startswith("[") and marker.endswith("]") else marker
    )
    compact_markers = {marker.replace(" ", ""), remote_marker.replace(" ", "")}
    marker_indexes = [
        index
        for index, line in enumerate(lines)
        if line == marker
        or line == remote_marker
        or line.replace(" ", "") in compact_markers
    ]
    if len(marker_indexes) == 1:
        lines[marker_indexes[0]] = marker
        normalized: list[str] = []
        for line in lines:
            normalized.append(
                re.sub(
                    r"\[([^\]\n]+)\]\(([^)\n]+)\)",
                    lambda match: (
                        match.group(1)
                        if match.group(1) == match.group(2)
                        else match.group(0)
                    ),
                    line,
                )
            )
        return "\n".join(normalized)
    return None


_MEEGLE_NUMERIC_LIST_RE = re.compile(
    r"\[((?:-?\d+(?:\.\d+)?\s*,\s*)+-?\d+(?:\.\d+)?)\]"
)


def _canonical_meegle_numeric_lists(content: str) -> str:
    """Match Meegle readback after it removes numeric-list brackets."""
    return _MEEGLE_NUMERIC_LIST_RE.sub(r"\1", content)


def _confirmed_content_matches(
    comments: list[dict[str, str]], marker: str, expected_content: str
) -> list[dict[str, str]]:
    canonical_expected = _canonical_meegle_numeric_lists(expected_content)
    return [
        item
        for item in _marker_matches(comments, marker)
        if (
            (remote := _canonical_remote_content(item["content"], marker)) is not None
            and _canonical_meegle_numeric_lists(remote) == canonical_expected
        )
    ]


class DeliveryDispatcher:
    def __init__(
        self,
        *,
        store: RcaDeliveryStore,
        config: DispatcherConfig,
        list_comments: ListComments,
        add_comment: AddComment,
        report_verifier: ReportVerifier,
        get_fields: GetFields | None = None,
        update_fields: UpdateFields | None = None,
        list_thread_replies: ListThreadReplies | None = None,
        add_thread_reply: AddThreadReply | None = None,
        patch_task_card: PatchTaskCard | None = None,
        now: Callable[[], datetime] = _utc_now,
        lease_owner: str | None = None,
        _effect_lease_renew_interval_seconds: float | None = None,
    ):
        self.store = store
        self.config = config
        self.list_comments = list_comments
        self.add_comment = add_comment
        self.get_fields = get_fields
        self.update_fields = update_fields
        self.list_thread_replies = list_thread_replies
        self.add_thread_reply = add_thread_reply
        self.patch_task_card = patch_task_card
        self.report_verifier = report_verifier
        self.now = now
        self.lease_owner = lease_owner or (
            f"rca-delivery-dispatcher:{socket.gethostname()}:{os.getpid()}"
        )
        self.stats = DispatchStats()
        self.runtime_identity: Mapping[str, Any] | None = None
        self._effect_lease_renew_interval_seconds = float(
            EFFECT_LEASE_RENEW_INTERVAL_SECONDS
            if _effect_lease_renew_interval_seconds is None
            else _effect_lease_renew_interval_seconds
        )
        if (
            self._effect_lease_renew_interval_seconds <= 0
            or self._effect_lease_renew_interval_seconds
            > MAX_EFFECT_LEASE_RENEW_INTERVAL_SECONDS
        ):
            raise ValueError(
                "effect lease renewal interval must be greater than zero and at most "
                f"{MAX_EFFECT_LEASE_RENEW_INTERVAL_SECONDS} seconds"
            )
        self._active_effect_lease_keeper: _EffectLeaseKeeper | None = None
        self._active_effect_lease_identity: tuple[str, str, int] | None = None

    @staticmethod
    def _claim_lease_identity(claim: DeliveryEffectClaim) -> tuple[str, str, int]:
        return claim.effect_key, claim.lease_token, claim.fence

    def _keeper_for_claim(self, claim: DeliveryEffectClaim) -> _EffectLeaseKeeper:
        keeper = self._active_effect_lease_keeper
        if (
            keeper is None
            or self._active_effect_lease_identity != self._claim_lease_identity(claim)
        ):
            raise RuntimeError("delivery_effect_lease_keeper_not_active")
        return keeper

    def _extend_effect_lease(self, claim: DeliveryEffectClaim) -> None:
        self.store.extend_effect_lease(
            claim=claim,
            lease_seconds=self.config.lease_seconds,
            now=self.now(),
        )
        self.stats.lease_extensions += 1

    def _start_effect_lease_keeper(
        self, claim: DeliveryEffectClaim
    ) -> _EffectLeaseKeeper:
        if self._active_effect_lease_keeper is not None:
            raise RuntimeError("delivery_effect_lease_keeper_overlap")
        keeper = _EffectLeaseKeeper(
            lambda: self._extend_effect_lease(claim),
            interval_seconds=self._effect_lease_renew_interval_seconds,
            thread_name=(
                f"{SERVICE_LABEL}-effect-lease-{claim.fence}-{claim.lease_token[:8]}"
            ),
            on_background_renewal=lambda: setattr(
                self.stats,
                "effect_lease_keeper_renewals",
                self.stats.effect_lease_keeper_renewals + 1,
            ),
            on_failure=lambda: setattr(
                self.stats,
                "effect_lease_keeper_failures",
                self.stats.effect_lease_keeper_failures + 1,
            ),
        )
        self._active_effect_lease_keeper = keeper
        self._active_effect_lease_identity = self._claim_lease_identity(claim)
        self.stats.effect_lease_keeper_started += 1
        self.stats.effect_lease_keeper_active = 1
        try:
            keeper.start()
        except BaseException:
            self._active_effect_lease_keeper = None
            self._active_effect_lease_identity = None
            self.stats.effect_lease_keeper_active = 0
            raise
        return keeper

    def _stop_effect_lease_keeper(self, keeper: _EffectLeaseKeeper) -> None:
        try:
            keeper.stop()
        finally:
            if self._active_effect_lease_keeper is keeper:
                self._active_effect_lease_keeper = None
                self._active_effect_lease_identity = None
            self.stats.effect_lease_keeper_active = 0
            self.stats.effect_lease_keeper_stopped += 1

    def _heartbeat(self, claim: DeliveryEffectClaim) -> None:
        self._keeper_for_claim(claim).renew_now()

    def _settle_claim(
        self,
        claim: DeliveryEffectClaim,
        mutation: Callable[[], Any],
        *,
        renew: bool = True,
    ) -> Any:
        keeper = self._keeper_for_claim(claim)
        if renew:
            return keeper.settle(mutation)
        return keeper.settle_without_renewal(mutation)

    @staticmethod
    def _contract_value(contract: Mapping[str, Any], *paths: tuple[str, ...]) -> Any:
        for path in paths:
            current: Any = contract
            for key in path:
                if not isinstance(current, Mapping) or key not in current:
                    current = None
                    break
                current = current[key]
            if current is not None:
                return current
        return None

    def _delivery_observation_fields(
        self,
        claim: DeliveryEffectClaim,
        *,
        content: str,
        remote_id: str,
        delivered_at: datetime,
    ) -> dict[str, Any]:
        contract = claim.contract if isinstance(claim.contract, Mapping) else {}
        payload = claim.payload if isinstance(claim.payload, Mapping) else {}
        terminal_class = str(payload.get("terminal_class") or "")
        quality = str(contract.get("quality_classification") or "")
        raw_gate_a_projection = contract.get("gate_a_projection")
        gate_a_projection: dict[str, Any] | None = None
        if raw_gate_a_projection is not None:
            try:
                gate_a_projection = validate_gate_a_projection(raw_gate_a_projection)
            except RcaEvidenceProjectionError as exc:
                raise DeliveryObservationError(
                    "observation_gate_a_projection_invalid", exc.code
                ) from exc
        if gate_a_projection is not None:
            level = str(gate_a_projection["level"])
        elif terminal_class == HONEST_NON_ATTRIBUTION:
            level = "L0_abstain"
        elif quality == "supported_attribution":
            level = "L2_attribution"
        else:
            level = "L1_observation"
        if gate_a_projection is not None:
            evaluator_projection = gate_a_projection.get("evaluator_projection")
            evaluators = (
                evaluator_projection.get("evaluators")
                if isinstance(evaluator_projection, Mapping)
                else []
            )
            hit_keys = {
                str(evaluator.get("key") or "").strip()
                for evaluator in evaluators
                if isinstance(evaluator, Mapping)
                and evaluator.get("status") == "supported"
                and evaluator.get("evidence_refs")
                and str(evaluator.get("key") or "").strip()
            }
            evaluator_hit_count = len(hit_keys)
        else:
            upstream = contract.get("upstream_dispatch")
            hit_keys = (
                upstream.get("hit_evaluator_keys")
                if isinstance(upstream, Mapping)
                else None
            )
            evaluator_hit_count = (
                len({
                    str(value).strip() for value in hit_keys if str(value or "").strip()
                })
                if isinstance(hit_keys, (list, tuple))
                else 0
            )
        has_attribution = evaluator_hit_count > 0
        publication = self._contract_value(contract, ("artifacts", "viz_publication"))
        viz_published = (
            isinstance(publication, Mapping)
            and publication.get("status") == "published"
        )
        viz_bytes = (
            publication.get("size", 0) if isinstance(publication, Mapping) else 0
        )
        evidence_count = self._contract_value(
            contract,
            ("evidence_channel_msg_count",),
            ("evidence", "evidence_channel_msg_count"),
            ("consumer_capability", "evidence_channel_msg_count"),
            (
                "consumer_capability",
                "evidence",
                "viz_lineage",
                "evidence_channel_msg_count",
            ),
            (
                "consumer_capability",
                "evidence",
                "viz_lineage",
                "evaluator_topic_message_count",
            ),
        )
        evidence_count_reason = (
            "sealed_contract_does_not_expose_evidence_channel_msg_count"
            if not isinstance(evidence_count, int) or isinstance(evidence_count, bool)
            else None
        )
        if evidence_count_reason:
            evidence_count = None
        public_result = self._contract_value(
            contract,
            ("public_result",),
            ("public_contract", "public_result"),
        )
        evidence_summary = (
            public_result.get("evidence_summary")
            if isinstance(public_result, Mapping)
            else None
        )
        refs = (
            evidence_summary.get("refs")
            if isinstance(evidence_summary, Mapping)
            else None
        )
        refs_nonempty = (
            bool(refs) if isinstance(refs, (list, tuple, str, Mapping)) else None
        )
        refs_reason = (
            "sealed_contract_does_not_expose_evidence_refs"
            if refs_nonempty is None
            else None
        )
        try:
            # business_triggers.created_at is the durable pipeline acceptance
            # boundary.  The store falls back to effect creation only for
            # isolated fixtures that do not contain the control schema.
            started_at = datetime.fromisoformat(
                str(
                    claim.business_accepted_at or claim.effect_created_at or ""
                ).replace("Z", "+00:00")
            )
            if started_at.tzinfo is None or started_at.utcoffset() is None:
                raise ValueError("work_started_at must be timezone-aware")
            elapsed = (
                delivered_at.astimezone(timezone.utc)
                - started_at.astimezone(timezone.utc)
            ).total_seconds()
        except (TypeError, ValueError) as exc:
            raise DeliveryObservationError(
                "observation_pipeline_start_invalid", str(exc)
            ) from exc
        if elapsed < 0:
            raise DeliveryObservationError("observation_pipeline_elapsed_negative")
        contract_inventory_pin = (
            self._contract_value(
                contract,
                ("inventory_pin",),
                ("evaluator_inventory", "inventory_sha256"),
                ("consumer_capability", "inventory_sha256"),
                ("consumer_capability", "evaluator_inventory", "inventory_sha256"),
            )
            or claim.manifest.get("inventory_pin")
            or claim.manifest.get("inventory_sha256")
        )
        inventory_pin = str(getattr(self.config, "inventory_pin", "") or "").strip()
        if (
            contract_inventory_pin
            and str(contract_inventory_pin).strip() != inventory_pin
        ):
            raise DeliveryObservationError("observation_inventory_pin_mismatch")
        configured_release_id = str(
            getattr(self.config, "observation_release_id", "")
            or getattr(self.config, "quarantine_release_id", "")
            or ""
        ).strip()
        contract_release_id = str(contract.get("release_id") or "").strip()
        if contract_release_id and contract_release_id != configured_release_id:
            raise DeliveryObservationError("observation_release_id_mismatch")
        fields: dict[str, Any] = {
            "work_item_id": claim.work_item_id,
            "case_key": claim.submission_key or claim.business_key,
            "delivered_at": _utc_iso(delivered_at),
            "level": level,
            "has_attribution": has_attribution,
            "evaluator_hit_count": evaluator_hit_count,
            "viz_published": viz_published,
            "viz_bytes": viz_bytes,
            "evidence_channel_msg_count": evidence_count,
            "evidence_refs_nonempty": refs_nonempty,
            "pipeline_elapsed_seconds": elapsed,
            "outcome_content_sha256": hashlib.sha256(
                content.encode("utf-8")
            ).hexdigest(),
            "remote_receipt_id": remote_id,
            "release_id": configured_release_id,
            "inventory_pin": inventory_pin,
        }
        if evidence_count_reason:
            fields["evidence_channel_msg_count_not_measured_reason"] = (
                evidence_count_reason
            )
        if refs_reason:
            fields["evidence_refs_nonempty_not_measured_reason"] = refs_reason
        fields["observation_id"] = delivery_observation_id(fields)
        return fields

    def _prepare_delivery_observation(
        self,
        claim: DeliveryEffectClaim,
        *,
        content: str,
        remote_id: str,
        delivered_at: datetime,
    ) -> dict[str, Any]:
        if not getattr(self.config, "observability_enabled", False):
            error = DeliveryObservationError("observability_disabled")
            self._record_observability_error(error)
            raise error
        try:
            return build_delivery_observation(
                self._delivery_observation_fields(
                    claim,
                    content=content,
                    remote_id=remote_id,
                    delivered_at=delivered_at,
                )
            )
        except (
            DeliveryObservationError,
            OSError,
            TypeError,
            ValueError,
            RuntimeError,
        ) as exc:
            self._record_observability_error(exc)
            raise

    @staticmethod
    def _observability_error_code(exc: BaseException) -> str:
        if isinstance(exc, DeliveryObservationError):
            return f"{exc.code}:{exc.detail}" if exc.detail else exc.code
        return type(exc).__name__

    def _record_observability_error(self, exc: BaseException) -> None:
        code = self._observability_error_code(exc)
        self.stats.observability_errors += 1
        self.stats.observability_last_error = code
        self.stats.observability_current_error = code

    def _flush_pending_delivery_observations(self) -> None:
        """Recover committed intents before admitting another provider write."""
        if not getattr(self.config, "observability_enabled", False):
            return
        path = getattr(self.config, "observability_path", default_observation_path())
        try:
            with delivery_observation_file_lock(path):
                ensure_delivery_observation_path(path)
                all_intents = self.store.list_delivery_observations()
                pending_intents = [
                    intent for intent in all_intents if intent.status == "pending"
                ]
                try:
                    snapshot = read_delivery_observation_receipt(path)
                except DeliveryObservationError as original_error:
                    if original_error.code != "observation_receipt_line_invalid":
                        raise
                    snapshot = None
                    for intent in pending_intents:
                        try:
                            append_result = append_delivery_observation_verified(
                                path, intent.payload
                            )
                        except DeliveryObservationError as candidate_error:
                            if candidate_error.code == "observation_receipt_duplicate_id":
                                try:
                                    snapshot = read_delivery_observation_receipt(path)
                                except DeliveryObservationError as reread_error:
                                    if reread_error.code == "observation_receipt_line_invalid":
                                        continue
                                    raise
                                break
                            if candidate_error.code == "observation_receipt_line_invalid":
                                continue
                            raise
                        snapshot = append_result.receipt
                        self.stats.observability_written += 1
                        break
                    if snapshot is None:
                        raise original_error
                observed_hashes = dict(snapshot.payload_sha256_by_id)

                self._reconcile_delivery_observation_receipt(
                    observed_hashes,
                    all_intents,
                    require_all=False,
                )

                acknowledged: list[tuple[str, str]] = []
                while True:
                    intents = self.store.list_pending_delivery_observations(limit=1000)
                    if not intents:
                        break
                    for intent in intents:
                        observed_sha256 = observed_hashes.get(intent.observation_id)
                        if observed_sha256 is None:
                            append_result = append_delivery_observation_verified(
                                path, intent.payload
                            )
                            appended_sha256 = delivery_observation_payload_sha256(
                                append_result.observation
                            )
                            if appended_sha256 != intent.payload_sha256:
                                raise DeliveryObservationError(
                                    "observation_outbox_payload_hash_mismatch",
                                    intent.observation_id,
                                )
                            observed_hashes = dict(
                                append_result.receipt.payload_sha256_by_id
                            )
                            self.stats.observability_written += 1
                        elif observed_sha256 != intent.payload_sha256:
                            raise DeliveryObservationError(
                                "observation_receipt_payload_hash_mismatch",
                                intent.observation_id,
                            )
                    # The append helper binds its own FD, but callers can still
                    # replace the pathname after it returns. Reopen and verify
                    # the live path before any durable outbox acknowledgement.
                    premark_snapshot = read_delivery_observation_receipt(path)
                    observed_hashes = dict(
                        premark_snapshot.payload_sha256_by_id
                    )
                    self._reconcile_delivery_observation_receipt(
                        observed_hashes,
                        self.store.list_delivery_observations(),
                        require_all=False,
                    )
                    for intent in intents:
                        if (
                            observed_hashes.get(intent.observation_id)
                            != intent.payload_sha256
                        ):
                            raise DeliveryObservationError(
                                "observation_appended_receipt_missing",
                                intent.observation_id,
                            )
                        marked = self.store.mark_delivery_observation_appended(
                            observation_id=intent.observation_id,
                            payload_sha256=intent.payload_sha256,
                            now=self.now(),
                        )
                        if marked:
                            acknowledged.append(
                                (intent.observation_id, intent.payload_sha256)
                            )

                try:
                    final_snapshot = read_delivery_observation_receipt(path)
                    observed_hashes = dict(final_snapshot.payload_sha256_by_id)
                    all_intents = self.store.list_delivery_observations()
                    self._reconcile_delivery_observation_receipt(
                        observed_hashes,
                        all_intents,
                        require_all=True,
                    )
                except Exception:
                    if acknowledged:
                        self.store.requeue_delivery_observations(
                            observations=acknowledged,
                        )
                    raise
                self.stats.observability_current_error = ""
        except (
            DeliveryObservationError,
            OSError,
            TypeError,
            ValueError,
            RuntimeError,
        ) as exc:
            self._record_observability_error(exc)
            raise

    @staticmethod
    def _reconcile_delivery_observation_receipt(
        observed_hashes: Mapping[str, str],
        intents: list[Any],
        *,
        require_all: bool,
    ) -> None:
        outbox_hashes: dict[str, str] = {}
        appended_ids: set[str] = set()
        for intent in intents:
            if intent.observation_id in outbox_hashes:
                raise DeliveryObservationError(
                    "observation_outbox_duplicate_id", intent.observation_id
                )
            outbox_hashes[intent.observation_id] = intent.payload_sha256
            if intent.status == "appended":
                appended_ids.add(intent.observation_id)
            elif intent.status != "pending":
                raise DeliveryObservationError(
                    "observation_outbox_status_invalid", str(intent.status)
                )
        for observation_id, observed_sha256 in observed_hashes.items():
            expected_sha256 = outbox_hashes.get(observation_id)
            if expected_sha256 is None:
                raise DeliveryObservationError(
                    "observation_receipt_untracked_id", observation_id
                )
            if observed_sha256 != expected_sha256:
                raise DeliveryObservationError(
                    "observation_receipt_payload_hash_mismatch", observation_id
                )
        required_ids = set(outbox_hashes) if require_all else appended_ids
        missing = required_ids.difference(observed_hashes)
        if missing:
            raise DeliveryObservationError(
                "observation_appended_receipt_missing", min(missing)
            )

    def _lease_lost(self, claim: DeliveryEffectClaim) -> DispatchOutcome:
        self.stats.lease_lost += 1
        return DispatchOutcome(
            status="lease_lost",
            effect_key=claim.effect_key,
            delivery_id=claim.delivery_id,
            attempt=claim.attempt,
            error_code="stale_delivery_effect_lease",
        )

    def _preflight_delivery_observation(
        self,
        claim: DeliveryEffectClaim,
        *,
        content: str,
    ) -> None:
        """Validate all observation fields before crossing a provider write boundary."""
        self._prepare_delivery_observation(
            claim,
            content=content,
            remote_id=f"preflight:{claim.request_id}",
            delivered_at=self.now(),
        )

    def _adjudication_binding_gate(
        self,
        claim: DeliveryEffectClaim,
        *,
        after_outward_boundary: bool,
    ) -> DispatchOutcome | None:
        try:
            self.store.validate_adjudication_effect_binding(
                claim=claim,
                now=self.now(),
            )
        except ConclusionAdjudicationError as exc:
            error_code = str(exc)
            if (
                error_code == "conclusion_adjudication_effect_activation_stale"
                or after_outward_boundary
            ):
                return self._adjudication_error_outcome(claim, exc)
            return self._quarantine(
                claim,
                error_code=error_code,
                detail="adjudication effect is not bound to its immutable ledger",
            )
        return None

    def _validate_external_write(
        self,
        claim: DeliveryEffectClaim,
        *,
        operation: str,
        target: str,
    ) -> dict[str, Any]:
        """Revalidate the immutable W5 fence immediately before a provider call."""
        try:
            self.store.validate_learning_lane_external_operation(
                business_key=claim.business_key,
                generation=claim.generation,
                operation=operation,
            )
        except RuntimeError as exc:
            if str(exc) in {
                "learning_lane_external_effect_forbidden",
                "learning_lane_admission_missing",
            }:
                raise ExternalWriteFenceError(str(exc)) from exc
            raise
        contract = claim.contract if isinstance(claim.contract, Mapping) else {}
        binding = contract.get("w3_execution_snapshot")
        if not isinstance(binding, Mapping):
            terminal_rerun = self._terminal_rerun_external_write_binding(
                claim,
                operation=operation,
                require_write_started=False,
            )
            if terminal_rerun is not None:
                return terminal_rerun
            if claim.effect_kind in {
                DELIVERY_EFFECT_KIND,
                DELIVERY_THREAD_EFFECT_KIND,
            }:
                try:
                    manual_claim = self._manual_provider_write_claim(claim)
                    if manual_claim is None:
                        raise RecordConflictError(
                            "manual_external_write_source_not_manual"
                        )
                    operation_kwargs = self._provider_operation_kwargs(
                        claim, operation
                    )
                    return dict(
                        revalidate_provider_write_claim(
                            manual_claim,
                            operation=operation,
                            **operation_kwargs,
                        )
                    )
                except (RecordConflictError, ExternalWriteFenceError) as exc:
                    if isinstance(exc, ExternalWriteFenceError):
                        raise
                    if str(exc) != "manual_external_write_source_not_manual":
                        raise ExternalWriteFenceError(str(exc)) from exc
            profile_terminal = self._profile_terminal_external_write_binding(
                claim,
                operation=operation,
                require_write_started=False,
            )
            if profile_terminal is not None:
                return profile_terminal
            if claim.effect_kind == DELIVERY_CARD_PATCH_EFFECT_KIND:
                raise ExternalWriteFenceError("external_write_fence_missing")
            if self.store.is_historical_external_write_effect(claim.effect_created_at):
                return self._validate_historical_external_write_epoch(claim)
            raise ExternalWriteFenceError("external_write_fence_missing")
        fence = binding.get("write_fence")
        core_sha = binding.get("snapshot_core_sha256")
        if not isinstance(fence, Mapping) or not core_sha:
            if claim.effect_kind == DELIVERY_CARD_PATCH_EFFECT_KIND:
                raise ExternalWriteFenceError("external_write_fence_missing")
            if self.store.is_historical_external_write_effect(claim.effect_created_at):
                return self._validate_historical_external_write_epoch(claim)
            raise ExternalWriteFenceError("external_write_fence_missing")
        try:
            live = self.store.validate_external_write_fence_binding(fence)
        except Exception as exc:
            code = str(exc) or "external_write_fence_epoch_not_current"
            if code not in {
                "external_write_fence_schema_invalid",
                "external_write_fence_epoch_not_current",
                "external_write_fence_operation_denied",
                "external_write_fence_identity_mismatch",
            }:
                code = "external_write_fence_epoch_not_current"
            raise ExternalWriteFenceError(code, str(exc)) from exc
        expected_thread = (
            str(live.get("thread_target") or "").strip()
            if claim.effect_kind == DELIVERY_THREAD_EFFECT_KIND
            else None
        )
        expected_issue = str(live.get("issue_target") or "").strip()
        expected_target_set_sha256 = str(live.get("target_set_sha256") or "").strip()
        if not expected_issue or not expected_target_set_sha256:
            raise ExternalWriteFenceError("external_write_fence_target_mismatch")
        if claim.effect_kind == DELIVERY_CARD_PATCH_EFFECT_KIND and (
            str(claim.payload.get("chat_id") or "").strip()
            != str(live.get("chat_id") or "").strip()
            or str(claim.payload.get("thread_id") or "").strip()
            != str(live.get("thread_target") or "").strip()
        ):
            raise ExternalWriteFenceError("external_write_fence_target_mismatch")
        validate_write_fence(
            fence,
            snapshot_core_sha256_value=str(core_sha),
            operation=operation,
            target=target,
            expected_epoch_id=live["epoch_id"],
            expected_ledger_id=live["ledger_id"],
            expected_business_key=claim.business_key,
            expected_submission_key=claim.submission_key,
            expected_generation=claim.generation,
            expected_issue_target=expected_issue,
            expected_thread_target=expected_thread,
            expected_target_set_sha256=expected_target_set_sha256,
            now=self.now(),
        )
        return dict(live)

    @staticmethod
    def _provider_operation_kwargs(
        claim: DeliveryEffectClaim, operation: str
    ) -> dict[str, str]:
        if operation == "feishu_thread_reply":
            return {
                "chat_id": str(claim.payload.get("chat_id") or "").strip(),
                "thread_id": str(claim.payload.get("thread_id") or "").strip(),
            }
        match = _FEISHU_ISSUE_URL_RE.fullmatch(str(claim.issue_url or "").strip())
        if match is None:
            raise ExternalWriteFenceError("external_write_fence_target_mismatch")
        return {
            "issue_project_key": match.group(1),
            "issue_work_item_id": match.group(2),
        }

    def _manual_provider_write_claim(
        self, claim: DeliveryEffectClaim
    ) -> ProviderWriteClaim | None:
        if not hasattr(self, "config"):
            return None
        control_store = RcaControlStore(
            self.config.control_db_path,
            require_current=True,
        )
        binding = control_store.manual_external_write_admission_for_effect(
            business_key=claim.business_key,
            submission_key=claim.submission_key,
            generation=claim.generation,
            delivery_id=claim.delivery_id,
            effect_kind=claim.effect_kind,
            target_key=claim.target_key,
        )
        if binding is None:
            return None
        return build_manual_provider_write_claim(
            binding["admission"], binding["source_identity"]
        )

    def _profile_terminal_external_write_binding(
        self,
        claim: DeliveryEffectClaim,
        *,
        operation: str,
        require_write_started: bool,
    ) -> dict[str, Any] | None:
        terminal_error_code = str(
            getattr(claim, "terminal_error_code", "") or ""
        ).strip()
        if (
            claim.effect_kind != DELIVERY_EFFECT_KIND
            or terminal_error_code not in OUTBOX_PROFILE_TERMINAL_WRITE_ERROR_CODES
        ):
            return None
        try:
            return self.store.validate_profile_terminal_external_write_binding(
                effect_key=claim.effect_key,
                delivery_id=claim.delivery_id,
                lease_token=claim.lease_token,
                lease_fence=claim.fence,
                operation=operation,
                issue_url=claim.issue_url,
                target_key=claim.target_key,
                business_key=claim.business_key,
                submission_key=claim.submission_key,
                generation=claim.generation,
                require_write_started=require_write_started,
                now=self.now(),
            )
        except RuntimeError as exc:
            code = str(exc)
            if code not in {
                "external_write_fence_schema_invalid",
                "external_write_fence_epoch_not_current",
                "external_write_fence_operation_denied",
                "external_write_fence_identity_mismatch",
                "external_write_fence_target_mismatch",
            }:
                code = "external_write_fence_identity_mismatch"
            raise ExternalWriteFenceError(code, str(exc)) from exc

    def _terminal_rerun_external_write_binding(
        self,
        claim: DeliveryEffectClaim,
        *,
        operation: str,
        require_write_started: bool,
    ) -> dict[str, Any] | None:
        if claim.effect_kind != DELIVERY_EFFECT_KIND or claim.generation < 2:
            return None
        try:
            return self.store.validate_terminal_rerun_external_write_binding(
                effect_key=claim.effect_key,
                delivery_id=claim.delivery_id,
                lease_token=claim.lease_token,
                lease_fence=claim.fence,
                operation=operation,
                issue_url=claim.issue_url,
                target_key=claim.target_key,
                business_key=claim.business_key,
                submission_key=claim.submission_key,
                generation=claim.generation,
                require_write_started=require_write_started,
                now=self.now(),
            )
        except RuntimeError as exc:
            code = str(exc)
            if code == "external_write_fence_identity_mismatch":
                conn = self.store._connect()
                try:
                    authority_present = False
                    for table in (
                        "rca_terminal_rerun_delivery_authorities",
                        "rca_historical_epoch_rerun_delivery_authorities",
                    ):
                        if not self.store._table_exists(conn, table):
                            continue
                        if (
                            conn.execute(
                                f"SELECT 1 FROM {table} "
                                "WHERE business_key = ? AND generation = ?",
                                (claim.business_key, claim.generation),
                            ).fetchone()
                            is not None
                        ):
                            authority_present = True
                            break
                finally:
                    conn.close()
                if not authority_present:
                    return None
            if code not in {
                "external_write_fence_schema_invalid",
                "external_write_fence_epoch_not_current",
                "external_write_fence_operation_denied",
                "external_write_fence_identity_mismatch",
                "external_write_fence_target_mismatch",
            }:
                code = "external_write_fence_identity_mismatch"
            raise ExternalWriteFenceError(code, str(exc)) from exc

    def _validate_historical_external_write_epoch(
        self, claim: DeliveryEffectClaim
    ) -> dict[str, Any]:
        try:
            epoch = require_resident_activation_epoch(
                self.store,
                allowed_states=RESIDENT_EXTERNAL_WRITE_STATES,
            )
        except ExternalWriteFenceError as exc:
            if exc.code == "resident_activation_epoch_state_invalid":
                raise StaleDeliveryEffectLeaseError(
                    f"delivery activation changed for {claim.effect_key}"
                ) from exc
            raise
        return dict(epoch)

    def _provider_write_guard(
        self,
        claim: DeliveryEffectClaim,
    ) -> ProviderWriteClaim:
        contract = claim.contract if isinstance(claim.contract, Mapping) else {}
        snapshot = contract.get("w3_execution_snapshot")
        fence = snapshot.get("write_fence") if isinstance(snapshot, Mapping) else None
        if isinstance(fence, Mapping):
            return build_write_fence_provider_claim(fence)
        terminal_rerun = self._terminal_rerun_external_write_binding(
            claim,
            operation="feishu_issue_comment",
            require_write_started=True,
        )
        if terminal_rerun is not None:
            return build_terminal_rerun_provider_claim(
                authority_sha256=terminal_rerun["authority_sha256"],
                outbox_id=terminal_rerun["outbox_id"],
                epoch_id=terminal_rerun["epoch_id"],
                activation_ledger_id=terminal_rerun["activation_ledger_id"],
                effect_key=terminal_rerun["effect_key"],
                delivery_id=terminal_rerun["delivery_id"],
                lease_token=terminal_rerun["lease_token"],
                lease_fence=terminal_rerun["lease_fence"],
                issue_target=terminal_rerun["issue_url"],
                target_key=terminal_rerun["target_key"],
                business_key=terminal_rerun["business_key"],
                submission_key=terminal_rerun["submission_key"],
                generation=terminal_rerun["generation"],
                project_key=terminal_rerun["project_key"],
                project_simple_name=terminal_rerun["project_simple_name"],
                work_item_type_key=terminal_rerun["work_item_type_key"],
                work_item_id=terminal_rerun["work_item_id"],
            )
        if claim.effect_kind in {DELIVERY_EFFECT_KIND, DELIVERY_THREAD_EFFECT_KIND}:
            manual_claim = self._manual_provider_write_claim(claim)
            if manual_claim is not None:
                return manual_claim
        profile_terminal = self._profile_terminal_external_write_binding(
            claim,
            operation="feishu_issue_comment",
            require_write_started=True,
        )
        if profile_terminal is not None:
            return build_profile_terminal_provider_claim(
                epoch_id=profile_terminal["epoch_id"],
                activation_ledger_id=profile_terminal["activation_ledger_id"],
                effect_key=profile_terminal["effect_key"],
                delivery_id=profile_terminal["delivery_id"],
                lease_token=profile_terminal["lease_token"],
                lease_fence=profile_terminal["lease_fence"],
                issue_target=profile_terminal["issue_url"],
                project_key=profile_terminal["project_key"],
                project_simple_name=profile_terminal["project_simple_name"],
                target_key=profile_terminal["target_key"],
                business_key=profile_terminal["business_key"],
                submission_key=profile_terminal["submission_key"],
                generation=profile_terminal["generation"],
                source_error_code=profile_terminal["source_error_code"],
            )
        if claim.effect_kind == DELIVERY_CARD_PATCH_EFFECT_KIND:
            raise ExternalWriteFenceError("external_write_fence_missing")
        epoch = self._validate_historical_external_write_epoch(claim)
        if claim.effect_kind == DELIVERY_THREAD_EFFECT_KIND:
            operations = ("feishu_thread_reply",)
        else:
            raw_field_updates = claim.payload.get("field_updates")
            has_field_updates = isinstance(raw_field_updates, list) and bool(
                raw_field_updates
            )
            operations = (
                ("feishu_issue_comment", "feishu_issue_field_update")
                if has_field_updates
                else ("feishu_issue_comment",)
            )
        return build_historical_epoch_provider_claim(
            epoch_id=str(epoch.get("epoch_id") or ""),
            effect_key=claim.effect_key,
            delivery_id=claim.delivery_id,
            lease_token=claim.lease_token,
            lease_fence=claim.fence,
            operations=operations,
            issue_target=claim.issue_url,
            chat_id=str(claim.payload.get("chat_id") or ""),
            thread_id=str(claim.payload.get("thread_id") or ""),
            submission_key=claim.submission_key,
        )

    @staticmethod
    def _adjudication_error_outcome(
        claim: DeliveryEffectClaim,
        exc: ConclusionAdjudicationError,
    ) -> DispatchOutcome:
        error_code = str(exc)
        return DispatchOutcome(
            status=(
                "activation_stale"
                if error_code == "conclusion_adjudication_effect_activation_stale"
                else "adjudication_invalid"
            ),
            effect_key=claim.effect_key,
            delivery_id=claim.delivery_id,
            attempt=claim.attempt,
            error_code=error_code,
        )

    def _retry(
        self,
        claim: DeliveryEffectClaim,
        *,
        error_code: str,
        detail: str,
        uncertain: bool,
        retry_after: Any = None,
        exact_delay_seconds: int | None = None,
    ) -> DispatchOutcome:
        delay_seconds = (
            retry_delay_seconds(claim.attempt, retry_after)
            if exact_delay_seconds is None
            else max(1, int(exact_delay_seconds))
        )
        mutation = self._settle_claim(
            claim,
            lambda: self.store.reschedule_effect(
                claim=claim,
                error_code=error_code,
                error_detail=detail,
                delay_seconds=delay_seconds,
                uncertain=uncertain,
                max_age_seconds=MAX_EFFECT_AGE_SECONDS,
                now=self.now(),
            ),
        )
        if mutation.effect_status == "quarantined":
            self.stats.quarantined += 1
        elif uncertain:
            self.stats.uncertain += 1
        else:
            self.stats.retried += 1
        return DispatchOutcome(
            status=mutation.effect_status,
            effect_key=claim.effect_key,
            delivery_id=claim.delivery_id,
            attempt=claim.attempt,
            error_code=error_code,
            next_attempt_at=mutation.next_attempt_at,
        )

    def _quarantine(
        self, claim: DeliveryEffectClaim, *, error_code: str, detail: str
    ) -> DispatchOutcome:
        self._settle_claim(
            claim,
            lambda: self.store.quarantine_effect(
                claim=claim,
                error_code=error_code,
                error_detail=detail,
                now=self.now(),
            ),
            renew=not identifies_adjudication_effect(
                claim.payload, target_key=claim.target_key
            ),
        )
        self.stats.quarantined += 1
        return DispatchOutcome(
            status="quarantined",
            effect_key=claim.effect_key,
            delivery_id=claim.delivery_id,
            attempt=claim.attempt,
            error_code=error_code,
        )

    def _open_circuit(
        self,
        claim: DeliveryEffectClaim,
        *,
        error_code: str,
        detail: str,
        uncertain: bool,
        retry_after: Any = None,
    ) -> DispatchOutcome:
        mutation = self._settle_claim(
            claim,
            lambda: self.store.reschedule_effect_and_open_circuit(
                claim=claim,
                error_code=error_code,
                error_detail=detail,
                delay_seconds=retry_delay_seconds(claim.attempt, retry_after),
                uncertain=uncertain,
                max_age_seconds=MAX_EFFECT_AGE_SECONDS,
                now=self.now(),
            ),
        )
        if mutation.effect_status == "quarantined":
            self.stats.quarantined += 1
        elif uncertain:
            self.stats.uncertain += 1
        else:
            self.stats.retried += 1
        self.stats.circuit_opened += 1
        return DispatchOutcome(
            status="circuit_open",
            effect_key=claim.effect_key,
            delivery_id=claim.delivery_id,
            attempt=claim.attempt,
            error_code=error_code,
            next_attempt_at=mutation.next_attempt_at,
        )

    def _boundary_failure(
        self,
        claim: DeliveryEffectClaim,
        result: Mapping[str, Any],
        *,
        uncertain_default: bool,
    ) -> DispatchOutcome:
        code = str(result.get("error_code") or "delivery_boundary_failed")[:120]
        detail = str(result.get("error") or code)[:1000]
        uncertain = bool(result.get("outcome_uncertain", uncertain_default))
        if code in _CIRCUIT_CODES:
            return self._open_circuit(
                claim,
                error_code=code,
                detail=detail,
                uncertain=uncertain,
                retry_after=result.get("retry_after_seconds"),
            )
        if result.get("permanent") is True:
            return self._quarantine(claim, error_code=code, detail=detail)
        return self._retry(
            claim,
            error_code=code,
            detail=detail,
            uncertain=uncertain,
            retry_after=result.get("retry_after_seconds"),
        )

    def dispatch_one(self) -> DispatchOutcome:
        self.stats.loops += 1
        if not self.config.enabled:
            return DispatchOutcome(status="disabled")
        self._flush_pending_delivery_observations()
        claim = self.store.claim_due_effect(
            lease_owner=self.lease_owner,
            lease_seconds=self.config.lease_seconds,
            max_age_seconds=MAX_EFFECT_AGE_SECONDS,
            now=self.now(),
            activation_required=self.config.activation_required,
        )
        if claim is None:
            circuits = self.store.delivery_dispatcher_circuits()
            for row in self.store.preview_dispatchable_effects(
                limit=100,
                activation_required=self.config.activation_required,
            ):
                circuit = circuits.get(str(row.get("effect_kind") or ""))
                if circuit is not None and circuit.is_open:
                    return DispatchOutcome(
                        status="circuit_open", error_code=circuit.reason_code
                    )
            self.stats.idle += 1
            return DispatchOutcome(status="idle")
        self.stats.claimed += 1
        keeper = self._start_effect_lease_keeper(claim)
        try:
            try:
                outcome = self._dispatch_claim(claim)
                keeper.checkpoint()
                return outcome
            except ConclusionAdjudicationError as exc:
                return self._adjudication_error_outcome(claim, exc)
            except StaleDeliveryEffectLeaseError:
                if identifies_adjudication_effect(
                    claim.payload, target_key=claim.target_key
                ):
                    try:
                        binding_failure = self._adjudication_binding_gate(
                            claim,
                            after_outward_boundary=True,
                        )
                    except StaleDeliveryEffectLeaseError:
                        binding_failure = None
                    if binding_failure is not None:
                        return binding_failure
                return self._lease_lost(claim)
        finally:
            self._stop_effect_lease_keeper(keeper)

    def _list_remote_effect(
        self, claim: DeliveryEffectClaim, validated: ValidatedEffect
    ) -> Mapping[str, Any]:
        if validated.effect_kind == DELIVERY_EFFECT_KIND:
            return self.list_comments(claim.project_key, claim.work_item_id)
        if validated.effect_kind != DELIVERY_THREAD_EFFECT_KIND:
            raise DeliveryContractError("delivery_effect_kind_unsupported")
        if self.list_thread_replies is None:
            return {
                "success": False,
                "error_code": "feishu_thread_dependency_unavailable",
                "error": "thread reply reader is not configured",
            }
        return self.list_thread_replies(validated.chat_id, validated.thread_id)

    def _read_field_updates(
        self, claim: DeliveryEffectClaim, validated: ValidatedEffect
    ) -> Mapping[str, Any]:
        if not validated.field_updates:
            return {"success": True, "fields": {}}
        if self.get_fields is None:
            return {
                "success": False,
                "error_code": "meegle_dependency_unavailable",
                "error": "work item field reader is not configured",
            }
        return self.get_fields(
            claim.project_key,
            claim.work_item_id,
            tuple(key for key, _value in validated.field_updates),
        )

    def _write_field_updates(
        self, claim: DeliveryEffectClaim, validated: ValidatedEffect
    ) -> Mapping[str, Any]:
        if not validated.field_updates:
            return {"success": True}
        if self.update_fields is None:
            return {
                "success": False,
                "outcome_uncertain": False,
                "error_code": "meegle_dependency_unavailable",
                "error": "work item field writer is not configured",
            }
        return self.update_fields(
            claim.project_key,
            claim.work_item_id,
            validated.field_updates,
        )

    def _add_remote_effect(
        self, claim: DeliveryEffectClaim, validated: ValidatedEffect
    ) -> Mapping[str, Any]:
        if validated.effect_kind == DELIVERY_EFFECT_KIND:
            return self.add_comment(
                claim.project_key, claim.work_item_id, validated.content
            )
        if validated.effect_kind != DELIVERY_THREAD_EFFECT_KIND:
            raise DeliveryContractError("delivery_effect_kind_unsupported")
        if self.add_thread_reply is None:
            return {
                "success": False,
                "outcome_uncertain": False,
                "error_code": "feishu_thread_dependency_unavailable",
                "error": "thread reply writer is not configured",
            }
        return self.add_thread_reply(
            validated.chat_id,
            validated.thread_id,
            validated.content,
            validated.idempotency_uuid,
        )

    def _verify_report_artifacts(
        self,
        claim: DeliveryEffectClaim,
        validated: ValidatedEffect,
        *,
        uncertain: bool,
    ) -> DispatchOutcome | None:
        for role, artifact_url, expected_size, expected_sha256 in validated.artifacts:
            self._heartbeat(claim)
            try:
                verification_raw = self.report_verifier(
                    artifact_url,
                    expected_size,
                    expected_sha256,
                )
            except Exception as exc:
                self._heartbeat(claim)
                return self._retry(
                    claim,
                    error_code="report_http_unavailable",
                    detail=type(exc).__name__,
                    uncertain=uncertain,
                )
            self._heartbeat(claim)
            if not isinstance(verification_raw, Mapping):
                return self._open_circuit(
                    claim,
                    error_code="delivery_boundary_contract_invalid",
                    detail="report_verifier must return an object",
                    uncertain=uncertain,
                )
            verification = dict(verification_raw)
            if verification.get("success") is not True:
                if verification.get("success") is not False:
                    verification = {
                        "success": False,
                        "error_code": "delivery_boundary_contract_invalid",
                    }
                return self._boundary_failure(
                    claim, verification, uncertain_default=uncertain
                )
            if (
                verification.get("status_code") != 200
                or verification.get("content_length") != expected_size
                or verification.get("sha256") != expected_sha256
            ):
                return self._quarantine(
                    claim,
                    error_code="report_http_verification_mismatch",
                    detail=f"successful verifier response does not match sealed {role}",
                )
        return None

    def _complete_from_marker(
        self,
        claim: DeliveryEffectClaim,
        validated: ValidatedEffect,
        match: Mapping[str, str],
        *,
        source: str,
    ) -> DispatchOutcome:
        remote_id = str(match["remote_id"])
        delivered_at = self.now()
        observation = self._prepare_delivery_observation(
            claim,
            content=validated.content,
            remote_id=remote_id,
            delivered_at=delivered_at,
        )
        self._settle_claim(
            claim,
            lambda: self.store.complete_effect(
                claim=claim,
                outcome="reconciled",
                remote_id=remote_id,
                receipt={
                    "remote_id": remote_id,
                    "marker": validated.marker,
                    "source": source,
                    "confirmed_content_sha256": hashlib.sha256(
                        validated.content.encode("utf-8")
                    ).hexdigest(),
                    "confirmed_report_url": claim.report_url,
                    "confirmed_field_keys": [
                        key for key, _value in validated.field_updates
                    ],
                },
                observation=observation,
                runtime_identity=self.runtime_identity,
                now=delivered_at,
            ),
        )
        self._flush_pending_delivery_observations()
        self.stats.reconciled += 1
        return DispatchOutcome(
            status="reconciled",
            effect_key=claim.effect_key,
            delivery_id=claim.delivery_id,
            attempt=claim.attempt,
            remote_id=remote_id,
        )

    def _suppress_quality_regression(
        self,
        claim: DeliveryEffectClaim,
        before_fields: Mapping[str, str],
        validated: ValidatedEffect,
    ) -> DispatchOutcome:
        error_code = "delivery_result_quality_regression_prevented"
        detail = (
            "existing Feishu RCA result has a concrete responsibility; proposed "
            "result is non-causal, so fields and comment were left unchanged"
        )
        self._settle_claim(
            claim,
            lambda: self.store.suppress_effect_for_quality_regression(
                claim=claim,
                error_code=error_code,
                error_detail=detail,
                receipt={
                    "source": error_code,
                    "existing_result_sha256": hashlib.sha256(
                        str(before_fields.get(RCA_RESULT_FIELD_KEY) or "").encode(
                            "utf-8"
                        )
                    ).hexdigest(),
                    "proposed_result_sha256": hashlib.sha256(
                        dict(validated.field_updates)
                        .get(RCA_RESULT_FIELD_KEY, "")
                        .encode("utf-8")
                    ).hexdigest(),
                    "preserved_field_keys": [
                        RCA_RESULT_FIELD_KEY,
                        RCA_REPORT_FIELD_KEY,
                    ],
                },
                now=self.now(),
            ),
        )
        self.stats.reconciled += 1
        return DispatchOutcome(
            status="superseded",
            effect_key=claim.effect_key,
            delivery_id=claim.delivery_id,
            attempt=claim.attempt,
            error_code=error_code,
        )

    def _dispatch_card_patch(
        self,
        claim: DeliveryEffectClaim,
        validated: ValidatedEffect,
    ) -> DispatchOutcome:
        if self.patch_task_card is None:
            return self._open_circuit(
                claim,
                error_code="feishu_card_dependency_unavailable",
                detail="task card patch transport is not configured",
                uncertain=False,
            )
        if claim.write_phase == "write_started":
            return self._quarantine(
                claim,
                error_code="feishu_card_patch_outcome_unknown",
                detail=(
                    "a prior card PATCH crossed the outward boundary without an "
                    "exact readback; refusing a duplicate write"
                ),
            )
        try:
            self.store.validate_card_patch_effect_binding(
                claim=claim,
                now=self.now(),
            )
        except DeliveryRecordConflictError as exc:
            return self._quarantine(
                claim,
                error_code=str(exc)[:120],
                detail="card patch is not bound to its immutable adjudication",
            )
        try:
            self._validate_external_write(
                claim,
                operation="feishu_card_patch",
                target=claim.issue_url,
            )
        except ExternalWriteFenceError as exc:
            return self._quarantine(claim, error_code=exc.code, detail=exc.detail)
        self._heartbeat(claim)

        if claim.write_phase == "prewrite":
            try:
                superseded = self.store.mark_effect_write_started(
                    claim=claim,
                    now=self.now(),
                    activation_required=self.config.activation_required,
                )
            except DeliveryRecordConflictError as exc:
                return self._quarantine(
                    claim,
                    error_code="delivery_card_patch_write_phase_invalid",
                    detail=str(exc),
                )
            if superseded is not None:
                self.stats.reconciled += 1
                return DispatchOutcome(
                    status="superseded",
                    effect_key=claim.effect_key,
                    delivery_id=claim.delivery_id,
                    attempt=claim.attempt,
                    error_code="delivery_effect_superseded_by_newer_settled_fields",
                )
        elif claim.write_phase != "write_started":
            return self._quarantine(
                claim,
                error_code="delivery_card_patch_write_phase_invalid",
                detail="card patch claim has an invalid write phase",
            )

        self._heartbeat(claim)
        thread_anchor = validated.thread_id.removeprefix("topic:")
        target = f"feishu:{validated.chat_id}"
        if thread_anchor:
            target = f"{target}:{thread_anchor}"
        try:
            provider_claim = self._provider_write_guard(claim)
            with _bound_provider_write_guard(provider_claim):
                patch_raw = self.patch_task_card(
                    target,
                    dict(validated.card_payload or {}),
                    message_id=validated.message_id,
                    provider_claim=provider_claim,
                )
        except ExternalWriteFenceError as exc:
            return self._quarantine(claim, error_code=exc.code, detail=exc.detail)
        except Exception as exc:
            self._heartbeat(claim)
            return self._boundary_failure(
                claim,
                _card_patch_exception_result(exc),
                uncertain_default=True,
            )
        self._heartbeat(claim)
        if not isinstance(patch_raw, Mapping):
            return self._open_circuit(
                claim,
                error_code="delivery_boundary_contract_invalid",
                detail="patch_task_card must return an object",
                uncertain=True,
            )
        patch_result = dict(patch_raw)
        if patch_result.get("success") is not True:
            if patch_result.get("success") is not False:
                patch_result = {
                    "success": False,
                    "outcome_uncertain": True,
                    "error_code": "delivery_boundary_contract_invalid",
                    "error": "patch_task_card omitted a boolean success field",
                }
            if (
                patch_result.get("error_code") == "feishu_card_patch_message_expired"
                and patch_result.get("permanent") is True
                and patch_result.get("outcome_uncertain") is False
            ):
                self._settle_claim(
                    claim,
                    lambda: self.store.suppress_expired_card_patch(
                        claim=claim,
                        error_detail=str(
                            patch_result.get("error") or "Feishu card message expired"
                        ),
                        receipt={
                            "source": "card_message_expired",
                            "message_id": validated.message_id,
                            "render_hash": validated.render_hash,
                            "adjudication_id": str(
                                claim.payload.get("adjudication_id") or ""
                            ),
                            "conclusion_state": str(
                                claim.payload.get("conclusion_state") or ""
                            ),
                            "correction_effect_key": str(
                                claim.payload.get("correction_effect_key") or ""
                            ),
                            "error_code": "feishu_card_patch_message_expired",
                        },
                        now=self.now(),
                    ),
                )
                self.stats.reconciled += 1
                return DispatchOutcome(
                    status="suppressed",
                    effect_key=claim.effect_key,
                    delivery_id=claim.delivery_id,
                    attempt=claim.attempt,
                    error_code="feishu_card_patch_message_expired",
                )
            return self._boundary_failure(
                claim,
                patch_result,
                uncertain_default=True,
            )
        remote_id = str(patch_result.get("message_id") or "").strip()
        if remote_id != validated.message_id:
            return self._open_circuit(
                claim,
                error_code="delivery_boundary_contract_invalid",
                detail="card patch response did not confirm the exact message id",
                uncertain=True,
            )
        delivered_at = self.now()
        observation = self._prepare_delivery_observation(
            claim,
            content=_canonical_json(validated.card_payload or claim.payload),
            remote_id=remote_id,
            delivered_at=delivered_at,
        )
        try:
            self._settle_claim(
                claim,
                lambda: self.store.complete_effect(
                    claim=claim,
                    outcome="ack",
                    remote_id=remote_id,
                    receipt={
                        "remote_id": remote_id,
                        "source": "relay_card_patch",
                        "render_hash": validated.render_hash,
                        "adjudication_id": str(
                            claim.payload.get("adjudication_id") or ""
                        ),
                        "conclusion_state": str(
                            claim.payload.get("conclusion_state") or ""
                        ),
                        "correction_effect_key": str(
                            claim.payload.get("correction_effect_key") or ""
                        ),
                    },
                    observation=observation,
                    runtime_identity=self.runtime_identity,
                    now=delivered_at,
                ),
            )
        except DeliveryRecordConflictError as exc:
            # The provider already acknowledged this irreversible PATCH. Even
            # if the fenced settlement lost a race, preserve the exact
            # observation intent without repeating or settling the write.
            self.store.record_postwrite_delivery_observation(
                claim=claim,
                remote_id=remote_id,
                observation=observation,
                now=delivered_at,
            )
            self._flush_pending_delivery_observations()
            return DispatchOutcome(
                status="activation_stale",
                effect_key=claim.effect_key,
                delivery_id=claim.delivery_id,
                attempt=claim.attempt,
                error_code=str(exc)[:120],
                remote_id=remote_id,
            )
        self._flush_pending_delivery_observations()
        self.stats.delivered += 1
        return DispatchOutcome(
            status="succeeded",
            effect_key=claim.effect_key,
            delivery_id=claim.delivery_id,
            attempt=claim.attempt,
            remote_id=remote_id,
        )

    def _dispatch_claim(self, claim: DeliveryEffectClaim) -> DispatchOutcome:
        prior_write_uncertain = claim.previous_status == "uncertain"
        adjudication_effect = identifies_adjudication_effect(
            claim.payload, target_key=claim.target_key
        )
        try:
            validated = _validate_effect(claim)
        except DeliveryContractError as exc:
            return self._quarantine(claim, error_code=exc.code, detail=exc.detail)
        preflight_content = (
            _canonical_json(validated.card_payload or claim.payload)
            if claim.effect_kind == DELIVERY_CARD_PATCH_EFFECT_KIND
            else validated.content
        )
        self._preflight_delivery_observation(
            claim,
            content=preflight_content,
        )
        if claim.effect_kind == DELIVERY_CARD_PATCH_EFFECT_KIND:
            return self._dispatch_card_patch(claim, validated)
        if adjudication_effect:
            binding_failure = self._adjudication_binding_gate(
                claim,
                after_outward_boundary=False,
            )
            if binding_failure is not None:
                return binding_failure
        initial_operation = (
            "feishu_thread_reply"
            if claim.effect_kind == DELIVERY_THREAD_EFFECT_KIND
            else (
                "feishu_issue_field_update"
                if validated.field_updates
                else "feishu_issue_comment"
            )
        )
        initial_target = (
            str(claim.payload.get("thread_id") or "").strip()
            if claim.effect_kind == DELIVERY_THREAD_EFFECT_KIND
            else claim.issue_url
        )
        try:
            self._validate_external_write(
                claim,
                operation=initial_operation,
                target=initial_target,
            )
        except ExternalWriteFenceError as exc:
            return self._quarantine(claim, error_code=exc.code, detail=exc.detail)
        self._heartbeat(claim)

        superseded = self.store.suppress_terminal_effect_if_newer_settled_fields(
            claim=claim,
            now=self.now(),
        )
        if superseded is not None:
            self.stats.reconciled += 1
            return DispatchOutcome(
                status="superseded",
                effect_key=claim.effect_key,
                delivery_id=claim.delivery_id,
                attempt=claim.attempt,
                error_code="delivery_effect_superseded_by_newer_settled_fields",
            )

        self._heartbeat(claim)
        try:
            before_raw = self._list_remote_effect(claim, validated)
        except Exception as exc:
            if adjudication_effect:
                binding_failure = self._adjudication_binding_gate(
                    claim,
                    after_outward_boundary=True,
                )
                if binding_failure is not None:
                    return binding_failure
            self._heartbeat(claim)
            return self._retry(
                claim,
                error_code=(
                    "feishu_comment_list_unavailable"
                    if claim.effect_kind == DELIVERY_EFFECT_KIND
                    else "feishu_thread_read_unavailable"
                ),
                detail=type(exc).__name__,
                uncertain=prior_write_uncertain,
            )
        if adjudication_effect:
            binding_failure = self._adjudication_binding_gate(
                claim,
                after_outward_boundary=True,
            )
            if binding_failure is not None:
                return binding_failure
        self._heartbeat(claim)
        comments, before = _strict_comments(before_raw)
        if comments is None:
            if prior_write_uncertain:
                before = dict(before)
                before["outcome_uncertain"] = True
            return self._boundary_failure(claim, before, uncertain_default=False)
        expected_field_keys = tuple(key for key, _value in validated.field_updates)
        try:
            before_fields_raw = self._read_field_updates(claim, validated)
        except Exception as exc:
            if adjudication_effect:
                binding_failure = self._adjudication_binding_gate(
                    claim,
                    after_outward_boundary=True,
                )
                if binding_failure is not None:
                    return binding_failure
            self._heartbeat(claim)
            return self._retry(
                claim,
                error_code="feishu_field_read_unavailable",
                detail=type(exc).__name__,
                uncertain=prior_write_uncertain,
            )
        if adjudication_effect:
            binding_failure = self._adjudication_binding_gate(
                claim,
                after_outward_boundary=True,
            )
            if binding_failure is not None:
                return binding_failure
        self._heartbeat(claim)
        before_fields, before_field_result = _strict_field_values(
            before_fields_raw, expected_field_keys
        )
        if before_fields is None:
            if prior_write_uncertain:
                before_field_result = dict(before_field_result)
                before_field_result["outcome_uncertain"] = True
            return self._boundary_failure(
                claim, before_field_result, uncertain_default=False
            )
        if (
            claim.effect_kind == DELIVERY_EFFECT_KIND
            and not adjudication_effect
            and _quality_regression_guard(before_fields, validated.field_updates)
        ):
            return self._suppress_quality_regression(claim, before_fields, validated)
        fields_match = _field_updates_match(before_fields, validated.field_updates)
        marker_matches = _marker_matches(comments, validated.marker)
        if len(marker_matches) > 1:
            return self._quarantine(
                claim,
                error_code="delivery_remote_marker_duplicate",
                detail="multiple comments contain the exact delivery marker",
            )
        matches = _confirmed_content_matches(
            comments, validated.marker, validated.content
        )
        if marker_matches and not matches:
            return self._quarantine(
                claim,
                error_code="delivery_remote_content_mismatch",
                detail="delivery marker exists without the canonical effect content",
            )
        existing_marker = matches[0] if matches else None
        if existing_marker is not None and fields_match:
            return self._complete_from_marker(
                claim,
                validated,
                existing_marker,
                source="read_before_write",
            )
        if adjudication_effect and claim.adjudication_comment_attempt_count > 0:
            return self._retry(
                claim,
                error_code="conclusion_adjudication_reconciliation_read_only",
                detail=(
                    "the single correction-comment attempt was already consumed; "
                    "fields and comments are read-only until the exact effect is visible"
                ),
                uncertain=True,
                exact_delay_seconds=UNCERTAIN_RECONCILIATION_POLL_SECONDS,
            )
        if (
            adjudication_effect
            and not fields_match
            and (claim.write_phase == "write_started" or existing_marker is not None)
        ):
            return self._retry(
                claim,
                error_code="conclusion_adjudication_field_reconciliation_read_only",
                detail=(
                    "the correction crossed an outward-write boundary; mismatched fields "
                    "must not be rewritten during reconciliation"
                ),
                uncertain=True,
                exact_delay_seconds=UNCERTAIN_RECONCILIATION_POLL_SECONDS,
            )

        recovery_write_count = 0
        if (
            prior_write_uncertain
            and existing_marker is None
            and not adjudication_effect
        ):
            reconciliation = self.store.record_effect_reconciliation_miss(
                claim=claim,
                visibility_grace_seconds=(
                    self.config.reconciliation_visibility_grace_seconds
                ),
                minimum_missing_reads=self.config.reconciliation_min_missing_reads,
                recovery_interval_seconds=self.config.recovery_write_interval_seconds,
                max_recovery_writes=MAX_RECOVERY_WRITES,
                now=self.now(),
            )
            if reconciliation.recovery_limit_exceeded:
                detail = (
                    f"marker remained absent after {MAX_RECOVERY_WRITES} controlled "
                    "recovery writes"
                )
                self._settle_claim(
                    claim,
                    lambda: self.store.quarantine_effect_and_open_circuit(
                        claim=claim,
                        error_code="delivery_recovery_write_limit_exceeded",
                        error_detail=detail,
                        now=self.now(),
                    ),
                )
                self.stats.quarantined += 1
                self.stats.circuit_opened += 1
                return DispatchOutcome(
                    status="quarantined",
                    effect_key=claim.effect_key,
                    delivery_id=claim.delivery_id,
                    attempt=claim.attempt,
                    error_code="delivery_recovery_write_limit_exceeded",
                )
            if not reconciliation.recovery_eligible:
                return self._retry(
                    claim,
                    error_code="delivery_uncertain_reconciliation_pending",
                    detail=(
                        "prior write outcome is uncertain and the marker is not yet "
                        "visible inside the bounded reconciliation window"
                    ),
                    uncertain=True,
                    exact_delay_seconds=UNCERTAIN_RECONCILIATION_POLL_SECONDS,
                )
            self._heartbeat(claim)
            try:
                confirmation_raw = self._list_remote_effect(claim, validated)
            except Exception as exc:
                self._heartbeat(claim)
                return self._retry(
                    claim,
                    error_code=(
                        "feishu_comment_list_unavailable"
                        if claim.effect_kind == DELIVERY_EFFECT_KIND
                        else "feishu_thread_read_unavailable"
                    ),
                    detail=type(exc).__name__,
                    uncertain=True,
                    exact_delay_seconds=UNCERTAIN_RECONCILIATION_POLL_SECONDS,
                )
            self._heartbeat(claim)
            confirmation_comments, confirmation = _strict_comments(confirmation_raw)
            if confirmation_comments is None:
                confirmation = dict(confirmation)
                confirmation["outcome_uncertain"] = True
                return self._boundary_failure(
                    claim,
                    confirmation,
                    uncertain_default=True,
                )
            confirmation_marker_matches = _marker_matches(
                confirmation_comments, validated.marker
            )
            if len(confirmation_marker_matches) > 1:
                return self._quarantine(
                    claim,
                    error_code="delivery_remote_marker_duplicate",
                    detail="multiple comments contain the exact delivery marker",
                )
            confirmation_matches = _confirmed_content_matches(
                confirmation_comments, validated.marker, validated.content
            )
            if confirmation_marker_matches and not confirmation_matches:
                return self._quarantine(
                    claim,
                    error_code="delivery_remote_content_mismatch",
                    detail="delivery marker exists without the canonical effect content",
                )
            if confirmation_matches and fields_match:
                return self._complete_from_marker(
                    claim,
                    validated,
                    confirmation_matches[0],
                    source="recovery_read_before_write",
                )
            if confirmation_matches:
                existing_marker = confirmation_matches[0]
            else:
                recovery_authorization = self.store.authorize_effect_recovery_write(
                    claim=claim,
                    visibility_grace_seconds=(
                        self.config.reconciliation_visibility_grace_seconds
                    ),
                    minimum_missing_reads=self.config.reconciliation_min_missing_reads,
                    recovery_interval_seconds=self.config.recovery_write_interval_seconds,
                    max_recovery_writes=MAX_RECOVERY_WRITES,
                    now=self.now(),
                    activation_required=self.config.activation_required,
                )
                if recovery_authorization is None:
                    return self._retry(
                        claim,
                        error_code="delivery_recovery_write_rate_limited",
                        detail="controlled recovery write authorization is not yet due",
                        uncertain=True,
                        exact_delay_seconds=UNCERTAIN_RECONCILIATION_POLL_SECONDS,
                    )
                recovery_write_count = recovery_authorization
        # W4-C4 applies at the last point before any external write.  A claim
        # already reconciled above has no write boundary and needs no network
        # probe; all field/comment writes (including bounded recovery writes)
        # must prove the sealed primary report first.
        verification_failure = self._verify_report_artifacts(
            claim,
            validated,
            uncertain=prior_write_uncertain,
        )
        if verification_failure is not None:
            return verification_failure
        if not prior_write_uncertain and not (
            adjudication_effect and claim.adjudication_comment_attempt_count > 0
        ):
            try:
                self._validate_external_write(
                    claim,
                    operation=(
                        "feishu_issue_field_update"
                        if validated.field_updates
                        else (
                            "feishu_thread_reply"
                            if claim.effect_kind == DELIVERY_THREAD_EFFECT_KIND
                            else "feishu_issue_comment"
                        )
                    ),
                    target=(
                        str(claim.payload.get("thread_id") or "").strip()
                        if claim.effect_kind == DELIVERY_THREAD_EFFECT_KIND
                        else claim.issue_url
                    ),
                )
            except ExternalWriteFenceError as exc:
                return self._quarantine(claim, error_code=exc.code, detail=exc.detail)
            self._heartbeat(claim)
            try:
                superseded = self.store.mark_effect_write_started(
                    claim=claim,
                    now=self.now(),
                    activation_required=self.config.activation_required,
                )
            except ConclusionAdjudicationError as exc:
                return self._adjudication_error_outcome(claim, exc)
            if superseded is not None:
                self.stats.reconciled += 1
                return DispatchOutcome(
                    status="superseded",
                    effect_key=claim.effect_key,
                    delivery_id=claim.delivery_id,
                    attempt=claim.attempt,
                    error_code=("delivery_effect_superseded_by_newer_settled_fields"),
                )
        if not fields_match:
            try:
                self._validate_external_write(
                    claim,
                    operation="feishu_issue_field_update",
                    target=claim.issue_url,
                )
            except ExternalWriteFenceError as exc:
                return self._quarantine(claim, error_code=exc.code, detail=exc.detail)
            try:
                with _bound_provider_write_guard(self._provider_write_guard(claim)):
                    update_raw = self._write_field_updates(claim, validated)
            except ExternalWriteFenceError as exc:
                return self._quarantine(
                    claim,
                    error_code=exc.code,
                    detail=exc.detail,
                )
            except Exception as exc:
                if adjudication_effect:
                    binding_failure = self._adjudication_binding_gate(
                        claim,
                        after_outward_boundary=True,
                    )
                    if binding_failure is not None:
                        return binding_failure
                self._heartbeat(claim)
                return self._retry(
                    claim,
                    error_code="feishu_field_update_outcome_unknown",
                    detail=type(exc).__name__,
                    uncertain=True,
                )
            if adjudication_effect:
                binding_failure = self._adjudication_binding_gate(
                    claim,
                    after_outward_boundary=True,
                )
                if binding_failure is not None:
                    return binding_failure
            self._heartbeat(claim)
            if not isinstance(update_raw, Mapping):
                return self._open_circuit(
                    claim,
                    error_code="delivery_boundary_contract_invalid",
                    detail="update_fields must return an object",
                    uncertain=True,
                )
            update_result = dict(update_raw)
            if update_result.get("success") is not True:
                if update_result.get("success") is not False:
                    update_result = {
                        "success": False,
                        "outcome_uncertain": True,
                        "error_code": "delivery_boundary_contract_invalid",
                    }
                return self._boundary_failure(
                    claim, update_result, uncertain_default=True
                )
            try:
                confirmed_fields_raw = self._read_field_updates(claim, validated)
            except Exception as exc:
                if adjudication_effect:
                    binding_failure = self._adjudication_binding_gate(
                        claim,
                        after_outward_boundary=True,
                    )
                    if binding_failure is not None:
                        return binding_failure
                self._heartbeat(claim)
                return self._retry(
                    claim,
                    error_code="feishu_field_postwrite_read_unavailable",
                    detail=type(exc).__name__,
                    uncertain=True,
                )
            if adjudication_effect:
                binding_failure = self._adjudication_binding_gate(
                    claim,
                    after_outward_boundary=True,
                )
                if binding_failure is not None:
                    return binding_failure
            self._heartbeat(claim)
            confirmed_fields, confirmed_field_result = _strict_field_values(
                confirmed_fields_raw, expected_field_keys
            )
            if confirmed_fields is None:
                confirmed_field_result = dict(confirmed_field_result)
                confirmed_field_result["outcome_uncertain"] = True
                return self._boundary_failure(
                    claim, confirmed_field_result, uncertain_default=True
                )
            if not _field_updates_match(confirmed_fields, validated.field_updates):
                return self._retry(
                    claim,
                    error_code="feishu_field_postwrite_confirmation_mismatch",
                    detail="attribution result/report fields did not match readback",
                    uncertain=True,
                )
            fields_match = True
        if existing_marker is not None:
            return self._complete_from_marker(
                claim,
                validated,
                existing_marker,
                source="field_repair_after_marker",
            )
        if adjudication_effect:
            try:
                authorized = self.store.authorize_adjudication_comment_attempt(
                    claim=claim,
                    now=self.now(),
                    activation_required=self.config.activation_required,
                )
            except ConclusionAdjudicationError as exc:
                return self._adjudication_error_outcome(claim, exc)
            if not authorized:
                return self._retry(
                    claim,
                    error_code="conclusion_adjudication_reconciliation_read_only",
                    detail="the correction-comment attempt token was already consumed",
                    uncertain=True,
                    exact_delay_seconds=UNCERTAIN_RECONCILIATION_POLL_SECONDS,
                )
        try:
            self._validate_external_write(
                claim,
                operation=(
                    "feishu_thread_reply"
                    if claim.effect_kind == DELIVERY_THREAD_EFFECT_KIND
                    else "feishu_issue_comment"
                ),
                target=(
                    str(claim.payload.get("thread_id") or "").strip()
                    if claim.effect_kind == DELIVERY_THREAD_EFFECT_KIND
                    else claim.issue_url
                ),
            )
            with _bound_provider_write_guard(self._provider_write_guard(claim)):
                add_raw = self._add_remote_effect(claim, validated)
        except ExternalWriteFenceError as exc:
            return self._quarantine(claim, error_code=exc.code, detail=exc.detail)
        except Exception as exc:
            if adjudication_effect:
                binding_failure = self._adjudication_binding_gate(
                    claim,
                    after_outward_boundary=True,
                )
                if binding_failure is not None:
                    return binding_failure
            self._heartbeat(claim)
            return self._retry(
                claim,
                error_code=(
                    "feishu_comment_add_outcome_unknown"
                    if claim.effect_kind == DELIVERY_EFFECT_KIND
                    else "feishu_thread_reply_outcome_unknown"
                ),
                detail=type(exc).__name__,
                uncertain=True,
            )
        if adjudication_effect:
            binding_failure = self._adjudication_binding_gate(
                claim,
                after_outward_boundary=True,
            )
            if binding_failure is not None:
                return binding_failure
        self._heartbeat(claim)
        if not isinstance(add_raw, Mapping):
            return self._open_circuit(
                claim,
                error_code="delivery_boundary_contract_invalid",
                detail="add_comment must return an object",
                uncertain=True,
            )
        add_result = dict(add_raw)
        if add_result.get("success") is not True:
            if add_result.get("success") is not False:
                add_result = {
                    "success": False,
                    "outcome_uncertain": True,
                    "error_code": "delivery_boundary_contract_invalid",
                }
            elif recovery_write_count:
                # The original write remains ambiguous even if this recovery call
                # reports a definite failure.
                add_result["outcome_uncertain"] = True
            return self._boundary_failure(claim, add_result, uncertain_default=True)
        remote_id = str(add_result.get("remote_id") or "").strip()
        if not _REMOTE_ID_RE.fullmatch(remote_id):
            return self._retry(
                claim,
                error_code=(
                    "feishu_add_remote_id_missing"
                    if claim.effect_kind == DELIVERY_EFFECT_KIND
                    else "feishu_thread_reply_remote_id_missing"
                ),
                detail="delivery success did not contain a strict remote_id",
                uncertain=True,
            )

        self._heartbeat(claim)
        try:
            after_raw = self._list_remote_effect(claim, validated)
        except Exception as exc:
            if adjudication_effect:
                binding_failure = self._adjudication_binding_gate(
                    claim,
                    after_outward_boundary=True,
                )
                if binding_failure is not None:
                    return binding_failure
            self._heartbeat(claim)
            return self._retry(
                claim,
                error_code="feishu_postwrite_confirmation_unavailable",
                detail=type(exc).__name__,
                uncertain=True,
            )
        if adjudication_effect:
            binding_failure = self._adjudication_binding_gate(
                claim,
                after_outward_boundary=True,
            )
            if binding_failure is not None:
                return binding_failure
        self._heartbeat(claim)
        after_comments, after = _strict_comments(after_raw)
        if after_comments is None:
            code = str(after.get("error_code") or "delivery_boundary_failed")
            if code in _CIRCUIT_CODES:
                return self._open_circuit(
                    claim,
                    error_code=code,
                    detail=str(after.get("error") or code),
                    uncertain=True,
                    retry_after=after.get("retry_after_seconds"),
                )
            return self._retry(
                claim,
                error_code=code,
                detail=str(after.get("error") or code),
                uncertain=True,
                retry_after=after.get("retry_after_seconds"),
            )
        after_marker_matches = _marker_matches(after_comments, validated.marker)
        if len(after_marker_matches) > 1:
            return self._quarantine(
                claim,
                error_code="delivery_remote_marker_duplicate",
                detail="multiple comments contain the exact delivery marker",
            )
        after_content_matches = _confirmed_content_matches(
            after_comments, validated.marker, validated.content
        )
        if after_marker_matches and not after_content_matches:
            return self._quarantine(
                claim,
                error_code="delivery_remote_content_mismatch",
                detail="delivery marker exists without the canonical effect content",
            )
        confirmed = [
            item for item in after_content_matches if item["remote_id"] == remote_id
        ]
        if len(confirmed) != 1:
            return self._retry(
                claim,
                error_code="feishu_postwrite_confirmation_mismatch",
                detail="canonical content and add remote_id were not confirmed together",
                uncertain=True,
            )
        try:
            final_fields_raw = self._read_field_updates(claim, validated)
        except Exception as exc:
            if adjudication_effect:
                binding_failure = self._adjudication_binding_gate(
                    claim,
                    after_outward_boundary=True,
                )
                if binding_failure is not None:
                    return binding_failure
            self._heartbeat(claim)
            return self._retry(
                claim,
                error_code="feishu_field_final_read_unavailable",
                detail=type(exc).__name__,
                uncertain=True,
            )
        if adjudication_effect:
            binding_failure = self._adjudication_binding_gate(
                claim,
                after_outward_boundary=True,
            )
            if binding_failure is not None:
                return binding_failure
        self._heartbeat(claim)
        final_fields, final_field_result = _strict_field_values(
            final_fields_raw, expected_field_keys
        )
        if final_fields is None or not _field_updates_match(
            final_fields, validated.field_updates
        ):
            detail = str(final_field_result.get("error") or "field readback mismatch")
            return self._retry(
                claim,
                error_code="feishu_field_final_confirmation_mismatch",
                detail=detail,
                uncertain=True,
            )
        delivered_at = self.now()
        observation = self._prepare_delivery_observation(
            claim,
            content=validated.content,
            remote_id=remote_id,
            delivered_at=delivered_at,
        )
        self._settle_claim(
            claim,
            lambda: self.store.complete_effect(
                claim=claim,
                outcome="ack",
                remote_id=remote_id,
                receipt={
                    "remote_id": remote_id,
                    "marker": validated.marker,
                    "source": (
                        "read_after_recovery_write"
                        if recovery_write_count
                        else "read_after_write"
                    ),
                    "recovery_write_count": recovery_write_count,
                    "confirmed_content_sha256": hashlib.sha256(
                        validated.content.encode("utf-8")
                    ).hexdigest(),
                    "confirmed_report_url": claim.report_url,
                    "confirmed_field_keys": list(expected_field_keys),
                },
                observation=observation,
                runtime_identity=self.runtime_identity,
                now=delivered_at,
            ),
        )
        self._flush_pending_delivery_observations()
        self.stats.delivered += 1
        return DispatchOutcome(
            status="succeeded",
            effect_key=claim.effect_key,
            delivery_id=claim.delivery_id,
            attempt=claim.attempt,
            remote_id=remote_id,
        )

    def dispatch_batch(self) -> list[DispatchOutcome]:
        outcomes: list[DispatchOutcome] = []
        for _ in range(self.config.batch_size):
            outcome = self.dispatch_one()
            outcomes.append(outcome)
            if outcome.status in {"disabled", "idle", "circuit_open", "lease_lost"}:
                break
        return outcomes


class HealthReporter:
    def __init__(self, config: DispatcherConfig, store: RcaDeliveryStore):
        self.config = config
        self.store = store
        self.started_at = _utc_iso()
        self.runtime_identity = build_runtime_identity(
            service_label=SERVICE_LABEL,
            script_path=Path(__file__),
            public_config=config.public_dict(),
            loaded_dependencies=RCA_DELIVERY_DISPATCHER_LOADED_DEPENDENCIES,
        )

    def write(
        self,
        *,
        state: str,
        stats: DispatchStats,
        last_outcome: DispatchOutcome | None = None,
    ) -> None:
        circuits = self.store.delivery_dispatcher_circuits()
        circuit = circuits[DELIVERY_EFFECT_KIND]
        store_health = self.store.health(
            activation_required=self.config.activation_required,
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
        body = {
            "schema_version": HEALTH_SCHEMA_VERSION,
            "healthy": (
                state
                not in {
                    "error",
                    "circuit_open",
                    "uncertain",
                    "quarantined",
                    "lease_lost",
                }
                and not any(value.is_open for value in circuits.values())
                and (not self.config.enabled or store_health.get("ok") is True)
                and not (
                    self.config.observability_enabled
                    and bool(stats.observability_current_error)
                )
            ),
            "state": state,
            "started_at": self.started_at,
            "updated_at": _utc_iso(),
            "runtime_identity": self.runtime_identity.to_dict(),
            "config": self.config.public_dict(),
            "stats": asdict(stats),
            "effect_lease_keeper": {
                "enabled": True,
                "renew_interval_seconds": EFFECT_LEASE_RENEW_INTERVAL_SECONDS,
                "active": stats.effect_lease_keeper_active == 1,
                "started": stats.effect_lease_keeper_started,
                "stopped": stats.effect_lease_keeper_stopped,
                "renewals": stats.effect_lease_keeper_renewals,
                "failures": stats.effect_lease_keeper_failures,
            },
            "last_outcome": asdict(last_outcome) if last_outcome else None,
            "circuit": asdict(circuit),
            "circuits": {
                effect_kind: asdict(value) for effect_kind, value in circuits.items()
            },
            "store": store_health,
        }
        path = self.config.health_path
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
        temporary.write_text(
            json.dumps(body, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.chmod(temporary, 0o600)
        os.replace(temporary, path)


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


def run_dispatch_loop(
    dispatcher: DeliveryDispatcher,
    *,
    once: bool = False,
    stop_requested: Callable[[], bool] | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> int:
    stop = stop_requested or (lambda: False)
    health = HealthReporter(dispatcher.config, dispatcher.store)
    dispatcher.runtime_identity = health.runtime_identity.to_dict()
    if not dispatcher.config.enabled:
        outcome = DispatchOutcome(status="disabled")
        health.write(state="disabled", stats=dispatcher.stats, last_outcome=outcome)
        return 0
    while not stop():
        try:
            health.write(
                state="running",
                stats=dispatcher.stats,
            )
            with _PeriodicHeartbeat(
                lambda: health.write(
                    state="running",
                    stats=dispatcher.stats,
                ),
                interval_seconds=_heartbeat_interval_seconds(
                    dispatcher.config.health_max_age_seconds
                ),
            ):
                outcomes = dispatcher.dispatch_batch()
            last = outcomes[-1]
            health.write(state=last.status, stats=dispatcher.stats, last_outcome=last)
        except Exception as exc:
            last = DispatchOutcome(
                status="error", error_code=f"{type(exc).__name__}: {exc}"[:120]
            )
            health.write(state="error", stats=dispatcher.stats, last_outcome=last)
            if once:
                return 2
            sleep(dispatcher.config.poll_interval_seconds)
            continue
        if once:
            return 0
        if last.status == "circuit_open":
            sleep(dispatcher.config.circuit_poll_interval_seconds)
        elif last.status in {"idle", "disabled", "lease_lost"}:
            sleep(dispatcher.config.poll_interval_seconds)
    health.write(state="stopped", stats=dispatcher.stats)
    return 0


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check-config", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--health", action="store_true")
    parser.add_argument("--health-max-age-seconds", type=int)
    parser.add_argument(
        "--clear-circuit",
        action="store_true",
        help="plan or apply an audited reset of one delivery circuit",
    )
    parser.add_argument("--operator", help="bounded operator identity")
    parser.add_argument("--reason", help="bounded reset reason")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="apply a reset; without this flag --clear-circuit is read-only",
    )
    parser.add_argument(
        "--receipt",
        type=Path,
        help="absolute, non-existing path for the immutable reset receipt",
    )
    parser.add_argument(
        "--materialize-reset",
        metavar="RESET_ID",
        help="recover a receipt from the durable audit without changing the DB",
    )
    parser.add_argument(
        "--dispose-pre-w3-effect",
        dest="pre_w3_effect_keys",
        action="append",
        metavar="EFFECT_KEY",
        help="plan or quarantine one exact pre-W3 pending issue effect",
    )
    parser.add_argument(
        "--materialize-pre-w3-disposition",
        metavar="DISPOSITION_ID",
        help="recover an immutable receipt from a durable disposition audit",
    )
    parser.add_argument("--backup", type=Path, help="pre-mutation SQLite backup")
    parser.add_argument("--backup-sha256")
    parser.add_argument(
        "--expected-active-release-binding-sha256",
        help="binding SHA from the immediately preceding reset plan",
    )
    parser.add_argument(
        "--expected-config-binding-sha256",
        help="config SHA from the immediately preceding reset plan",
    )
    parser.add_argument(
        "--expected-plan-id",
        help="plan id from the immediately preceding reset plan",
    )
    parser.add_argument(
        "--expected-before-state-sha256",
        help="exact before-state SHA from the immediately preceding reset plan",
    )
    parser.add_argument("--expected-effect-set-sha256")
    parser.add_argument("--expected-database-logical-sha256")
    parser.add_argument("--expected-disposition-id")
    parser.add_argument("--expected-activation-sha256")
    parser.add_argument("--expected-live-env-sha256")
    parser.add_argument("--expected-tool-provenance-sha256")
    parser.add_argument(
        "--effect-kind",
        choices=sorted(DELIVERY_EFFECT_KINDS),
        default=DELIVERY_EFFECT_KIND,
    )
    return parser


def _validate_circuit_reset_arguments(args: argparse.Namespace) -> None:
    reset_only_flags = (args.operator, args.reason, args.receipt)
    expected_binding = args.expected_active_release_binding_sha256
    expected_config = args.expected_config_binding_sha256
    expected_plan = args.expected_plan_id
    expected_before = args.expected_before_state_sha256
    disposition_expected = (
        args.expected_effect_set_sha256,
        args.expected_database_logical_sha256,
        args.expected_disposition_id,
        args.expected_activation_sha256,
        args.expected_live_env_sha256,
        args.expected_tool_provenance_sha256,
    )
    modes = (
        bool(args.clear_circuit),
        bool(args.materialize_reset),
        bool(args.pre_w3_effect_keys),
        bool(args.materialize_pre_w3_disposition),
    )
    if sum(modes) > 1:
        raise ValueError("delivery_circuit_reset_modes_conflict")
    if args.materialize_reset:
        if (
            args.operator is not None
            or args.reason is not None
            or args.apply
            or expected_binding is not None
            or expected_config is not None
            or expected_plan is not None
            or expected_before is not None
            or any(value is not None for value in disposition_expected)
            or args.backup is not None
            or args.backup_sha256 is not None
        ):
            raise ValueError("delivery_circuit_reset_recovery_flags_conflict")
        if any((args.check_config, args.dry_run, args.health, args.once)):
            raise ValueError("delivery_circuit_reset_flags_conflict")
        if args.receipt is None:
            raise ValueError("delivery_circuit_reset_receipt_required")
        return
    if args.clear_circuit:
        if args.operator is None or args.reason is None:
            raise ValueError("delivery_circuit_reset_operator_and_reason_required")
        if any((args.check_config, args.dry_run, args.health, args.once)):
            raise ValueError("delivery_circuit_reset_flags_conflict")
        if args.receipt is None:
            raise ValueError("delivery_circuit_reset_receipt_required")
        if (
            any(value is not None for value in disposition_expected)
            or args.backup is not None
            or args.backup_sha256 is not None
        ):
            raise ValueError("pre_w3_effect_disposition_flags_conflict")
        if args.apply and expected_binding is None:
            raise ValueError(
                "delivery_circuit_reset_expected_active_binding_required"
            )
        if args.apply and expected_config is None:
            raise ValueError("delivery_circuit_reset_expected_config_required")
        if args.apply and expected_plan is None:
            raise ValueError("delivery_circuit_reset_expected_plan_required")
        if args.apply and expected_before is None:
            raise ValueError("delivery_circuit_reset_expected_before_required")
        if expected_binding is not None and (
            _SHA256_RE.fullmatch(str(expected_binding)) is None
            or str(expected_binding) == "0" * 64
        ):
            raise ValueError("delivery_circuit_reset_expected_active_binding_invalid")
        if expected_config is not None and (
            _SHA256_RE.fullmatch(str(expected_config)) is None
            or str(expected_config) == "0" * 64
        ):
            raise ValueError("delivery_circuit_reset_expected_config_invalid")
        if expected_plan is not None and (
            _SHA256_RE.fullmatch(str(expected_plan)) is None
            or str(expected_plan) == "0" * 64
        ):
            raise ValueError("delivery_circuit_reset_expected_plan_invalid")
        if expected_before is not None and (
            _SHA256_RE.fullmatch(str(expected_before)) is None
            or str(expected_before) == "0" * 64
        ):
            raise ValueError("delivery_circuit_reset_expected_before_invalid")
        return
    if args.materialize_pre_w3_disposition:
        if (
            args.operator is not None
            or args.reason is not None
            or args.apply
            or expected_binding is not None
            or expected_config is not None
            or expected_plan is not None
            or expected_before is not None
            or any(value is not None for value in disposition_expected)
            or args.backup is not None
            or args.backup_sha256 is not None
            or any((args.check_config, args.dry_run, args.health, args.once))
        ):
            raise ValueError("pre_w3_effect_disposition_recovery_flags_conflict")
        if args.receipt is None:
            raise ValueError("pre_w3_effect_disposition_receipt_required")
        return
    if args.pre_w3_effect_keys:
        if args.operator is None or args.reason is None:
            raise ValueError(
                "pre_w3_effect_disposition_operator_and_reason_required"
            )
        if args.receipt is None:
            raise ValueError("pre_w3_effect_disposition_receipt_required")
        if args.backup is None or args.backup_sha256 is None:
            raise ValueError("pre_w3_effect_disposition_backup_required")
        if args.effect_kind != DELIVERY_EFFECT_KIND:
            raise ValueError("pre_w3_effect_disposition_effect_kind_invalid")
        if any((args.check_config, args.dry_run, args.health, args.once)):
            raise ValueError("pre_w3_effect_disposition_flags_conflict")
        expected_values = (
            expected_binding,
            expected_config,
            expected_plan,
            expected_before,
            *disposition_expected,
        )
        if args.apply and any(value is None for value in expected_values):
            raise ValueError("pre_w3_effect_disposition_expected_bindings_required")
        if not args.apply and any(value is not None for value in expected_values):
            raise ValueError("pre_w3_effect_disposition_expected_bindings_apply_only")
        if args.apply and any(
            _SHA256_RE.fullmatch(str(value)) is None or str(value) == "0" * 64
            for value in expected_values
        ):
            raise ValueError("pre_w3_effect_disposition_expected_binding_invalid")
        return
    if (
        any(reset_only_flags)
        or args.apply
        or expected_binding is not None
        or expected_config is not None
        or expected_plan is not None
        or expected_before is not None
        or any(value is not None for value in disposition_expected)
        or args.backup is not None
        or args.backup_sha256 is not None
    ):
        raise ValueError("delivery_circuit_reset_arguments_require_clear_circuit")


def _run_circuit_reset_command(
    *,
    args: argparse.Namespace,
    config: DispatcherConfig,
    store: RcaDeliveryStore,
) -> int:
    if args.materialize_reset:
        receipt_path = _absolute_new_circuit_reset_receipt_path(args.receipt)
        audit = store.delivery_dispatcher_circuit_reset_audit(
            args.materialize_reset,
            effect_kind=args.effect_kind,
        )
        if audit is None:
            raise RuntimeError("delivery_circuit_reset_audit_missing")
        envelope = _build_delivery_circuit_reset_recovery_envelope(
            audit,
            receipt_path,
        )
        try:
            receipt_sha256 = _write_immutable_circuit_reset_receipt(
                receipt_path,
                envelope,
                expected_parent_identity={
                    "device": envelope["materialized_destination"]["binding"][
                        "parent_device"
                    ],
                    "inode": envelope["materialized_destination"]["binding"][
                        "parent_inode"
                    ],
                },
            )
        except Exception as exc:
            raise ValueError(
                "delivery_circuit_reset_recovery_materialization_failed"
            ) from exc
        print(
            json.dumps(
                {
                    "ok": True,
                    "recovered": True,
                    "reset_id": args.materialize_reset,
                    "effect_kind": args.effect_kind,
                    "receipt": str(receipt_path),
                    "receipt_sha256": receipt_sha256,
                    "receipt_fingerprint": envelope["receipt_fingerprint"],
                    "source_receipt_fingerprint": audit["receipt_fingerprint"],
                    "planned_destination_binding": audit["destination_binding"],
                    "external_effects_triggered": False,
                },
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
        )
        return 0

    operator, reason = _validate_circuit_reset_text(args.operator, args.reason)
    receipt_path = (
        _absolute_new_circuit_reset_receipt_path(args.receipt)
        if args.receipt is not None
        else None
    )
    active_binding = _active_release_binding_snapshot(config)
    expected_binding = args.expected_active_release_binding_sha256
    expected_config = args.expected_config_binding_sha256
    expected_plan = args.expected_plan_id
    expected_before = args.expected_before_state_sha256
    if expected_binding is not None and active_binding["sha256"] != expected_binding:
        raise RuntimeError("delivery_circuit_reset_active_binding_changed")
    config_binding_sha256 = _circuit_reset_config_binding(config)
    if expected_config is not None and config_binding_sha256 != expected_config:
        raise RuntimeError("delivery_circuit_reset_config_changed")
    tool_provenance = _circuit_reset_tool_provenance()
    before = store.delivery_dispatcher_circuit_reset_state(args.effect_kind)
    if before is None:
        raise RuntimeError("delivery_circuit_reset_state_missing")
    if before["circuit"]["state"] != "open":
        raise RuntimeError("delivery_circuit_reset_requires_open_circuit")
    before_state_sha256 = _circuit_reset_sha256(before["circuit"])
    if expected_before is not None and before_state_sha256 != expected_before:
        raise RuntimeError("delivery_circuit_reset_before_state_changed")
    recorded_at = _utc_now().isoformat()
    planned = _build_delivery_circuit_reset_receipt(
        config=config,
        effect_kind=args.effect_kind,
        operator=operator,
        reason=reason,
        before=before,
        recorded_at=recorded_at,
        receipt_path=receipt_path,
        active_release_binding=active_binding,
        tool_provenance=tool_provenance,
    )
    if expected_plan is not None and planned["plan_id"] != expected_plan:
        raise RuntimeError("delivery_circuit_reset_plan_changed")
    if not args.apply:
        print(
            json.dumps(
                {
                    "ok": True,
                    "mode": "plan",
                    "applied": False,
                    "external_effects_triggered": False,
                    "receipt_path": str(receipt_path),
                    "plan": planned,
                },
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
        )
        return 0

    if _active_release_binding_snapshot(config) != active_binding:
        raise RuntimeError("delivery_circuit_reset_active_binding_changed")
    if _circuit_reset_config_binding(DispatcherConfig.from_env()) != config_binding_sha256:
        raise RuntimeError("delivery_circuit_reset_config_changed")
    if _circuit_reset_tool_provenance() != tool_provenance:
        raise RuntimeError("delivery_circuit_reset_tool_provenance_changed")
    reset_at = datetime.fromisoformat(recorded_at)
    _before, after = store.close_delivery_dispatcher_circuit_with_audit(
        effect_kind=args.effect_kind,
        audit=planned,
        now=reset_at,
    )
    if (
        after["circuit"] != planned["after"]
        or after["permanent_failure"] != planned["permanent_failure_after"]
    ):
        raise RuntimeError("delivery_circuit_reset_post_state_mismatch")
    try:
        receipt_sha256 = _write_immutable_circuit_reset_receipt(
            receipt_path,
            planned,
            expected_parent_identity={
                "device": planned["destination_binding"]["parent_device"],
                "inode": planned["destination_binding"]["parent_inode"],
            },
        )
    except Exception as exc:
        raise DeliveryCircuitResetReceiptMaterializationError(
            reset_id=planned["reset_id"],
            receipt_path=receipt_path,
            cause=exc,
        ) from exc
    print(
        json.dumps(
            {
                "ok": True,
                "applied": True,
                "command": "clear-delivery-circuit",
                "effect_kind": args.effect_kind,
                "reset_id": planned["reset_id"],
                "receipt": str(receipt_path),
                "receipt_sha256": receipt_sha256,
                "receipt_fingerprint": planned["receipt_fingerprint"],
                "pre_state": planned["pre_state"],
                "post_state": planned["post_state"],
                "effect_delta": planned["effect_delta"],
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    )
    return 0


def _run_pre_w3_effect_disposition_command(
    *,
    args: argparse.Namespace,
    config: DispatcherConfig,
    store: RcaDeliveryStore,
) -> int:
    if args.materialize_pre_w3_disposition:
        receipt_path = _absolute_new_circuit_reset_receipt_path(args.receipt)
        audit = store.pre_w3_effect_disposition_audit(
            args.materialize_pre_w3_disposition
        )
        if audit is None:
            raise RuntimeError("pre_w3_effect_disposition_audit_missing")
        envelope = _build_pre_w3_effect_disposition_recovery_envelope(
            audit,
            receipt_path,
        )
        try:
            receipt_sha256 = _write_immutable_circuit_reset_receipt(
                receipt_path,
                envelope,
                expected_parent_identity={
                    "device": envelope["materialized_destination"]["binding"][
                        "parent_device"
                    ],
                    "inode": envelope["materialized_destination"]["binding"][
                        "parent_inode"
                    ],
                },
            )
        except Exception as exc:
            raise ValueError(
                "pre_w3_effect_disposition_recovery_materialization_failed"
            ) from exc
        print(json.dumps({
            "ok": True,
            "recovered": True,
            "disposition_id": audit["disposition_id"],
            "receipt": str(receipt_path),
            "receipt_sha256": receipt_sha256,
            "receipt_fingerprint": envelope["receipt_fingerprint"],
            "source_receipt_fingerprint": audit["receipt_fingerprint"],
            "external_effects_triggered": False,
            "provider_calls_performed": 0,
        }, ensure_ascii=False, indent=2, sort_keys=True))
        return 0

    operator, reason = _validate_circuit_reset_text(args.operator, args.reason)
    if args.apply:
        existing = store.pre_w3_effect_disposition_audit(
            args.expected_disposition_id
        )
        if existing is not None:
            expected_existing = {
                "disposition_id": args.expected_disposition_id,
                "plan_id": args.expected_plan_id,
                "before_snapshot_sha256": args.expected_before_state_sha256,
                "effect_set_sha256": args.expected_effect_set_sha256,
                "database_logical_sha256": (
                    args.expected_database_logical_sha256
                ),
                "activation_sha256": args.expected_activation_sha256,
                "active_release_binding_sha256": (
                    args.expected_active_release_binding_sha256
                ),
                "config_binding_sha256": args.expected_config_binding_sha256,
                "live_env_sha256": args.expected_live_env_sha256,
                "tool_provenance_sha256": args.expected_tool_provenance_sha256,
            }
            observed_existing = {
                "disposition_id": existing["disposition_id"],
                "plan_id": existing["plan_id"],
                "before_snapshot_sha256": existing["before_snapshot_sha256"],
                "effect_set_sha256": existing["effect_set_sha256"],
                "database_logical_sha256": existing["before"][
                    "control_db_logical_digest"
                ]["sha256"],
                "activation_sha256": existing["before"]["current_activation"][
                    "sha256"
                ],
                "active_release_binding_sha256": existing[
                    "active_release_binding"
                ]["sha256"],
                "config_binding_sha256": existing["config_binding_sha256"],
                "live_env_sha256": existing["active_release_binding"][
                    "live_env_sha256"
                ],
                "tool_provenance_sha256": existing["tool_provenance_sha256"],
            }
            changed = [
                key
                for key, value in expected_existing.items()
                if observed_existing[key] != value
            ]
            requested_keys = sorted(
                str(value or "").strip() for value in args.pre_w3_effect_keys
            )
            if (
                changed
                or requested_keys != existing["effect_keys"]
                or operator != existing["operator"]
                or reason != existing["reason"]
                or not store.pre_w3_effect_disposition_is_applied(existing)
            ):
                raise RuntimeError(
                    "pre_w3_effect_disposition_idempotent_retry_mismatch"
                )
            current_backup = _pre_w3_effect_disposition_backup_binding(
                args.backup,
                args.backup_sha256,
                source_path=config.control_db_path,
                expected_snapshot=existing["before"],
                verify_source_logical_digest=False,
            )
            stable_backup_fields = {
                "path",
                "sha256",
                "size_bytes",
                "device",
                "inode",
                "mtime_ns",
                "journal_mode",
                "quick_check",
                "foreign_key_check",
                "snapshot_sha256",
                "effect_set_sha256",
                "logical_digest_sha256",
            }
            if any(
                current_backup[field] != existing["backup_binding"][field]
                for field in stable_backup_fields
            ):
                raise RuntimeError(
                    "pre_w3_effect_disposition_backup_changed"
                )
            requested_receipt = Path(args.receipt).expanduser()
            if (
                not requested_receipt.is_absolute()
                or str(requested_receipt) != str(requested_receipt.absolute())
            ):
                raise ValueError(
                    "pre_w3_effect_disposition_receipt_path_invalid"
                )
            receipt_path = requested_receipt.absolute()
            receipt_sha256 = _existing_pre_w3_disposition_receipt_sha256(
                receipt_path,
                existing,
            )
            if receipt_sha256 is None:
                receipt_path = _absolute_new_circuit_reset_receipt_path(args.receipt)
                receipt_sha256 = _write_immutable_circuit_reset_receipt(
                    receipt_path,
                    existing,
                    expected_parent_identity={
                        "device": existing["destination_binding"]["parent_device"],
                        "inode": existing["destination_binding"]["parent_inode"],
                    },
                )
            print(json.dumps({
                "ok": True,
                "applied": False,
                "idempotent": True,
                "command": PRE_W3_EFFECT_DISPOSITION_COMMAND,
                "disposition_id": existing["disposition_id"],
                "effect_keys": existing["effect_keys"],
                "receipt": str(receipt_path),
                "receipt_sha256": receipt_sha256,
                "receipt_fingerprint": existing["receipt_fingerprint"],
                "effect_delta": {
                    **existing["effect_delta"],
                    "database_rows_repeated": 0,
                },
                "external_effects_triggered": False,
                "provider_calls_performed": 0,
            }, ensure_ascii=False, indent=2, sort_keys=True))
            return 0
    receipt_path = _absolute_new_circuit_reset_receipt_path(args.receipt)
    active_binding = _pre_w3_effect_disposition_active_release_binding(config)
    config_binding_sha256 = _circuit_reset_config_binding(config)
    tool_provenance = _circuit_reset_tool_provenance()
    tool_provenance_sha256 = _pre_w3_disposition_sha256(tool_provenance)
    recorded_at = _utc_now()
    snapshot = store.pre_w3_effect_disposition_snapshot(
        effect_keys=args.pre_w3_effect_keys,
    )
    backup_binding = _pre_w3_effect_disposition_backup_binding(
        args.backup,
        args.backup_sha256,
        source_path=config.control_db_path,
        expected_snapshot=snapshot,
    )
    planned = _build_pre_w3_effect_disposition_receipt(
        config=config,
        snapshot=snapshot,
        operator=operator,
        reason=reason,
        recorded_at=recorded_at.isoformat(),
        receipt_path=receipt_path,
        backup_binding=backup_binding,
        active_release_binding=active_binding,
        tool_provenance=tool_provenance,
    )
    observed = {
        "disposition_id": planned["disposition_id"],
        "plan_id": planned["plan_id"],
        "before_snapshot_sha256": planned["before_snapshot_sha256"],
        "effect_set_sha256": planned["effect_set_sha256"],
        "database_logical_sha256": planned["before"][
            "control_db_logical_digest"
        ]["sha256"],
        "activation_sha256": planned["before"]["current_activation"]["sha256"],
        "active_release_binding_sha256": planned["active_release_binding"]["sha256"],
        "config_binding_sha256": planned["config_binding_sha256"],
        "live_env_sha256": planned["active_release_binding"]["live_env_sha256"],
        "tool_provenance_sha256": planned["tool_provenance_sha256"],
    }
    if not args.apply:
        print(json.dumps({
            "ok": True,
            "mode": "plan",
            "applied": False,
            "external_effects_triggered": False,
            "provider_calls_performed": 0,
            "receipt_path": str(receipt_path),
            "expected_apply": {
                "expected_disposition_id": observed["disposition_id"],
                "expected_plan_id": observed["plan_id"],
                "expected_before_state_sha256": observed[
                    "before_snapshot_sha256"
                ],
                "expected_effect_set_sha256": observed["effect_set_sha256"],
                "expected_database_logical_sha256": observed[
                    "database_logical_sha256"
                ],
                "expected_activation_sha256": observed["activation_sha256"],
                "expected_active_release_binding_sha256": observed[
                    "active_release_binding_sha256"
                ],
                "expected_config_binding_sha256": observed[
                    "config_binding_sha256"
                ],
                "expected_live_env_sha256": observed["live_env_sha256"],
                "expected_tool_provenance_sha256": observed[
                    "tool_provenance_sha256"
                ],
            },
            "plan": planned,
        }, ensure_ascii=False, indent=2, sort_keys=True))
        return 0

    expected = {
        "disposition_id": args.expected_disposition_id,
        "plan_id": args.expected_plan_id,
        "before_snapshot_sha256": args.expected_before_state_sha256,
        "effect_set_sha256": args.expected_effect_set_sha256,
        "database_logical_sha256": args.expected_database_logical_sha256,
        "activation_sha256": args.expected_activation_sha256,
        "active_release_binding_sha256": (
            args.expected_active_release_binding_sha256
        ),
        "config_binding_sha256": args.expected_config_binding_sha256,
        "live_env_sha256": args.expected_live_env_sha256,
        "tool_provenance_sha256": args.expected_tool_provenance_sha256,
    }
    changed = [key for key, value in expected.items() if observed[key] != value]
    if changed:
        raise RuntimeError(
            "pre_w3_effect_disposition_plan_changed:" + ",".join(changed)
        )
    if (
        _pre_w3_effect_disposition_active_release_binding(config) != active_binding
        or _circuit_reset_config_binding(DispatcherConfig.from_env())
        != config_binding_sha256
        or _circuit_reset_tool_provenance() != tool_provenance
        or _pre_w3_effect_disposition_backup_binding(
            args.backup,
            args.backup_sha256,
            source_path=config.control_db_path,
            expected_snapshot=planned["before"],
        )
        != backup_binding
    ):
        raise RuntimeError("pre_w3_effect_disposition_external_binding_changed")
    applied_audit, applied = store.quarantine_pre_w3_effects_with_audit(
        audit=planned,
        now=recorded_at,
    )
    try:
        receipt_sha256 = _write_immutable_circuit_reset_receipt(
            receipt_path,
            applied_audit,
            expected_parent_identity={
                "device": planned["destination_binding"]["parent_device"],
                "inode": planned["destination_binding"]["parent_inode"],
            },
        )
    except Exception as exc:
        raise PreW3EffectDispositionReceiptMaterializationError(
            disposition_id=planned["disposition_id"],
            receipt_path=receipt_path,
            cause=exc,
        ) from exc
    print(json.dumps({
        "ok": True,
        "applied": applied,
        "idempotent": not applied,
        "command": PRE_W3_EFFECT_DISPOSITION_COMMAND,
        "disposition_id": planned["disposition_id"],
        "effect_keys": planned["effect_keys"],
        "receipt": str(receipt_path),
        "receipt_sha256": receipt_sha256,
        "receipt_fingerprint": planned["receipt_fingerprint"],
        "effect_delta": planned["effect_delta"],
        "external_effects_triggered": False,
        "provider_calls_performed": 0,
    }, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


def load_delivery_dispatcher_environment(
    env_file: str | Path | None = None,
) -> Path:
    path = Path(
        env_file
        or os.environ.get(f"{ENV_PREFIX}ENV_FILE")
        or Path(get_hermes_home()) / ".env"
    ).expanduser()
    load_dotenv(path, override=False, interpolate=False)
    return path


def main(argv: list[str] | None = None) -> int:
    load_delivery_dispatcher_environment()
    args = _parser().parse_args(argv)
    try:
        _validate_circuit_reset_arguments(args)
        config = DispatcherConfig.from_env()
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
        print(
            json.dumps(
                {
                    "ok": quarantine_baseline["ready"],
                    "config": config.public_dict(),
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
        reset_mode = bool(
            args.clear_circuit
            or args.materialize_reset
            or args.pre_w3_effect_keys
            or args.materialize_pre_w3_disposition
        )
        store = RcaDeliveryStore(
            config.control_db_path,
            require_current=True,
            read_only=reset_mode and not args.apply,
            ensure_current_rows=not reset_mode,
        )
    except (OSError, RuntimeError, sqlite3.Error) as exc:
        print(
            json.dumps(
                {"ok": False, "error": f"delivery_store_unavailable: {exc}"},
                ensure_ascii=False,
            )
        )
        return 2
    if args.pre_w3_effect_keys or args.materialize_pre_w3_disposition:
        try:
            return _run_pre_w3_effect_disposition_command(
                args=args,
                config=config,
                store=store,
            )
        except PreW3EffectDispositionReceiptMaterializationError as exc:
            print(json.dumps({
                "ok": False,
                "error": type(exc).__name__,
                "message": str(exc),
                "recovery_required": True,
                "disposition_id": exc.disposition_id,
                "meta_key": exc.meta_key,
                "receipt": str(exc.receipt_path),
                "external_effects_triggered": False,
                "provider_calls_performed": 0,
            }, ensure_ascii=False))
            return 2
        except Exception as exc:
            print(json.dumps({
                "ok": False,
                "error": type(exc).__name__,
                "message": str(exc),
                "external_effects_triggered": False,
                "provider_calls_performed": 0,
            }, ensure_ascii=False))
            return 2
    if args.clear_circuit or args.materialize_reset:
        try:
            return _run_circuit_reset_command(
                args=args,
                config=config,
                store=store,
            )
        except DeliveryCircuitResetReceiptMaterializationError as exc:
            print(
                json.dumps(
                    {
                        "ok": False,
                        "error": type(exc).__name__,
                        "message": str(exc),
                        "recovery_required": True,
                        "reset_id": exc.reset_id,
                        "meta_key": exc.meta_key,
                        "receipt": str(exc.receipt_path),
                        "external_effects_triggered": False,
                    },
                    ensure_ascii=False,
                )
            )
            return 2
        except Exception as exc:
            print(
                json.dumps(
                    {
                        "ok": False,
                        "error": type(exc).__name__,
                        "message": str(exc),
                        "external_effects_triggered": False,
                    },
                    ensure_ascii=False,
                )
            )
            return 2
    if args.dry_run:
        rows = store.preview_dispatchable_effects(
            limit=config.batch_size,
            activation_required=config.activation_required,
        )
        print(
            json.dumps(
                {
                    "ok": True,
                    "dry_run": True,
                    "external_writes": False,
                    "enabled": config.enabled,
                    "circuit": asdict(store.delivery_dispatcher_circuit()),
                    "circuits": {
                        effect_kind: asdict(value)
                        for effect_kind, value in (
                            store.delivery_dispatcher_circuits().items()
                        )
                    },
                    "rows": rows,
                },
                ensure_ascii=False,
            )
        )
        return 0

    if config.enabled:
        try:
            require_resident_activation_epoch(
                RcaControlStore(config.control_db_path, require_current=True)
            )
        except ExternalWriteFenceError as exc:
            print(
                json.dumps(
                    {"ok": False, "error": exc.code, "detail": exc.detail},
                    ensure_ascii=False,
                )
            )
            return 2

    adapter = MeegleIssueCommentAdapter()
    thread_adapter = FeishuThreadReplyAdapter()
    card_sender: Any | None = None

    def patch_task_card(
        target: str,
        card_payload: Mapping[str, Any],
        message_id: str | None = None,
        *,
        provider_claim: ProviderWriteClaim | None = None,
    ) -> Mapping[str, Any]:
        nonlocal card_sender
        if card_sender is None:
            from scripts.pnc_completion_notice_relay import FeishuHotSender

            card_sender = FeishuHotSender()
        raw = card_sender.send_task_card(
            target,
            dict(card_payload),
            message_id=message_id,
            provider_claim=provider_claim,
        )
        if not isinstance(raw, Mapping):
            return {
                "success": False,
                "outcome_uncertain": True,
                "error_code": "delivery_boundary_contract_invalid",
                "error": "relay task card patch returned a non-object",
            }
        value = dict(raw)
        if value.get("success") is True:
            return value
        value["success"] = False
        value.setdefault("outcome_uncertain", True)
        value.setdefault("error_code", "feishu_card_patch_failed")
        value.setdefault("error", "task card patch failed")
        return value

    dispatcher = DeliveryDispatcher(
        store=store,
        config=config,
        list_comments=adapter.list_comments,
        add_comment=adapter.add_comment,
        get_fields=adapter.get_fields,
        update_fields=adapter.update_fields,
        list_thread_replies=thread_adapter.list_replies,
        add_thread_reply=thread_adapter.add_reply,
        patch_task_card=patch_task_card,
        report_verifier=lambda url, size, sha256: default_report_verifier(
            url,
            size,
            sha256,
            timeout_seconds=config.report_http_timeout_seconds,
        ),
    )
    stop = {"requested": False}

    def request_stop(_signum, _frame):
        stop["requested"] = True

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    return run_dispatch_loop(
        dispatcher, once=args.once, stop_requested=lambda: stop["requested"]
    )


if __name__ == "__main__":
    raise SystemExit(main())
