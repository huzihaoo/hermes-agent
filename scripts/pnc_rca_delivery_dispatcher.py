#!/usr/bin/env python3
"""Deliver durable RCA effects as exactly one Feishu Project issue comment."""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import socket
import sqlite3
import sys
import threading
import time
from typing import Any, Callable, Mapping
from urllib import error as urllib_error
from urllib import request as urllib_request

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from dotenv import load_dotenv

from gateway.pnc_rca_delivery_contract import (
    DELIVERY_EFFECT_KIND,
    DELIVERY_EFFECT_KINDS,
    DELIVERY_EFFECT_SCHEMA_VERSION,
    DELIVERY_REPORT_LINK_KIND,
    DELIVERY_THREAD_EFFECT_KIND,
    TERMINAL_DELIVERY_EFFECT_SCHEMA_VERSION,
    TERMINAL_DELIVERY_EFFECT_SCHEMA_VERSION_V1,
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
    compute_delivery_effect_key,
    compute_delivery_effect_payload_sha256,
    delivery_effect_idempotency_uuid,
    delivery_effect_marker,
    validate_report_asset_url,
    validate_report_url,
    validate_delivery_subscription_target,
    verify_persisted_artifact_inventory,
)
from gateway.pnc_rca_delivery_quarantine_baseline import (
    disabled_quarantine_baseline_status,
    quarantine_baseline_settings,
    read_quarantine_baseline_status,
)
from gateway.pnc_rca_delivery_store import (
    DeliveryEffectClaim,
    RcaDeliveryStore,
    StaleDeliveryEffectLeaseError,
)
from gateway.pnc_rca_runtime_identity import (
    MAX_HEALTH_FUTURE_SKEW_SECONDS,
    RCA_DELIVERY_DISPATCHER_LOADED_DEPENDENCIES,
    build_runtime_identity,
    runtime_identity_is_valid,
)
from hermes_constants import get_hermes_home
from scripts.pnc_foxglove_delivery import (
    canonical_viz_mcap_cifs_path,
    canonical_viz_mcap_path,
    validate_foxglove_url,
)


ENV_PREFIX = "HERMES_RCA_DELIVERY_DISPATCHER_"
HEALTH_SCHEMA_VERSION = "pnc_rca_delivery_dispatcher_health_v2"
SERVICE_LABEL = "local.pnc.rca-delivery-dispatcher"
MAX_EFFECT_AGE_SECONDS = 86_400
MEEGLE_COMMENT_PAGE_TIMEOUT_SECONDS = 12
MAX_MEEGLE_COMMENT_PAGES = 5
MAX_MEEGLE_COMMENTS = 500
MAX_EXTERNAL_BOUNDARY_TIMEOUT_SECONDS = (
    MEEGLE_COMMENT_PAGE_TIMEOUT_SECONDS * (MAX_MEEGLE_COMMENT_PAGES + 1)
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
_FEISHU_ISSUE_URL_RE = re.compile(
    r"^https://project\.feishu\.cn/([A-Za-z0-9._-]+)/issue/detail/([0-9]+)$"
)
_PROJECT_SIMPLE_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_VIZ_REPORT_STATUSES = frozenset({"report_ready"})
_CIRCUIT_CODES = frozenset({
    "feishu_auth_failed",
    "feishu_permission_denied",
    "meegle_dependency_unavailable",
    "feishu_thread_dependency_unavailable",
    "meegle_response_invalid",
    "delivery_boundary_contract_invalid",
    "report_http_auth_or_permission",
})


ListComments = Callable[[str, str], Mapping[str, Any]]
AddComment = Callable[[str, str, str], Mapping[str, Any]]
GetFields = Callable[[str, str, tuple[str, ...]], Mapping[str, Any]]
UpdateFields = Callable[
    [str, str, tuple[tuple[str, str], ...]], Mapping[str, Any]
]
ListThreadReplies = Callable[[str, str], Mapping[str, Any]]
AddThreadReply = Callable[[str, str, str, str], Mapping[str, Any]]
ReportVerifier = Callable[[str, int, str], Mapping[str, Any]]


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

    def stop(self) -> None:
        self._stop.set()
        if not self._started:
            return
        self._thread.join(
            timeout=max(1.0, self._interval_seconds + 6.0)
        )
        if self._thread.is_alive():
            raise RuntimeError("delivery_effect_lease_keeper_stop_timeout")


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


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
            "reconciliation_min_missing_reads": (
                self.reconciliation_min_missing_reads
            ),
            "recovery_write_interval_seconds": (
                self.recovery_write_interval_seconds
            ),
            "quarantine_baseline_path": str(self.quarantine_baseline_path),
            "quarantine_baseline_sha256": self.quarantine_baseline_sha256,
            "quarantine_release_id": self.quarantine_release_id,
            "quarantine_bootstrap_epoch_id": self.quarantine_bootstrap_epoch_id,
            "quarantine_active_release_binding_path": str(
                self.quarantine_active_release_binding_path
            ),
            "quarantine_live_env_path": str(self.quarantine_live_env_path),
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


def default_report_verifier(
    report_url: str,
    expected_size: int,
    expected_sha256: str,
    *,
    timeout_seconds: int = 10,
    monotonic: Callable[[], float] = time.monotonic,
    opener: Any | None = None,
) -> Mapping[str, Any]:
    """No-redirect HEAD+GET verification under one wall-clock deadline."""
    try:
        validate_report_asset_url(report_url)
    except DeliveryContractError as exc:
        return {
            "success": False,
            "permanent": True,
            "error_code": exc.code,
            "error": exc.detail,
        }
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
    }

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
        if field_keys not in {
            (RCA_RESULT_FIELD_KEY,),
            (RCA_RESULT_FIELD_KEY, RCA_REPORT_FIELD_KEY),
        }:
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
            if (
                isinstance(metadata, Mapping)
                and isinstance(metadata.get("data"), Mapping)
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
            {"field_key": key, "field_value": value}
            for key, value in field_updates
        ]
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
        anchor = value[len("topic:"):].strip()
        if not _REMOTE_ID_RE.fullmatch(anchor):
            raise ValueError("thread_id contains an invalid topic root")
        return anchor

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
                BaseRequest.builder()
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
        try:
            result = await asyncio.wait_for(
                self._adapter.send(
                    chat_id,
                    content,
                    metadata={
                        "thread_id": thread_id,
                        "idempotency_uuid": idempotency_uuid,
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


def _validate_effect(claim: DeliveryEffectClaim) -> ValidatedEffect:
    if claim.effect_kind not in DELIVERY_EFFECT_KINDS or claim.required is not True:
        raise DeliveryContractError(
            "delivery_effect_kind_unsupported",
            "dispatcher only accepts required RCA delivery effects",
        )
    if claim.outcome != "success":
        return _validate_terminal_effect(claim)
    payload = claim.payload
    schema_version = str(payload.get("schema_version") or "")
    if schema_version != DELIVERY_EFFECT_SCHEMA_VERSION:
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
            "schema_version", "delivery_id", "effect_kind", "target_key",
            "project_key", "project_simple_name", "work_item_type_key",
            "work_item_id", "issue_url",
            "artifact_set_id", "report_url", "report_cifs_path", "report_status",
            "viz_mcap_vm", "foxglove_url",
            "requires_human_review", "conclusion", "effect_key",
            "semantic_payload_sha256", "marker", "comment_content",
            "field_updates", *report_link_fields,
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
            "schema_version", "delivery_id", "effect_kind", "target_key",
            "project_key", "project_simple_name", "work_item_type_key",
            "work_item_id", "issue_url",
            "artifact_set_id", "report_url", "report_cifs_path", "report_status",
            "viz_mcap_vm", "foxglove_url",
            "requires_human_review", "conclusion", "platform", "chat_id",
            "thread_id", "reply_anchor_message_id", "source_message_id",
            "requester_id", "reply_in_thread", "output_cap", "effect_key",
            "semantic_payload_sha256", "marker", "idempotency_uuid",
            "message_content", "field_updates", *report_link_fields,
        }
        content_field = "message_content"
    if set(payload) != exact_payload_keys:
        raise DeliveryContractError("delivery_effect_payload_shape_invalid")
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
    if not validate_foxglove_url(claim.report_url, expected_viz_path):
        raise DeliveryContractError("delivery_effect_report_url_invalid")
    expected_report_cifs_path = canonical_viz_mcap_cifs_path(
        manifest_submission_key
    )
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
    if (
        payload.get("requires_human_review") is not True
        or payload.get("report_status") not in _VIZ_REPORT_STATUSES
    ):
        raise DeliveryContractError("delivery_effect_review_boundary_invalid")
    expected_report_link = claim.report_url
    expected_field_updates = [
        {
            "field_key": RCA_RESULT_FIELD_KEY,
            "field_value": payload.get("conclusion"),
        },
        {
            "field_key": RCA_REPORT_FIELD_KEY,
            "field_value": expected_report_link,
        },
    ]
    if payload.get("field_updates") != expected_field_updates:
        raise DeliveryContractError("delivery_effect_field_updates_invalid")
    validate_report_url(
        claim.manifest.get("report_url"),
        submission_key=manifest_submission_key,
        artifact_set_id=claim.artifact_set_id,
    )
    verified_artifacts = verify_persisted_artifact_inventory(
        manifest=claim.manifest,
        stored_artifacts=claim.artifacts,
        expected_artifact_set_id=claim.artifact_set_id,
    )
    semantic_sha = compute_delivery_effect_payload_sha256(
        payload, claim.effect_kind
    )
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
        or content.splitlines()[0] != marker
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
            report_cifs_path=expected_report_cifs_path,
        )
    else:
        expected_content = build_thread_reply_content(
            marker=marker,
            work_item_id=claim.work_item_id,
            report_status=str(payload.get("report_status") or ""),
            conclusion=conclusion,
            report_url=claim.report_url,
            issue_url=expected_issue_url,
        )
    if content != expected_content:
        raise DeliveryContractError("delivery_effect_content_invalid")
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
    return ValidatedEffect(
        effect_kind=claim.effect_kind,
        marker=marker,
        content=content,
        artifacts=(),
        field_updates=(
            (
                RCA_RESULT_FIELD_KEY,
                str(payload.get("conclusion") or ""),
            ),
            (RCA_REPORT_FIELD_KEY, str(expected_report_link or "")),
        ) if claim.effect_kind == DELIVERY_EFFECT_KIND else (),
        chat_id=str(payload.get("chat_id") or ""),
        thread_id=str(payload.get("thread_id") or ""),
        idempotency_uuid=str(payload.get("idempotency_uuid") or ""),
    )


def _validate_terminal_effect(claim: DeliveryEffectClaim) -> ValidatedEffect:
    if claim.outcome not in TERMINAL_DELIVERY_OUTCOMES:
        raise DeliveryContractError("terminal_delivery_outcome_invalid")
    if (
        claim.artifact_set_id != claim.outcome_key
        or not re.fullmatch(r"g1q3-rca-terminal-v1-[0-9a-f]{64}", claim.outcome_key)
        or claim.issue_url
        or claim.report_url
        or claim.manifest != {}
        or claim.artifacts != []
    ):
        raise DeliveryContractError("terminal_delivery_artifact_boundary_invalid")
    payload = claim.payload
    schema_version = str(payload.get("schema_version") or "")
    if schema_version == TERMINAL_DELIVERY_EFFECT_SCHEMA_VERSION_V1:
        diagnostic_code = ""
    elif schema_version == TERMINAL_DELIVERY_EFFECT_SCHEMA_VERSION:
        diagnostic_code = str(claim.contract.get("diagnostic_code") or "")
        diagnostic_detail = str(claim.contract.get("diagnostic_detail") or "")
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
        schema_version=schema_version,
    )
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
    if claim.effect_kind == DELIVERY_THREAD_EFFECT_KIND:
        expected_uuid = delivery_effect_idempotency_uuid(claim.effect_key)
        if payload.get("idempotency_uuid") != expected_uuid:
            raise DeliveryContractError("delivery_effect_idempotency_invalid")
    field_updates: tuple[tuple[str, str], ...] = ()
    if (
        claim.effect_kind == DELIVERY_EFFECT_KIND
        and schema_version == TERMINAL_DELIVERY_EFFECT_SCHEMA_VERSION
    ):
        field_updates = (
            (RCA_RESULT_FIELD_KEY, primary.diagnostic_result),
        )
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


def _marker_matches(
    comments: list[dict[str, str]], marker: str
) -> list[dict[str, str]]:
    variants = {marker}
    remote_marker = marker
    if marker.startswith("[") and marker.endswith("]"):
        # Meegle preserves the marker text but strips Markdown link brackets.
        remote_marker = marker[1:-1]
        variants.add(remote_marker)
    return [
        item
        for item in comments
        if any(
            line in variants or line.replace(" ", "") == remote_marker
            for line in item["content"].splitlines()
        )
    ]


def _canonical_remote_content(content: str, marker: str) -> str | None:
    lines = [line for line in content.splitlines() if line != ""]
    if not lines:
        return None
    remote_marker = marker[1:-1] if marker.startswith("[") and marker.endswith("]") else marker
    first_line = lines[0]
    if (
        first_line == marker
        or first_line == remote_marker
        or first_line.replace(" ", "") == remote_marker
    ):
        lines[0] = marker
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
            (remote := _canonical_remote_content(item["content"], marker))
            is not None
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
            or self._active_effect_lease_identity
            != self._claim_lease_identity(claim)
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
                f"{SERVICE_LABEL}-effect-lease-{claim.fence}-"
                f"{claim.lease_token[:8]}"
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
    ) -> Any:
        return self._keeper_for_claim(claim).settle(mutation)

    def _lease_lost(self, claim: DeliveryEffectClaim) -> DispatchOutcome:
        self.stats.lease_lost += 1
        return DispatchOutcome(
            status="lease_lost",
            effect_key=claim.effect_key,
            delivery_id=claim.delivery_id,
            attempt=claim.attempt,
            error_code="stale_delivery_effect_lease",
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
            except StaleDeliveryEffectLeaseError:
                return self._lease_lost(claim)
        finally:
            self._stop_effect_lease_keeper(keeper)

    def _list_remote_effect(
        self, claim: DeliveryEffectClaim, validated: ValidatedEffect
    ) -> Mapping[str, Any]:
        if validated.effect_kind == DELIVERY_EFFECT_KIND:
            return self.list_comments(claim.project_key, claim.work_item_id)
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
                runtime_identity=self.runtime_identity,
                now=self.now(),
            ),
        )
        self.stats.reconciled += 1
        return DispatchOutcome(
            status="reconciled",
            effect_key=claim.effect_key,
            delivery_id=claim.delivery_id,
            attempt=claim.attempt,
            remote_id=remote_id,
        )

    def _dispatch_claim(self, claim: DeliveryEffectClaim) -> DispatchOutcome:
        prior_write_uncertain = claim.previous_status == "uncertain"
        self._heartbeat(claim)
        try:
            validated = _validate_effect(claim)
        except DeliveryContractError as exc:
            return self._quarantine(claim, error_code=exc.code, detail=exc.detail)

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

        verification_failure = self._verify_report_artifacts(
            claim,
            validated,
            uncertain=prior_write_uncertain,
        )
        if verification_failure is not None:
            return verification_failure

        self._heartbeat(claim)
        try:
            before_raw = self._list_remote_effect(claim, validated)
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
                uncertain=prior_write_uncertain,
            )
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
            self._heartbeat(claim)
            return self._retry(
                claim,
                error_code="feishu_field_read_unavailable",
                detail=type(exc).__name__,
                uncertain=prior_write_uncertain,
            )
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
        fields_match = _field_updates_match(
            before_fields, validated.field_updates
        )
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

        recovery_write_count = 0
        if prior_write_uncertain and existing_marker is None:
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
        if not prior_write_uncertain:
            self._heartbeat(claim)
            superseded = self.store.mark_effect_write_started(
                claim=claim,
                now=self.now(),
                activation_required=self.config.activation_required,
            )
            if superseded is not None:
                self.stats.reconciled += 1
                return DispatchOutcome(
                    status="superseded",
                    effect_key=claim.effect_key,
                    delivery_id=claim.delivery_id,
                    attempt=claim.attempt,
                    error_code=(
                        "delivery_effect_superseded_by_newer_settled_fields"
                    ),
                )
        if not fields_match:
            try:
                update_raw = self._write_field_updates(claim, validated)
            except Exception as exc:
                self._heartbeat(claim)
                return self._retry(
                    claim,
                    error_code="feishu_field_update_outcome_unknown",
                    detail=type(exc).__name__,
                    uncertain=True,
                )
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
                self._heartbeat(claim)
                return self._retry(
                    claim,
                    error_code="feishu_field_postwrite_read_unavailable",
                    detail=type(exc).__name__,
                    uncertain=True,
                )
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
        try:
            add_raw = self._add_remote_effect(claim, validated)
        except Exception as exc:
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
            self._heartbeat(claim)
            return self._retry(
                claim,
                error_code="feishu_postwrite_confirmation_unavailable",
                detail=type(exc).__name__,
                uncertain=True,
            )
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
            item
            for item in after_content_matches
            if item["remote_id"] == remote_id
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
            self._heartbeat(claim)
            return self._retry(
                claim,
                error_code="feishu_field_final_read_unavailable",
                detail=type(exc).__name__,
                uncertain=True,
            )
        self._heartbeat(claim)
        final_fields, final_field_result = _strict_field_values(
            final_fields_raw, expected_field_keys
        )
        if final_fields is None or not _field_updates_match(
            final_fields, validated.field_updates
        ):
            detail = str(
                final_field_result.get("error") or "field readback mismatch"
            )
            return self._retry(
                claim,
                error_code="feishu_field_final_confirmation_mismatch",
                detail=detail,
                uncertain=True,
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
                runtime_identity=self.runtime_identity,
                now=self.now(),
            ),
        )
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
            quarantine_bootstrap_epoch_id=(
                self.config.quarantine_bootstrap_epoch_id
            ),
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
                and (
                    not self.config.enabled or store_health.get("ok") is True
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
                effect_kind: asdict(value)
                for effect_kind, value in circuits.items()
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
    parser.add_argument("--clear-circuit", action="store_true")
    parser.add_argument(
        "--effect-kind",
        choices=sorted(DELIVERY_EFFECT_KINDS),
        default=DELIVERY_EFFECT_KIND,
    )
    return parser


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
        store = RcaDeliveryStore(config.control_db_path, require_current=True)
    except (OSError, RuntimeError, sqlite3.Error) as exc:
        print(
            json.dumps(
                {"ok": False, "error": f"delivery_store_unavailable: {exc}"},
                ensure_ascii=False,
            )
        )
        return 2
    if args.clear_circuit:
        circuit = store.close_delivery_dispatcher_circuit(
            effect_kind=args.effect_kind
        )
        print(json.dumps({"ok": True, "circuit": asdict(circuit)}, ensure_ascii=False))
        return 0
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

    adapter = MeegleIssueCommentAdapter()
    thread_adapter = FeishuThreadReplyAdapter()
    dispatcher = DeliveryDispatcher(
        store=store,
        config=config,
        list_comments=adapter.list_comments,
        add_comment=adapter.add_comment,
        get_fields=adapter.get_fields,
        update_fields=adapter.update_fields,
        list_thread_replies=thread_adapter.list_replies,
        add_thread_reply=thread_adapter.add_reply,
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
