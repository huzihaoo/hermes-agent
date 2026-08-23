#!/usr/bin/env python3
"""Status-first dispatcher for the direct MiniStore outbox.

The dispatcher owns only the MiniStore lease/retry lifecycle.  The external
task status read and create operation are injected at the boundary so the
state machine can be exercised without importing a live client, LaunchAgent,
activation, W3, release, or the legacy ControlStore dispatcher.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import signal
import stat
import sys
import tempfile
import time
from typing import Any, Callable, Mapping

from dotenv import dotenv_values

from gateway.pnc_rca_admission import (
    RcaAdmissionError,
    validate_rca_admission,
)
from gateway.pnc_rca_mini_store import MiniOutboxClaim, MiniStore
from gateway.pnc_rca_schema import validate_vm_execution_request_envelope


RETRY_DELAYS_SECONDS = (5, 30, 120, 600, 3_600)
MAX_ERROR_DETAIL = 500

_MISSING_STATES = frozenset({"missing"})
_UNKNOWN_STATES = frozenset({
    "absent",
    "unknown",
    "unavailable",
    "not_found",
    "not-found",
    "read_error",
    "read-error",
    "error",
    "timeout",
    "timed_out",
    "transient_error",
    "transient-error",
})
_EXISTING_STATES = frozenset({
    "accepted",
    "blocked",
    "cancelled",
    "canceled",
    "claimed",
    "completed",
    "created",
    "failed",
    "existing",
    "in_progress",
    "in-progress",
    "pending",
    "queued",
    "running",
    "started",
    "submitted",
    "succeeded",
    "success",
    "quarantined",
})


class MiniOutboxDispatcherError(RuntimeError):
    """Base class for errors owned by the dispatcher boundary."""

    code = "dispatcher_error"


class PermanentDispatchError(MiniOutboxDispatcherError):
    """A malformed or contradictory request/status that must be quarantined."""

    code = "permanent_dispatch_error"


# Keep the longer name available to callers that use the exception as a type.
PermanentDispatcherError = PermanentDispatchError


class UnknownDispatchError(MiniOutboxDispatcherError):
    """A boundary operation whose durable outcome cannot yet be determined."""

    code = "unknown_dispatch_error"


class StatusSchemaError(PermanentDispatchError):
    """The injected status callback returned an invalid response shape."""

    code = "status_schema_error"


class IdentityMismatchError(PermanentDispatchError):
    """The observed task identity does not belong to this submission key."""

    code = "identity_mismatch"


class CreateSchemaError(PermanentDispatchError):
    """The injected create callback returned an invalid response shape."""

    code = "create_schema_error"


class CreateRejectedError(UnknownDispatchError):
    """Create returned a syntactically valid but unsuccessful response."""

    code = "create_rejected"


class StatusReadError(UnknownDispatchError):
    """The status read could not establish a fact about the task."""

    code = "status_read_error"


class StatusUnknownError(UnknownDispatchError):
    """The status endpoint explicitly reported an indeterminate state."""

    code = "status_unknown"


@dataclass(frozen=True)
class DirectStatus:
    """Normalized status returned by the injected status callback."""

    state: str
    submission_key: str = ""
    business_key: str = ""
    generation: int | None = None
    origin_source_id: str = ""


@dataclass(frozen=True)
class DirectDispatchResult:
    status: str
    outbox_id: int | None = None
    submission_key: str = ""
    error_code: str = ""
    error_detail: str = ""


BuildRequest = Callable[[Mapping[str, Any], MiniOutboxClaim], Mapping[str, Any] | str]
StatusRequest = Callable[[str, MiniOutboxClaim], Mapping[str, Any] | DirectStatus]
CreateRequest = Callable[[str, MiniOutboxClaim], Mapping[str, Any]]
BoundaryCheck = Callable[[Mapping[str, Any], MiniOutboxClaim], None]
RequestCheck = Callable[[Mapping[str, Any], MiniOutboxClaim], None]


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _text(value: Any) -> str:
    return str(value or "").strip()


def _error_code(exc: BaseException) -> str:
    return _text(getattr(exc, "code", "")) or type(exc).__name__


def _error_detail(exc: BaseException) -> str:
    return str(exc)[:MAX_ERROR_DETAIL]


def _identity_values(response: Mapping[str, Any]) -> dict[str, Any]:
    """Collect optional identity fields from common status response envelopes."""
    values: dict[str, Any] = {}

    def add_value(key: str, value: Any) -> None:
        if key in values and values[key] != value:
            raise StatusSchemaError(f"status response has conflicting {key} identities")
        values[key] = value

    for key in ("submission_key", "task_id", "rca_submission_key"):
        if key in response and _text(response[key]):
            add_value(key, response[key])

    nested_values: list[Any] = []
    for key in ("identity", "execution_identity"):
        if key in response:
            nested_values.append(response[key])
    for nested in nested_values:
        if nested is None or nested == "":
            continue
        if isinstance(nested, Mapping):
            for key in (
                "submission_key",
                "task_id",
                "rca_submission_key",
                "business_key",
                "generation",
                "origin_source_id",
            ):
                if (
                    key in nested
                    and nested[key] is not None
                    and (key == "generation" or _text(nested[key]))
                ):
                    add_value(key, nested[key])
        elif isinstance(nested, str):
            add_value("submission_key", nested)
        else:
            raise StatusSchemaError(
                "status response identity must be an object or text"
            )

    for key in ("business_key", "generation", "origin_source_id"):
        if (
            key in response
            and response[key] is not None
            and (key == "generation" or _text(response[key]))
        ):
            add_value(key, response[key])
    return values


def _normalize_status(
    value: Mapping[str, Any] | DirectStatus,
    claim: MiniOutboxClaim,
) -> DirectStatus:
    if isinstance(value, DirectStatus):
        response: Mapping[str, Any] = {
            "state": value.state,
            "submission_key": value.submission_key,
            "business_key": value.business_key,
            "generation": value.generation,
            "origin_source_id": value.origin_source_id,
        }
    elif isinstance(value, Mapping):
        response = value
    else:  # pragma: no cover - protected by the callback contract
        raise StatusSchemaError("status callback must return a mapping")

    raw_state = response.get("state")
    if not isinstance(raw_state, str) or not raw_state.strip():
        raise StatusSchemaError("status response state is required")
    state = raw_state.strip().lower().replace(" ", "_")
    if state in _MISSING_STATES:
        normalized_state = "missing"
    elif state in _UNKNOWN_STATES:
        normalized_state = "unknown"
    elif state in _EXISTING_STATES:
        normalized_state = state
    else:
        raise StatusSchemaError(f"unsupported status state: {raw_state!r}")

    values = _identity_values(response)
    submission_values = {
        _text(values[key])
        for key in ("submission_key", "task_id", "rca_submission_key")
        if key in values and _text(values[key])
    }
    if len(submission_values) > 1:
        raise StatusSchemaError("status response has conflicting submission identities")
    observed_submission_key = next(iter(submission_values), "")
    if observed_submission_key and observed_submission_key != claim.submission_key:
        raise IdentityMismatchError(
            f"observed submission key {observed_submission_key!r} does not match "
            f"{claim.submission_key!r}"
        )

    observed_business_key = _text(values.get("business_key"))
    if observed_business_key and observed_business_key != claim.business_key:
        raise IdentityMismatchError("observed business key does not match claim")

    observed_generation = values.get("generation")
    if observed_generation is not None:
        if isinstance(observed_generation, bool) or not isinstance(
            observed_generation, int
        ):
            raise StatusSchemaError("status response generation must be an integer")
        if observed_generation != claim.generation:
            raise IdentityMismatchError("observed generation does not match claim")

    observed_origin_source_id = _text(values.get("origin_source_id"))
    if (
        observed_origin_source_id
        and observed_origin_source_id != claim.origin_source_id
    ):
        raise IdentityMismatchError(
            "observed origin source identity does not match claim"
        )

    # A missing response is only authoritative when it is exactly a missing
    # response.  An identity attached to it is contradictory and fail-closed.
    if normalized_state == "missing" and values:
        raise StatusSchemaError("missing status must not carry an identity")
    if normalized_state not in {"missing", "unknown"} and not observed_submission_key:
        raise StatusSchemaError(
            "existing status must carry an explicit submission identity"
        )
    return DirectStatus(
        state=normalized_state,
        submission_key=observed_submission_key,
        business_key=observed_business_key,
        generation=observed_generation,
        origin_source_id=observed_origin_source_id,
    )


class MiniOutboxDispatcher:
    """Lease, status-check, create, reconcile, and settle one outbox request."""

    def __init__(
        self,
        store: MiniStore,
        *,
        lease_owner: str,
        status: StatusRequest | None = None,
        create: CreateRequest | None = None,
        build_request: BuildRequest | None = None,
        admission_check: BoundaryCheck | None = None,
        request_check: RequestCheck | None = None,
        now: Callable[[], datetime] = _now,
        lease_seconds: int = 300,
    ) -> None:
        if not _text(lease_owner):
            raise ValueError("lease_owner must not be empty")
        if status is None:
            raise ValueError("status callback is required")
        if create is None:
            raise ValueError("create callback is required")
        self.store = store
        self.lease_owner = _text(lease_owner)
        self.status = status
        self.create = create
        self.build_request = build_request
        self.admission_check = admission_check
        self.request_check = request_check
        self.now = now
        self.lease_seconds = lease_seconds

    def _claim_failure(
        self,
        claim: MiniOutboxClaim,
        *,
        error_code: str,
        error_detail: str,
        permanent: bool = False,
    ) -> DirectDispatchResult:
        attempt = max(1, claim.attempt_count)
        retryable = not permanent and attempt < len(RETRY_DELAYS_SECONDS)
        self.store.fail_outbox(
            claim.outbox_id,
            lease_owner=self.lease_owner,
            error_code=error_code,
            error_detail=error_detail,
            retry_at=(
                self.now()
                + timedelta(
                    seconds=RETRY_DELAYS_SECONDS[
                        min(attempt - 1, len(RETRY_DELAYS_SECONDS) - 1)
                    ]
                )
                if retryable
                else None
            ),
            quarantine=not retryable,
            now=self.now(),
        )
        return DirectDispatchResult(
            "retry" if retryable else "quarantined",
            claim.outbox_id,
            claim.submission_key,
            error_code,
            error_detail,
        )

    def _failure_from_exception(
        self,
        claim: MiniOutboxClaim,
        exc: BaseException,
        *,
        status_read: bool = False,
    ) -> DirectDispatchResult:
        permanent = isinstance(exc, PermanentDispatchError)
        if status_read and not permanent and not isinstance(exc, UnknownDispatchError):
            exc = StatusReadError(_error_detail(exc))
        return self._claim_failure(
            claim,
            error_code=_error_code(exc),
            error_detail=_error_detail(exc),
            permanent=permanent,
        )

    def _read_status(self, claim: MiniOutboxClaim) -> DirectStatus:
        try:
            observed = self.status(claim.submission_key, claim)
        except PermanentDispatchError:
            raise
        except UnknownDispatchError:
            raise
        except Exception as exc:
            raise StatusReadError(_error_detail(exc)) from exc
        try:
            normalized = _normalize_status(observed, claim)
        except PermanentDispatchError:
            raise
        except Exception as exc:  # pragma: no cover - defensive boundary
            raise StatusSchemaError(_error_detail(exc)) from exc
        if normalized.state == "unknown":
            raise StatusUnknownError("status endpoint returned an unknown state")
        return normalized

    @staticmethod
    def _validate_claim_payload(
        claim: MiniOutboxClaim,
    ) -> Mapping[str, Any]:
        try:
            payload = json.loads(claim.payload_json)
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise PermanentDispatchError("outbox payload JSON is invalid") from exc
        if not isinstance(payload, Mapping):
            raise PermanentDispatchError("outbox payload must be an object")
        admission = payload.get("admission")
        if not isinstance(admission, Mapping):
            raise PermanentDispatchError("outbox admission is missing")
        try:
            validated_admission = validate_rca_admission(admission)
        except (RcaAdmissionError, TypeError, ValueError) as exc:
            raise PermanentDispatchError(str(exc)) from exc
        if validated_admission.submission_key != claim.submission_key:
            raise IdentityMismatchError(
                "outbox submission key does not match admission"
            )
        expected_fields = {
            "submission_key": claim.submission_key,
            "business_key": claim.business_key,
            "generation": claim.generation,
            "source_event_id": claim.source_event_id,
            "origin_source_id": claim.origin_source_id,
        }
        for field, expected in expected_fields.items():
            if field in payload and payload[field] != expected:
                raise IdentityMismatchError(f"outbox {field} does not match claim")
        if "origin_source_id" not in payload:
            raise PermanentDispatchError("outbox origin source identity is missing")
        return payload

    @staticmethod
    def _request_value(request: Mapping[str, Any] | str) -> Mapping[str, Any]:
        if isinstance(request, str):
            try:
                request = json.loads(request)
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                raise PermanentDispatchError(
                    "execution request JSON is invalid"
                ) from exc
        if not isinstance(request, Mapping):
            raise PermanentDispatchError("execution request must be an object")
        return request

    @staticmethod
    def _validate_create_result(
        value: Any,
        claim: MiniOutboxClaim,
    ) -> Mapping[str, Any]:
        if not isinstance(value, Mapping):
            raise CreateSchemaError("create callback must return a mapping")
        if not isinstance(value.get("success"), bool):
            raise CreateSchemaError("create response success must be a boolean")
        task_id = value.get("task_id")
        if not isinstance(task_id, str) or not task_id.strip():
            raise CreateSchemaError("create response task_id is required")
        if task_id.strip() != claim.submission_key:
            raise IdentityMismatchError("create response task_id does not match claim")
        state = value.get("state")
        if state is not None and (not isinstance(state, str) or not state.strip()):
            raise CreateSchemaError("create response state must be non-empty text")

        # Reuse the status identity validator without treating the create result
        # as status authority.  Post-status remains mandatory either way.
        probe = dict(value)
        probe["state"] = "submitted"
        try:
            _normalize_status(probe, claim)
        except IdentityMismatchError:
            raise
        except StatusSchemaError as exc:
            raise CreateSchemaError(str(exc)) from exc
        if value["success"] is not True:
            detail = _text(value.get("error") or value.get("detail") or state)
            raise CreateRejectedError(detail or "create callback reported failure")
        return value

    def _build_and_freeze(
        self,
        payload: Mapping[str, Any],
        claim: MiniOutboxClaim,
    ) -> str:
        if claim.request_json:
            request = self._request_value(claim.request_json)
        elif self.build_request is None:
            raise PermanentDispatchError("final execution request builder is required")
        else:
            try:
                built_request = self.build_request(payload, claim)
            except PermanentDispatchError:
                raise
            except (TypeError, ValueError) as exc:
                raise PermanentDispatchError(str(exc)) from exc
            request = self._request_value(built_request)
        try:
            request_payload = validate_vm_execution_request_envelope(request)
        except (TypeError, ValueError) as exc:
            raise PermanentDispatchError(str(exc)) from exc
        if self.request_check is not None:
            try:
                self.request_check(request_payload, claim)
            except PermanentDispatchError:
                raise
            except (TypeError, ValueError) as exc:
                raise PermanentDispatchError(str(exc)) from exc
        request_json, _ = self.store.freeze_execution_request(
            claim.outbox_id,
            lease_owner=self.lease_owner,
            request=request_payload,
            now=self.now(),
        )
        return request_json

    def _complete(
        self,
        claim: MiniOutboxClaim,
        result: Mapping[str, Any],
        *,
        dispatch_status: str,
    ) -> DirectDispatchResult:
        self.store.complete_outbox(
            claim.outbox_id,
            lease_owner=self.lease_owner,
            result=result,
            now=self.now(),
        )
        return DirectDispatchResult(
            dispatch_status,
            claim.outbox_id,
            claim.submission_key,
        )

    def _reconcile_after_create(
        self,
        claim: MiniOutboxClaim,
        *,
        create_error: BaseException | None,
    ) -> DirectDispatchResult:
        try:
            post_status = self._read_status(claim)
        except UnknownDispatchError as exc:
            # A failed create is never evidence that no task exists.  Keep the
            # same submission key pending until a later read can establish it.
            return self._failure_from_exception(claim, exc, status_read=True)
        except PermanentDispatchError as exc:
            return self._failure_from_exception(claim, exc)

        if post_status.state == "missing":
            detail = "create completed without an observable task"
            if create_error is not None:
                detail = f"{detail}; {_error_code(create_error)}: {_error_detail(create_error)}"
            return self._claim_failure(
                claim,
                error_code="post_status_missing",
                error_detail=detail[:MAX_ERROR_DETAIL],
            )

        result: dict[str, Any] = {
            "status": "reconciled",
            "submission_key": claim.submission_key,
            "observed_state": post_status.state,
        }
        if create_error is not None:
            result.update({
                "create_error_code": _error_code(create_error),
                "create_error_detail": _error_detail(create_error),
            })
        return self._complete(claim, result, dispatch_status="completed")

    def dispatch_one(self) -> DirectDispatchResult:
        claims = self.store.claim_outbox(
            lease_owner=self.lease_owner,
            lease_seconds=self.lease_seconds,
            limit=1,
            now=self.now(),
        )
        if not claims:
            return DirectDispatchResult("idle")
        claim = claims[0]

        try:
            payload = self._validate_claim_payload(claim)
        except PermanentDispatchError as exc:
            return self._failure_from_exception(claim, exc)

        try:
            pre_status = self._read_status(claim)
        except UnknownDispatchError as exc:
            return self._failure_from_exception(claim, exc, status_read=True)
        except PermanentDispatchError as exc:
            return self._failure_from_exception(claim, exc)

        if pre_status.state != "missing":
            return self._complete(
                claim,
                {
                    "status": "deduped",
                    "submission_key": claim.submission_key,
                    "observed_state": pre_status.state,
                },
                dispatch_status="deduped",
            )

        try:
            if self.admission_check is not None:
                self.admission_check(payload, claim)
            request_json = self._build_and_freeze(payload, claim)
        except PermanentDispatchError as exc:
            return self._failure_from_exception(claim, exc)
        except Exception as exc:
            return self._failure_from_exception(claim, exc)

        create_error: BaseException | None = None
        try:
            create_result = self.create(request_json, claim)
            self._validate_create_result(create_result, claim)
        except Exception as exc:
            create_error = exc
        return self._reconcile_after_create(claim, create_error=create_error)


MINI_DISPATCHER_SERVICE_LABEL = "local.pnc.rca-mini-outbox-dispatcher"
MINI_DISPATCHER_CONFIG_SCHEMA_VERSION = "pnc_rca_mini_outbox_dispatcher_config_v1"
MINI_DISPATCHER_HEALTH_SCHEMA_VERSION = "pnc_rca_mini_outbox_dispatcher_health_v1"
MINI_DISPATCHER_ENV_PREFIX = "HERMES_RCA_DIRECT_OUTBOX_"
DIRECT_KAFKA_GROUP_ENV_NAMES = (
    "HERMES_RCA_DIRECT_KAFKA_GROUP_ID",
    "HERMES_RCA_DIRECT_KAFKA_GROUP",
    "HERMES_RCA_DIRECT_GROUP_ID",
)
DIRECT_DEFAULT_GROUP_ID = "rca_direct_path"
DIRECT_RUNTIME_RELATIVE = Path("runtime/pnc_agent/feishu_issue_kafka_rca_direct")
DIRECT_ENV_FILENAME = "direct.env"
DIRECT_DB_FILENAME = "mini.sqlite3"
DIRECT_CONSUMER_HEALTH_FILENAME = "consumer_health.json"
DIRECT_DISPATCHER_HEALTH_FILENAME = "outbox_dispatcher_health.json"
DIRECT_DISPATCHER_PYTHON = (
    "/Users/songying/.hermes/runtime/venvs/hermes-v0.18.2-b85e919-sealed/bin/python"
)
DIRECT_DISPATCHER_INSTALLED_TARGET = (
    "/Users/songying/.hermes/runtime/pnc_agent/"
    "feishu_issue_kafka_rca_direct/pnc_rca_mini_outbox_dispatcher.py"
)
DIRECT_DISPATCHER_ENV_PATH = (
    "/Users/songying/.hermes/runtime/pnc_agent/feishu_issue_kafka_rca_direct/direct.env"
)
DIRECT_DISPATCHER_LOG_ROOT = (
    "/Users/songying/.hermes/runtime/pnc_agent/feishu_issue_kafka_rca_direct"
)
DIRECT_DISPATCHER_AUTH_PRINCIPAL = "pnc-rca-direct-outbox"
DIRECT_DISPATCHER_AUTH_CAPABILITY = "g1q3_rca_direct_vm_submit"
MAX_HEALTH_ERROR_TYPE = 120
MAX_HEALTH_ERROR_CODE = 120


class MiniDispatcherConfigError(ValueError):
    """The independent dispatcher configuration is unsafe or incomplete."""


def direct_runtime_root(hermes_home: str | Path | None = None) -> Path:
    """Return the one writable root owned by the direct consumer/dispatcher."""

    home = Path(hermes_home or Path.home() / ".hermes").expanduser()
    if not home.is_absolute():
        raise MiniDispatcherConfigError("hermes_home_must_be_absolute")
    return home / DIRECT_RUNTIME_RELATIVE


def _absolute_path(value: Any, *, field: str) -> Path:
    path = Path(str(value or "")).expanduser()
    if not path.is_absolute():
        raise MiniDispatcherConfigError(f"{field}_must_be_absolute")
    if "\x00" in str(path):
        raise MiniDispatcherConfigError(f"{field}_contains_nul")
    return path


def _path_within(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def _reject_symlink_components(path: Path, *, stop: Path | None = None) -> None:
    """Reject symlinked existing components without resolving the target."""

    current = path if path.is_absolute() else path.absolute()
    stop_path = stop.absolute() if stop is not None else None
    components: list[Path] = []
    while True:
        components.append(current)
        if current.parent == current or (
            stop_path is not None and current == stop_path
        ):
            break
        current = current.parent
    for item in reversed(components):
        try:
            info = item.lstat()
        except FileNotFoundError:
            continue
        if stat.S_ISLNK(info.st_mode):
            raise MiniDispatcherConfigError("path_symlink_component_forbidden")


def _ensure_runtime_root(root: Path, *, create: bool) -> Path:
    """Validate/create the private direct runtime directory."""

    root = _absolute_path(root, field="runtime_root")
    # macOS exposes /var and some temporary directories as compatibility
    # symlinks.  The controlled boundary starts at HERMES_HOME; system
    # ancestors outside that boundary are not part of the contract.
    boundary = root.parents[2] if len(root.parents) > 2 else root.parent
    if create:
        _reject_symlink_components(root, stop=boundary)
        root.mkdir(parents=True, exist_ok=True, mode=0o700)
    _reject_symlink_components(root, stop=boundary)
    try:
        info = root.lstat()
    except FileNotFoundError as exc:
        raise MiniDispatcherConfigError("runtime_root_missing") from exc
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
        raise MiniDispatcherConfigError("runtime_root_must_be_directory")
    if int(info.st_uid) != os.geteuid():
        raise MiniDispatcherConfigError("runtime_root_owner_mismatch")
    if stat.S_IMODE(info.st_mode) & 0o077:
        raise MiniDispatcherConfigError(
            "runtime_root_must_not_be_group_or_world_accessible"
        )
    return root


def _validate_bound_path(
    path: Path,
    root: Path,
    *,
    field: str,
    filename: str,
    required_mode: int | None = None,
) -> Path:
    path = _absolute_path(path, field=field)
    if path != root / filename or not _path_within(path, root):
        raise MiniDispatcherConfigError(f"{field}_must_use_direct_runtime_path")
    _reject_symlink_components(path, stop=root)
    try:
        info = path.lstat()
    except FileNotFoundError:
        return path
    if stat.S_ISLNK(info.st_mode):
        raise MiniDispatcherConfigError(f"{field}_symlink_forbidden")
    if not stat.S_ISREG(info.st_mode):
        raise MiniDispatcherConfigError(f"{field}_must_be_regular_file")
    if info.st_nlink != 1:
        raise MiniDispatcherConfigError(f"{field}_hardlink_forbidden")
    if int(info.st_uid) != os.geteuid():
        raise MiniDispatcherConfigError(f"{field}_owner_mismatch")
    if required_mode is not None and stat.S_IMODE(info.st_mode) != required_mode:
        raise MiniDispatcherConfigError(f"{field}_mode_must_be_{required_mode:04o}")
    return path


def _validate_env_file(path: Path, root: Path) -> Path:
    path = _validate_bound_path(
        path,
        root,
        field="env_file",
        filename=DIRECT_ENV_FILENAME,
        required_mode=0o600,
    )
    try:
        info = path.lstat()
    except FileNotFoundError as exc:
        raise MiniDispatcherConfigError("env_file_missing") from exc
    return path


def _bool_setting(
    source: Mapping[str, Any], names: tuple[str, ...], *, default: bool = False
) -> bool:
    value: Any = None
    present = False
    for name in names:
        if name in source:
            value = source[name]
            present = True
            break
    if not present or value is None or str(value).strip() == "":
        return default
    normalized = str(value).strip().lower()
    if normalized not in {"true", "false"}:
        raise MiniDispatcherConfigError(f"{names[0]}_must_be_true_or_false")
    return normalized == "true"


def _int_setting(
    source: Mapping[str, Any],
    names: tuple[str, ...],
    *,
    default: int,
    minimum: int,
    maximum: int,
) -> int:
    value: Any = default
    for name in names:
        if name in source and source[name] not in {None, ""}:
            value = source[name]
            break
    try:
        parsed = int(str(value).strip())
    except (TypeError, ValueError) as exc:
        raise MiniDispatcherConfigError(f"{names[0]}_must_be_integer") from exc
    if isinstance(value, bool) or not minimum <= parsed <= maximum:
        raise MiniDispatcherConfigError(f"{names[0]}_out_of_bounds")
    return parsed


def _text_setting(
    source: Mapping[str, Any], names: tuple[str, ...], *, default: str = ""
) -> str:
    for name in names:
        if name in source and source[name] is not None:
            value = str(source[name]).strip()
            if value:
                return value
    return default


@dataclass(frozen=True, slots=True)
class MiniOutboxDispatcherConfig:
    """Non-secret settings for the independent direct mini outbox process."""

    enabled: bool = False
    submit_enabled: bool = False
    group_id: str = DIRECT_DEFAULT_GROUP_ID
    db_path: Path = Path()
    health_path: Path = Path()
    env_file: Path = Path()
    lease_owner: str = MINI_DISPATCHER_SERVICE_LABEL
    lease_seconds: int = 300
    poll_interval_seconds: int = 5
    max_batch_size: int = 1
    auth_principal: str = DIRECT_DISPATCHER_AUTH_PRINCIPAL
    auth_capability: str = DIRECT_DISPATCHER_AUTH_CAPABILITY

    @classmethod
    def from_env(
        cls,
        env: Mapping[str, Any] | None = None,
        *,
        hermes_home: str | Path | None = None,
        env_file: str | Path | None = None,
    ) -> "MiniOutboxDispatcherConfig":
        source = os.environ if env is None else env
        root = direct_runtime_root(hermes_home)
        # Parsing a config object directly must enforce the same fixed,
        # owner-only runtime root as the file-loading path.  Callers cannot
        # bypass this by skipping ``load_mini_outbox_environment``.
        _ensure_runtime_root(root, create=False)
        env_path = _absolute_path(
            env_file
            or _text_setting(
                source,
                (
                    f"{MINI_DISPATCHER_ENV_PREFIX}ENV_FILE",
                    "HERMES_RCA_DIRECT_ENV_FILE",
                ),
                default=str(root / DIRECT_ENV_FILENAME),
            ),
            field="env_file",
        )
        if env_path != root / DIRECT_ENV_FILENAME:
            raise MiniDispatcherConfigError("env_file_must_use_direct_runtime_path")

        enabled = _bool_setting(
            source,
            (
                f"{MINI_DISPATCHER_ENV_PREFIX}ENABLED",
                "HERMES_RCA_MINI_OUTBOX_ENABLED",
            ),
        )
        submit_enabled = _bool_setting(
            source,
            (
                f"{MINI_DISPATCHER_ENV_PREFIX}SUBMIT_ENABLED",
                "HERMES_RCA_MINI_OUTBOX_SUBMIT_ENABLED",
            ),
        )
        if submit_enabled and not enabled:
            raise MiniDispatcherConfigError(
                "submit_enabled_requires_dispatcher_enabled"
            )
        group_id = _text_setting(source, DIRECT_KAFKA_GROUP_ENV_NAMES)
        if not group_id:
            group_id = DIRECT_DEFAULT_GROUP_ID
        if enabled and group_id != DIRECT_DEFAULT_GROUP_ID:
            raise MiniDispatcherConfigError(
                "production_direct_group_must_be_rca_direct_path"
            )

        db_path = _absolute_path(
            _text_setting(
                source,
                (
                    f"{MINI_DISPATCHER_ENV_PREFIX}DB_PATH",
                    "HERMES_RCA_DIRECT_DB_PATH",
                ),
                default=str(root / DIRECT_DB_FILENAME),
            ),
            field="db_path",
        )
        health_path = _absolute_path(
            _text_setting(
                source,
                (
                    f"{MINI_DISPATCHER_ENV_PREFIX}HEALTH_PATH",
                    "HERMES_RCA_DIRECT_OUTBOX_HEALTH_PATH",
                ),
                default=str(root / DIRECT_DISPATCHER_HEALTH_FILENAME),
            ),
            field="health_path",
        )
        _validate_bound_path(
            db_path,
            root,
            field="db_path",
            filename=DIRECT_DB_FILENAME,
        )
        _validate_bound_path(
            health_path,
            root,
            field="health_path",
            filename=DIRECT_DISPATCHER_HEALTH_FILENAME,
        )
        lease_owner = _text_setting(
            source,
            (f"{MINI_DISPATCHER_ENV_PREFIX}LEASE_OWNER",),
            default=MINI_DISPATCHER_SERVICE_LABEL,
        )
        if lease_owner != MINI_DISPATCHER_SERVICE_LABEL:
            raise MiniDispatcherConfigError("lease_owner_must_match_service_label")
        auth_principal = _text_setting(
            source,
            (
                f"{MINI_DISPATCHER_ENV_PREFIX}AUTH_PRINCIPAL",
                "HERMES_RCA_DIRECT_VM_AUTH_PRINCIPAL",
            ),
            default=DIRECT_DISPATCHER_AUTH_PRINCIPAL,
        )
        auth_capability = _text_setting(
            source,
            (
                f"{MINI_DISPATCHER_ENV_PREFIX}AUTH_CAPABILITY",
                "HERMES_RCA_DIRECT_VM_AUTH_CAPABILITY",
            ),
            default=DIRECT_DISPATCHER_AUTH_CAPABILITY,
        )
        for name, value in (
            ("auth_principal", auth_principal),
            ("auth_capability", auth_capability),
        ):
            if not value or any(ord(char) < 32 for char in value):
                raise MiniDispatcherConfigError(f"{name}_invalid")
        if auth_principal != DIRECT_DISPATCHER_AUTH_PRINCIPAL:
            raise MiniDispatcherConfigError("auth_principal_must_match_direct_contract")
        if auth_capability != DIRECT_DISPATCHER_AUTH_CAPABILITY:
            raise MiniDispatcherConfigError(
                "auth_capability_must_match_direct_contract"
            )
        return cls(
            enabled=enabled,
            submit_enabled=submit_enabled,
            group_id=group_id,
            db_path=db_path,
            health_path=health_path,
            env_file=env_path,
            lease_owner=lease_owner,
            lease_seconds=_int_setting(
                source,
                (f"{MINI_DISPATCHER_ENV_PREFIX}LEASE_SECONDS",),
                default=300,
                minimum=1,
                maximum=86_400,
            ),
            poll_interval_seconds=_int_setting(
                source,
                (f"{MINI_DISPATCHER_ENV_PREFIX}POLL_INTERVAL_SECONDS",),
                default=5,
                minimum=1,
                maximum=3_600,
            ),
            max_batch_size=_int_setting(
                source,
                (f"{MINI_DISPATCHER_ENV_PREFIX}MAX_BATCH_SIZE",),
                default=1,
                minimum=1,
                maximum=1_000,
            ),
            auth_principal=auth_principal,
            auth_capability=auth_capability,
        )

    def public_dict(self) -> dict[str, Any]:
        return {
            "schema_version": MINI_DISPATCHER_CONFIG_SCHEMA_VERSION,
            "service_label": MINI_DISPATCHER_SERVICE_LABEL,
            "enabled": self.enabled,
            "submit_enabled": self.submit_enabled,
            "group_id": self.group_id,
            "db_path": str(self.db_path),
            "health_path": str(self.health_path),
            "env_file": str(self.env_file),
            "lease_owner": self.lease_owner,
            "lease_seconds": self.lease_seconds,
            "poll_interval_seconds": self.poll_interval_seconds,
            "max_batch_size": self.max_batch_size,
            "auth_principal": self.auth_principal,
            "auth_capability": self.auth_capability,
        }


def load_mini_outbox_environment(
    env_file: str | Path | None = None,
    *,
    environ: Mapping[str, Any] | None = None,
    hermes_home: str | Path | None = None,
) -> tuple[dict[str, str], Path]:
    """Load the direct env contract without mutating ``os.environ``.

    The file is intentionally narrower than Hermes' general ``.env``.  It is
    the only place where the direct Kafka/VM credentials may be supplied to
    the resident processes, and therefore must be owner-only ``0600``.
    """

    source = {
        str(key): str(value)
        for key, value in (os.environ if environ is None else environ).items()
        if value is not None
    }
    root = direct_runtime_root(hermes_home)
    requested = env_file or _text_setting(
        source,
        (
            f"{MINI_DISPATCHER_ENV_PREFIX}ENV_FILE",
            "HERMES_RCA_DIRECT_ENV_FILE",
        ),
        default=str(root / DIRECT_ENV_FILENAME),
    )
    path = _absolute_path(requested, field="env_file")
    _ensure_runtime_root(root, create=False)
    path = _validate_env_file(path, root)
    try:
        values = dotenv_values(path)
    except (OSError, ValueError) as exc:
        raise MiniDispatcherConfigError("env_file_read_failed") from exc
    for key, value in values.items():
        if value is not None and key not in source:
            source[str(key)] = str(value)
    return source, path


def _safe_error_code(exc: BaseException) -> str:
    value = str(getattr(exc, "code", "") or type(exc).__name__).strip()
    return value[:MAX_HEALTH_ERROR_CODE] or "dispatcher_error"


def _safe_health_result(result: DirectDispatchResult | None) -> dict[str, Any] | None:
    if result is None:
        return None
    return {
        "status": str(result.status)[:64],
        "outbox_id": result.outbox_id,
        "submission_key": str(result.submission_key)[:160],
        "error_code": str(result.error_code)[:MAX_HEALTH_ERROR_CODE],
    }


class MiniOutboxHealthReporter:
    """Atomic, payload-free liveness writer for the mini dispatcher."""

    def __init__(
        self,
        path: str | Path,
        *,
        config: MiniOutboxDispatcherConfig | None = None,
    ) -> None:
        self.path = _absolute_path(path, field="health_path")
        self.config = config
        self.last_result: DirectDispatchResult | None = None
        self.last_error: dict[str, str] | None = None
        self.stats: dict[str, int] = {
            "iterations": 0,
            "idle": 0,
            "deduped": 0,
            "completed": 0,
            "retry": 0,
            "quarantined": 0,
            "errors": 0,
        }

    def record(self, result: DirectDispatchResult) -> None:
        self.last_result = result
        self.stats["iterations"] += 1
        if result.status in self.stats:
            self.stats[result.status] += 1

    def error(self, phase: str, exc: BaseException) -> None:
        self.stats["errors"] += 1
        self.last_error = {
            "phase": str(phase)[:64],
            "type": type(exc).__name__[:MAX_HEALTH_ERROR_TYPE],
            "code": _safe_error_code(exc),
            "at": datetime.now(timezone.utc).isoformat(),
        }

    def observation(
        self,
        *,
        state: str,
        healthy: bool | None = None,
    ) -> dict[str, Any]:
        state = str(state)[:64]
        business_ready = bool(
            self.config is not None
            and self.config.enabled
            and self.config.submit_enabled
            and state in {"running", "idle"}
        )
        process_healthy = state != "error"
        body: dict[str, Any] = {
            "schema_version": MINI_DISPATCHER_HEALTH_SCHEMA_VERSION,
            "service_label": MINI_DISPATCHER_SERVICE_LABEL,
            "state": state,
            "ok": process_healthy and business_ready,
            "healthy": process_healthy if healthy is None else bool(healthy),
            "business_ready": business_ready,
            "observed_at": datetime.now(timezone.utc).isoformat(),
            "stats": dict(self.stats),
            "last_result": _safe_health_result(self.last_result),
        }
        if self.config is not None:
            # Paths are contract metadata, not payloads.  Credentials are
            # deliberately absent from ``public_dict`` and this observation.
            body["config"] = self.config.public_dict()
        if self.last_error is not None:
            body["last_error"] = dict(self.last_error)
        return body

    def write(self, *, state: str, healthy: bool | None = None) -> dict[str, Any]:
        body = self.observation(state=state, healthy=healthy)
        parent = self.path.parent
        root = self.config.db_path.parent if self.config is not None else parent
        if self.config is not None:
            _ensure_runtime_root(root, create=True)
            if self.path != root / DIRECT_DISPATCHER_HEALTH_FILENAME:
                raise MiniDispatcherConfigError(
                    "health_path_must_use_direct_runtime_path"
                )
        else:
            parent.mkdir(parents=True, exist_ok=True)
        _reject_symlink_components(
            self.path, stop=root if self.config is not None else None
        )
        try:
            existing = self.path.lstat()
        except FileNotFoundError:
            existing = None
        if existing is not None and (
            stat.S_ISLNK(existing.st_mode)
            or not stat.S_ISREG(existing.st_mode)
            or existing.st_nlink != 1
        ):
            raise MiniDispatcherConfigError("health_path_regular_file_required")
        fd, temporary = tempfile.mkstemp(
            prefix=f".{self.path.name}.", suffix=".tmp", dir=str(parent)
        )
        try:
            os.fchmod(fd, 0o600)
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(body, handle, ensure_ascii=False, sort_keys=True)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self.path)
            os.chmod(self.path, 0o600)
        finally:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass
        return body


def read_mini_outbox_health(
    path: str | Path,
    *,
    max_age_seconds: int = 60,
) -> dict[str, Any]:
    """Read and validate a bounded health observation without secrets."""

    if isinstance(max_age_seconds, bool) or not 1 <= int(max_age_seconds) <= 86_400:
        raise ValueError("health max age out of bounds")
    target = _absolute_path(path, field="health_path")
    try:
        info = target.lstat()
    except FileNotFoundError:
        return {"ok": False, "error": "health_file_missing"}
    if (
        stat.S_ISLNK(info.st_mode)
        or not stat.S_ISREG(info.st_mode)
        or int(info.st_uid) != os.geteuid()
        or stat.S_IMODE(info.st_mode) != 0o600
    ):
        return {"ok": False, "error": "health_file_contract_invalid"}
    try:
        body = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {"ok": False, "error": "health_file_invalid"}
    if (
        not isinstance(body, dict)
        or body.get("schema_version") != MINI_DISPATCHER_HEALTH_SCHEMA_VERSION
    ):
        return {"ok": False, "error": "health_schema_invalid"}
    observed_raw = body.get("observed_at")
    try:
        observed = datetime.fromisoformat(str(observed_raw))
        if observed.tzinfo is None:
            observed = observed.replace(tzinfo=timezone.utc)
        age = (datetime.now(timezone.utc) - observed).total_seconds()
    except (TypeError, ValueError):
        return {"ok": False, "error": "health_timestamp_invalid"}
    result = dict(body)
    result["health_age_seconds"] = age
    result["ok"] = bool(body.get("ok")) and age <= int(max_age_seconds)
    if age < -60:
        result["ok"] = False
        result["error"] = "health_timestamp_from_future"
    elif age > int(max_age_seconds):
        result["error"] = "health_stale"
    return result


class PrebuiltExecutionRequestError(PermanentDispatchError):
    """The outbox does not carry one complete, direct-safe v2 request."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


def _prebuilt_execution_request(
    payload: Mapping[str, Any],
    claim: MiniOutboxClaim,
) -> Mapping[str, Any]:
    """Return only a prebuilt request; never synthesize from intake metadata."""

    value: Any = None
    if claim.request_json:
        try:
            value = json.loads(claim.request_json)
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise PrebuiltExecutionRequestError(
                "prebuilt_execution_request_invalid"
            ) from exc
    elif "execution_request" in payload:
        value = payload.get("execution_request")
    if value is None:
        raise PrebuiltExecutionRequestError("prebuilt_execution_request_required")
    if not isinstance(value, Mapping):
        raise PrebuiltExecutionRequestError("prebuilt_execution_request_invalid")
    return value


def _direct_source_refs(claim: MiniOutboxClaim) -> dict[str, Any]:
    return {
        "origin_source_id": claim.origin_source_id,
        "source_event_id": claim.source_event_id,
        "generation": claim.generation,
        "business_key": claim.business_key,
        "submission_key": claim.submission_key,
    }


def build_strict_direct_vm_request(
    execution_request: Mapping[str, Any],
    claim: MiniOutboxClaim,
    *,
    auth_principal: str = DIRECT_DISPATCHER_AUTH_PRINCIPAL,
    auth_capability: str = DIRECT_DISPATCHER_AUTH_CAPABILITY,
) -> Any:
    """Seal one prebuilt request with the strict, gate-free direct facade."""

    if not isinstance(execution_request, Mapping):
        raise PrebuiltExecutionRequestError("prebuilt_execution_request_invalid")
    data = execution_request.get("data")
    data = data if isinstance(data, Mapping) else {}
    policy = execution_request.get("execution_policy")
    policy = policy if isinstance(policy, Mapping) else {}
    artifact_root = str(
        data.get("artifact_root") or policy.get("artifact_root") or ""
    ).strip()
    artifact_cifs_root = str(data.get("artifact_cifs_root") or "").strip()
    try:
        from gateway.pnc_rca_direct_vm_submit import build_direct_vm_request

        return build_direct_vm_request(
            task_id=claim.submission_key,
            submission_key=claim.submission_key,
            auth={
                "principal": auth_principal,
                "capability": auth_capability,
            },
            source_refs=_direct_source_refs(claim),
            execution_request=execution_request,
            artifact_root=artifact_root,
            artifact_cifs_root=artifact_cifs_root,
        )
    except PrebuiltExecutionRequestError:
        raise
    except (TypeError, ValueError) as exc:
        raise PrebuiltExecutionRequestError(
            "prebuilt_execution_request_invalid"
        ) from exc


def validate_prebuilt_execution_request(
    execution_request: Mapping[str, Any],
    claim: MiniOutboxClaim,
    *,
    auth_principal: str = DIRECT_DISPATCHER_AUTH_PRINCIPAL,
    auth_capability: str = DIRECT_DISPATCHER_AUTH_CAPABILITY,
) -> None:
    """Validate a complete v2 request without calling a transport."""

    build_strict_direct_vm_request(
        execution_request,
        claim,
        auth_principal=auth_principal,
        auth_capability=auth_capability,
    )


def build_prebuilt_execution_request(
    payload: Mapping[str, Any],
    claim: MiniOutboxClaim,
) -> Mapping[str, Any]:
    """Dispatcher builder that accepts only an attached immutable request."""

    return _prebuilt_execution_request(payload, claim)


class DirectVmDispatcherBoundary:
    """Identity-checking adapter between MiniOutbox and the VM transport."""

    def __init__(
        self,
        config: MiniOutboxDispatcherConfig,
        *,
        transport: Any | None = None,
    ) -> None:
        if not config.enabled or not config.submit_enabled:
            raise MiniDispatcherConfigError(
                "direct_vm_boundary_requires_submit_enabled"
            )
        self.config = config
        if transport is None:
            from gateway.pnc_rca_direct_vm_transport import build_direct_vm_transport

            transport = build_direct_vm_transport({"create_enabled": True})
        self.transport = transport

    @staticmethod
    def _payload(claim: MiniOutboxClaim) -> Mapping[str, Any]:
        return MiniOutboxDispatcher._validate_claim_payload(claim)

    def _request(
        self,
        claim: MiniOutboxClaim,
        *,
        execution_request: Mapping[str, Any] | None = None,
    ) -> Any:
        if execution_request is None:
            if claim.request_json:
                try:
                    execution_request = json.loads(claim.request_json)
                except (TypeError, ValueError, json.JSONDecodeError) as exc:
                    raise PrebuiltExecutionRequestError(
                        "prebuilt_execution_request_invalid"
                    ) from exc
                if not isinstance(execution_request, Mapping):
                    raise PrebuiltExecutionRequestError(
                        "prebuilt_execution_request_invalid"
                    )
            else:
                execution_request = _prebuilt_execution_request(
                    self._payload(claim), claim
                )
        return build_strict_direct_vm_request(
            execution_request,
            claim,
            auth_principal=self.config.auth_principal,
            auth_capability=self.config.auth_capability,
        )

    def status(self, task_id: str, claim: MiniOutboxClaim) -> Mapping[str, Any]:
        if task_id != claim.submission_key:
            raise IdentityMismatchError("status task identity does not match request")
        observed = self.transport.status(task_id)
        if not isinstance(observed, Mapping):
            raise StatusSchemaError("direct VM status must be an object")
        state = str(observed.get("state") or "").strip().lower()
        if observed.get("task_id") != task_id:
            raise StatusSchemaError("direct VM status task identity is invalid")
        if state == "missing":
            if observed.get("submission_key") or observed.get("identity_sha256"):
                raise StatusSchemaError("missing direct VM status carried identity")
            return {"state": "missing"}
        if state == "unknown":
            return {"state": "unknown"}
        if state not in {"existing", "completed", "failed"}:
            raise StatusSchemaError("direct VM status state is invalid")
        if observed.get("submission_key") != claim.submission_key:
            raise IdentityMismatchError("direct VM submission identity mismatch")
        identity_sha256 = str(observed.get("identity_sha256") or "")
        if len(identity_sha256) != 64:
            raise StatusSchemaError("direct VM status identity hash is invalid")
        # A lookup key alone is insufficient for dedupe.  Reconstruct the
        # exact prebuilt request and compare its canonical identity before
        # accepting an existing/completed task as ours.
        expected = self._request(claim)
        if identity_sha256 != expected.identity_sha256:
            raise IdentityMismatchError("direct VM status contract identity mismatch")
        return {
            "state": state,
            "submission_key": claim.submission_key,
            "business_key": claim.business_key,
            "generation": claim.generation,
            "origin_source_id": claim.origin_source_id,
        }

    def create(self, request_json: str, claim: MiniOutboxClaim) -> Mapping[str, Any]:
        try:
            execution_request = json.loads(request_json)
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise PrebuiltExecutionRequestError(
                "prebuilt_execution_request_invalid"
            ) from exc
        if not isinstance(execution_request, Mapping):
            raise PrebuiltExecutionRequestError("prebuilt_execution_request_invalid")
        expected = self._request(claim, execution_request=execution_request)
        observed = self.transport.create(expected.to_dict())
        if not isinstance(observed, Mapping):
            raise CreateSchemaError("direct VM create must return an object")
        if not isinstance(observed.get("accepted"), bool):
            raise CreateSchemaError("direct VM create accepted flag is required")
        if observed.get("task_id") != expected.task_id:
            raise IdentityMismatchError("direct VM create task identity mismatch")
        if observed.get("submission_key") != expected.submission_key:
            raise IdentityMismatchError("direct VM create submission identity mismatch")
        if observed.get("identity_sha256") != expected.identity_sha256:
            raise IdentityMismatchError("direct VM create contract identity mismatch")
        return {
            "success": observed["accepted"] is True,
            "task_id": expected.task_id,
            "state": "accepted" if observed["accepted"] is True else "conflict",
        }


def _validate_direct_outbox_payload(
    payload: Mapping[str, Any], claim: MiniOutboxClaim
) -> None:
    if payload.get("schema_version") != "pnc_rca_mini_outbox_v2":
        raise PermanentDispatchError("direct outbox schema is invalid")
    if claim.action != "submit_rca_issue_intake":
        raise PermanentDispatchError("direct outbox action is invalid")


def build_mini_outbox_dispatcher(
    config: MiniOutboxDispatcherConfig,
    *,
    transport: Any | None = None,
) -> MiniOutboxDispatcher:
    """Build the live dispatcher only after both effect flags are enabled."""

    if not config.enabled or not config.submit_enabled:
        raise MiniDispatcherConfigError("dispatcher_submit_not_enabled")
    root = _ensure_runtime_root(config.db_path.parent, create=False)
    db_path = _validate_bound_path(
        config.db_path,
        root,
        field="db_path",
        filename=DIRECT_DB_FILENAME,
    )
    if not db_path.exists():
        raise MiniDispatcherConfigError("db_file_missing")
    boundary = DirectVmDispatcherBoundary(config, transport=transport)
    store = MiniStore(db_path)

    def check_request(request: Mapping[str, Any], claim: MiniOutboxClaim) -> None:
        validate_prebuilt_execution_request(
            request,
            claim,
            auth_principal=config.auth_principal,
            auth_capability=config.auth_capability,
        )

    return MiniOutboxDispatcher(
        store,
        lease_owner=config.lease_owner,
        status=boundary.status,
        create=boundary.create,
        build_request=build_prebuilt_execution_request,
        admission_check=_validate_direct_outbox_payload,
        request_check=check_request,
        lease_seconds=config.lease_seconds,
    )


def run_mini_outbox_dispatcher(
    config: MiniOutboxDispatcherConfig,
    *,
    health: MiniOutboxHealthReporter | None = None,
    dispatcher_factory: Callable[
        [MiniOutboxDispatcherConfig], MiniOutboxDispatcher
    ] = build_mini_outbox_dispatcher,
    stop_requested: Callable[[], bool] | None = None,
    max_iterations: int | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> int:
    """Run the resident loop; disabled modes never open DB or transport."""

    if max_iterations is not None and (
        isinstance(max_iterations, bool) or max_iterations < 1
    ):
        raise ValueError("max_iterations_must_be_positive")
    reporter = health or MiniOutboxHealthReporter(config.health_path, config=config)
    if not config.enabled:
        reporter.write(state="disabled", healthy=True)
        return 0
    if not config.submit_enabled:
        reporter.write(state="submit_disabled", healthy=True)
        return 0

    should_stop = stop_requested or (lambda: False)
    reporter.write(state="starting", healthy=True)
    try:
        dispatcher = dispatcher_factory(config)
    except Exception as exc:
        reporter.error("startup", exc)
        reporter.write(state="error", healthy=False)
        raise
    iterations = 0
    while not should_stop():
        batch_was_idle = False
        for _ in range(config.max_batch_size):
            try:
                result = dispatcher.dispatch_one()
            except Exception as exc:
                reporter.error("dispatch", exc)
                reporter.write(state="error", healthy=False)
                raise
            reporter.record(result)
            batch_was_idle = result.status == "idle"
            if batch_was_idle:
                break
        reporter.write(
            state="idle" if batch_was_idle else "running",
            healthy=True,
        )
        iterations += 1
        if max_iterations is not None and iterations >= max_iterations:
            break
        if not should_stop():
            sleep(float(config.poll_interval_seconds))
    reporter.write(state="stopped", healthy=True)
    return 0


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--env-file",
        default=DIRECT_DISPATCHER_ENV_PATH,
        help="owner-only direct.env path (must be the fixed direct runtime path)",
    )
    parser.add_argument(
        "--hermes-home",
        help="test-only alternate Hermes home; production plist leaves this unset",
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--check-config",
        action="store_true",
        help="validate and print only non-secret configuration",
    )
    mode.add_argument(
        "--health",
        action="store_true",
        help="read the dispatcher health file without opening DB or transport",
    )
    mode.add_argument(
        "--dry-run",
        action="store_true",
        help="validate configuration without opening DB or transport",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="run one enabled dispatcher iteration",
    )
    parser.add_argument(
        "--max-iterations",
        type=int,
        help="bounded resident-loop iterations for a local process probe",
    )
    parser.add_argument(
        "--health-max-age-seconds",
        type=int,
        default=60,
        help="maximum accepted health age for --health",
    )
    return parser


def _print_json(payload: Mapping[str, Any], *, stream: Any = None) -> None:
    print(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        file=stream or sys.stdout,
    )


def main(argv: list[str] | None = None) -> int:
    """Run the independent dispatcher or one side-effect-free probe."""

    args = build_arg_parser().parse_args(argv)
    if args.once and args.max_iterations is not None:
        _print_json(
            {"ok": False, "error": "once_and_max_iterations_are_mutually_exclusive"},
            stream=sys.stderr,
        )
        return 2
    if args.max_iterations is not None and args.max_iterations < 1:
        _print_json(
            {"ok": False, "error": "max_iterations_must_be_positive"},
            stream=sys.stderr,
        )
        return 2
    try:
        source, env_path = load_mini_outbox_environment(
            args.env_file,
            hermes_home=args.hermes_home,
        )
        config = MiniOutboxDispatcherConfig.from_env(
            source,
            hermes_home=args.hermes_home,
            env_file=env_path,
        )
        if args.check_config or args.dry_run:
            _print_json({
                "ok": True,
                "mode": "check-config" if args.check_config else "dry-run",
                "validation_scope": "config_only_no_db_or_transport",
                "config": config.public_dict(),
            })
            return 0
        if args.health:
            observation = read_mini_outbox_health(
                config.health_path,
                max_age_seconds=args.health_max_age_seconds,
            )
            _print_json(observation)
            return 0 if observation.get("ok") is True else 2

        stopping = False

        def request_stop(_signum: int, _frame: Any) -> None:
            nonlocal stopping
            stopping = True

        signal.signal(signal.SIGTERM, request_stop)
        signal.signal(signal.SIGINT, request_stop)
        os.umask(0o077)
        reporter = MiniOutboxHealthReporter(config.health_path, config=config)
        return run_mini_outbox_dispatcher(
            config,
            health=reporter,
            stop_requested=lambda: stopping,
            max_iterations=1 if args.once else args.max_iterations,
        )
    except Exception as exc:
        _print_json(
            {"ok": False, "error": type(exc).__name__},
            stream=sys.stderr,
        )
        return 2


__all__ = [
    "DIRECT_CONSUMER_HEALTH_FILENAME",
    "DIRECT_DB_FILENAME",
    "DIRECT_DEFAULT_GROUP_ID",
    "DIRECT_DISPATCHER_AUTH_CAPABILITY",
    "DIRECT_DISPATCHER_AUTH_PRINCIPAL",
    "DIRECT_DISPATCHER_ENV_PATH",
    "DIRECT_DISPATCHER_INSTALLED_TARGET",
    "DIRECT_DISPATCHER_LOG_ROOT",
    "DIRECT_DISPATCHER_PYTHON",
    "DIRECT_DISPATCHER_HEALTH_FILENAME",
    "DIRECT_ENV_FILENAME",
    "DIRECT_RUNTIME_RELATIVE",
    "DirectDispatchResult",
    "DirectVmDispatcherBoundary",
    "DirectStatus",
    "CreateRejectedError",
    "CreateSchemaError",
    "IdentityMismatchError",
    "MINI_DISPATCHER_CONFIG_SCHEMA_VERSION",
    "MINI_DISPATCHER_ENV_PREFIX",
    "MINI_DISPATCHER_HEALTH_SCHEMA_VERSION",
    "MINI_DISPATCHER_SERVICE_LABEL",
    "MiniDispatcherConfigError",
    "MiniOutboxDispatcherConfig",
    "MiniOutboxHealthReporter",
    "MiniOutboxDispatcher",
    "MiniOutboxDispatcherError",
    "PermanentDispatchError",
    "PermanentDispatcherError",
    "PrebuiltExecutionRequestError",
    "StatusReadError",
    "StatusSchemaError",
    "StatusUnknownError",
    "UnknownDispatchError",
    "build_arg_parser",
    "build_mini_outbox_dispatcher",
    "build_prebuilt_execution_request",
    "build_strict_direct_vm_request",
    "direct_runtime_root",
    "load_mini_outbox_environment",
    "main",
    "read_mini_outbox_health",
    "run_mini_outbox_dispatcher",
    "validate_prebuilt_execution_request",
]


if __name__ == "__main__":
    raise SystemExit(main())
