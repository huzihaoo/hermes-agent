#!/usr/bin/env python3
"""Status-first dispatcher for the direct MiniStore outbox.

The dispatcher owns only the MiniStore lease/retry lifecycle.  The external
task status read and create operation are injected at the boundary so the
state machine can be exercised without importing a live client, LaunchAgent,
activation, W3, release, or the legacy ControlStore dispatcher.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
from typing import Any, Callable, Mapping

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


__all__ = [
    "DirectDispatchResult",
    "DirectStatus",
    "CreateRejectedError",
    "CreateSchemaError",
    "IdentityMismatchError",
    "MiniOutboxDispatcher",
    "MiniOutboxDispatcherError",
    "PermanentDispatchError",
    "PermanentDispatcherError",
    "StatusReadError",
    "StatusSchemaError",
    "StatusUnknownError",
    "UnknownDispatchError",
]
