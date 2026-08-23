#!/usr/bin/env python3
"""Independent dispatcher for the direct MiniStore outbox.

The dispatcher owns only the MiniStore lease/retry lifecycle.  Downstream VM
submission, storage admission, and derived-capacity calls are injected at the
boundary, making the chain testable without importing activation, W3, release,
or the legacy ControlStore dispatcher.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
from typing import Any, Callable, Mapping

from gateway.pnc_rca_admission import validate_rca_admission
from gateway.pnc_rca_mini_store import MiniOutboxClaim, MiniStore
from gateway.pnc_rca_schema import validate_vm_execution_request_envelope


RETRY_DELAYS_SECONDS = (5, 30, 120, 600, 3_600)
MAX_ERROR_DETAIL = 500


@dataclass(frozen=True)
class DirectDispatchResult:
    status: str
    outbox_id: int | None = None
    submission_key: str = ""
    error_code: str = ""
    error_detail: str = ""


BuildRequest = Callable[[Mapping[str, Any], MiniOutboxClaim], Mapping[str, Any] | str]
SubmitRequest = Callable[[str, MiniOutboxClaim], Mapping[str, Any] | str]
BoundaryCheck = Callable[[Mapping[str, Any], MiniOutboxClaim], None]


def _now() -> datetime:
    return datetime.now(timezone.utc)


class MiniOutboxDispatcher:
    """Lease, validate, freeze, and submit one direct outbox request."""

    def __init__(
        self,
        store: MiniStore,
        *,
        lease_owner: str,
        build_request: BuildRequest | None = None,
        submit: SubmitRequest | None = None,
        admission_check: BoundaryCheck | None = None,
        derived_capacity_check: BoundaryCheck | None = None,
        now: Callable[[], datetime] = _now,
        lease_seconds: int = 300,
    ) -> None:
        if not str(lease_owner or "").strip():
            raise ValueError("lease_owner must not be empty")
        if submit is None:
            raise ValueError("submit callback is required")
        self.store = store
        self.lease_owner = str(lease_owner).strip()
        self.build_request = build_request
        self.submit = submit
        self.admission_check = admission_check
        self.derived_capacity_check = derived_capacity_check
        self.now = now
        self.lease_seconds = lease_seconds

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
            payload = json.loads(claim.payload_json)
            if not isinstance(payload, Mapping):
                raise ValueError("outbox payload must be an object")
            admission = payload.get("admission")
            if not isinstance(admission, Mapping):
                raise ValueError("outbox admission is missing")
            validated_admission = validate_rca_admission(admission)
            if validated_admission.submission_key != claim.submission_key:
                raise ValueError("outbox submission key does not match admission")
            if claim.origin_source_id != str(payload.get("origin_source_id") or ""):
                raise ValueError("outbox origin source identity mismatch")
            if self.admission_check is not None:
                self.admission_check(payload, claim)
            if self.derived_capacity_check is not None:
                self.derived_capacity_check(payload, claim)
            if claim.request_json:
                request = claim.request_json
            elif self.build_request is None:
                raise ValueError("final execution request builder is required")
            else:
                request = self.build_request(payload, claim)
            request_payload = validate_vm_execution_request_envelope(request)
            request_json, _ = self.store.freeze_execution_request(
                claim.outbox_id,
                lease_owner=self.lease_owner,
                request=request_payload,
                now=self.now(),
            )
            result = self.submit(request_json, claim)
            self.store.complete_outbox(
                claim.outbox_id,
                lease_owner=self.lease_owner,
                result=result,
                now=self.now(),
            )
            return DirectDispatchResult(
                "completed", claim.outbox_id, claim.submission_key
            )
        except Exception as exc:
            attempt = max(1, claim.attempt_count)
            delay = RETRY_DELAYS_SECONDS[
                min(attempt - 1, len(RETRY_DELAYS_SECONDS) - 1)
            ]
            retryable = attempt < len(RETRY_DELAYS_SECONDS)
            self.store.fail_outbox(
                claim.outbox_id,
                lease_owner=self.lease_owner,
                error_code=type(exc).__name__,
                error_detail=str(exc)[:MAX_ERROR_DETAIL],
                retry_at=self.now() + timedelta(seconds=delay) if retryable else None,
                quarantine=not retryable,
                now=self.now(),
            )
            return DirectDispatchResult(
                "retry" if retryable else "quarantined",
                claim.outbox_id,
                claim.submission_key,
                type(exc).__name__,
                str(exc)[:MAX_ERROR_DETAIL],
            )


__all__ = ["DirectDispatchResult", "MiniOutboxDispatcher"]
