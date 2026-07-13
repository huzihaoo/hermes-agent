"""Governed, irreversible RCA production capacity transition primitives.

This module deliberately stops before any runtime or database mutation.  It
defines the immutable evidence and file-system rules that a later transition
executor must satisfy when moving from bootstrap capacity to steady capacity.
"""

from __future__ import annotations

import errno
import fcntl
import hashlib
import hmac
import json
import math
import os
import re
import secrets
import stat
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping, Sequence


SAMPLE_SCHEMA_VERSION = "rca_capacity_sample_v3"
TRANSITION_AUTHORIZATION_SCHEMA_VERSION = (
    "rca_capacity_steady_transition_authorization_v2"
)
TRANSITION_RECEIPT_SCHEMA_VERSION = "rca_capacity_steady_transition_receipt_v2"
COMMIT_MARKER_SCHEMA_VERSION = "rca_capacity_steady_commit_marker_v2"
STEADY_TRANSITION_INTENT_SCHEMA_VERSION = "rca_capacity_steady_transition_intent_v1"

SAMPLE_HMAC_DOMAIN = b"hermes-rca-prod/capacity-sample/v3\x00"
TRANSITION_AUTHORIZATION_HMAC_DOMAIN = (
    b"hermes-rca-prod/capacity-transition-authorization/v2\x00"
)
TRANSITION_RECEIPT_HMAC_DOMAIN = b"hermes-rca-prod/capacity-transition-receipt/v2\x00"
COMMIT_MARKER_HMAC_DOMAIN = b"hermes-rca-prod/capacity-commit-marker/v2\x00"
STEADY_TRANSITION_INTENT_HMAC_DOMAIN = (
    b"hermes-rca-prod/capacity-steady-transition-intent/v1\x00"
)

BOOTSTRAP_PRODUCTION = "BOOTSTRAP_PRODUCTION"
STEADY_READY = "STEADY_READY"
STEADY_ACTIVE = "STEADY_ACTIVE"
STEADY_BLOCKED = "STEADY_BLOCKED"

MIN_STEADY_SAMPLES = 20
MAX_STEADY_SAMPLES = 200
MIN_SAMPLE_WINDOW = timedelta(days=7)
PRODUCER_WINDOW_BUFFER = timedelta(hours=6)
MIN_PRODUCER_DEADLINE_REMAINING = MIN_SAMPLE_WINDOW + PRODUCER_WINDOW_BUFFER
MAX_SAMPLE_WINDOW = timedelta(days=31)
MAX_SAMPLE_GAP = timedelta(hours=24)
MAX_LATEST_SAMPLE_AGE = timedelta(hours=24)
MAX_TRANSITION_AUTHORIZATION_TTL = timedelta(hours=1)
MAX_CLOCK_SKEW = timedelta(seconds=5)
MAX_LEDGER_BYTES = 16 * 1024 * 1024
MAX_LEDGER_SAMPLES = MAX_STEADY_SAMPLES
MAX_ARTIFACT_BYTES = 1024 * 1024
MIN_HMAC_KEY_BYTES = 32

HEX64_RE = re.compile(r"^[0-9a-f]{64}$")
IDENTITY_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")

SAMPLE_FIELDS = {
    "schema_version",
    "sample_id",
    "release_id",
    "bootstrap_epoch_id",
    "release_bom_sha256",
    "active_release_binding_sha256",
    "task_id",
    "attempt_id",
    "admission_receipt_sha256",
    "admission_receipt_fingerprint",
    "task_manifest_sha256",
    "producer_activation_receipt_sha256",
    "producer_activation_receipt_fingerprint",
    "vm_terminal_receipt_sha256",
    "vm_terminal_receipt_fingerprint",
    "host_success_receipt_sha256",
    "host_success_receipt_fingerprint",
    "terminal_status",
    "root_peak_bytes",
    "delivery_peak_bytes",
    "delivery_used_bytes",
    "input_materialized_bytes",
    "observed_at",
    "sample_fingerprint",
    "sample_hmac_sha256",
}
TRANSITION_AUTHORIZATION_FIELDS = {
    "schema_version",
    "authorization_id",
    "issued_at",
    "expires_at",
    "release_id",
    "bootstrap_epoch_id",
    "release_bom_sha256",
    "active_release_binding_sha256",
    "target_generation",
    "sample_ledger_sha256",
    "sample_count",
    "first_observed_at",
    "last_observed_at",
    "approval",
    "authorization_fingerprint",
    "authorization_hmac_sha256",
}
TRANSITION_APPROVAL_FIELDS = {
    "approval_id",
    "approval_evidence_sha256",
    "authorized_by",
    "authorized_role",
}
TRANSITION_RECEIPT_FIELDS = {
    "schema_version",
    "receipt_id",
    "created_at",
    "release_id",
    "bootstrap_epoch_id",
    "release_bom_sha256",
    "active_release_binding_sha256",
    "target_generation",
    "sample_ledger_sha256",
    "sample_count",
    "first_observed_at",
    "last_observed_at",
    "transition_authorization_sha256",
    "transition_authorization_fingerprint",
    "from_state",
    "to_state",
    "receipt_fingerprint",
    "receipt_hmac_sha256",
}
COMMIT_MARKER_FIELDS = {
    "schema_version",
    "marker_id",
    "committed_at",
    "release_id",
    "bootstrap_epoch_id",
    "release_bom_sha256",
    "active_release_binding_sha256",
    "target_generation",
    "sample_ledger_sha256",
    "transition_authorization_sha256",
    "transition_authorization_fingerprint",
    "transition_receipt_sha256",
    "transition_receipt_fingerprint",
    "from_state",
    "effective_state",
    "marker_fingerprint",
    "marker_hmac_sha256",
}
STEADY_TRANSITION_INTENT_FIELDS = {
    "schema_version",
    "intent_id",
    "created_at",
    "ratchet_origin_release_id",
    "ratchet_origin_bootstrap_epoch_id",
    "expected_generation",
    "target_generation",
    "sample_ledger_sha256",
    "business_activation_epoch_id",
    "operator",
    "reason",
    "transition_authorization",
    "transition_receipt",
    "commit_marker",
    "intent_fingerprint",
    "intent_hmac_sha256",
}

PERSISTED_CAPACITY_STATE_FIELDS = {
    "singleton_id",
    "release_id",
    "bootstrap_epoch_id",
    "state",
    "generation",
    "final_ledger_sha256",
    "transition_authorization_sha256",
    "transition_authorization_fingerprint",
    "transition_receipt_sha256",
    "transition_receipt_fingerprint",
    "commit_marker_sha256",
    "commit_marker_fingerprint",
    "evidence_bundle_sha256",
    "evidence_bundle_fingerprint",
    "authorization_issued_at",
    "authorization_expires_at",
    "receipt_created_at",
    "marker_committed_at",
    "bootstrap_initialized_at",
    "steady_activated_at",
    "updated_at",
}


class CapacityTransitionError(RuntimeError):
    """A stable, non-sensitive failure at the capacity transition boundary."""

    def __init__(self, code: str):
        self.code = str(code or "rca_capacity_transition_invalid")[:120]
        super().__init__(self.code)


@dataclass(frozen=True)
class CapacityLedgerSnapshot:
    samples: tuple[dict[str, Any], ...]
    ledger_sha256: str
    release_id: str | None
    bootstrap_epoch_id: str | None
    release_bom_sha256: str | None
    active_release_binding_sha256: str | None
    sample_count: int
    first_observed_at: str | None
    last_observed_at: str | None
    window_seconds: float
    max_gap_seconds: float
    steady_qualified: bool


def canonical_bytes(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError, RecursionError) as exc:
        raise CapacityTransitionError("rca_capacity_not_canonical") from exc


def sha256_canonical(value: Any) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def _strict_json_loads(raw: str) -> Any:
    def object_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise CapacityTransitionError("rca_capacity_json_duplicate_key")
            result[key] = value
        return result

    try:
        return json.loads(
            raw,
            object_pairs_hook=object_pairs,
            parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)),
        )
    except CapacityTransitionError:
        raise
    except (UnicodeError, ValueError, TypeError, RecursionError) as exc:
        raise CapacityTransitionError("rca_capacity_json_invalid") from exc


def _utc(value: datetime | None = None) -> datetime:
    current = value or datetime.now(timezone.utc)
    if current.tzinfo is None or current.utcoffset() is None:
        raise CapacityTransitionError("rca_capacity_time_invalid")
    return current.astimezone(timezone.utc)


def _format_timestamp(value: datetime) -> str:
    normalized = _utc(value)
    timespec = "microseconds" if normalized.microsecond else "seconds"
    return normalized.isoformat(timespec=timespec).replace("+00:00", "Z")


def _timestamp(value: Any) -> datetime:
    if not isinstance(value, str) or value != value.strip():
        raise CapacityTransitionError("rca_capacity_time_invalid")
    text = value
    candidate = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(candidate)
    except (TypeError, ValueError, OverflowError) as exc:
        raise CapacityTransitionError("rca_capacity_time_invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise CapacityTransitionError("rca_capacity_time_invalid")
    normalized = parsed.astimezone(timezone.utc)
    if text != _format_timestamp(normalized):
        raise CapacityTransitionError("rca_capacity_time_not_canonical")
    return normalized


def _persisted_timestamp(value: Any) -> datetime:
    if not isinstance(value, str) or value != value.strip():
        raise CapacityTransitionError("rca_capacity_time_invalid")
    text = value
    candidate = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(candidate)
    except (TypeError, ValueError, OverflowError) as exc:
        raise CapacityTransitionError("rca_capacity_time_invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise CapacityTransitionError("rca_capacity_time_invalid")
    return parsed.astimezone(timezone.utc)


def validate_producer_deadline_window(
    *,
    activated_at: datetime | str,
    deadline: datetime | str,
) -> dict[str, Any]:
    """Require enough authorization time for the irreversible sample latch."""

    activated = (
        _utc(activated_at)
        if isinstance(activated_at, datetime)
        else _persisted_timestamp(activated_at)
    )
    expires = (
        _utc(deadline)
        if isinstance(deadline, datetime)
        else _persisted_timestamp(deadline)
    )
    remaining = expires - activated
    if remaining < MIN_PRODUCER_DEADLINE_REMAINING:
        raise CapacityTransitionError(
            "rca_capacity_producer_window_insufficient"
        )
    return {
        "activated_at": _format_timestamp(activated),
        "deadline": _format_timestamp(expires),
        "remaining_seconds": remaining.total_seconds(),
        "minimum_remaining_seconds": (
            MIN_PRODUCER_DEADLINE_REMAINING.total_seconds()
        ),
    }


def validate_producer_live_horizon(
    *,
    observed_at: datetime | str,
    deadline: datetime | str,
) -> dict[str, Any]:
    """Require enough live authorization horizon to form a steady ledger."""

    observed = (
        _utc(observed_at)
        if isinstance(observed_at, datetime)
        else _persisted_timestamp(observed_at)
    )
    expires = (
        _utc(deadline)
        if isinstance(deadline, datetime)
        else _persisted_timestamp(deadline)
    )
    remaining = expires - observed
    if remaining < MIN_SAMPLE_WINDOW:
        raise CapacityTransitionError(
            "rca_capacity_producer_horizon_insufficient"
        )
    return {
        "observed_at": _format_timestamp(observed),
        "deadline": _format_timestamp(expires),
        "remaining_seconds": remaining.total_seconds(),
        "minimum_remaining_seconds": MIN_SAMPLE_WINDOW.total_seconds(),
    }


def _identity(value: Any, code: str) -> str:
    if not isinstance(value, str) or value != value.strip():
        raise CapacityTransitionError(code)
    normalized = value
    if not IDENTITY_RE.fullmatch(normalized):
        raise CapacityTransitionError(code)
    return normalized


def _hex(value: Any, code: str) -> str:
    if not isinstance(value, str) or value != value.strip():
        raise CapacityTransitionError(code)
    normalized = value.lower()
    if value != normalized or not HEX64_RE.fullmatch(normalized):
        raise CapacityTransitionError(code)
    return normalized


def _integer(value: Any, code: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise CapacityTransitionError(code)
    return value


def _audit_reason(value: Any) -> str:
    if not isinstance(value, str) or value != value.strip():
        raise CapacityTransitionError("rca_capacity_transition_reason_invalid")
    if not value or len(value.encode("utf-8")) > 1000:
        raise CapacityTransitionError("rca_capacity_transition_reason_invalid")
    return value


def _hmac_key(value: bytes | None) -> bytes:
    if not isinstance(value, bytes) or len(value) < MIN_HMAC_KEY_BYTES:
        raise CapacityTransitionError("rca_capacity_hmac_key_required")
    return value


def _signed_body(
    value: Mapping[str, Any],
    *,
    fingerprint_field: str,
    hmac_field: str,
) -> bytes:
    return canonical_bytes({
        key: item
        for key, item in value.items()
        if key not in {fingerprint_field, hmac_field}
    })


def _sign_evidence(
    value: Mapping[str, Any],
    *,
    fingerprint_field: str,
    hmac_field: str,
    domain: bytes,
    hmac_key: bytes,
) -> dict[str, Any]:
    signed = dict(value)
    body = _signed_body(
        signed,
        fingerprint_field=fingerprint_field,
        hmac_field=hmac_field,
    )
    signed[fingerprint_field] = hashlib.sha256(body).hexdigest()
    signed[hmac_field] = hmac.new(
        _hmac_key(hmac_key), domain + body, hashlib.sha256
    ).hexdigest()
    return signed


def _validate_signed_evidence(
    value: Mapping[str, Any],
    *,
    fingerprint_field: str,
    hmac_field: str,
    domain: bytes,
    hmac_key: bytes,
    code: str,
) -> None:
    body = _signed_body(
        value,
        fingerprint_field=fingerprint_field,
        hmac_field=hmac_field,
    )
    fingerprint = hashlib.sha256(body).hexdigest()
    raw_fingerprint = value.get(fingerprint_field)
    raw_signature = value.get(hmac_field)
    if (
        not isinstance(raw_fingerprint, str)
        or raw_fingerprint != raw_fingerprint.lower()
        or not isinstance(raw_signature, str)
        or raw_signature != raw_signature.lower()
    ):
        raise CapacityTransitionError(code)
    signature = raw_signature
    if (
        not hmac.compare_digest(raw_fingerprint, fingerprint)
        or not HEX64_RE.fullmatch(signature)
        or not hmac.compare_digest(
            signature,
            hmac.new(_hmac_key(hmac_key), domain + body, hashlib.sha256).hexdigest(),
        )
    ):
        raise CapacityTransitionError(code)


def issue_capacity_sample(
    *,
    sample_id: str,
    release_id: str,
    bootstrap_epoch_id: str,
    release_bom_sha256: str,
    active_release_binding_sha256: str,
    task_id: str,
    attempt_id: str,
    admission_receipt_sha256: str,
    admission_receipt_fingerprint: str,
    task_manifest_sha256: str,
    producer_activation_receipt_sha256: str,
    producer_activation_receipt_fingerprint: str,
    vm_terminal_receipt_sha256: str,
    vm_terminal_receipt_fingerprint: str,
    host_success_receipt_sha256: str,
    host_success_receipt_fingerprint: str,
    terminal_status: str,
    root_peak_bytes: int,
    delivery_peak_bytes: int,
    delivery_used_bytes: int,
    input_materialized_bytes: int,
    observed_at: datetime,
    hmac_key: bytes,
) -> dict[str, Any]:
    sample: dict[str, Any] = {
        "schema_version": SAMPLE_SCHEMA_VERSION,
        "sample_id": sample_id,
        "release_id": release_id,
        "bootstrap_epoch_id": bootstrap_epoch_id,
        "release_bom_sha256": release_bom_sha256,
        "active_release_binding_sha256": active_release_binding_sha256,
        "task_id": task_id,
        "attempt_id": attempt_id,
        "admission_receipt_sha256": admission_receipt_sha256,
        "admission_receipt_fingerprint": admission_receipt_fingerprint,
        "task_manifest_sha256": task_manifest_sha256,
        "producer_activation_receipt_sha256": producer_activation_receipt_sha256,
        "producer_activation_receipt_fingerprint": (
            producer_activation_receipt_fingerprint
        ),
        "vm_terminal_receipt_sha256": vm_terminal_receipt_sha256,
        "vm_terminal_receipt_fingerprint": vm_terminal_receipt_fingerprint,
        "host_success_receipt_sha256": host_success_receipt_sha256,
        "host_success_receipt_fingerprint": host_success_receipt_fingerprint,
        "terminal_status": terminal_status,
        "root_peak_bytes": root_peak_bytes,
        "delivery_peak_bytes": delivery_peak_bytes,
        "delivery_used_bytes": delivery_used_bytes,
        "input_materialized_bytes": input_materialized_bytes,
        "observed_at": _format_timestamp(observed_at),
    }
    sample = _sign_evidence(
        sample,
        fingerprint_field="sample_fingerprint",
        hmac_field="sample_hmac_sha256",
        domain=SAMPLE_HMAC_DOMAIN,
        hmac_key=hmac_key,
    )
    return validate_capacity_sample(sample, hmac_key=hmac_key)


def validate_capacity_sample(sample: Any, *, hmac_key: bytes) -> dict[str, Any]:
    if not isinstance(sample, Mapping) or set(sample) != SAMPLE_FIELDS:
        raise CapacityTransitionError("rca_capacity_sample_schema_invalid")
    if sample.get("schema_version") != SAMPLE_SCHEMA_VERSION:
        raise CapacityTransitionError("rca_capacity_sample_schema_invalid")
    for field in (
        "sample_id",
        "release_id",
        "bootstrap_epoch_id",
        "task_id",
        "attempt_id",
    ):
        _identity(sample.get(field), f"rca_capacity_sample_{field}_invalid")
    for field in (
        "release_bom_sha256",
        "active_release_binding_sha256",
        "admission_receipt_sha256",
        "admission_receipt_fingerprint",
        "task_manifest_sha256",
        "producer_activation_receipt_sha256",
        "producer_activation_receipt_fingerprint",
        "vm_terminal_receipt_sha256",
        "vm_terminal_receipt_fingerprint",
        "host_success_receipt_sha256",
        "host_success_receipt_fingerprint",
    ):
        _hex(sample.get(field), f"rca_capacity_sample_{field}_invalid")
    for field in ("root_peak_bytes", "delivery_peak_bytes", "delivery_used_bytes"):
        _integer(sample.get(field), f"rca_capacity_sample_{field}_invalid")
    if sample.get("terminal_status") != "succeeded":
        raise CapacityTransitionError("rca_capacity_sample_terminal_not_successful")
    if sample["delivery_used_bytes"] > sample["delivery_peak_bytes"]:
        raise CapacityTransitionError("rca_capacity_sample_delivery_metrics_invalid")
    if (
        _integer(
            sample.get("input_materialized_bytes"),
            "rca_capacity_sample_input_materialized_invalid",
        )
        != 0
    ):
        raise CapacityTransitionError("rca_capacity_sample_input_materialized_nonzero")
    _timestamp(sample.get("observed_at"))
    _validate_signed_evidence(
        sample,
        fingerprint_field="sample_fingerprint",
        hmac_field="sample_hmac_sha256",
        domain=SAMPLE_HMAC_DOMAIN,
        hmac_key=hmac_key,
        code="rca_capacity_sample_signature_invalid",
    )
    return dict(sample)


def _ledger_bytes(samples: Sequence[Mapping[str, Any]]) -> bytes:
    return b"".join(canonical_bytes(sample) + b"\n" for sample in samples)


def validate_sample_ledger(
    samples: Sequence[Any], *, hmac_key: bytes
) -> CapacityLedgerSnapshot:
    if isinstance(samples, (str, bytes, bytearray)) or not isinstance(
        samples, Sequence
    ):
        raise CapacityTransitionError("rca_capacity_ledger_schema_invalid")
    if len(samples) > MAX_LEDGER_SAMPLES:
        raise CapacityTransitionError("rca_capacity_ledger_too_many_samples")
    key = _hmac_key(hmac_key)
    validated = tuple(
        validate_capacity_sample(sample, hmac_key=key) for sample in samples
    )
    raw = _ledger_bytes(validated)
    if len(raw) > MAX_LEDGER_BYTES:
        raise CapacityTransitionError("rca_capacity_ledger_too_large")
    if not validated:
        return CapacityLedgerSnapshot(
            samples=(),
            ledger_sha256=hashlib.sha256(b"").hexdigest(),
            release_id=None,
            bootstrap_epoch_id=None,
            release_bom_sha256=None,
            active_release_binding_sha256=None,
            sample_count=0,
            first_observed_at=None,
            last_observed_at=None,
            window_seconds=0.0,
            max_gap_seconds=0.0,
            steady_qualified=False,
        )
    release_id = validated[0]["release_id"]
    epoch_id = validated[0]["bootstrap_epoch_id"]
    release_bom_sha256 = validated[0]["release_bom_sha256"]
    active_release_binding_sha256 = validated[0]["active_release_binding_sha256"]
    producer_activation_receipt_sha256 = validated[0][
        "producer_activation_receipt_sha256"
    ]
    producer_activation_receipt_fingerprint = validated[0][
        "producer_activation_receipt_fingerprint"
    ]
    unique_fields = {
        field: set()
        for field in (
            "sample_id",
            "sample_fingerprint",
            "sample_hmac_sha256",
            "task_id",
            "attempt_id",
            "admission_receipt_sha256",
            "admission_receipt_fingerprint",
            "task_manifest_sha256",
            "vm_terminal_receipt_sha256",
            "vm_terminal_receipt_fingerprint",
            "host_success_receipt_sha256",
            "host_success_receipt_fingerprint",
        )
    }
    observed: list[datetime] = []
    for sample in validated:
        if sample["release_id"] != release_id:
            raise CapacityTransitionError("rca_capacity_ledger_release_mismatch")
        if sample["bootstrap_epoch_id"] != epoch_id:
            raise CapacityTransitionError("rca_capacity_ledger_epoch_mismatch")
        if sample["release_bom_sha256"] != release_bom_sha256:
            raise CapacityTransitionError("rca_capacity_ledger_release_bom_mismatch")
        if sample["active_release_binding_sha256"] != active_release_binding_sha256:
            raise CapacityTransitionError("rca_capacity_ledger_active_binding_mismatch")
        if (
            sample["producer_activation_receipt_sha256"]
            != producer_activation_receipt_sha256
            or sample["producer_activation_receipt_fingerprint"]
            != producer_activation_receipt_fingerprint
        ):
            raise CapacityTransitionError(
                "rca_capacity_ledger_producer_activation_binding_mismatch"
            )
        for field, values in unique_fields.items():
            candidate = sample[field]
            if candidate in values:
                raise CapacityTransitionError(
                    f"rca_capacity_ledger_duplicate_{field}"[:120]
                )
            values.add(candidate)
        observed.append(_timestamp(sample["observed_at"]))
    gaps: list[float] = []
    for previous, current in zip(observed, observed[1:], strict=False):
        gap = current - previous
        if gap <= timedelta(0):
            raise CapacityTransitionError("rca_capacity_ledger_chronology_invalid")
        if gap > MAX_SAMPLE_GAP:
            raise CapacityTransitionError("rca_capacity_ledger_gap_exceeded")
        gaps.append(gap.total_seconds())
    window = observed[-1] - observed[0]
    qualified = (
        MIN_STEADY_SAMPLES <= len(validated) <= MAX_STEADY_SAMPLES
        and MIN_SAMPLE_WINDOW <= window <= MAX_SAMPLE_WINDOW
    )
    return CapacityLedgerSnapshot(
        samples=validated,
        ledger_sha256=hashlib.sha256(raw).hexdigest(),
        release_id=release_id,
        bootstrap_epoch_id=epoch_id,
        release_bom_sha256=release_bom_sha256,
        active_release_binding_sha256=active_release_binding_sha256,
        sample_count=len(validated),
        first_observed_at=validated[0]["observed_at"],
        last_observed_at=validated[-1]["observed_at"],
        window_seconds=window.total_seconds(),
        max_gap_seconds=max(gaps, default=0.0),
        steady_qualified=qualified,
    )


def _require_owner_only_stat(value: os.stat_result) -> None:
    if (
        not stat.S_ISREG(value.st_mode)
        or stat.S_IMODE(value.st_mode) != 0o600
        or value.st_uid != os.geteuid()
        or value.st_nlink != 1
    ):
        raise CapacityTransitionError("rca_capacity_file_not_owner_only")


def _stat_identity(value: os.stat_result) -> tuple[int, ...]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_uid,
        value.st_nlink,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _require_private_directory_stat(value: os.stat_result) -> None:
    if (
        not stat.S_ISDIR(value.st_mode)
        or value.st_uid != os.geteuid()
        or stat.S_IMODE(value.st_mode) & 0o022
    ):
        raise CapacityTransitionError("rca_capacity_artifact_parent_invalid")


def _open_parent_directory(path: Path) -> int:
    if not hasattr(os, "O_NOFOLLOW"):
        raise CapacityTransitionError("rca_capacity_no_follow_unavailable")
    flags = (
        os.O_RDONLY
        | os.O_NOFOLLOW
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_DIRECTORY", 0)
    )
    try:
        descriptor = os.open(path.parent, flags)
    except OSError as exc:
        raise CapacityTransitionError("rca_capacity_artifact_parent_invalid") from exc
    try:
        _require_private_directory_stat(os.fstat(descriptor))
    except Exception:
        os.close(descriptor)
        raise
    return descriptor


def _open_owner_only_at(
    parent_descriptor: int,
    name: str,
    flags: int,
    *,
    create: bool = False,
) -> int:
    open_flags = flags | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
    if create:
        open_flags |= os.O_CREAT
    try:
        descriptor = os.open(name, open_flags, 0o600, dir_fd=parent_descriptor)
    except OSError as exc:
        raise CapacityTransitionError("rca_capacity_file_unavailable") from exc
    try:
        _require_owner_only_stat(os.fstat(descriptor))
    except Exception:
        os.close(descriptor)
        raise
    return descriptor


def _path_stat_at(parent_descriptor: int, name: str) -> os.stat_result:
    try:
        value = os.stat(name, dir_fd=parent_descriptor, follow_symlinks=False)
    except OSError as exc:
        raise CapacityTransitionError("rca_capacity_file_unavailable") from exc
    _require_owner_only_stat(value)
    return value


def _fsync(descriptor: int, *, code: str) -> None:
    try:
        os.fsync(descriptor)
    except OSError as exc:
        raise CapacityTransitionError(code) from exc


@contextmanager
def capacity_flock(
    lock_path: str | Path,
    *,
    exclusive: bool,
    timeout_seconds: float = 5.0,
) -> Iterator[int]:
    """Hold a validated owner-only shared or exclusive advisory lock."""

    if (
        isinstance(timeout_seconds, bool)
        or not isinstance(timeout_seconds, (int, float))
        or not math.isfinite(timeout_seconds)
        or timeout_seconds < 0
    ):
        raise CapacityTransitionError("rca_capacity_lock_timeout_invalid")
    path = Path(lock_path).expanduser().absolute()
    parent_descriptor = _open_parent_directory(path)
    descriptor = _open_owner_only_at(
        parent_descriptor, path.name, os.O_RDWR, create=True
    )
    operation = fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH
    deadline = time.monotonic() + timeout_seconds
    try:
        while True:
            try:
                fcntl.flock(descriptor, operation | fcntl.LOCK_NB)
                if _stat_identity(_path_stat_at(parent_descriptor, path.name)) != (
                    _stat_identity(os.fstat(descriptor))
                ):
                    raise CapacityTransitionError("rca_capacity_lock_identity_changed")
                break
            except BlockingIOError as exc:
                if time.monotonic() >= deadline:
                    raise CapacityTransitionError("rca_capacity_lock_timeout") from exc
                time.sleep(min(0.01, max(0.001, timeout_seconds / 20)))
        yield descriptor
    finally:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)
            os.close(parent_descriptor)


def _read_owner_only_bytes_with_identity(
    path: Path, *, max_bytes: int
) -> tuple[bytes, tuple[int, ...]]:
    parent_descriptor = _open_parent_directory(path)
    descriptor = _open_owner_only_at(parent_descriptor, path.name, os.O_RDONLY)
    try:
        before = os.fstat(descriptor)
        if before.st_size < 1 or before.st_size > max_bytes:
            raise CapacityTransitionError("rca_capacity_file_size_invalid")
        chunks: list[bytes] = []
        remaining = max_bytes + 1
        while remaining:
            chunk = os.read(descriptor, min(64 * 1024, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        raw = b"".join(chunks)
        after = os.fstat(descriptor)
        if (
            len(raw) > max_bytes
            or _stat_identity(before) != _stat_identity(after)
            or _stat_identity(_path_stat_at(parent_descriptor, path.name))
            != _stat_identity(after)
        ):
            raise CapacityTransitionError("rca_capacity_file_changed_during_read")
        return raw, _stat_identity(after)
    finally:
        os.close(descriptor)
        os.close(parent_descriptor)


def _read_owner_only_bytes(path: Path, *, max_bytes: int) -> bytes:
    return _read_owner_only_bytes_with_identity(path, max_bytes=max_bytes)[0]


def read_owner_only_json(path: str | Path) -> tuple[dict[str, Any], str]:
    """Read a strict owner-only JSON artifact and return its raw SHA-256."""

    raw = _read_owner_only_bytes(
        Path(path).expanduser().absolute(), max_bytes=MAX_ARTIFACT_BYTES
    )
    try:
        decoded = raw.decode("utf-8")
    except UnicodeError as exc:
        raise CapacityTransitionError("rca_capacity_json_invalid") from exc
    value = _strict_json_loads(decoded)
    if not isinstance(value, dict):
        raise CapacityTransitionError("rca_capacity_json_object_required")
    if canonical_bytes(value) != raw:
        raise CapacityTransitionError("rca_capacity_json_not_canonical")
    return value, hashlib.sha256(raw).hexdigest()


def publish_owner_only_no_clobber(
    path: str | Path,
    artifact: Mapping[str, Any],
) -> dict[str, Any]:
    """Atomically publish an owner-only JSON artifact without replacement."""

    target = Path(path).expanduser().absolute()
    parent_descriptor = _open_parent_directory(target)
    raw = canonical_bytes(dict(artifact))
    if len(raw) > MAX_ARTIFACT_BYTES:
        raise CapacityTransitionError("rca_capacity_artifact_too_large")
    temporary_name = f".{target.name}.tmp-{os.getpid()}-{secrets.token_hex(8)}"
    descriptor: int | None = None

    def existing_result() -> dict[str, Any]:
        observed = _read_owner_only_bytes(target, max_bytes=MAX_ARTIFACT_BYTES)
        if observed != raw:
            raise CapacityTransitionError("rca_capacity_artifact_exists")
        return {
            "path": str(target),
            "sha256": hashlib.sha256(observed).hexdigest(),
            "size_bytes": len(observed),
        }

    try:
        try:
            existing = os.stat(
                target.name, dir_fd=parent_descriptor, follow_symlinks=False
            )
        except FileNotFoundError:
            existing = None
        except OSError as exc:
            raise CapacityTransitionError("rca_capacity_artifact_exists") from exc
        if existing is not None:
            try:
                _require_owner_only_stat(existing)
            except CapacityTransitionError as exc:
                raise CapacityTransitionError("rca_capacity_artifact_exists") from exc
            return existing_result()
        descriptor = os.open(
            temporary_name,
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | getattr(os, "O_CLOEXEC", 0)
            | os.O_NOFOLLOW,
            0o600,
            dir_fd=parent_descriptor,
        )
        _require_owner_only_stat(os.fstat(descriptor))
        view = memoryview(raw)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise CapacityTransitionError("rca_capacity_artifact_write_failed")
            view = view[written:]
        _fsync(descriptor, code="rca_capacity_artifact_fsync_failed")
        temporary_identity = _stat_identity(os.fstat(descriptor))
        if _stat_identity(_path_stat_at(parent_descriptor, temporary_name)) != (
            temporary_identity
        ):
            raise CapacityTransitionError("rca_capacity_artifact_temporary_changed")
        os.close(descriptor)
        descriptor = None
        try:
            os.link(
                temporary_name,
                target.name,
                src_dir_fd=parent_descriptor,
                dst_dir_fd=parent_descriptor,
                follow_symlinks=False,
            )
        except FileExistsError as exc:
            try:
                return existing_result()
            except CapacityTransitionError as inner:
                raise inner from exc
        except OSError as exc:
            if exc.errno == errno.EEXIST:
                try:
                    return existing_result()
                except CapacityTransitionError as inner:
                    raise inner from exc
            raise CapacityTransitionError(
                "rca_capacity_artifact_publish_failed"
            ) from exc
        try:
            linked = os.stat(
                target.name, dir_fd=parent_descriptor, follow_symlinks=False
            )
        except OSError as exc:
            raise CapacityTransitionError(
                "rca_capacity_artifact_publish_identity_invalid"
            ) from exc
        if (
            not stat.S_ISREG(linked.st_mode)
            or stat.S_IMODE(linked.st_mode) != 0o600
            or linked.st_uid != os.geteuid()
            or linked.st_nlink != 2
            or (
                linked.st_dev,
                linked.st_ino,
                linked.st_mode,
                linked.st_uid,
                linked.st_size,
            )
            != (
                temporary_identity[0],
                temporary_identity[1],
                temporary_identity[2],
                temporary_identity[3],
                temporary_identity[5],
            )
        ):
            raise CapacityTransitionError(
                "rca_capacity_artifact_publish_identity_invalid"
            )
        os.unlink(temporary_name, dir_fd=parent_descriptor)
        _fsync(parent_descriptor, code="rca_capacity_artifact_directory_fsync_failed")
        written_raw = _read_owner_only_bytes(target, max_bytes=MAX_ARTIFACT_BYTES)
        if written_raw != raw:
            raise CapacityTransitionError("rca_capacity_artifact_verify_failed")
        return {
            "path": str(target),
            "sha256": hashlib.sha256(written_raw).hexdigest(),
            "size_bytes": len(raw),
        }
    finally:
        if descriptor is not None:
            os.close(descriptor)
        try:
            os.unlink(temporary_name, dir_fd=parent_descriptor)
        except FileNotFoundError:
            pass
        os.close(parent_descriptor)


def _read_sample_ledger_unlocked(
    path: Path, *, hmac_key: bytes
) -> CapacityLedgerSnapshot:
    raw = _read_owner_only_bytes(path, max_bytes=MAX_LEDGER_BYTES)
    if not raw.endswith(b"\n"):
        raise CapacityTransitionError("rca_capacity_ledger_truncated")
    lines = raw.splitlines()
    if len(lines) > MAX_LEDGER_SAMPLES:
        raise CapacityTransitionError("rca_capacity_ledger_too_many_samples")
    samples: list[dict[str, Any]] = []
    for line in lines:
        if not line:
            raise CapacityTransitionError("rca_capacity_ledger_line_invalid")
        try:
            value = _strict_json_loads(line.decode("utf-8"))
        except UnicodeError as exc:
            raise CapacityTransitionError("rca_capacity_ledger_line_invalid") from exc
        if not isinstance(value, dict) or canonical_bytes(value) != line:
            raise CapacityTransitionError("rca_capacity_ledger_line_not_canonical")
        samples.append(value)
    snapshot = validate_sample_ledger(samples, hmac_key=hmac_key)
    if snapshot.ledger_sha256 != hashlib.sha256(raw).hexdigest():
        raise CapacityTransitionError("rca_capacity_ledger_sha_invalid")
    return snapshot


def read_sample_ledger(
    path: str | Path,
    *,
    hmac_key: bytes,
    timeout_seconds: float = 5.0,
) -> CapacityLedgerSnapshot:
    ledger_path = Path(path).expanduser().absolute()
    lock_path = ledger_path.with_name(f"{ledger_path.name}.lock")
    with capacity_flock(lock_path, exclusive=False, timeout_seconds=timeout_seconds):
        return _read_sample_ledger_unlocked(ledger_path, hmac_key=hmac_key)


def validate_persisted_capacity_state(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != PERSISTED_CAPACITY_STATE_FIELDS:
        raise CapacityTransitionError("rca_capacity_persisted_state_schema_invalid")
    if (
        _integer(
            value.get("singleton_id"),
            "rca_capacity_persisted_state_identity_invalid",
            minimum=1,
        )
        != 1
    ):
        raise CapacityTransitionError("rca_capacity_persisted_state_identity_invalid")
    _identity(value.get("release_id"), "rca_capacity_persisted_release_id_invalid")
    _identity(
        value.get("bootstrap_epoch_id"),
        "rca_capacity_persisted_bootstrap_epoch_id_invalid",
    )
    state = value.get("state")
    generation = _integer(
        value.get("generation"), "rca_capacity_persisted_generation_invalid", minimum=1
    )
    initialized = _persisted_timestamp(value.get("bootstrap_initialized_at"))
    updated = _persisted_timestamp(value.get("updated_at"))
    if updated < initialized:
        raise CapacityTransitionError("rca_capacity_persisted_state_time_invalid")
    evidence_fields = (
        "final_ledger_sha256",
        "transition_authorization_sha256",
        "transition_authorization_fingerprint",
        "transition_receipt_sha256",
        "transition_receipt_fingerprint",
        "commit_marker_sha256",
        "commit_marker_fingerprint",
        "evidence_bundle_sha256",
        "evidence_bundle_fingerprint",
    )
    transition_time_fields = (
        "authorization_issued_at",
        "authorization_expires_at",
        "receipt_created_at",
        "marker_committed_at",
    )
    if state == BOOTSTRAP_PRODUCTION:
        if generation != 1:
            raise CapacityTransitionError("rca_capacity_bootstrap_generation_invalid")
        if any(value.get(field) is not None for field in evidence_fields):
            raise CapacityTransitionError("rca_capacity_bootstrap_evidence_present")
        if any(value.get(field) is not None for field in transition_time_fields):
            raise CapacityTransitionError("rca_capacity_bootstrap_evidence_present")
        if value.get("steady_activated_at") is not None:
            raise CapacityTransitionError("rca_capacity_bootstrap_evidence_present")
    elif state == STEADY_ACTIVE:
        if generation < 2:
            raise CapacityTransitionError("rca_capacity_steady_generation_invalid")
        for field in evidence_fields:
            _hex(value.get(field), f"rca_capacity_persisted_{field}_invalid")
        issued, expires, created, committed = (
            _persisted_timestamp(value.get(field)) for field in transition_time_fields
        )
        activated = _persisted_timestamp(value.get("steady_activated_at"))
        if not (
            initialized <= issued <= created <= committed <= activated <= updated
            and issued < expires
            and committed <= expires
        ):
            raise CapacityTransitionError("rca_capacity_persisted_state_time_invalid")
    else:
        raise CapacityTransitionError("rca_capacity_persisted_state_value_invalid")
    return dict(value)


def _write_all(descriptor: int, raw: bytes, *, code: str) -> None:
    view = memoryview(raw)
    while view:
        try:
            written = os.write(descriptor, view)
        except OSError as exc:
            raise CapacityTransitionError(code) from exc
        if written <= 0:
            raise CapacityTransitionError(code)
        view = view[written:]


def _optional_owner_only_stat_at(
    parent_descriptor: int, name: str
) -> os.stat_result | None:
    try:
        value = os.stat(name, dir_fd=parent_descriptor, follow_symlinks=False)
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise CapacityTransitionError("rca_capacity_file_unavailable") from exc
    _require_owner_only_stat(value)
    return value


def append_capacity_sample(
    path: str | Path,
    sample: Mapping[str, Any],
    *,
    hmac_key: bytes,
    persisted_state_loader: Callable[[], Mapping[str, Any]],
    timeout_seconds: float = 5.0,
) -> CapacityLedgerSnapshot:
    """Logically append one sample through an atomic copy-on-write transaction.

    The caller owns the outer global transition lock.  This function acquires
    the ledger lock before invoking the DB state loader, preserving the required
    global -> ledger -> SQLite lock order.
    """

    candidate = validate_capacity_sample(sample, hmac_key=hmac_key)
    if not callable(persisted_state_loader):
        raise CapacityTransitionError("rca_capacity_persisted_state_loader_required")
    ledger_path = Path(path).expanduser().absolute()
    lock_path = ledger_path.with_name(f"{ledger_path.name}.lock")
    with capacity_flock(lock_path, exclusive=True, timeout_seconds=timeout_seconds):
        capacity_state = validate_persisted_capacity_state(persisted_state_loader())
        if capacity_state["state"] == STEADY_ACTIVE:
            raise CapacityTransitionError("rca_capacity_ledger_frozen_after_steady")
        if (
            capacity_state["release_id"] != candidate["release_id"]
            or capacity_state["bootstrap_epoch_id"] != candidate["bootstrap_epoch_id"]
        ):
            raise CapacityTransitionError(
                "rca_capacity_ledger_persisted_state_mismatch"
            )
        parent_descriptor = _open_parent_directory(ledger_path)
        temporary_name = f".{ledger_path.name}.txn-{os.getpid()}-{secrets.token_hex(8)}"
        descriptor: int | None = None
        try:
            prior_stat = _optional_owner_only_stat_at(
                parent_descriptor, ledger_path.name
            )
            if prior_stat is not None:
                current = _read_sample_ledger_unlocked(ledger_path, hmac_key=hmac_key)
                if candidate in current.samples:
                    return current
                prior_identity = _stat_identity(prior_stat)
                if (
                    _stat_identity(_path_stat_at(parent_descriptor, ledger_path.name))
                    != prior_identity
                ):
                    raise CapacityTransitionError(
                        "rca_capacity_ledger_identity_changed"
                    )
                combined = [*current.samples, candidate]
            else:
                prior_identity = None
                combined = [candidate]
            validated = validate_sample_ledger(combined, hmac_key=hmac_key)
            if validated.window_seconds > MAX_SAMPLE_WINDOW.total_seconds():
                raise CapacityTransitionError(
                    "rca_capacity_steady_sample_window_exceeded"
                )
            raw = _ledger_bytes(validated.samples)
            descriptor = os.open(
                temporary_name,
                os.O_WRONLY
                | os.O_CREAT
                | os.O_EXCL
                | os.O_NOFOLLOW
                | getattr(os, "O_CLOEXEC", 0),
                0o600,
                dir_fd=parent_descriptor,
            )
            _require_owner_only_stat(os.fstat(descriptor))
            _write_all(
                descriptor, raw, code="rca_capacity_ledger_transaction_write_failed"
            )
            _fsync(descriptor, code="rca_capacity_ledger_transaction_fsync_failed")
            temporary_identity = _stat_identity(os.fstat(descriptor))
            if (
                _stat_identity(_path_stat_at(parent_descriptor, temporary_name))
                != temporary_identity
            ):
                raise CapacityTransitionError(
                    "rca_capacity_ledger_transaction_identity_changed"
                )
            os.close(descriptor)
            descriptor = None
            observed_prior = _optional_owner_only_stat_at(
                parent_descriptor, ledger_path.name
            )
            if prior_identity is None:
                if observed_prior is not None:
                    raise CapacityTransitionError("rca_capacity_ledger_create_raced")
                try:
                    os.link(
                        temporary_name,
                        ledger_path.name,
                        src_dir_fd=parent_descriptor,
                        dst_dir_fd=parent_descriptor,
                        follow_symlinks=False,
                    )
                except OSError as exc:
                    raise CapacityTransitionError(
                        "rca_capacity_ledger_transaction_publish_failed"
                    ) from exc
                os.unlink(temporary_name, dir_fd=parent_descriptor)
            else:
                if (
                    observed_prior is None
                    or _stat_identity(observed_prior) != prior_identity
                ):
                    raise CapacityTransitionError(
                        "rca_capacity_ledger_identity_changed"
                    )
                try:
                    os.replace(
                        temporary_name,
                        ledger_path.name,
                        src_dir_fd=parent_descriptor,
                        dst_dir_fd=parent_descriptor,
                    )
                except OSError as exc:
                    raise CapacityTransitionError(
                        "rca_capacity_ledger_transaction_publish_failed"
                    ) from exc
            _fsync(
                parent_descriptor,
                code="rca_capacity_ledger_transaction_directory_fsync_failed",
            )
        finally:
            if descriptor is not None:
                os.close(descriptor)
            try:
                os.unlink(temporary_name, dir_fd=parent_descriptor)
            except FileNotFoundError:
                pass
            os.close(parent_descriptor)
        observed = _read_sample_ledger_unlocked(ledger_path, hmac_key=hmac_key)
        if observed.ledger_sha256 != validated.ledger_sha256:
            raise CapacityTransitionError("rca_capacity_ledger_append_verify_failed")
        return observed


def _require_qualified_ledger(ledger: CapacityLedgerSnapshot) -> None:
    if ledger.sample_count < MIN_STEADY_SAMPLES:
        raise CapacityTransitionError("rca_capacity_steady_samples_insufficient")
    if ledger.sample_count > MAX_STEADY_SAMPLES:
        raise CapacityTransitionError("rca_capacity_steady_sample_count_exceeded")
    if ledger.window_seconds < MIN_SAMPLE_WINDOW.total_seconds():
        raise CapacityTransitionError("rca_capacity_steady_sample_window_below_minimum")
    if ledger.window_seconds > MAX_SAMPLE_WINDOW.total_seconds():
        raise CapacityTransitionError("rca_capacity_steady_sample_window_exceeded")
    if ledger.max_gap_seconds > MAX_SAMPLE_GAP.total_seconds():
        raise CapacityTransitionError("rca_capacity_steady_sample_gap_exceeded")
    if not ledger.steady_qualified:
        raise CapacityTransitionError("rca_capacity_steady_samples_insufficient")


def issue_transition_authorization(
    *,
    ledger: CapacityLedgerSnapshot,
    authorization_id: str,
    approval_id: str,
    approval_evidence_sha256: str,
    authorized_by: str,
    authorized_role: str,
    issued_at: datetime,
    expires_at: datetime,
    persisted_state: Mapping[str, Any],
    hmac_key: bytes,
) -> dict[str, Any]:
    _require_qualified_ledger(ledger)
    state = validate_persisted_capacity_state(persisted_state)
    if state["state"] != BOOTSTRAP_PRODUCTION:
        raise CapacityTransitionError("rca_capacity_transition_state_not_bootstrap")
    if (
        state["release_id"] != ledger.release_id
        or state["bootstrap_epoch_id"] != ledger.bootstrap_epoch_id
    ):
        raise CapacityTransitionError("rca_capacity_transition_state_binding_invalid")
    authorization: dict[str, Any] = {
        "schema_version": TRANSITION_AUTHORIZATION_SCHEMA_VERSION,
        "authorization_id": authorization_id,
        "issued_at": _format_timestamp(issued_at),
        "expires_at": _format_timestamp(expires_at),
        "release_id": ledger.release_id,
        "bootstrap_epoch_id": ledger.bootstrap_epoch_id,
        "release_bom_sha256": ledger.release_bom_sha256,
        "active_release_binding_sha256": ledger.active_release_binding_sha256,
        "target_generation": state["generation"] + 1,
        "sample_ledger_sha256": ledger.ledger_sha256,
        "sample_count": ledger.sample_count,
        "first_observed_at": ledger.first_observed_at,
        "last_observed_at": ledger.last_observed_at,
        "approval": {
            "approval_id": approval_id,
            "approval_evidence_sha256": approval_evidence_sha256,
            "authorized_by": authorized_by,
            "authorized_role": authorized_role,
        },
    }
    authorization = _sign_evidence(
        authorization,
        fingerprint_field="authorization_fingerprint",
        hmac_field="authorization_hmac_sha256",
        domain=TRANSITION_AUTHORIZATION_HMAC_DOMAIN,
        hmac_key=hmac_key,
    )
    return validate_transition_authorization(
        authorization, ledger=ledger, now=issued_at, hmac_key=hmac_key
    )


def validate_transition_authorization(
    authorization: Any,
    *,
    ledger: CapacityLedgerSnapshot,
    now: datetime,
    hmac_key: bytes,
    allow_historical: bool = False,
) -> dict[str, Any]:
    _require_qualified_ledger(ledger)
    if (
        not isinstance(authorization, Mapping)
        or set(authorization) != TRANSITION_AUTHORIZATION_FIELDS
        or authorization.get("schema_version")
        != TRANSITION_AUTHORIZATION_SCHEMA_VERSION
    ):
        raise CapacityTransitionError(
            "rca_capacity_transition_authorization_schema_invalid"
        )
    approval = authorization.get("approval")
    if not isinstance(approval, Mapping) or set(approval) != TRANSITION_APPROVAL_FIELDS:
        raise CapacityTransitionError("rca_capacity_transition_approval_schema_invalid")
    _identity(
        authorization.get("authorization_id"), "rca_capacity_authorization_id_invalid"
    )
    _identity(approval.get("approval_id"), "rca_capacity_approval_id_invalid")
    _identity(approval.get("authorized_by"), "rca_capacity_authorized_by_invalid")
    if approval.get("authorized_role") != "owner":
        raise CapacityTransitionError("rca_capacity_transition_owner_required")
    _hex(
        approval.get("approval_evidence_sha256"),
        "rca_capacity_approval_evidence_invalid",
    )
    expected_bindings = {
        "release_id": ledger.release_id,
        "bootstrap_epoch_id": ledger.bootstrap_epoch_id,
        "release_bom_sha256": ledger.release_bom_sha256,
        "active_release_binding_sha256": ledger.active_release_binding_sha256,
        "sample_ledger_sha256": ledger.ledger_sha256,
        "sample_count": ledger.sample_count,
        "first_observed_at": ledger.first_observed_at,
        "last_observed_at": ledger.last_observed_at,
    }
    if any(authorization.get(key) != value for key, value in expected_bindings.items()):
        raise CapacityTransitionError("rca_capacity_transition_ledger_binding_invalid")
    _integer(
        authorization.get("target_generation"),
        "rca_capacity_transition_generation_invalid",
        minimum=2,
    )
    issued = _timestamp(authorization.get("issued_at"))
    expires = _timestamp(authorization.get("expires_at"))
    current = _utc(now)
    last_sample = _timestamp(ledger.last_observed_at)
    if (
        expires <= issued
        or expires - issued > MAX_TRANSITION_AUTHORIZATION_TTL
        or issued < last_sample
        or issued - last_sample > MAX_LATEST_SAMPLE_AGE
        or issued > current + MAX_CLOCK_SKEW
        or (not allow_historical and current > expires)
    ):
        raise CapacityTransitionError(
            "rca_capacity_transition_authorization_time_invalid"
        )
    _validate_signed_evidence(
        authorization,
        fingerprint_field="authorization_fingerprint",
        hmac_field="authorization_hmac_sha256",
        domain=TRANSITION_AUTHORIZATION_HMAC_DOMAIN,
        hmac_key=hmac_key,
        code="rca_capacity_transition_authorization_tampered",
    )
    return dict(authorization)


def issue_transition_receipt(
    *,
    ledger: CapacityLedgerSnapshot,
    authorization: Mapping[str, Any],
    receipt_id: str,
    created_at: datetime,
    hmac_key: bytes,
) -> dict[str, Any]:
    validated_authorization = validate_transition_authorization(
        authorization, ledger=ledger, now=created_at, hmac_key=hmac_key
    )
    receipt: dict[str, Any] = {
        "schema_version": TRANSITION_RECEIPT_SCHEMA_VERSION,
        "receipt_id": receipt_id,
        "created_at": _format_timestamp(created_at),
        "release_id": ledger.release_id,
        "bootstrap_epoch_id": ledger.bootstrap_epoch_id,
        "release_bom_sha256": ledger.release_bom_sha256,
        "active_release_binding_sha256": ledger.active_release_binding_sha256,
        "target_generation": validated_authorization["target_generation"],
        "sample_ledger_sha256": ledger.ledger_sha256,
        "sample_count": ledger.sample_count,
        "first_observed_at": ledger.first_observed_at,
        "last_observed_at": ledger.last_observed_at,
        "transition_authorization_sha256": sha256_canonical(validated_authorization),
        "transition_authorization_fingerprint": validated_authorization[
            "authorization_fingerprint"
        ],
        "from_state": BOOTSTRAP_PRODUCTION,
        "to_state": STEADY_ACTIVE,
    }
    receipt = _sign_evidence(
        receipt,
        fingerprint_field="receipt_fingerprint",
        hmac_field="receipt_hmac_sha256",
        domain=TRANSITION_RECEIPT_HMAC_DOMAIN,
        hmac_key=hmac_key,
    )
    return validate_transition_receipt(
        receipt,
        ledger=ledger,
        authorization=validated_authorization,
        now=created_at,
        hmac_key=hmac_key,
    )


def validate_transition_receipt(
    receipt: Any,
    *,
    ledger: CapacityLedgerSnapshot,
    authorization: Mapping[str, Any],
    now: datetime,
    hmac_key: bytes,
    allow_historical: bool = False,
) -> dict[str, Any]:
    validated_authorization = validate_transition_authorization(
        authorization,
        ledger=ledger,
        now=now,
        hmac_key=hmac_key,
        allow_historical=allow_historical,
    )
    if (
        not isinstance(receipt, Mapping)
        or set(receipt) != TRANSITION_RECEIPT_FIELDS
        or receipt.get("schema_version") != TRANSITION_RECEIPT_SCHEMA_VERSION
    ):
        raise CapacityTransitionError("rca_capacity_transition_receipt_schema_invalid")
    _identity(receipt.get("receipt_id"), "rca_capacity_transition_receipt_id_invalid")
    expected_bindings = {
        "release_id": ledger.release_id,
        "bootstrap_epoch_id": ledger.bootstrap_epoch_id,
        "release_bom_sha256": ledger.release_bom_sha256,
        "active_release_binding_sha256": ledger.active_release_binding_sha256,
        "target_generation": validated_authorization["target_generation"],
        "sample_ledger_sha256": ledger.ledger_sha256,
        "sample_count": ledger.sample_count,
        "first_observed_at": ledger.first_observed_at,
        "last_observed_at": ledger.last_observed_at,
        "transition_authorization_sha256": sha256_canonical(validated_authorization),
        "transition_authorization_fingerprint": validated_authorization[
            "authorization_fingerprint"
        ],
        "from_state": BOOTSTRAP_PRODUCTION,
        "to_state": STEADY_ACTIVE,
    }
    if any(receipt.get(key) != value for key, value in expected_bindings.items()):
        raise CapacityTransitionError("rca_capacity_transition_receipt_binding_invalid")
    created = _timestamp(receipt.get("created_at"))
    issued = _timestamp(validated_authorization["issued_at"])
    expires = _timestamp(validated_authorization["expires_at"])
    if created < issued or created > expires or created > _utc(now) + MAX_CLOCK_SKEW:
        raise CapacityTransitionError("rca_capacity_transition_receipt_time_invalid")
    _validate_signed_evidence(
        receipt,
        fingerprint_field="receipt_fingerprint",
        hmac_field="receipt_hmac_sha256",
        domain=TRANSITION_RECEIPT_HMAC_DOMAIN,
        hmac_key=hmac_key,
        code="rca_capacity_transition_receipt_tampered",
    )
    return dict(receipt)


def issue_steady_commit_marker(
    *,
    ledger: CapacityLedgerSnapshot,
    authorization: Mapping[str, Any],
    receipt: Mapping[str, Any],
    marker_id: str,
    committed_at: datetime,
    hmac_key: bytes,
) -> dict[str, Any]:
    validated_receipt = validate_transition_receipt(
        receipt,
        ledger=ledger,
        authorization=authorization,
        now=committed_at,
        hmac_key=hmac_key,
    )
    validated_authorization = validate_transition_authorization(
        authorization, ledger=ledger, now=committed_at, hmac_key=hmac_key
    )
    marker: dict[str, Any] = {
        "schema_version": COMMIT_MARKER_SCHEMA_VERSION,
        "marker_id": marker_id,
        "committed_at": _format_timestamp(committed_at),
        "release_id": ledger.release_id,
        "bootstrap_epoch_id": ledger.bootstrap_epoch_id,
        "release_bom_sha256": ledger.release_bom_sha256,
        "active_release_binding_sha256": ledger.active_release_binding_sha256,
        "target_generation": validated_authorization["target_generation"],
        "sample_ledger_sha256": ledger.ledger_sha256,
        "transition_authorization_sha256": sha256_canonical(validated_authorization),
        "transition_authorization_fingerprint": validated_authorization[
            "authorization_fingerprint"
        ],
        "transition_receipt_sha256": sha256_canonical(validated_receipt),
        "transition_receipt_fingerprint": validated_receipt["receipt_fingerprint"],
        "from_state": BOOTSTRAP_PRODUCTION,
        "effective_state": STEADY_ACTIVE,
    }
    marker = _sign_evidence(
        marker,
        fingerprint_field="marker_fingerprint",
        hmac_field="marker_hmac_sha256",
        domain=COMMIT_MARKER_HMAC_DOMAIN,
        hmac_key=hmac_key,
    )
    return validate_steady_commit_marker(
        marker,
        ledger=ledger,
        authorization=validated_authorization,
        receipt=validated_receipt,
        now=committed_at,
        hmac_key=hmac_key,
    )


def validate_steady_commit_marker(
    marker: Any,
    *,
    ledger: CapacityLedgerSnapshot,
    authorization: Mapping[str, Any],
    receipt: Mapping[str, Any],
    now: datetime,
    hmac_key: bytes,
) -> dict[str, Any]:
    validated_authorization = validate_transition_authorization(
        authorization,
        ledger=ledger,
        now=now,
        hmac_key=hmac_key,
        allow_historical=True,
    )
    validated_receipt = validate_transition_receipt(
        receipt,
        ledger=ledger,
        authorization=validated_authorization,
        now=now,
        hmac_key=hmac_key,
        allow_historical=True,
    )
    if (
        not isinstance(marker, Mapping)
        or set(marker) != COMMIT_MARKER_FIELDS
        or marker.get("schema_version") != COMMIT_MARKER_SCHEMA_VERSION
    ):
        raise CapacityTransitionError("rca_capacity_commit_marker_schema_invalid")
    _identity(marker.get("marker_id"), "rca_capacity_commit_marker_id_invalid")
    expected_bindings = {
        "release_id": ledger.release_id,
        "bootstrap_epoch_id": ledger.bootstrap_epoch_id,
        "release_bom_sha256": ledger.release_bom_sha256,
        "active_release_binding_sha256": ledger.active_release_binding_sha256,
        "target_generation": validated_authorization["target_generation"],
        "sample_ledger_sha256": ledger.ledger_sha256,
        "transition_authorization_sha256": sha256_canonical(validated_authorization),
        "transition_authorization_fingerprint": validated_authorization[
            "authorization_fingerprint"
        ],
        "transition_receipt_sha256": sha256_canonical(validated_receipt),
        "transition_receipt_fingerprint": validated_receipt["receipt_fingerprint"],
        "from_state": BOOTSTRAP_PRODUCTION,
        "effective_state": STEADY_ACTIVE,
    }
    if any(marker.get(key) != value for key, value in expected_bindings.items()):
        raise CapacityTransitionError("rca_capacity_commit_marker_binding_invalid")
    committed = _timestamp(marker.get("committed_at"))
    created = _timestamp(validated_receipt["created_at"])
    expires = _timestamp(validated_authorization["expires_at"])
    if (
        committed < created
        or committed > expires
        or committed > _utc(now) + MAX_CLOCK_SKEW
    ):
        raise CapacityTransitionError("rca_capacity_commit_marker_time_invalid")
    _validate_signed_evidence(
        marker,
        fingerprint_field="marker_fingerprint",
        hmac_field="marker_hmac_sha256",
        domain=COMMIT_MARKER_HMAC_DOMAIN,
        hmac_key=hmac_key,
        code="rca_capacity_commit_marker_tampered",
    )
    return dict(marker)


def issue_steady_transition_intent(
    *,
    ledger: CapacityLedgerSnapshot,
    authorization: Mapping[str, Any],
    receipt: Mapping[str, Any],
    marker: Mapping[str, Any],
    intent_id: str,
    business_activation_epoch_id: str,
    operator: str,
    reason: str,
    created_at: datetime,
    hmac_key: bytes,
) -> dict[str, Any]:
    """Bind the complete, exact transition prefix before publishing any artifact."""

    validated_authorization = validate_transition_authorization(
        authorization,
        ledger=ledger,
        now=created_at,
        hmac_key=hmac_key,
    )
    validated_receipt = validate_transition_receipt(
        receipt,
        ledger=ledger,
        authorization=validated_authorization,
        now=created_at,
        hmac_key=hmac_key,
    )
    validated_marker = validate_steady_commit_marker(
        marker,
        ledger=ledger,
        authorization=validated_authorization,
        receipt=validated_receipt,
        now=created_at,
        hmac_key=hmac_key,
    )
    intent: dict[str, Any] = {
        "schema_version": STEADY_TRANSITION_INTENT_SCHEMA_VERSION,
        "intent_id": intent_id,
        "created_at": _format_timestamp(created_at),
        "ratchet_origin_release_id": ledger.release_id,
        "ratchet_origin_bootstrap_epoch_id": ledger.bootstrap_epoch_id,
        "expected_generation": validated_authorization["target_generation"] - 1,
        "target_generation": validated_authorization["target_generation"],
        "sample_ledger_sha256": ledger.ledger_sha256,
        "business_activation_epoch_id": business_activation_epoch_id,
        "operator": operator,
        "reason": reason,
        "transition_authorization": validated_authorization,
        "transition_receipt": validated_receipt,
        "commit_marker": validated_marker,
    }
    signed = _sign_evidence(
        intent,
        fingerprint_field="intent_fingerprint",
        hmac_field="intent_hmac_sha256",
        domain=STEADY_TRANSITION_INTENT_HMAC_DOMAIN,
        hmac_key=hmac_key,
    )
    return validate_steady_transition_intent(
        signed,
        ledger=ledger,
        now=created_at,
        hmac_key=hmac_key,
    )


def validate_steady_transition_intent(
    intent: Any,
    *,
    ledger: CapacityLedgerSnapshot,
    now: datetime,
    hmac_key: bytes,
    allow_historical: bool = False,
) -> dict[str, Any]:
    if (
        not isinstance(intent, Mapping)
        or set(intent) != STEADY_TRANSITION_INTENT_FIELDS
        or intent.get("schema_version") != STEADY_TRANSITION_INTENT_SCHEMA_VERSION
    ):
        raise CapacityTransitionError("rca_capacity_transition_intent_schema_invalid")
    _identity(intent.get("intent_id"), "rca_capacity_transition_intent_id_invalid")
    _identity(
        intent.get("business_activation_epoch_id"),
        "rca_capacity_transition_business_epoch_invalid",
    )
    _identity(intent.get("operator"), "rca_capacity_transition_operator_invalid")
    _audit_reason(intent.get("reason"))
    authorization = validate_transition_authorization(
        intent.get("transition_authorization"),
        ledger=ledger,
        now=now,
        hmac_key=hmac_key,
        allow_historical=allow_historical,
    )
    receipt = validate_transition_receipt(
        intent.get("transition_receipt"),
        ledger=ledger,
        authorization=authorization,
        now=now,
        hmac_key=hmac_key,
        allow_historical=allow_historical,
    )
    marker = validate_steady_commit_marker(
        intent.get("commit_marker"),
        ledger=ledger,
        authorization=authorization,
        receipt=receipt,
        now=now,
        hmac_key=hmac_key,
    )
    target_generation = _integer(
        intent.get("target_generation"),
        "rca_capacity_transition_intent_generation_invalid",
        minimum=2,
    )
    expected = {
        "ratchet_origin_release_id": ledger.release_id,
        "ratchet_origin_bootstrap_epoch_id": ledger.bootstrap_epoch_id,
        "expected_generation": target_generation - 1,
        "target_generation": authorization["target_generation"],
        "sample_ledger_sha256": ledger.ledger_sha256,
        "operator": authorization["approval"]["authorized_by"],
        "transition_authorization": authorization,
        "transition_receipt": receipt,
        "commit_marker": marker,
    }
    if target_generation != authorization["target_generation"] or any(
        intent.get(field) != value for field, value in expected.items()
    ):
        raise CapacityTransitionError("rca_capacity_transition_intent_binding_invalid")
    created = _timestamp(intent.get("created_at"))
    issued = _timestamp(authorization["issued_at"])
    expires = _timestamp(authorization["expires_at"])
    marker_created = _timestamp(marker["committed_at"])
    if (
        created < issued
        or created < marker_created
        or created > expires
        or created > _utc(now) + MAX_CLOCK_SKEW
    ):
        raise CapacityTransitionError("rca_capacity_transition_intent_time_invalid")
    _validate_signed_evidence(
        intent,
        fingerprint_field="intent_fingerprint",
        hmac_field="intent_hmac_sha256",
        domain=STEADY_TRANSITION_INTENT_HMAC_DOMAIN,
        hmac_key=hmac_key,
        code="rca_capacity_transition_intent_tampered",
    )
    return dict(intent)


def _coerce_ledger(
    value: CapacityLedgerSnapshot | Sequence[Any], *, hmac_key: bytes
) -> CapacityLedgerSnapshot:
    if isinstance(value, CapacityLedgerSnapshot):
        verified = validate_sample_ledger(value.samples, hmac_key=hmac_key)
        if verified != value:
            raise CapacityTransitionError("rca_capacity_ledger_snapshot_tampered")
        return verified
    return validate_sample_ledger(value, hmac_key=hmac_key)


def _validate_steady_state_bindings(
    state: Mapping[str, Any],
    *,
    ledger: CapacityLedgerSnapshot,
    authorization: Mapping[str, Any],
    receipt: Mapping[str, Any],
    marker: Mapping[str, Any],
    evidence_bundle_sha256: str,
    evidence_bundle_fingerprint: str,
) -> None:
    expected = {
        "release_id": ledger.release_id,
        "bootstrap_epoch_id": ledger.bootstrap_epoch_id,
        "generation": authorization["target_generation"],
        "final_ledger_sha256": ledger.ledger_sha256,
        "transition_authorization_sha256": sha256_canonical(authorization),
        "transition_authorization_fingerprint": authorization[
            "authorization_fingerprint"
        ],
        "transition_receipt_sha256": sha256_canonical(receipt),
        "transition_receipt_fingerprint": receipt["receipt_fingerprint"],
        "commit_marker_sha256": sha256_canonical(marker),
        "commit_marker_fingerprint": marker["marker_fingerprint"],
        "evidence_bundle_sha256": _hex(
            evidence_bundle_sha256, "rca_capacity_evidence_bundle_sha_invalid"
        ),
        "evidence_bundle_fingerprint": _hex(
            evidence_bundle_fingerprint,
            "rca_capacity_evidence_bundle_fingerprint_invalid",
        ),
    }
    if any(state.get(field) != value for field, value in expected.items()):
        raise CapacityTransitionError("rca_capacity_persisted_state_binding_invalid")
    expected_times = {
        "authorization_issued_at": authorization["issued_at"],
        "authorization_expires_at": authorization["expires_at"],
        "receipt_created_at": receipt["created_at"],
        "marker_committed_at": marker["committed_at"],
    }
    if any(
        _persisted_timestamp(state.get(field)) != _timestamp(value)
        for field, value in expected_times.items()
    ):
        raise CapacityTransitionError("rca_capacity_persisted_state_binding_invalid")


def resolve_effective_capacity(
    *,
    ledger: CapacityLedgerSnapshot | Sequence[Any],
    now: datetime,
    hmac_key: bytes,
    persisted_state: Mapping[str, Any] | None,
    transition_intent: Mapping[str, Any] | None = None,
    transition_authorization: Mapping[str, Any] | None = None,
    transition_receipt: Mapping[str, Any] | None = None,
    commit_marker: Mapping[str, Any] | None = None,
    evidence_bundle_sha256: str | None = None,
    evidence_bundle_fingerprint: str | None = None,
    bootstrap_production_authorized: bool = True,
) -> dict[str, Any]:
    """Resolve capacity from the persisted monotonic DB state and exact evidence."""

    snapshot: CapacityLedgerSnapshot | None = None
    capacity_state: dict[str, Any] | None = None
    persisted_claims_steady = bool(
        isinstance(persisted_state, Mapping)
        and persisted_state.get("state") == STEADY_ACTIVE
    )
    try:
        snapshot = _coerce_ledger(ledger, hmac_key=hmac_key)
        if persisted_state is None:
            raise CapacityTransitionError("rca_capacity_persisted_state_missing")
        capacity_state = validate_persisted_capacity_state(persisted_state)
        if snapshot.sample_count and (
            capacity_state["release_id"] != snapshot.release_id
            or capacity_state["bootstrap_epoch_id"] != snapshot.bootstrap_epoch_id
        ):
            raise CapacityTransitionError(
                "rca_capacity_persisted_state_binding_invalid"
            )
        supplied_transition_evidence = any(
            value is not None
            for value in (
                transition_intent,
                transition_authorization,
                transition_receipt,
                commit_marker,
                evidence_bundle_sha256,
                evidence_bundle_fingerprint,
            )
        )
        if capacity_state["state"] == STEADY_ACTIVE:
            if (
                transition_intent is None
                or transition_authorization is None
                or transition_receipt is None
                or commit_marker is None
                or evidence_bundle_sha256 is None
                or evidence_bundle_fingerprint is None
            ):
                raise CapacityTransitionError("rca_capacity_steady_evidence_missing")
            validated_intent = validate_steady_transition_intent(
                transition_intent,
                ledger=snapshot,
                now=now,
                hmac_key=hmac_key,
                allow_historical=True,
            )
            if (
                validated_intent["transition_authorization"] != transition_authorization
                or validated_intent["transition_receipt"] != transition_receipt
                or validated_intent["commit_marker"] != commit_marker
            ):
                raise CapacityTransitionError(
                    "rca_capacity_transition_intent_binding_invalid"
                )
            validated_marker = validate_steady_commit_marker(
                commit_marker,
                ledger=snapshot,
                authorization=transition_authorization,
                receipt=transition_receipt,
                now=now,
                hmac_key=hmac_key,
            )
            validated_authorization = validate_transition_authorization(
                transition_authorization,
                ledger=snapshot,
                now=now,
                hmac_key=hmac_key,
                allow_historical=True,
            )
            validated_receipt = validate_transition_receipt(
                transition_receipt,
                ledger=snapshot,
                authorization=validated_authorization,
                now=now,
                hmac_key=hmac_key,
                allow_historical=True,
            )
            _validate_steady_state_bindings(
                capacity_state,
                ledger=snapshot,
                authorization=validated_authorization,
                receipt=validated_receipt,
                marker=validated_marker,
                evidence_bundle_sha256=evidence_bundle_sha256,
                evidence_bundle_fingerprint=evidence_bundle_fingerprint,
            )
            return {
                "state": STEADY_ACTIVE,
                "capacity_mode": "steady",
                "reason_code": "rca_capacity_steady_commit_valid",
                "sample_count": snapshot.sample_count,
                "ledger_sha256": snapshot.ledger_sha256,
                "irreversible": True,
            }
        if supplied_transition_evidence:
            raise CapacityTransitionError(
                "rca_capacity_transition_in_progress_or_orphaned"
            )
        if snapshot.steady_qualified:
            return {
                "state": STEADY_READY,
                "capacity_mode": "bootstrap",
                "reason_code": "rca_capacity_steady_samples_qualified",
                "sample_count": snapshot.sample_count,
                "ledger_sha256": snapshot.ledger_sha256,
                "irreversible": False,
            }
        if not bootstrap_production_authorized:
            raise CapacityTransitionError("rca_capacity_bootstrap_not_authorized")
        return {
            "state": BOOTSTRAP_PRODUCTION,
            "capacity_mode": "bootstrap",
            "reason_code": "rca_capacity_steady_samples_insufficient",
            "sample_count": snapshot.sample_count,
            "ledger_sha256": snapshot.ledger_sha256,
            "irreversible": False,
        }
    except CapacityTransitionError as exc:
        return {
            "state": STEADY_BLOCKED,
            "capacity_mode": "blocked",
            "reason_code": exc.code,
            "sample_count": snapshot.sample_count if snapshot is not None else 0,
            "ledger_sha256": snapshot.ledger_sha256 if snapshot is not None else None,
            "irreversible": bool(
                persisted_claims_steady
                or (
                    capacity_state is not None
                    and capacity_state.get("state") == STEADY_ACTIVE
                )
            ),
        }
