"""Strict host boundary for remote-read RCA derived-capacity reservations."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, ROUND_CEILING
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import posixpath
import re
import subprocess
from typing import Any, Callable, Mapping

from gateway.pnc_rca_data_access import (
    RemoteDataAccessError,
    validate_remote_data_access,
)


DERIVED_RESERVATION_REQUEST_SCHEMA_VERSION = (
    "g1q3_rca_derived_capacity_reservation_request_v1"
)
DERIVED_RESERVATION_CONTRACT_SCHEMA_VERSION = (
    "g1q3_rca_derived_capacity_reservation_contract_v1"
)
DERIVED_RESERVATION_RECEIPT_SCHEMA_VERSION = "g1q3_rca_derived_capacity_reservation_v1"
DERIVED_PRECREATE_ABORT_SCHEMA_VERSION = (
    "g1q3_rca_derived_capacity_precreate_release_v1"
)
DEFAULT_SSH_MINI_AGENT = str(Path.home() / ".local" / "bin" / "ssh-mini-agent")
REMOTE_VM_REPO_ROOT = "/home/mini/data3/yj-evaluation-server"
REMOTE_DERIVED_RESERVATION_MODULE = (
    f"{REMOTE_VM_REPO_ROOT}/api/g1q3_rca/derived_capacity_reservation.py"
)

CAPACITY_SCOPE = "derived_artifact_and_cache"
DEFAULT_EXPECTED_ARTIFACT_CACHE_BYTES = 1_000_000_000
TMP_PATH = "/mnt/tmp"
# Remote-read RCA materializes only task-owned derived data below /mnt/tmp.
# Keep both ABI budget buckets, but bind them to the mount that is actually
# written so the reservation cannot claim protection on an unrelated share.
HFS_PATH = TMP_PATH
TMP_MULTIPLIER = Decimal("1.0")
HFS_MULTIPLIER = Decimal("2.25")
RESERVE_RATIO_TEXT = "0.30"
RESERVATION_TTL_SECONDS = 1800
DEFAULT_BOUNDARY_TIMEOUT_SECONDS = 120
MAX_BOUNDARY_TIMEOUT_SECONDS = 120
MAX_RESPONSE_BYTES = 2 * 1024 * 1024
MAX_DERIVED_RESERVATION_RECEIPT_BYTES = 64 * 1024
MAX_RESERVATION_TIMESTAMP_TEXT_BYTES = 64

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SAFE_SUBMISSION_KEY_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,191}$")
_FORBIDDEN_MDI_RE = re.compile(
    r"\bmdi\s+(?:download|refresh2?|clip|event)\b", re.IGNORECASE
)
_RETURNED_STATES = frozenset({"reserved", "active", "waiting_capacity", "released"})
_ADMITTED_STATES = frozenset({"reserved", "active"})

RunFunc = Callable[..., subprocess.CompletedProcess[str]]


class DerivedCapacityReservationError(RuntimeError):
    """A stable, non-sensitive host reservation boundary failure."""

    def __init__(self, code: str, detail: str = ""):
        self.code = str(code or "derived_capacity_reservation_failed")[:120]
        self.detail = str(detail or self.code)[:1000]
        super().__init__(self.detail)


def _fail(code: str, detail: str) -> DerivedCapacityReservationError:
    return DerivedCapacityReservationError(code, detail)


def _canonical_json(value: Any) -> str:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError, OverflowError, RecursionError) as exc:
        raise _fail(
            "derived_capacity_reservation_contract_invalid",
            "reservation contract is not canonical JSON",
        ) from exc


def _strict_json_loads(value: str) -> Any:
    def object_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, item in pairs:
            if key in result:
                raise ValueError(f"duplicate key: {key}")
            result[key] = item
        return result

    def reject_constant(value: str) -> None:
        raise ValueError(f"invalid JSON constant: {value}")

    return json.loads(
        value,
        object_pairs_hook=object_pairs,
        parse_constant=reject_constant,
    )


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _contains_forbidden_mdi(value: Any) -> bool:
    stack = [value]
    seen_containers: set[int] = set()
    visited = 0
    while stack:
        current = stack.pop()
        visited += 1
        if visited > 10_000:
            return True
        if isinstance(current, str):
            if _FORBIDDEN_MDI_RE.search(current) is not None:
                return True
            continue
        if isinstance(current, Mapping):
            identity = id(current)
            if identity in seen_containers:
                continue
            seen_containers.add(identity)
            if any(str(key).lower() == "pdcl_download_cmd" for key in current):
                return True
            stack.extend(current.values())
            continue
        if isinstance(current, (list, tuple)):
            identity = id(current)
            if identity in seen_containers:
                continue
            seen_containers.add(identity)
            stack.extend(current)
    return False


def canonical_data_access_sha256(data_access: Mapping[str, Any]) -> str:
    """Hash the canonical remote-reader ABI without retaining its source value."""
    if not isinstance(data_access, Mapping) or _contains_forbidden_mdi(data_access):
        raise _fail(
            "derived_capacity_reservation_request_invalid",
            "data_access must be a non-MDI object",
        )
    try:
        validated = validate_remote_data_access(data_access)
    except RemoteDataAccessError as exc:
        raise _fail(
            "derived_capacity_reservation_request_invalid",
            "data_access is not the production remote-read contract",
        ) from exc
    return _sha256_json(validated)


def _required_text(value: Any, field_name: str, *, maximum: int = 512) -> str:
    text = str(value or "").strip()
    if not text or len(text) > maximum or "\x00" in text:
        raise _fail(
            "derived_capacity_reservation_request_invalid",
            f"{field_name} is invalid",
        )
    return text


def _sha256(value: Any, field_name: str) -> str:
    text = str(value or "").strip()
    if not _SHA256_RE.fullmatch(text):
        raise _fail(
            "derived_capacity_reservation_contract_invalid",
            f"{field_name} must be lowercase SHA-256",
        )
    return text


def _normalized_artifact_root(value: Any, submission_key: str) -> str:
    raw = _required_text(value, "artifact_root", maximum=1024)
    if not raw.startswith("/") or "\\" in raw or ".." in PurePosixPath(raw).parts:
        raise _fail(
            "derived_capacity_reservation_request_invalid",
            "artifact_root must be a safe absolute POSIX path",
        )
    normalized = posixpath.normpath(raw)
    expected = f"{TMP_PATH}/{submission_key}"
    if normalized != expected:
        raise _fail(
            "derived_capacity_reservation_request_invalid",
            f"artifact_root must be exactly {expected}/",
        )
    return expected + "/"


def _positive_int(value: Any, field_name: str, *, maximum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise _fail(
            "derived_capacity_reservation_request_invalid",
            f"{field_name} must be a positive integer",
        )
    if maximum is not None and value > maximum:
        raise _fail(
            "derived_capacity_reservation_request_invalid",
            f"{field_name} exceeds maximum",
        )
    return value


def _ceil_product(value: int, multiplier: Decimal) -> int:
    return int((Decimal(value) * multiplier).to_integral_value(rounding=ROUND_CEILING))


def _byte_totals(tmp: int, hfs: int) -> dict[str, int]:
    return {"tmp": tmp, "hfs": hfs, "total": tmp + hfs}


@dataclass(frozen=True)
class DerivedCapacityReservationRequest:
    submission_key: str
    task_id: str
    business_key: str
    data_access_sha256: str
    artifact_root: str
    expected_artifact_cache_bytes: int = DEFAULT_EXPECTED_ARTIFACT_CACHE_BYTES
    timeout_seconds: int = DEFAULT_BOUNDARY_TIMEOUT_SECONDS

    def __post_init__(self) -> None:
        submission_key = _required_text(
            self.submission_key, "submission_key", maximum=192
        )
        if not _SAFE_SUBMISSION_KEY_RE.fullmatch(submission_key):
            raise _fail(
                "derived_capacity_reservation_request_invalid",
                "submission_key is not a safe path segment",
            )
        task_id = _required_text(self.task_id, "task_id", maximum=512)
        if task_id != submission_key:
            raise _fail(
                "derived_capacity_reservation_request_invalid",
                "task_id must equal submission_key",
            )
        business_key = _required_text(self.business_key, "business_key", maximum=512)
        data_access_sha256 = _sha256(self.data_access_sha256, "data_access_sha256")
        artifact_root = _normalized_artifact_root(self.artifact_root, submission_key)
        expected_bytes = _positive_int(
            self.expected_artifact_cache_bytes,
            "expected_artifact_cache_bytes",
            maximum=1_000_000_000_000,
        )
        timeout = _positive_int(
            self.timeout_seconds,
            "timeout_seconds",
            maximum=MAX_BOUNDARY_TIMEOUT_SECONDS,
        )
        object.__setattr__(self, "submission_key", submission_key)
        object.__setattr__(self, "task_id", task_id)
        object.__setattr__(self, "business_key", business_key)
        object.__setattr__(self, "data_access_sha256", data_access_sha256)
        object.__setattr__(self, "artifact_root", artifact_root)
        object.__setattr__(self, "expected_artifact_cache_bytes", expected_bytes)
        object.__setattr__(self, "timeout_seconds", timeout)

    @property
    def requested_bytes(self) -> dict[str, int]:
        return _byte_totals(
            _ceil_product(self.expected_artifact_cache_bytes, TMP_MULTIPLIER),
            _ceil_product(self.expected_artifact_cache_bytes, HFS_MULTIPLIER),
        )

    def contract(self) -> dict[str, Any]:
        return {
            "schema_version": DERIVED_RESERVATION_CONTRACT_SCHEMA_VERSION,
            "execution_identity": {
                "submission_key": self.submission_key,
                "task_id": self.task_id,
                "business_key": self.business_key,
                "data_access_sha256": self.data_access_sha256,
                "artifact_root": self.artifact_root.rstrip("/"),
            },
            "capacity_policy": {
                "scope": CAPACITY_SCOPE,
                "atomic_reservation": True,
                "expected_artifact_cache_bytes_per_case": (
                    self.expected_artifact_cache_bytes
                ),
                "tmp_path": TMP_PATH,
                "hfs_path": HFS_PATH,
                "tmp_multiplier": float(TMP_MULTIPLIER),
                "hfs_multiplier": float(HFS_MULTIPLIER),
                "reserve_ratio": RESERVE_RATIO_TEXT,
            },
        }

    def payload(self) -> dict[str, Any]:
        return {
            "schema_version": DERIVED_RESERVATION_REQUEST_SCHEMA_VERSION,
            "execution_identity": dict(self.contract()["execution_identity"]),
            "capacity_policy": dict(self.contract()["capacity_policy"]),
            "ttl_seconds": RESERVATION_TTL_SECONDS,
        }


@dataclass(frozen=True)
class DerivedCapacityReservationDecision:
    admitted: bool
    status: str
    receipt: dict[str, Any]

    @property
    def blocked(self) -> bool:
        return self.status == "waiting_capacity"

    @property
    def reconcile_only(self) -> bool:
        return self.status == "released"


def _mapping(value: Any, field_name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise _fail(
            "derived_capacity_reservation_schema_invalid",
            f"{field_name} must be an object",
        )
    return value


def _exact_keys(value: Mapping[str, Any], expected: set[str], field_name: str) -> None:
    if set(value) != expected:
        raise _fail(
            "derived_capacity_reservation_schema_invalid",
            f"{field_name} fields mismatch",
        )


def _exact_int(value: Any, field_name: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise _fail(
            "derived_capacity_reservation_schema_invalid",
            f"{field_name} must be an integer >= {minimum}",
        )
    return value


def _timestamp(value: Any, field_name: str, *, optional: bool = False) -> str | None:
    if value is None and optional:
        return None
    text = str(value or "").strip()
    if len(text.encode("utf-8")) > MAX_RESERVATION_TIMESTAMP_TEXT_BYTES:
        raise _fail(
            "derived_capacity_reservation_schema_invalid",
            f"{field_name} is too long",
        )
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise _fail(
            "derived_capacity_reservation_schema_invalid",
            f"{field_name} is not an ISO-8601 timestamp",
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise _fail(
            "derived_capacity_reservation_schema_invalid",
            f"{field_name} must be timezone-aware",
        )
    return text


def _validate_byte_totals(
    value: Any,
    field_name: str,
    *,
    expected: Mapping[str, int] | None = None,
) -> dict[str, int]:
    body = _mapping(value, field_name)
    _exact_keys(body, {"tmp", "hfs", "total"}, field_name)
    result = {
        "tmp": _exact_int(body.get("tmp"), f"{field_name}.tmp"),
        "hfs": _exact_int(body.get("hfs"), f"{field_name}.hfs"),
        "total": _exact_int(body.get("total"), f"{field_name}.total"),
    }
    if result["total"] != result["tmp"] + result["hfs"]:
        raise _fail(
            "derived_capacity_reservation_schema_invalid",
            f"{field_name}.total mismatch",
        )
    if expected is not None and result != dict(expected):
        raise _fail(
            "derived_capacity_reservation_schema_invalid",
            f"{field_name} does not match the request",
        )
    return result


def _validate_capacity(
    value: Any,
    request: DerivedCapacityReservationRequest,
    *,
    status: str,
) -> list[str]:
    capacity = _mapping(value, "capacity")
    expected_keys = {
        "scope",
        "atomic_reservation",
        "observed_at",
        "paths",
        "reserve_ratio",
        "required_bytes",
        "total_bytes",
        "available_bytes",
        "reserve_bytes",
        "outstanding_held_bytes",
        "effective_admittable_bytes",
        "admitted",
        "blockers",
    }
    _exact_keys(capacity, expected_keys, "capacity")
    if (
        capacity.get("scope") != CAPACITY_SCOPE
        or capacity.get("atomic_reservation") is not True
        or capacity.get("paths") != {"tmp": TMP_PATH, "hfs": HFS_PATH}
        or capacity.get("reserve_ratio") != RESERVE_RATIO_TEXT
    ):
        raise _fail(
            "derived_capacity_reservation_schema_invalid",
            "capacity policy identity mismatch",
        )
    _timestamp(capacity.get("observed_at"), "capacity.observed_at")
    required = _validate_byte_totals(
        capacity.get("required_bytes"),
        "capacity.required_bytes",
        expected=request.requested_bytes,
    )
    total = _validate_byte_totals(capacity.get("total_bytes"), "capacity.total_bytes")
    available = _validate_byte_totals(
        capacity.get("available_bytes"), "capacity.available_bytes"
    )
    reserve = _validate_byte_totals(
        capacity.get("reserve_bytes"), "capacity.reserve_bytes"
    )
    outstanding = _validate_byte_totals(
        capacity.get("outstanding_held_bytes"),
        "capacity.outstanding_held_bytes",
    )
    effective = _validate_byte_totals(
        capacity.get("effective_admittable_bytes"),
        "capacity.effective_admittable_bytes",
    )
    # tmp/hfs are logical budget buckets over one physical task-output mount.
    # Physical capacity is represented once in tmp; hfs must stay zero so a
    # single /mnt/tmp pool cannot be counted twice.
    for name, evidence in (
        ("total", total),
        ("available", available),
        ("reserve", reserve),
        ("outstanding", outstanding),
        ("effective", effective),
    ):
        if evidence["hfs"] != 0 or evidence["total"] != evidence["tmp"]:
            raise _fail(
                "derived_capacity_reservation_schema_invalid",
                f"capacity {name} bytes must describe one shared physical pool",
            )
    if available["tmp"] > total["tmp"]:
        raise _fail(
            "derived_capacity_reservation_schema_invalid",
            "capacity available bytes exceed total bytes",
        )
    expected_reserve = _ceil_product(
        total["tmp"], Decimal(RESERVE_RATIO_TEXT)
    )
    if reserve["tmp"] != expected_reserve:
        raise _fail(
            "derived_capacity_reservation_schema_invalid",
            "capacity shared-pool reserve bytes mismatch",
        )
    expected_effective = max(
        0, available["tmp"] - reserve["tmp"] - outstanding["tmp"]
    )
    if effective["tmp"] != expected_effective:
        raise _fail(
            "derived_capacity_reservation_schema_invalid",
            "capacity shared-pool effective bytes mismatch",
        )
    blockers = capacity.get("blockers")
    if not isinstance(blockers, list) or not all(
        isinstance(item, str) and item for item in blockers
    ):
        raise _fail(
            "derived_capacity_reservation_schema_invalid",
            "capacity blockers must be a list of codes",
        )
    expected_blockers = (
        ["task_output_publisher_insufficient_derived_capacity"]
        if effective["tmp"] < required["total"]
        else []
    )
    if list(blockers) != expected_blockers:
        raise _fail(
            "derived_capacity_reservation_schema_invalid",
            "capacity blockers do not match byte evidence",
        )
    admitted = capacity.get("admitted")
    if not isinstance(admitted, bool) or admitted is not (not blockers):
        raise _fail(
            "derived_capacity_reservation_schema_invalid",
            "capacity admission flag mismatch",
        )
    if status in _ADMITTED_STATES and not admitted:
        raise _fail(
            "derived_capacity_reservation_schema_invalid",
            "admitted reservation has blocked capacity evidence",
        )
    if status == "waiting_capacity" and admitted:
        raise _fail(
            "derived_capacity_reservation_schema_invalid",
            "waiting reservation has admitted capacity evidence",
        )
    return list(blockers)


def _validate_reservation(
    value: Any,
    request: DerivedCapacityReservationRequest,
    *,
    status: str,
    reservation_id: str,
    contract_sha256: str,
    fence: int,
) -> dict[str, int]:
    reservation = _mapping(value, "reservation")
    expected_keys = {
        "reservation_id",
        "submission_key",
        "contract_sha256",
        "state",
        "fence",
        "run_id",
        "requested_bytes",
        "held_bytes",
        "created_at",
        "updated_at",
        "lease_expires_at",
        "activated_at",
        "released_at",
    }
    _exact_keys(reservation, expected_keys, "reservation")
    if (
        reservation.get("reservation_id") != reservation_id
        or reservation.get("submission_key") != request.submission_key
        or reservation.get("contract_sha256") != contract_sha256
        or reservation.get("state") != status
        or reservation.get("fence") != fence
    ):
        raise _fail(
            "derived_capacity_reservation_identity_mismatch",
            "nested reservation identity mismatch",
        )
    requested = _validate_byte_totals(
        reservation.get("requested_bytes"),
        "reservation.requested_bytes",
        expected=request.requested_bytes,
    )
    expected_held = requested if status in _ADMITTED_STATES else _byte_totals(0, 0)
    held = _validate_byte_totals(
        reservation.get("held_bytes"),
        "reservation.held_bytes",
        expected=expected_held,
    )
    run_id = str(reservation.get("run_id") or "")
    if status == "active" and run_id != request.task_id:
        raise _fail(
            "derived_capacity_reservation_identity_mismatch",
            "active reservation run_id mismatch",
        )
    if status in {"reserved", "waiting_capacity"} and run_id:
        raise _fail(
            "derived_capacity_reservation_identity_mismatch",
            "unactivated reservation must not carry run_id",
        )
    _timestamp(reservation.get("created_at"), "reservation.created_at")
    _timestamp(reservation.get("updated_at"), "reservation.updated_at")
    lease = _timestamp(
        reservation.get("lease_expires_at"),
        "reservation.lease_expires_at",
        optional=status == "released",
    )
    activated = _timestamp(
        reservation.get("activated_at"), "reservation.activated_at", optional=True
    )
    released = _timestamp(
        reservation.get("released_at"), "reservation.released_at", optional=True
    )
    if status == "active" and activated is None:
        raise _fail(
            "derived_capacity_reservation_schema_invalid",
            "active reservation is missing activation time",
        )
    if status in {"reserved", "waiting_capacity"} and activated is not None:
        raise _fail(
            "derived_capacity_reservation_schema_invalid",
            "unactivated reservation has activation time",
        )
    if status == "released":
        if lease is not None or released is None or run_id not in {"", request.task_id}:
            raise _fail(
                "derived_capacity_reservation_schema_invalid",
                "released reservation lifecycle mismatch",
            )
    elif released is not None:
        raise _fail(
            "derived_capacity_reservation_schema_invalid",
            "non-terminal reservation has release time",
        )
    return held


def validate_derived_capacity_reservation_receipt(
    value: Mapping[str, Any],
    request: DerivedCapacityReservationRequest,
) -> DerivedCapacityReservationDecision:
    """Validate and detach a complete VM reservation receipt."""
    if not isinstance(request, DerivedCapacityReservationRequest):
        raise _fail(
            "derived_capacity_reservation_request_invalid",
            "request must be DerivedCapacityReservationRequest",
        )
    receipt = _mapping(value, "receipt")
    expected_keys = {
        "schema_version",
        "request_schema_version",
        "ok",
        "status",
        "reservation_id",
        "submission_key",
        "contract_sha256",
        "fence",
        "operation",
        "idempotent",
        "observed_at",
        "contract",
        "reservation",
        "capacity",
        "blocker",
    }
    _exact_keys(receipt, expected_keys, "receipt")
    if _contains_forbidden_mdi(receipt):
        raise _fail(
            "derived_capacity_reservation_schema_invalid",
            "reservation receipt contains a forbidden MDI field or command",
        )
    if (
        receipt.get("schema_version") != DERIVED_RESERVATION_RECEIPT_SCHEMA_VERSION
        or receipt.get("request_schema_version")
        != DERIVED_RESERVATION_REQUEST_SCHEMA_VERSION
    ):
        raise _fail(
            "derived_capacity_reservation_schema_invalid",
            "reservation schema mismatch",
        )
    status = str(receipt.get("status") or "")
    if status not in _RETURNED_STATES:
        raise _fail(
            "derived_capacity_reservation_status_invalid",
            "reservation returned an invalid state",
        )
    admitted = status in _ADMITTED_STATES
    if receipt.get("ok") is not admitted:
        raise _fail(
            "derived_capacity_reservation_schema_invalid",
            "receipt ok/status mismatch",
        )
    if receipt.get("operation") != "reserve" or not isinstance(
        receipt.get("idempotent"), bool
    ):
        raise _fail(
            "derived_capacity_reservation_schema_invalid",
            "receipt operation/idempotence mismatch",
        )
    if status == "released" and receipt.get("idempotent") is not True:
        raise _fail(
            "derived_capacity_reservation_schema_invalid",
            "released reservation must be an idempotent observation",
        )
    _timestamp(receipt.get("observed_at"), "receipt.observed_at")
    reservation_id = _required_text(
        receipt.get("reservation_id"), "receipt.reservation_id", maximum=128
    )
    if receipt.get("submission_key") != request.submission_key:
        raise _fail(
            "derived_capacity_reservation_identity_mismatch",
            "receipt submission_key mismatch",
        )
    fence = _exact_int(receipt.get("fence"), "receipt.fence", minimum=1)
    contract_sha256 = _sha256(receipt.get("contract_sha256"), "receipt.contract_sha256")
    expected_contract = request.contract()
    if receipt.get("contract") != expected_contract:
        raise _fail(
            "derived_capacity_reservation_contract_invalid",
            "embedded contract mismatch",
        )
    if contract_sha256 != _sha256_json(expected_contract):
        raise _fail(
            "derived_capacity_reservation_contract_invalid",
            "contract SHA-256 mismatch",
        )
    _validate_reservation(
        receipt.get("reservation"),
        request,
        status=status,
        reservation_id=reservation_id,
        contract_sha256=contract_sha256,
        fence=fence,
    )
    capacity_blockers = _validate_capacity(
        receipt.get("capacity"), request, status=status
    )
    blocker = receipt.get("blocker")
    if status in _ADMITTED_STATES:
        if blocker is not None:
            raise _fail(
                "derived_capacity_reservation_schema_invalid",
                "admitted reservation must not contain a blocker",
            )
    elif status == "waiting_capacity":
        if blocker != {
            "kind": "derived_capacity_waiting",
            "retryable": True,
            "capacity_blockers": capacity_blockers,
        }:
            raise _fail(
                "derived_capacity_reservation_schema_invalid",
                "waiting reservation blocker mismatch",
            )
    elif blocker != {
        "kind": "derived_capacity_reservation_released_reconcile_only",
        "retryable": False,
        "reconcile_only": True,
        "create_allowed": False,
    }:
        raise _fail(
            "derived_capacity_reservation_schema_invalid",
            "released reservation blocker mismatch",
        )
    canonical_text = _canonical_json(receipt)
    if len(canonical_text.encode("utf-8")) > MAX_DERIVED_RESERVATION_RECEIPT_BYTES:
        raise _fail(
            "derived_capacity_reservation_schema_invalid",
            "reservation receipt exceeds the fixed VM envelope",
        )
    canonical_receipt = _strict_json_loads(canonical_text)
    return DerivedCapacityReservationDecision(
        admitted=admitted,
        status=status,
        receipt=canonical_receipt,
    )


def validate_derived_capacity_precreate_abort_receipt(
    value: Mapping[str, Any],
    request: DerivedCapacityReservationRequest,
    reservation_receipt: Mapping[str, Any],
) -> dict[str, Any]:
    """Validate a redrivable VM pre-create abort against a reserved hold."""
    reservation = validate_derived_capacity_reservation_receipt(
        reservation_receipt, request
    )
    if reservation.status != "reserved":
        raise _fail(
            "derived_capacity_reservation_abort_precreate_invalid",
            "only an unactivated reserved hold may use the abort boundary",
        )
    if not isinstance(value, Mapping):
        raise _fail(
            "derived_capacity_reservation_abort_precreate_response_invalid",
            "pre-create abort receipt is not an object",
        )
    receipt = value
    expected_keys = {
        "schema_version",
        "operation",
        "released",
        "idempotent",
        "observed_at",
        "reservation_id",
        "submission_key",
        "task_id",
        "contract_sha256",
        "fence",
        "prior_state",
        "state",
        "held_bytes",
    }
    if set(receipt) != expected_keys or _contains_forbidden_mdi(receipt):
        raise _fail(
            "derived_capacity_reservation_abort_precreate_response_invalid",
            "pre-create abort receipt fields mismatch",
        )
    idempotent = receipt.get("idempotent")
    prior_state = receipt.get("prior_state")
    if (
        receipt.get("schema_version") != DERIVED_PRECREATE_ABORT_SCHEMA_VERSION
        or receipt.get("operation") != "abort_precreate"
        or receipt.get("released") is not True
        or not isinstance(idempotent, bool)
        or receipt.get("state") != "expired"
        or prior_state not in {"reserved", "expired"}
        or (idempotent is True) != (prior_state == "expired")
    ):
        raise _fail(
            "derived_capacity_reservation_abort_precreate_response_invalid",
            "pre-create abort lifecycle mismatch",
        )
    try:
        _timestamp(receipt.get("observed_at"), "abort receipt observed_at")
        abort_fence = _exact_int(
            receipt.get("fence"), "abort receipt fence", minimum=1
        )
        held = _validate_byte_totals(
            receipt.get("held_bytes"), "abort receipt held_bytes"
        )
    except DerivedCapacityReservationError as exc:
        raise _fail(
            "derived_capacity_reservation_abort_precreate_response_invalid",
            "pre-create abort receipt payload is invalid",
        ) from exc
    if held != _byte_totals(0, 0):
        raise _fail(
            "derived_capacity_reservation_abort_precreate_response_invalid",
            "pre-create abort did not release all held bytes",
        )
    if (
        receipt.get("reservation_id") != reservation.receipt["reservation_id"]
        or receipt.get("submission_key") != request.submission_key
        or receipt.get("task_id") != request.task_id
        or receipt.get("contract_sha256")
        != reservation.receipt["contract_sha256"]
        or abort_fence != reservation.receipt["fence"]
    ):
        raise _fail(
            "derived_capacity_reservation_abort_precreate_identity_mismatch",
            "pre-create abort receipt does not match the reserved hold",
        )
    return _strict_json_loads(_canonical_json(receipt))


def _remote_reservation_script(
    request: DerivedCapacityReservationRequest,
) -> str:
    request_json = _canonical_json(request.payload())
    return f"""
import importlib.util
import json
from pathlib import Path
import sys

REPO_ROOT = Path({REMOTE_VM_REPO_ROOT!r})
MODULE_PATH = Path({REMOTE_DERIVED_RESERVATION_MODULE!r})
REQUEST = json.loads({request_json!r})
if not REPO_ROOT.is_absolute() or MODULE_PATH != REPO_ROOT / "api/g1q3_rca/derived_capacity_reservation.py":
    raise RuntimeError("derived_capacity_reservation_module_path_invalid")
sys.path.insert(0, str(REPO_ROOT))
spec = importlib.util.spec_from_file_location("g1q3_rca_derived_capacity_reservation", MODULE_PATH)
if spec is None or spec.loader is None:
    raise RuntimeError("derived_capacity_reservation_module_unloadable")
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)
result = module.reserve_derived_capacity(REQUEST)
print(json.dumps(result, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False))
""".strip()


def _remote_precreate_abort_script(
    request: DerivedCapacityReservationRequest,
    reservation_receipt: Mapping[str, Any],
) -> str:
    payload = {
        "reservation_id": reservation_receipt.get("reservation_id"),
        "submission_key": request.submission_key,
        "task_id": request.task_id,
        "contract_sha256": reservation_receipt.get("contract_sha256"),
        "fence": reservation_receipt.get("fence"),
    }
    payload_json = _canonical_json(payload)
    return f"""
import importlib.util
import json
from pathlib import Path
import sys

REPO_ROOT = Path({REMOTE_VM_REPO_ROOT!r})
MODULE_PATH = Path({REMOTE_DERIVED_RESERVATION_MODULE!r})
REQUEST = json.loads({payload_json!r})
if not REPO_ROOT.is_absolute() or MODULE_PATH != REPO_ROOT / "api/g1q3_rca/derived_capacity_reservation.py":
    raise RuntimeError("derived_capacity_reservation_module_path_invalid")
sys.path.insert(0, str(REPO_ROOT))
spec = importlib.util.spec_from_file_location("g1q3_rca_derived_capacity_reservation", MODULE_PATH)
if spec is None or spec.loader is None:
    raise RuntimeError("derived_capacity_reservation_module_unloadable")
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)
result = module.abort_precreate_derived_capacity(
    reservation_id=REQUEST["reservation_id"],
    submission_key=REQUEST["submission_key"],
    task_id=REQUEST["task_id"],
    contract_sha256=REQUEST["contract_sha256"],
    fence=REQUEST["fence"],
)
print(json.dumps(result, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False))
""".strip()


def reserve_derived_capacity(
    request: DerivedCapacityReservationRequest,
    *,
    run: RunFunc | None = None,
) -> DerivedCapacityReservationDecision:
    """Create or reconcile one atomic reservation through the fixed VM boundary."""
    if not isinstance(request, DerivedCapacityReservationRequest):
        raise _fail(
            "derived_capacity_reservation_request_invalid",
            "request must be DerivedCapacityReservationRequest",
        )
    wrapper = Path(DEFAULT_SSH_MINI_AGENT)
    repo_root = PurePosixPath(REMOTE_VM_REPO_ROOT)
    module_path = PurePosixPath(REMOTE_DERIVED_RESERVATION_MODULE)
    if (
        not wrapper.is_absolute()
        or not repo_root.is_absolute()
        or not module_path.is_absolute()
        or module_path != repo_root / "api/g1q3_rca/derived_capacity_reservation.py"
    ):
        raise _fail(
            "derived_capacity_reservation_wrapper_invalid",
            "fixed wrapper or VM module path is invalid",
        )
    environment = os.environ.copy()
    environment["SSH_MINI_AGENT_TIMEOUT"] = str(request.timeout_seconds)
    runner = run or subprocess.run
    try:
        process = runner(
            [str(wrapper), "run_py_json"],
            input=_remote_reservation_script(request),
            text=True,
            capture_output=True,
            timeout=request.timeout_seconds,
            check=False,
            env=environment,
        )
    except subprocess.TimeoutExpired as exc:
        raise _fail(
            "derived_capacity_reservation_timeout",
            "derived-capacity reservation boundary timed out",
        ) from exc
    except OSError as exc:
        raise _fail(
            "derived_capacity_reservation_call_failed",
            f"reservation wrapper failed: {type(exc).__name__}",
        ) from exc
    if process.returncode != 0:
        raise _fail(
            "derived_capacity_reservation_call_failed",
            f"ssh-mini-agent returned rc={process.returncode}",
        )
    stdout = process.stdout if isinstance(process.stdout, str) else ""
    if len(stdout.encode("utf-8")) > MAX_RESPONSE_BYTES:
        raise _fail(
            "derived_capacity_reservation_response_invalid",
            "reservation response exceeds size limit",
        )
    try:
        payload = _strict_json_loads(stdout)
    except (json.JSONDecodeError, ValueError, RecursionError) as exc:
        raise _fail(
            "derived_capacity_reservation_response_invalid",
            "ssh-mini-agent returned invalid reservation JSON",
        ) from exc
    if not isinstance(payload, Mapping):
        raise _fail(
            "derived_capacity_reservation_response_invalid",
            "reservation response is not an object",
        )
    return validate_derived_capacity_reservation_receipt(payload, request)


def abort_precreate_derived_capacity(
    request: DerivedCapacityReservationRequest,
    reservation_receipt: Mapping[str, Any],
    *,
    run: RunFunc | None = None,
) -> dict[str, Any]:
    """Abort a reserved hold after definitive proof no task was created."""
    reservation = validate_derived_capacity_reservation_receipt(
        reservation_receipt, request
    )
    if reservation.status != "reserved":
        raise _fail(
            "derived_capacity_reservation_abort_precreate_invalid",
            "only a reserved pre-create hold may be aborted",
        )
    wrapper = Path(DEFAULT_SSH_MINI_AGENT)
    repo_root = PurePosixPath(REMOTE_VM_REPO_ROOT)
    module_path = PurePosixPath(REMOTE_DERIVED_RESERVATION_MODULE)
    if (
        not wrapper.is_absolute()
        or not repo_root.is_absolute()
        or not module_path.is_absolute()
        or module_path != repo_root / "api/g1q3_rca/derived_capacity_reservation.py"
    ):
        raise _fail(
            "derived_capacity_reservation_abort_precreate_wrapper_invalid",
            "fixed pre-create abort wrapper or VM module path is invalid",
        )
    environment = os.environ.copy()
    environment["SSH_MINI_AGENT_TIMEOUT"] = str(request.timeout_seconds)
    runner = run or subprocess.run
    try:
        process = runner(
            [str(wrapper), "run_py_json"],
            input=_remote_precreate_abort_script(request, reservation.receipt),
            text=True,
            capture_output=True,
            timeout=request.timeout_seconds,
            check=False,
            env=environment,
        )
    except subprocess.TimeoutExpired as exc:
        raise _fail(
            "derived_capacity_reservation_abort_precreate_timeout",
            "pre-create abort boundary timed out",
        ) from exc
    except OSError as exc:
        raise _fail(
            "derived_capacity_reservation_abort_precreate_call_failed",
            f"pre-create abort wrapper failed: {type(exc).__name__}",
        ) from exc
    if process.returncode != 0:
        raise _fail(
            "derived_capacity_reservation_abort_precreate_call_failed",
            f"ssh-mini-agent returned rc={process.returncode}",
        )
    stdout = process.stdout if isinstance(process.stdout, str) else ""
    if len(stdout.encode("utf-8")) > MAX_RESPONSE_BYTES:
        raise _fail(
            "derived_capacity_reservation_abort_precreate_response_invalid",
            "pre-create abort response exceeds size limit",
        )
    try:
        payload = _strict_json_loads(stdout)
    except (json.JSONDecodeError, ValueError, RecursionError) as exc:
        raise _fail(
            "derived_capacity_reservation_abort_precreate_response_invalid",
            "ssh-mini-agent returned invalid pre-create abort JSON",
        ) from exc
    if not isinstance(payload, Mapping):
        raise _fail(
            "derived_capacity_reservation_abort_precreate_response_invalid",
            "pre-create abort response is not an object",
        )
    return validate_derived_capacity_precreate_abort_receipt(
        payload, request, reservation.receipt
    )


__all__ = [
    "CAPACITY_SCOPE",
    "DEFAULT_BOUNDARY_TIMEOUT_SECONDS",
    "DEFAULT_EXPECTED_ARTIFACT_CACHE_BYTES",
    "DERIVED_RESERVATION_CONTRACT_SCHEMA_VERSION",
    "DERIVED_PRECREATE_ABORT_SCHEMA_VERSION",
    "DERIVED_RESERVATION_RECEIPT_SCHEMA_VERSION",
    "DERIVED_RESERVATION_REQUEST_SCHEMA_VERSION",
    "DerivedCapacityReservationDecision",
    "DerivedCapacityReservationError",
    "DerivedCapacityReservationRequest",
    "HFS_PATH",
    "REMOTE_DERIVED_RESERVATION_MODULE",
    "RESERVATION_TTL_SECONDS",
    "TMP_PATH",
    "canonical_data_access_sha256",
    "abort_precreate_derived_capacity",
    "reserve_derived_capacity",
    "validate_derived_capacity_precreate_abort_receipt",
    "validate_derived_capacity_reservation_receipt",
]
