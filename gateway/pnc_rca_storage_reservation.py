"""Strict host boundary for VM-backed G1Q3 RCA capacity reservations."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import hashlib
import json
import math
import os
from pathlib import Path, PurePosixPath
import posixpath
import re
import subprocess
from typing import Any, Callable, Mapping


STORAGE_RESERVATION_REQUEST_SCHEMA_VERSION = (
    "g1q3_rca_capacity_reservation_request_v1"
)
STORAGE_RESERVATION_CONTRACT_SCHEMA_VERSION = (
    "g1q3_rca_capacity_reservation_contract_v1"
)
STORAGE_RESERVATION_RECEIPT_SCHEMA_VERSION = (
    "g1q3_rca_capacity_reservation_v1"
)

DEFAULT_SSH_MINI_AGENT = str(
    Path.home() / ".local" / "bin" / "ssh-mini-agent"
)
REMOTE_VM_REPO_ROOT = "/home/mini/data3/yj-evaluation-server"
REMOTE_STORAGE_RESERVATION_MODULE = (
    f"{REMOTE_VM_REPO_ROOT}/api/g1q3_rca/storage_reservation.py"
)

REQUESTED_CASES = 1
ASSUMED_CASES_PER_DAY = 200
EXPECTED_INPUT_BYTES_PER_CASE = 8_400_000_000
TMP_PATH = "/mnt/tmp"
HFS_PATH = (
    "/mnt/minieye/pdcl/department/perception_test_team/G1Q3_RCA"
)
RESERVE_RATIO_TEXT = "0.30"
RESERVATION_TTL_SECONDS = 1800
DEFAULT_BOUNDARY_TIMEOUT_SECONDS = 120
MAX_BOUNDARY_TIMEOUT_SECONDS = 120
MAX_RESPONSE_BYTES = 2 * 1024 * 1024

TMP_BYTES_PER_CASE = EXPECTED_INPUT_BYTES_PER_CASE
HFS_BYTES_PER_CASE = 18_900_000_000
TOTAL_BYTES_PER_CASE = TMP_BYTES_PER_CASE + HFS_BYTES_PER_CASE

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SAFE_SUBMISSION_KEY_RE = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9._-]{0,191}$"
)
_RESERVATION_STATES = frozenset(
    {"reserved", "active", "waiting_capacity", "released", "expired"}
)
_ADMITTED_STATES = frozenset({"reserved", "active"})
_HEALTH_STATES = frozenset(
    {"reserved", "active", "waiting_capacity", "released", "expired"}
)

RunFunc = Callable[..., subprocess.CompletedProcess[str]]


class StorageReservationError(RuntimeError):
    """A stable, non-sensitive host reservation boundary failure."""

    def __init__(self, code: str, detail: str = ""):
        self.code = str(code or "storage_reservation_failed")[:120]
        self.detail = str(detail or self.code)[:1000]
        super().__init__(self.detail)


def _fail(code: str, detail: str) -> StorageReservationError:
    return StorageReservationError(code, detail)


def _required_text(
    value: Any,
    field_name: str,
    *,
    maximum: int = 512,
) -> str:
    text = str(value or "").strip()
    if not text:
        raise _fail(
            "storage_reservation_request_invalid",
            f"{field_name} is required",
        )
    if len(text) > maximum:
        raise _fail(
            "storage_reservation_request_invalid",
            f"{field_name} is too long",
        )
    if "\x00" in text:
        raise _fail(
            "storage_reservation_request_invalid",
            f"{field_name} contains NUL",
        )
    return text


def _canonical_json(value: Any) -> str:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise _fail(
            "storage_reservation_contract_invalid",
            "reservation contract is not canonical JSON",
        ) from exc


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _normalized_artifact_root(value: Any, submission_key: str) -> str:
    raw = _required_text(value, "artifact_root", maximum=1024)
    if not raw.startswith("/") or "\\" in raw:
        raise _fail(
            "storage_reservation_request_invalid",
            "artifact_root must be an absolute POSIX path",
        )
    if ".." in PurePosixPath(raw).parts:
        raise _fail(
            "storage_reservation_request_invalid",
            "artifact_root must not traverse parents",
        )
    normalized = posixpath.normpath(raw)
    expected = f"{TMP_PATH}/{submission_key}"
    if normalized != expected:
        raise _fail(
            "storage_reservation_request_invalid",
            f"artifact_root must be exactly {expected}/",
        )
    return expected + "/"


@dataclass(frozen=True)
class StorageReservationRequest:
    """Stable execution identity used for one production case reservation.

    ``business_key`` is the VM execution contract's issue/case identity. For
    Kafka issue intake this is the Feishu work item ID, not the hashed host
    admission business key.
    """

    submission_key: str
    task_id: str
    business_key: str
    pdcl_download_cmd: str = field(repr=False)
    artifact_root: str
    timeout_seconds: int = DEFAULT_BOUNDARY_TIMEOUT_SECONDS

    def __post_init__(self) -> None:
        submission_key = _required_text(
            self.submission_key, "submission_key", maximum=192
        )
        if not _SAFE_SUBMISSION_KEY_RE.fullmatch(submission_key):
            raise _fail(
                "storage_reservation_request_invalid",
                "submission_key is not a safe path segment",
            )
        task_id = _required_text(self.task_id, "task_id", maximum=512)
        business_key = _required_text(
            self.business_key, "business_key", maximum=512
        )
        command = _required_text(
            self.pdcl_download_cmd,
            "pdcl_download_cmd",
            maximum=4096,
        )
        if "\n" in command or "\r" in command:
            raise _fail(
                "storage_reservation_request_invalid",
                "pdcl_download_cmd must be one line",
            )
        artifact_root = _normalized_artifact_root(
            self.artifact_root, submission_key
        )
        if (
            isinstance(self.timeout_seconds, bool)
            or not isinstance(self.timeout_seconds, int)
            or not 1 <= self.timeout_seconds <= MAX_BOUNDARY_TIMEOUT_SECONDS
        ):
            raise _fail(
                "storage_reservation_request_invalid",
                "timeout_seconds must be an integer in [1, 120]",
            )
        object.__setattr__(self, "submission_key", submission_key)
        object.__setattr__(self, "task_id", task_id)
        object.__setattr__(self, "business_key", business_key)
        object.__setattr__(self, "pdcl_download_cmd", command)
        object.__setattr__(self, "artifact_root", artifact_root)

    def contract(self) -> dict[str, Any]:
        return {
            "schema_version": STORAGE_RESERVATION_CONTRACT_SCHEMA_VERSION,
            "execution_identity": {
                "submission_key": self.submission_key,
                "task_id": self.task_id,
                "business_key": self.business_key,
                "pdcl_download_cmd_sha256": hashlib.sha256(
                    self.pdcl_download_cmd.encode("utf-8")
                ).hexdigest(),
                # VM v1's execution_reservation_contract canonicalizes through
                # pathlib.Path, which removes the trailing slash.
                "artifact_root": self.artifact_root.rstrip("/"),
            },
            "capacity_policy": _contract_capacity_policy(),
        }

    def payload(self) -> dict[str, Any]:
        return {
            "schema_version": STORAGE_RESERVATION_REQUEST_SCHEMA_VERSION,
            "execution_identity": {
                "submission_key": self.submission_key,
                "task_id": self.task_id,
                "business_key": self.business_key,
                "pdcl_download_cmd": self.pdcl_download_cmd,
                "artifact_root": self.artifact_root,
            },
            "capacity_policy": _contract_capacity_policy(),
            "ttl_seconds": RESERVATION_TTL_SECONDS,
        }


@dataclass(frozen=True)
class StorageReservationDecision:
    admitted: bool
    status: str
    receipt: dict[str, Any]

    @property
    def blocked(self) -> bool:
        return self.status == "waiting_capacity"

    @property
    def reconcile_only(self) -> bool:
        return self.status == "released"


def _contract_capacity_policy() -> dict[str, Any]:
    return {
        "requested_cases": REQUESTED_CASES,
        "assumed_cases_per_day": ASSUMED_CASES_PER_DAY,
        "expected_input_bytes_per_case": EXPECTED_INPUT_BYTES_PER_CASE,
        "tmp_path": TMP_PATH,
        "hfs_path": HFS_PATH,
        "reserve_ratio": RESERVE_RATIO_TEXT,
    }


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


def _mapping(value: Any, field_name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise _fail(
            "storage_reservation_schema_invalid",
            f"{field_name} must be an object",
        )
    return value


def _exact_keys(
    value: Mapping[str, Any],
    expected: set[str],
    field_name: str,
) -> None:
    actual = set(value)
    if actual != expected:
        raise _fail(
            "storage_reservation_schema_invalid",
            f"{field_name} fields mismatch",
        )


def _exact_int(
    value: Any,
    field_name: str,
    *,
    minimum: int = 0,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise _fail(
            "storage_reservation_schema_invalid",
            f"{field_name} must be an integer >= {minimum}",
        )
    return value


def _number(value: Any, field_name: str, *, minimum: float = 0.0) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise _fail(
            "storage_reservation_schema_invalid",
            f"{field_name} must be numeric",
        )
    number = float(value)
    if not math.isfinite(number) or number < minimum:
        raise _fail(
            "storage_reservation_schema_invalid",
            f"{field_name} must be finite and >= {minimum}",
        )
    return number


def _timestamp(
    value: Any,
    field_name: str,
    *,
    optional: bool = False,
) -> str | None:
    if value is None and optional:
        return None
    text = str(value or "").strip()
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise _fail(
            "storage_reservation_schema_invalid",
            f"{field_name} is not an ISO-8601 timestamp",
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise _fail(
            "storage_reservation_schema_invalid",
            f"{field_name} must be timezone-aware",
        )
    return text


def _sha256(value: Any, field_name: str) -> str:
    text = str(value or "").strip()
    if not _SHA256_RE.fullmatch(text):
        raise _fail(
            "storage_reservation_contract_invalid",
            f"{field_name} must be lowercase SHA-256",
        )
    return text


def _validate_byte_totals(
    value: Any,
    field_name: str,
    *,
    expected_tmp: int | None = None,
    expected_hfs: int | None = None,
) -> dict[str, int]:
    mapping = _mapping(value, field_name)
    _exact_keys(mapping, {"tmp", "hfs", "total"}, field_name)
    tmp = _exact_int(mapping.get("tmp"), f"{field_name}.tmp")
    hfs = _exact_int(mapping.get("hfs"), f"{field_name}.hfs")
    total = _exact_int(mapping.get("total"), f"{field_name}.total")
    if total != tmp + hfs:
        raise _fail(
            "storage_reservation_schema_invalid",
            f"{field_name}.total mismatch",
        )
    if expected_tmp is not None and tmp != expected_tmp:
        raise _fail(
            "storage_reservation_schema_invalid",
            f"{field_name}.tmp mismatch",
        )
    if expected_hfs is not None and hfs != expected_hfs:
        raise _fail(
            "storage_reservation_schema_invalid",
            f"{field_name}.hfs mismatch",
        )
    return {"tmp": tmp, "hfs": hfs, "total": total}


def _validate_receipt_reservation(
    value: Any,
    request: StorageReservationRequest,
    *,
    status: str,
    reservation_id: str,
    contract_sha256: str,
    fence: int,
) -> dict[str, Any]:
    reservation = _mapping(value, "reservation")
    expected_keys = {
        "reservation_id",
        "submission_key",
        "contract_sha256",
        "requested_cases",
        "assumed_cases_per_day",
        "expected_input_bytes_per_case",
        "paths",
        "reserve_ratio",
        "requested_bytes",
        "held_bytes",
        "state",
        "fence",
        "run_id",
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
            "storage_reservation_identity_mismatch",
            "nested reservation identity mismatch",
        )
    if (
        reservation.get("requested_cases") != REQUESTED_CASES
        or reservation.get("assumed_cases_per_day")
        != ASSUMED_CASES_PER_DAY
        or reservation.get("expected_input_bytes_per_case")
        != EXPECTED_INPUT_BYTES_PER_CASE
    ):
        raise _fail(
            "storage_reservation_schema_invalid",
            "nested reservation production policy mismatch",
        )
    paths = _mapping(reservation.get("paths"), "reservation.paths")
    if dict(paths) != {"tmp": TMP_PATH, "hfs": HFS_PATH}:
        raise _fail(
            "storage_reservation_schema_invalid",
            "nested reservation paths mismatch",
        )
    if abs(_number(reservation.get("reserve_ratio"), "reserve_ratio") - 0.30) > 1e-12:
        raise _fail(
            "storage_reservation_schema_invalid",
            "nested reservation reserve ratio mismatch",
        )
    requested_bytes = _validate_byte_totals(
        reservation.get("requested_bytes"),
        "reservation.requested_bytes",
        expected_tmp=TMP_BYTES_PER_CASE,
        expected_hfs=HFS_BYTES_PER_CASE,
    )
    expected_hold = requested_bytes if status in _ADMITTED_STATES else {
        "tmp": 0,
        "hfs": 0,
        "total": 0,
    }
    held_bytes = _validate_byte_totals(
        reservation.get("held_bytes"),
        "reservation.held_bytes",
        expected_tmp=expected_hold["tmp"],
        expected_hfs=expected_hold["hfs"],
    )
    run_id = str(reservation.get("run_id") or "")
    if status == "active" and run_id != request.task_id:
        raise _fail(
            "storage_reservation_identity_mismatch",
            "active reservation run_id mismatch",
        )
    if status in {"reserved", "waiting_capacity"} and run_id:
        raise _fail(
            "storage_reservation_identity_mismatch",
            f"{status} reservation must not be bound to a run_id",
        )
    _timestamp(reservation.get("created_at"), "reservation.created_at")
    _timestamp(reservation.get("updated_at"), "reservation.updated_at")
    lease_expires_at = _timestamp(
        reservation.get("lease_expires_at"),
        "reservation.lease_expires_at",
        optional=status == "released",
    )
    activated_at = _timestamp(
        reservation.get("activated_at"),
        "reservation.activated_at",
        optional=True,
    )
    released_at = _timestamp(
        reservation.get("released_at"),
        "reservation.released_at",
        optional=True,
    )
    if status == "active" and activated_at is None:
        raise _fail(
            "storage_reservation_schema_invalid",
            "reservation activated_at/status mismatch",
        )
    if status in {"reserved", "waiting_capacity"} and activated_at is not None:
        raise _fail(
            "storage_reservation_schema_invalid",
            "unactivated reservation must not have activated_at",
        )
    if status == "released":
        if lease_expires_at is not None or released_at is None:
            raise _fail(
                "storage_reservation_schema_invalid",
                "released reservation lease/released_at mismatch",
            )
        if run_id not in {"", request.task_id}:
            raise _fail(
                "storage_reservation_identity_mismatch",
                "released reservation run_id mismatch",
            )
        if not run_id and activated_at is not None:
            raise _fail(
                "storage_reservation_schema_invalid",
                "released unbound reservation must not have activated_at",
            )
    elif released_at is not None:
        raise _fail(
            "storage_reservation_schema_invalid",
            "non-terminal reservation must not have released_at",
        )
    return {
        "requested_bytes": requested_bytes,
        "held_bytes": held_bytes,
    }


def _validate_capacity_policy(value: Any) -> None:
    policy = _mapping(value, "capacity.policy")
    expected_keys = {
        "requested_cases",
        "concurrency_reserve_cases",
        "requested_cases_scope",
        "assumed_cases_per_day",
        "assumed_cases_per_day_scope",
        "expected_input_bytes_per_case",
        "input_unit",
        "gb_definition_bytes",
        "reserve_ratio",
        "reserve_percent",
        "tmp_multiplier",
        "hfs_multiplier",
        "total_multiplier",
    }
    _exact_keys(policy, expected_keys, "capacity.policy")
    expected_exact = {
        "requested_cases": REQUESTED_CASES,
        "concurrency_reserve_cases": REQUESTED_CASES,
        "requested_cases_scope": "this_admission_capacity_reservation_only",
        "assumed_cases_per_day": ASSUMED_CASES_PER_DAY,
        "assumed_cases_per_day_scope": "days_horizon_calculation_only",
        "expected_input_bytes_per_case": EXPECTED_INPUT_BYTES_PER_CASE,
        "input_unit": "bytes",
        "gb_definition_bytes": 1_000_000_000,
    }
    for key, expected in expected_exact.items():
        if policy.get(key) != expected:
            raise _fail(
                "storage_reservation_schema_invalid",
                f"capacity.policy.{key} mismatch",
            )
    expected_numbers = {
        "reserve_ratio": 0.30,
        "reserve_percent": 30.0,
        "tmp_multiplier": 1.0,
        "hfs_multiplier": 2.25,
        "total_multiplier": 3.25,
    }
    for key, expected in expected_numbers.items():
        if abs(_number(policy.get(key), f"capacity.policy.{key}") - expected) > 1e-12:
            raise _fail(
                "storage_reservation_schema_invalid",
                f"capacity.policy.{key} mismatch",
            )


def _validate_capacity_target(
    value: Any,
    *,
    index: int,
    observed_at: str,
) -> dict[str, Any]:
    target = _mapping(value, f"capacity.targets[{index}]")
    name = str(target.get("name") or "")
    expected = {
        "tmp": {
            "path": TMP_PATH,
            "multiplier": 1.0,
            "bytes_per_case": TMP_BYTES_PER_CASE,
        },
        "hfs": {
            "path": HFS_PATH,
            "multiplier": 2.25,
            "bytes_per_case": HFS_BYTES_PER_CASE,
        },
    }.get(name)
    if expected is None:
        raise _fail(
            "storage_reservation_schema_invalid",
            f"capacity.targets[{index}].name is invalid",
        )
    if (
        target.get("path") != expected["path"]
        or target.get("observed_at") != observed_at
        or abs(
            _number(
                target.get("multiplier"),
                f"capacity.targets[{index}].multiplier",
            )
            - float(expected["multiplier"])
        )
        > 1e-12
        or target.get("bytes_per_case") != expected["bytes_per_case"]
        or target.get("required_bytes") != expected["bytes_per_case"]
    ):
        raise _fail(
            "storage_reservation_schema_invalid",
            f"capacity.targets[{index}] production policy mismatch",
        )
    outstanding = _exact_int(
        target.get("outstanding_held_bytes"),
        f"capacity.targets[{index}].outstanding_held_bytes",
    )
    effective = _exact_int(
        target.get("effective_admittable_bytes"),
        f"capacity.targets[{index}].effective_admittable_bytes",
    )
    max_cases = _exact_int(
        target.get("max_additional_cases_after_reservations"),
        f"capacity.targets[{index}].max_additional_cases_after_reservations",
    )
    if max_cases != effective // int(expected["bytes_per_case"]):
        raise _fail(
            "storage_reservation_schema_invalid",
            f"capacity.targets[{index}] effective max cases mismatch",
        )
    ok_after = target.get("ok_after_reservations")
    blocker = target.get("reservation_blocker")
    if not isinstance(ok_after, bool):
        raise _fail(
            "storage_reservation_schema_invalid",
            f"capacity.targets[{index}].ok_after_reservations must be boolean",
        )
    if (ok_after and blocker is not None) or (
        not ok_after and not isinstance(blocker, str)
    ):
        raise _fail(
            "storage_reservation_schema_invalid",
            f"capacity.targets[{index}] blocker/status mismatch",
        )
    return {
        "name": name,
        "outstanding_held_bytes": outstanding,
        "max_cases": max_cases,
        "horizon": _number(
            target.get("days_horizon_after_reservations"),
            f"capacity.targets[{index}].days_horizon_after_reservations",
        ),
        "ok": ok_after,
        "blocker": blocker,
    }


def _validate_capacity(
    value: Any,
    *,
    receipt_status: str,
) -> dict[str, Any]:
    capacity = _mapping(value, "capacity")
    required_keys = {
        "schema_version",
        "observed_at",
        "ok",
        "status",
        "blockers",
        "policy",
        "required_bytes_total",
        "max_additional_cases",
        "days_horizon_at_assumed_cases_per_day",
        "max_additional_cases_after_reservations",
        "days_horizon_after_reservations",
        "targets",
        "outstanding_held_bytes",
    }
    _exact_keys(capacity, required_keys, "capacity")
    if capacity.get("schema_version") != STORAGE_RESERVATION_RECEIPT_SCHEMA_VERSION:
        raise _fail(
            "storage_reservation_schema_invalid",
            "capacity schema mismatch",
        )
    observed_at = _timestamp(capacity.get("observed_at"), "capacity.observed_at")
    _validate_capacity_policy(capacity.get("policy"))
    if capacity.get("required_bytes_total") != TOTAL_BYTES_PER_CASE:
        raise _fail(
            "storage_reservation_schema_invalid",
            "capacity required_bytes_total mismatch",
        )
    _exact_int(capacity.get("max_additional_cases"), "capacity.max_additional_cases")
    _number(
        capacity.get("days_horizon_at_assumed_cases_per_day"),
        "capacity.days_horizon_at_assumed_cases_per_day",
    )
    raw_targets = capacity.get("targets")
    if not isinstance(raw_targets, list) or len(raw_targets) != 2:
        raise _fail(
            "storage_reservation_schema_invalid",
            "capacity.targets must contain tmp and hfs",
        )
    targets = [
        _validate_capacity_target(
            target,
            index=index,
            observed_at=str(observed_at),
        )
        for index, target in enumerate(raw_targets)
    ]
    if {target["name"] for target in targets} != {"tmp", "hfs"}:
        raise _fail(
            "storage_reservation_schema_invalid",
            "capacity.targets must be exactly tmp and hfs",
        )
    outstanding = _mapping(
        capacity.get("outstanding_held_bytes"),
        "capacity.outstanding_held_bytes",
    )
    _exact_keys(outstanding, {"tmp", "hfs"}, "capacity.outstanding_held_bytes")
    for target in targets:
        if outstanding.get(target["name"]) != target["outstanding_held_bytes"]:
            raise _fail(
                "storage_reservation_schema_invalid",
                "capacity outstanding holds mismatch",
            )
    blockers = capacity.get("blockers")
    if not isinstance(blockers, list) or not all(
        isinstance(item, str) and item for item in blockers
    ):
        raise _fail(
            "storage_reservation_schema_invalid",
            "capacity.blockers must be a list of codes",
        )
    target_blockers = [target["blocker"] for target in targets if target["blocker"]]
    if blockers != target_blockers:
        raise _fail(
            "storage_reservation_schema_invalid",
            "capacity blocker list mismatch",
        )
    capacity_admitted = capacity.get("ok") is True
    expected_capacity_admitted = (
        None if receipt_status == "released" else receipt_status in _ADMITTED_STATES
    )
    if (
        not isinstance(capacity.get("ok"), bool)
        or capacity.get("status")
        != ("pass" if capacity_admitted else "blocked")
        or (capacity_admitted and blockers)
        or (not capacity_admitted and not blockers)
        or (capacity_admitted and not all(target["ok"] for target in targets))
        or (not capacity_admitted and all(target["ok"] for target in targets))
        or (
            expected_capacity_admitted is not None
            and capacity_admitted is not expected_capacity_admitted
        )
    ):
        raise _fail(
            "storage_reservation_schema_invalid",
            "capacity status is inconsistent with reservation status",
        )
    effective_max = _exact_int(
        capacity.get("max_additional_cases_after_reservations"),
        "capacity.max_additional_cases_after_reservations",
    )
    if effective_max != min(int(target["max_cases"]) for target in targets):
        raise _fail(
            "storage_reservation_schema_invalid",
            "capacity effective max cases mismatch",
        )
    effective_horizon = _number(
        capacity.get("days_horizon_after_reservations"),
        "capacity.days_horizon_after_reservations",
    )
    if abs(effective_horizon - min(float(target["horizon"]) for target in targets)) > 0.0015:
        raise _fail(
            "storage_reservation_schema_invalid",
            "capacity effective horizon mismatch",
        )
    if (capacity_admitted and effective_max < 1) or (
        not capacity_admitted and effective_max >= 1
    ):
        raise _fail(
            "storage_reservation_schema_invalid",
            "capacity admission decision mismatch",
        )
    return {"blockers": list(blockers)}


def _validate_blocker(
    value: Any,
    *,
    status: str,
    capacity_blockers: list[str],
) -> None:
    if status in _ADMITTED_STATES:
        if value is not None:
            raise _fail(
                "storage_reservation_schema_invalid",
                "admitted reservation must not contain a blocker",
            )
        return
    if status == "released":
        blocker = _mapping(value, "blocker")
        expected = {
            "kind": "reservation_released_reconcile_only",
            "retryable": False,
            "reconcile_only": True,
            "create_allowed": False,
        }
        if dict(blocker) != expected:
            raise _fail(
                "storage_reservation_schema_invalid",
                "released reservation reconcile-only blocker mismatch",
            )
        return
    blocker = _mapping(value, "blocker")
    if (
        blocker.get("kind") != "storage_capacity_waiting"
        or blocker.get("retryable") is not True
        or blocker.get("capacity_blockers") != capacity_blockers
    ):
        raise _fail(
            "storage_reservation_schema_invalid",
            "waiting_capacity blocker mismatch",
        )


def _validate_health(
    value: Any,
    *,
    status: str,
    reservation_held: Mapping[str, int],
) -> None:
    health = _mapping(value, "health")
    _exact_keys(
        health,
        {"state_counts", "held_bytes", "recovered_expired_count"},
        "health",
    )
    counts = _mapping(health.get("state_counts"), "health.state_counts")
    if set(counts) != _HEALTH_STATES:
        raise _fail(
            "storage_reservation_schema_invalid",
            "health.state_counts fields mismatch",
        )
    normalized_counts = {
        name: _exact_int(value, f"health.state_counts.{name}")
        for name, value in counts.items()
    }
    if normalized_counts[status] < 1:
        raise _fail(
            "storage_reservation_schema_invalid",
            "health state count does not include this reservation",
        )
    held = _validate_byte_totals(health.get("held_bytes"), "health.held_bytes")
    if (
        held["tmp"] < reservation_held["tmp"]
        or held["hfs"] < reservation_held["hfs"]
    ):
        raise _fail(
            "storage_reservation_schema_invalid",
            "health held bytes are below this reservation",
        )
    _exact_int(
        health.get("recovered_expired_count"),
        "health.recovered_expired_count",
    )


def validate_storage_reservation_receipt(
    value: Mapping[str, Any],
    request: StorageReservationRequest,
) -> StorageReservationDecision:
    """Validate a VM v1 receipt and preserve it for the worker request."""
    receipt = _mapping(value, "receipt")
    expected_top_keys = {
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
        "health",
    }
    _exact_keys(receipt, expected_top_keys, "receipt")
    if (
        receipt.get("schema_version")
        != STORAGE_RESERVATION_RECEIPT_SCHEMA_VERSION
        or receipt.get("request_schema_version")
        != STORAGE_RESERVATION_REQUEST_SCHEMA_VERSION
    ):
        raise _fail(
            "storage_reservation_schema_invalid",
            "storage reservation schema mismatch",
        )
    status = str(receipt.get("status") or "")
    if status not in _RESERVATION_STATES:
        raise _fail(
            "storage_reservation_status_invalid",
            "unknown storage reservation status",
        )
    if status not in _ADMITTED_STATES and status not in {
        "waiting_capacity",
        "released",
    }:
        raise _fail(
            "storage_reservation_status_invalid",
            f"storage reservation is terminal: {status}",
        )
    admitted = status in _ADMITTED_STATES
    if receipt.get("ok") is not admitted:
        raise _fail(
            "storage_reservation_schema_invalid",
            "receipt ok/status mismatch",
        )
    if receipt.get("operation") != "reserve" or not isinstance(
        receipt.get("idempotent"), bool
    ):
        raise _fail(
            "storage_reservation_schema_invalid",
            "receipt operation/idempotent mismatch",
        )
    if status == "released" and receipt.get("idempotent") is not True:
        raise _fail(
            "storage_reservation_schema_invalid",
            "released reserve receipt must be idempotent",
        )
    _timestamp(receipt.get("observed_at"), "receipt.observed_at")
    reservation_id = _required_text(
        receipt.get("reservation_id"),
        "receipt.reservation_id",
        maximum=128,
    )
    if receipt.get("submission_key") != request.submission_key:
        raise _fail(
            "storage_reservation_identity_mismatch",
            "receipt submission_key mismatch",
        )
    fence = _exact_int(receipt.get("fence"), "receipt.fence", minimum=1)
    contract_sha256 = _sha256(
        receipt.get("contract_sha256"), "receipt.contract_sha256"
    )
    expected_contract = request.contract()
    if receipt.get("contract") != expected_contract:
        raise _fail(
            "storage_reservation_contract_invalid",
            "embedded reservation contract mismatch",
        )
    if contract_sha256 != _sha256_json(expected_contract):
        raise _fail(
            "storage_reservation_contract_invalid",
            "reservation contract SHA-256 mismatch",
        )
    reservation_summary = _validate_receipt_reservation(
        receipt.get("reservation"),
        request,
        status=status,
        reservation_id=reservation_id,
        contract_sha256=contract_sha256,
        fence=fence,
    )
    capacity_summary = _validate_capacity(
        receipt.get("capacity"), receipt_status=status
    )
    _validate_blocker(
        receipt.get("blocker"),
        status=status,
        capacity_blockers=capacity_summary["blockers"],
    )
    _validate_health(
        receipt.get("health"),
        status=status,
        reservation_held=reservation_summary["held_bytes"],
    )
    canonical_receipt = _strict_json_loads(_canonical_json(receipt))
    return StorageReservationDecision(
        admitted=admitted,
        status=status,
        receipt=canonical_receipt,
    )


def _remote_reservation_script(request: StorageReservationRequest) -> str:
    request_json = _canonical_json(request.payload())
    return f"""
import importlib.util
import json
from pathlib import Path
import sys

REPO_ROOT = Path({REMOTE_VM_REPO_ROOT!r})
MODULE_PATH = Path({REMOTE_STORAGE_RESERVATION_MODULE!r})
REQUEST = json.loads({request_json!r})
if not REPO_ROOT.is_absolute() or MODULE_PATH != REPO_ROOT / "api/g1q3_rca/storage_reservation.py":
    raise RuntimeError("storage_reservation_module_path_invalid")
sys.path.insert(0, str(REPO_ROOT))
spec = importlib.util.spec_from_file_location("g1q3_rca_storage_reservation", MODULE_PATH)
if spec is None or spec.loader is None:
    raise RuntimeError("storage_reservation_module_unloadable")
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)
result = module.reserve_execution_capacity(REQUEST)
print(json.dumps(result, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False))
""".strip()


def reserve_storage_capacity(
    request: StorageReservationRequest,
    *,
    run: RunFunc | None = None,
) -> StorageReservationDecision:
    """Create or recover one VM reservation through the fixed SSH boundary."""
    if not isinstance(request, StorageReservationRequest):
        raise _fail(
            "storage_reservation_request_invalid",
            "request must be StorageReservationRequest",
        )
    wrapper = Path(DEFAULT_SSH_MINI_AGENT)
    repo_root = PurePosixPath(REMOTE_VM_REPO_ROOT)
    module_path = PurePosixPath(REMOTE_STORAGE_RESERVATION_MODULE)
    if (
        not wrapper.is_absolute()
        or not repo_root.is_absolute()
        or not module_path.is_absolute()
        or module_path != repo_root / "api/g1q3_rca/storage_reservation.py"
    ):
        raise _fail(
            "storage_reservation_wrapper_invalid",
            "fixed reservation wrapper or VM module path is invalid",
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
            "storage_reservation_timeout",
            "storage reservation boundary timed out",
        ) from exc
    except OSError as exc:
        raise _fail(
            "storage_reservation_call_failed",
            f"storage reservation wrapper failed: {type(exc).__name__}",
        ) from exc
    if process.returncode != 0:
        raise _fail(
            "storage_reservation_call_failed",
            f"ssh-mini-agent returned rc={process.returncode}",
        )
    stdout = process.stdout if isinstance(process.stdout, str) else ""
    if len(stdout.encode("utf-8")) > MAX_RESPONSE_BYTES:
        raise _fail(
            "storage_reservation_response_invalid",
            "storage reservation response exceeds size limit",
        )
    try:
        payload = _strict_json_loads(stdout)
    except (json.JSONDecodeError, ValueError) as exc:
        raise _fail(
            "storage_reservation_response_invalid",
            "ssh-mini-agent returned invalid reservation JSON",
        ) from exc
    if not isinstance(payload, Mapping):
        raise _fail(
            "storage_reservation_response_invalid",
            "ssh-mini-agent reservation response is not an object",
        )
    return validate_storage_reservation_receipt(payload, request)


__all__ = [
    "ASSUMED_CASES_PER_DAY",
    "DEFAULT_BOUNDARY_TIMEOUT_SECONDS",
    "DEFAULT_SSH_MINI_AGENT",
    "EXPECTED_INPUT_BYTES_PER_CASE",
    "HFS_PATH",
    "REMOTE_STORAGE_RESERVATION_MODULE",
    "REMOTE_VM_REPO_ROOT",
    "REQUESTED_CASES",
    "RESERVATION_TTL_SECONDS",
    "RESERVE_RATIO_TEXT",
    "STORAGE_RESERVATION_CONTRACT_SCHEMA_VERSION",
    "STORAGE_RESERVATION_RECEIPT_SCHEMA_VERSION",
    "STORAGE_RESERVATION_REQUEST_SCHEMA_VERSION",
    "StorageReservationDecision",
    "StorageReservationError",
    "StorageReservationRequest",
    "TMP_PATH",
    "reserve_storage_capacity",
    "validate_storage_reservation_receipt",
]
