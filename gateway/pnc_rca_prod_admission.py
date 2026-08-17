"""Host-only signed admission boundary for RCA production VM tasks."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import re
import secrets
import shlex
import subprocess
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Mapping

SCHEMA_VERSION = "hermes-rca-prod-live-admission/v1"
SNAPSHOT_SCHEMA_VERSION = "hermes-rca-prod-resource-snapshot/v1"
RESOURCE_POLICY_VERSION = "hermes-rca-prod-live-resource-policy/v1"
TRUST_SCOPE = "trusted_host_service_create_once_bridge"
HMAC_ENV = "HERMES_RCA_PROD_ADMISSION_HMAC_KEY"
MAX_TTL_SECONDS = 120
MAX_RESOURCE_OUTPUT_BYTES = 1024 * 1024
DEFAULT_RESOURCE_TIMEOUT_SECONDS = 15
DEFAULT_RESOURCE_PATH = Path.home() / ".local" / "bin" / "ssh-mini-resource"
VM_FIXED_CLI = "./api/g1q3_rca/scripts/run_rca_service_request.py"
VM_TASK_ROOT = "/home/mini/.hermes/shared-state/tasks"
MIN_ROOT_AVAILABLE_BYTES = 400 * 1024**3
MIN_DELIVERY_AVAILABLE_BYTES = 512 * 1024**3
MIN_MEMORY_AVAILABLE_BYTES = 16 * 1024**3
MIN_SWAP_FREE_RATIO = 0.05
MAX_LOAD_PER_CPU = 0.85
MAX_DNP_REAL = 4
MAX_DNP_LIKE = 12
MAX_MCAP_RSS_BYTES = 24 * 1024**3
MAX_MCAP_PROCESS_COUNT = 2
MAX_CONCURRENCY = 4
HEX64_RE = re.compile(r"^[0-9a-f]{64}$")
TASK_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")

RECEIPT_FIELDS = {
    "schema_version", "receipt_id", "issued_at", "expires_at", "decision",
    "resource_class", "capacity_mode", "trust_scope", "single_task",
    "queue_if_blocked", "bypass_requested", "bindings", "resource_policy",
    "resource_snapshot", "resource_snapshot_sha256", "receipt_fingerprint",
    "hmac_sha256",
}
BINDING_FIELDS = {
    "task_id", "attempt_id", "work_dir", "reservation_id",
    "reservation_fence", "reservation_contract_sha256", "goal_sha256",
    "command_sha256", "contract_sha256",
}
RESOURCE_POLICY_FIELDS = {
    "policy_version", "resource_check", "max_concurrency",
    "input_materialization", "root_required_available_bytes",
    "delivery_required_available_bytes",
}
SNAPSHOT_FIELDS = {
    "schema_version", "observed_at", "root_available_bytes",
    "delivery_available_bytes", "root_device", "delivery_device",
    "delivery_filesystem", "delivery_mount_rw", "delivery_writable",
    "memory_available_bytes", "swap_free_ratio", "load1", "cpu_count",
    "dnp_real", "dnp_like", "mcap_rss_bytes", "mcap_process_count",
}

RunFunc = Callable[..., subprocess.CompletedProcess[str]]


class RcaProdAdmissionError(RuntimeError):
    """Stable non-sensitive failure at the Host production admission boundary."""

    def __init__(self, code: str, *, retryable: bool = True):
        self.code = str(code or "rca_prod_admission_failed")[:120]
        self.retryable = bool(retryable)
        super().__init__(self.code)


@dataclass(frozen=True)
class RcaProdAdmission:
    receipt: dict[str, Any]
    meta: dict[str, Any]
    key_fingerprint: str


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
        raise RcaProdAdmissionError("rca_prod_contract_not_canonical", retryable=False) from exc


def sha256_value(value: Any) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def goal_sha256(goal: str) -> str:
    return hashlib.sha256(str(goal).encode("utf-8")).hexdigest()


def build_rca_prod_command_argv(task_id: str) -> list[str]:
    normalized = str(task_id or "").strip()
    if not TASK_ID_RE.fullmatch(normalized):
        raise RcaProdAdmissionError("rca_prod_task_id_invalid", retryable=False)
    goal_path = f"{VM_TASK_ROOT}/{normalized}/goal.md"
    return [
        VM_FIXED_CLI,
        "--task-id",
        normalized,
        "--goal-path",
        goal_path,
    ]


def command_sha256(command: list[str]) -> str:
    return sha256_value([str(part) for part in command])


def _load_hmac_key(raw: str | bytes | None = None) -> bytes:
    if isinstance(raw, bytes):
        if len(raw) < 32:
            raise RcaProdAdmissionError("rca_prod_hmac_key_invalid", retryable=False)
        return raw
    value = (raw if raw is not None else os.environ.get(HMAC_ENV, "")).strip()
    try:
        if value.startswith("hex:"):
            key = bytes.fromhex(value[4:])
        elif value.startswith("base64:"):
            key = base64.b64decode(value[7:], validate=True)
        else:
            raise ValueError
    except Exception as exc:
        raise RcaProdAdmissionError("rca_prod_hmac_key_invalid", retryable=False) from exc
    if len(key) < 32:
        raise RcaProdAdmissionError("rca_prod_hmac_key_invalid", retryable=False)
    return key


def hmac_key_fingerprint(raw: str | bytes | None = None) -> str:
    return hashlib.sha256(_load_hmac_key(raw)).hexdigest()


def _strict_json_loads(raw: str) -> Any:
    def object_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        value: dict[str, Any] = {}
        for key, item in pairs:
            if key in value:
                raise ValueError("duplicate key")
            value[key] = item
        return value

    return json.loads(
        raw,
        object_pairs_hook=object_pairs,
        parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)),
    )


def _timestamp(value: Any) -> datetime:
    text = str(value or "").strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("timezone required")
    return parsed.astimezone(timezone.utc)


def _now(value: datetime | None = None) -> datetime:
    current = value or datetime.now(timezone.utc)
    if current.tzinfo is None or current.utcoffset() is None:
        raise RcaProdAdmissionError("rca_prod_time_invalid", retryable=False)
    return current.astimezone(timezone.utc)


def _require_hex(value: Any, code: str) -> str:
    normalized = str(value or "").strip().lower()
    if not HEX64_RE.fullmatch(normalized):
        raise RcaProdAdmissionError(code, retryable=False)
    return normalized


def _require_int(value: Any, code: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise RcaProdAdmissionError(code, retryable=False)
    return value


def _receipt_body(receipt: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in receipt.items()
        if key not in {"receipt_fingerprint", "hmac_sha256"}
    }


def _validate_snapshot(
    snapshot: Any,
    *,
    now: datetime,
    modeled_root_bytes: int,
    modeled_delivery_bytes: int,
) -> None:
    if not isinstance(snapshot, Mapping) or set(snapshot) != SNAPSHOT_FIELDS:
        raise RcaProdAdmissionError("rca_prod_snapshot_schema_invalid")
    if snapshot.get("schema_version") != SNAPSHOT_SCHEMA_VERSION:
        raise RcaProdAdmissionError("rca_prod_snapshot_schema_invalid")
    try:
        age = (now - _timestamp(snapshot.get("observed_at"))).total_seconds()
    except (TypeError, ValueError, OverflowError) as exc:
        raise RcaProdAdmissionError("rca_prod_snapshot_time_invalid") from exc
    if age < -5 or age > MAX_TTL_SECONDS:
        raise RcaProdAdmissionError("rca_prod_snapshot_stale")
    root_available = _require_int(
        snapshot.get("root_available_bytes"), "rca_prod_snapshot_capacity_invalid"
    )
    delivery_available = _require_int(
        snapshot.get("delivery_available_bytes"), "rca_prod_snapshot_capacity_invalid"
    )
    if root_available < max(MIN_ROOT_AVAILABLE_BYTES, modeled_root_bytes):
        raise RcaProdAdmissionError("rca_prod_root_capacity_blocked")
    if delivery_available < max(MIN_DELIVERY_AVAILABLE_BYTES, modeled_delivery_bytes):
        raise RcaProdAdmissionError("rca_prod_delivery_capacity_blocked")
    if str(snapshot.get("root_device") or "") == str(snapshot.get("delivery_device") or ""):
        raise RcaProdAdmissionError("rca_prod_delivery_device_invalid")
    if str(snapshot.get("delivery_filesystem") or "").lower() not in {"cifs", "smb3"}:
        raise RcaProdAdmissionError("rca_prod_delivery_filesystem_invalid")
    if snapshot.get("delivery_mount_rw") is not True or snapshot.get("delivery_writable") is not True:
        raise RcaProdAdmissionError("rca_prod_delivery_not_writable")
    if _require_int(snapshot.get("memory_available_bytes"), "rca_prod_memory_invalid") < MIN_MEMORY_AVAILABLE_BYTES:
        raise RcaProdAdmissionError("rca_prod_memory_blocked")
    try:
        swap_ratio = float(snapshot.get("swap_free_ratio"))
        load1 = float(snapshot.get("load1"))
        cpu_count = int(snapshot.get("cpu_count"))
    except (TypeError, ValueError, OverflowError) as exc:
        raise RcaProdAdmissionError("rca_prod_pressure_invalid") from exc
    if swap_ratio < MIN_SWAP_FREE_RATIO:
        raise RcaProdAdmissionError("rca_prod_swap_blocked")
    if cpu_count < 1 or load1 < 0 or load1 > cpu_count * MAX_LOAD_PER_CPU:
        raise RcaProdAdmissionError("rca_prod_load_blocked")
    for field, limit, code in (
        ("dnp_real", MAX_DNP_REAL, "rca_prod_dnp_real_blocked"),
        ("dnp_like", MAX_DNP_LIKE, "rca_prod_dnp_like_blocked"),
        ("mcap_rss_bytes", MAX_MCAP_RSS_BYTES, "rca_prod_mcap_memory_blocked"),
        ("mcap_process_count", MAX_MCAP_PROCESS_COUNT, "rca_prod_mcap_process_blocked"),
    ):
        if _require_int(snapshot.get(field), "rca_prod_pressure_invalid") > limit:
            raise RcaProdAdmissionError(code)


def live_resource_policy() -> dict[str, Any]:
    return {
        "policy_version": RESOURCE_POLICY_VERSION,
        "resource_check": "per_task_live_snapshot",
        "max_concurrency": MAX_CONCURRENCY,
        "input_materialization": "forbidden",
        "root_required_available_bytes": MIN_ROOT_AVAILABLE_BYTES,
        "delivery_required_available_bytes": MIN_DELIVERY_AVAILABLE_BYTES,
    }


def _live_resource_policy(value: Any) -> dict[str, Any]:
    expected = live_resource_policy()
    if not isinstance(value, Mapping) or set(value) != RESOURCE_POLICY_FIELDS:
        raise RcaProdAdmissionError("rca_prod_resource_policy_invalid", retryable=False)
    if dict(value) != expected:
        raise RcaProdAdmissionError("rca_prod_resource_policy_invalid", retryable=False)
    return expected


def validate_resource_report(
    report: Any,
    *,
    now: datetime | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    current = _now(now)
    if not isinstance(report, Mapping):
        raise RcaProdAdmissionError("rca_prod_resource_report_invalid")
    if (
        report.get("resource_class") != "rca_prod"
        or report.get("ok_for_submit") is not True
        or report.get("ok_for_rca_prod_submit") is not True
        or list(report.get("reasons") or [])
        or list(report.get("rca_prod_reasons") or [])
    ):
        raise RcaProdAdmissionError("rca_prod_resource_blocked")
    snapshot = report.get("rca_prod_snapshot")
    if not isinstance(snapshot, Mapping):
        raise RcaProdAdmissionError("rca_prod_snapshot_schema_invalid")
    if sha256_value(snapshot) != str(report.get("rca_prod_snapshot_sha256") or ""):
        raise RcaProdAdmissionError("rca_prod_snapshot_hash_invalid")
    capacity = live_resource_policy()
    _validate_snapshot(
        snapshot,
        now=current,
        modeled_root_bytes=capacity["root_required_available_bytes"],
        modeled_delivery_bytes=capacity["delivery_required_available_bytes"],
    )
    return dict(snapshot), capacity


def run_resource_preflight(
    *,
    resource_path: Path = DEFAULT_RESOURCE_PATH,
    timeout_seconds: int = DEFAULT_RESOURCE_TIMEOUT_SECONDS,
    run_func: RunFunc = subprocess.run,
    now: datetime | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    env = dict(os.environ)
    env.pop(HMAC_ENV, None)
    command = [str(resource_path), "--json", "--resource-class", "rca_prod"]
    try:
        result = run_func(
            command,
            text=True,
            capture_output=True,
            timeout=timeout_seconds,
            env=env,
        )
    except subprocess.TimeoutExpired as exc:
        raise RcaProdAdmissionError("rca_prod_resource_timeout") from exc
    except Exception as exc:
        raise RcaProdAdmissionError("rca_prod_resource_unavailable") from exc
    stdout = result.stdout or ""
    if result.returncode != 0:
        raise RcaProdAdmissionError("rca_prod_resource_unavailable")
    if not stdout or len(stdout.encode("utf-8", errors="replace")) > MAX_RESOURCE_OUTPUT_BYTES:
        raise RcaProdAdmissionError("rca_prod_resource_output_invalid")
    try:
        report = _strict_json_loads(stdout)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise RcaProdAdmissionError("rca_prod_resource_output_invalid") from exc
    return validate_resource_report(report, now=now)


def _sign_receipt(receipt: dict[str, Any], key: bytes) -> dict[str, Any]:
    signed = dict(receipt)
    signed.pop("receipt_fingerprint", None)
    signed.pop("hmac_sha256", None)
    body = canonical_bytes(signed)
    signed["receipt_fingerprint"] = hashlib.sha256(body).hexdigest()
    signed["hmac_sha256"] = hmac.new(key, body, hashlib.sha256).hexdigest()
    return signed


def validate_rca_prod_receipt(
    receipt: Any,
    *,
    expected_bindings: Mapping[str, Any],
    hmac_key: str | bytes | None = None,
    now: datetime | None = None,
    allow_historical: bool = False,
) -> dict[str, Any]:
    if not isinstance(receipt, Mapping) or set(receipt) != RECEIPT_FIELDS:
        raise RcaProdAdmissionError("rca_prod_receipt_schema_invalid", retryable=False)
    bindings = receipt.get("bindings")
    capacity = receipt.get("resource_policy")
    snapshot = receipt.get("resource_snapshot")
    if not isinstance(bindings, Mapping) or set(bindings) != BINDING_FIELDS:
        raise RcaProdAdmissionError("rca_prod_receipt_schema_invalid", retryable=False)
    if not isinstance(capacity, Mapping) or set(capacity) != RESOURCE_POLICY_FIELDS:
        raise RcaProdAdmissionError("rca_prod_receipt_schema_invalid", retryable=False)
    if (
        receipt.get("schema_version") != SCHEMA_VERSION
        or receipt.get("capacity_mode") != "steady"
        or receipt.get("trust_scope") != TRUST_SCOPE
        or receipt.get("decision") != "allow"
        or receipt.get("resource_class") != "rca_prod"
        or receipt.get("single_task") is not True
        or receipt.get("queue_if_blocked") is not False
        or receipt.get("bypass_requested") is not False
    ):
        raise RcaProdAdmissionError("rca_prod_receipt_policy_invalid", retryable=False)
    if not str(receipt.get("receipt_id") or "").strip() or not str(
        bindings.get("attempt_id") or ""
    ).strip():
        raise RcaProdAdmissionError("rca_prod_receipt_identity_invalid", retryable=False)
    key = _load_hmac_key(hmac_key)
    body = canonical_bytes(_receipt_body(receipt))
    fingerprint = hashlib.sha256(body).hexdigest()
    signature = str(receipt.get("hmac_sha256") or "").lower()
    if (
        not hmac.compare_digest(str(receipt.get("receipt_fingerprint") or ""), fingerprint)
        or not HEX64_RE.fullmatch(signature)
        or not hmac.compare_digest(signature, hmac.new(key, body, hashlib.sha256).hexdigest())
    ):
        raise RcaProdAdmissionError("rca_prod_receipt_signature_invalid", retryable=False)
    current = _now(now)
    try:
        issued = _timestamp(receipt.get("issued_at"))
        expires = _timestamp(receipt.get("expires_at"))
    except (TypeError, ValueError, OverflowError) as exc:
        raise RcaProdAdmissionError("rca_prod_receipt_time_invalid", retryable=False) from exc
    ttl = (expires - issued).total_seconds()
    if ttl <= 0 or ttl > MAX_TTL_SECONDS or issued > current + timedelta(seconds=5):
        raise RcaProdAdmissionError("rca_prod_receipt_time_invalid", retryable=False)
    if not allow_historical and not (issued - timedelta(seconds=5) <= current <= expires):
        raise RcaProdAdmissionError("rca_prod_receipt_expired")
    normalized_expected = {key: str(value) for key, value in expected_bindings.items()}
    if set(normalized_expected) != BINDING_FIELDS or {
        key: str(value) for key, value in bindings.items()
    } != normalized_expected:
        raise RcaProdAdmissionError("rca_prod_receipt_binding_invalid", retryable=False)
    policy = _live_resource_policy(capacity)
    root_required = policy["root_required_available_bytes"]
    delivery_required = policy["delivery_required_available_bytes"]
    if sha256_value(snapshot) != str(receipt.get("resource_snapshot_sha256") or ""):
        raise RcaProdAdmissionError("rca_prod_snapshot_hash_invalid")
    snapshot_now = issued if allow_historical else current
    _validate_snapshot(
        snapshot,
        now=snapshot_now,
        modeled_root_bytes=root_required,
        modeled_delivery_bytes=delivery_required,
    )
    return dict(receipt)


def issue_rca_prod_admission(
    *,
    task_id: str,
    submission_key: str,
    goal: str,
    contract_sha256: str,
    reservation_id: str,
    reservation_fence: int | str,
    reservation_contract_sha256: str,
    resource_path: Path = DEFAULT_RESOURCE_PATH,
    run_func: RunFunc = subprocess.run,
    hmac_key: str | None = None,
    now: datetime | None = None,
    attempt_id: str | None = None,
    receipt_id: str | None = None,
) -> RcaProdAdmission:
    current = _now(now)
    normalized_task = str(task_id or "").strip()
    if normalized_task != str(submission_key or "").strip() or not TASK_ID_RE.fullmatch(normalized_task):
        raise RcaProdAdmissionError("rca_prod_task_identity_invalid", retryable=False)
    normalized_contract = _require_hex(contract_sha256, "rca_prod_contract_invalid")
    normalized_reservation_contract = _require_hex(
        reservation_contract_sha256, "rca_prod_reservation_invalid"
    )
    normalized_reservation = str(reservation_id or "").strip()
    normalized_fence = str(reservation_fence or "").strip()
    if not normalized_reservation or not normalized_fence:
        raise RcaProdAdmissionError("rca_prod_reservation_invalid", retryable=False)
    command = build_rca_prod_command_argv(normalized_task)
    goal_hash = goal_sha256(goal)
    command_hash = command_sha256(command)
    key = _load_hmac_key(hmac_key)
    snapshot, capacity = run_resource_preflight(
        resource_path=resource_path,
        run_func=run_func,
        now=current,
    )
    expires = current + timedelta(seconds=MAX_TTL_SECONDS)
    normalized_attempt = str(attempt_id or f"attempt-{secrets.token_hex(16)}")
    normalized_receipt = str(receipt_id or f"receipt-{secrets.token_hex(16)}")
    if (
        not normalized_attempt
        or len(normalized_attempt) > 128
        or not normalized_receipt
        or len(normalized_receipt) > 128
    ):
        raise RcaProdAdmissionError("rca_prod_receipt_identity_invalid", retryable=False)
    bindings = {
        "task_id": normalized_task,
        "attempt_id": normalized_attempt,
        "work_dir": f"/mnt/tmp/{normalized_task}",
        "reservation_id": normalized_reservation,
        "reservation_fence": normalized_fence,
        "reservation_contract_sha256": normalized_reservation_contract,
        "goal_sha256": goal_hash,
        "command_sha256": command_hash,
        "contract_sha256": normalized_contract,
    }
    receipt_body = {
        "schema_version": SCHEMA_VERSION,
        "receipt_id": normalized_receipt,
        "issued_at": current.replace(microsecond=0).isoformat(),
        "expires_at": expires.replace(microsecond=0).isoformat(),
        "decision": "allow",
        "resource_class": "rca_prod",
        "capacity_mode": "steady",
        "trust_scope": TRUST_SCOPE,
        "single_task": True,
        "queue_if_blocked": False,
        "bypass_requested": False,
        "bindings": bindings,
        "resource_policy": capacity,
        "resource_snapshot": snapshot,
        "resource_snapshot_sha256": sha256_value(snapshot),
    }
    receipt = _sign_receipt(receipt_body, key)
    validate_rca_prod_receipt(
        receipt,
        expected_bindings=bindings,
        hmac_key=key,
        now=current,
    )
    meta = {
        "resource_class": "rca_prod",
        "lane": "heavy",
        "queue_if_blocked": False,
        "resource_gate_bypass": False,
        "rca_prod_capacity_mode": "steady",
        "rca_prod_attempt_id": normalized_attempt,
        "reservation_id": normalized_reservation,
        "reservation_fence": normalized_fence,
        "reservation_contract_sha256": normalized_reservation_contract,
        "rca_prod_goal_sha256": goal_hash,
        "rca_prod_command_sha256": command_hash,
        "rca_prod_contract_sha256": normalized_contract,
        "rca_prod_admission_receipt": receipt,
        "rca_prod_admission_key_fingerprint": hashlib.sha256(key).hexdigest(),
    }
    return RcaProdAdmission(
        receipt=receipt,
        meta=meta,
        key_fingerprint=meta["rca_prod_admission_key_fingerprint"],
    )


def validate_existing_rca_prod_meta(
    meta: Any,
    *,
    task_id: str,
    goal: str,
    contract_sha256: str,
    reservation_id: str,
    reservation_fence: int | str,
    reservation_contract_sha256: str,
    hmac_key: str | None = None,
    now: datetime | None = None,
) -> None:
    if not isinstance(meta, Mapping):
        raise RcaProdAdmissionError("rca_prod_existing_identity_invalid", retryable=False)
    command_hash = command_sha256(build_rca_prod_command_argv(task_id))
    goal_hash = goal_sha256(goal)
    stable = {
        "resource_class": "rca_prod",
        "lane": "heavy",
        "queue_if_blocked": False,
        "resource_gate_bypass": False,
        "reservation_id": str(reservation_id),
        "reservation_fence": str(reservation_fence),
        "reservation_contract_sha256": str(reservation_contract_sha256),
        "rca_prod_goal_sha256": goal_hash,
        "rca_prod_command_sha256": command_hash,
        "rca_prod_contract_sha256": str(contract_sha256),
        "rca_prod_capacity_mode": "steady",
    }
    if any(meta.get(key) != value for key, value in stable.items()):
        raise RcaProdAdmissionError("rca_prod_existing_identity_invalid", retryable=False)
    attempt_id = str(meta.get("rca_prod_attempt_id") or "").strip()
    if not attempt_id:
        raise RcaProdAdmissionError("rca_prod_existing_identity_invalid", retryable=False)
    bindings = {
        "task_id": task_id,
        "attempt_id": attempt_id,
        "work_dir": f"/mnt/tmp/{task_id}",
        "reservation_id": str(reservation_id),
        "reservation_fence": str(reservation_fence),
        "reservation_contract_sha256": str(reservation_contract_sha256),
        "goal_sha256": goal_hash,
        "command_sha256": command_hash,
        "contract_sha256": str(contract_sha256),
    }
    validate_rca_prod_receipt(
        meta.get("rca_prod_admission_receipt"),
        expected_bindings=bindings,
        hmac_key=hmac_key,
        now=now,
        allow_historical=True,
    )
