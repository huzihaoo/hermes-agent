"""Fail-closed evidence chain for RCA production capacity samples."""

from __future__ import annotations

import base64
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import secrets
import stat
import subprocess
from typing import Any, Callable, Mapping, Sequence

from gateway import pnc_rca_capacity_transition as transition
from gateway.pnc_rca_prod_admission import (
    RcaProdAdmissionError,
    validate_rca_prod_bootstrap_receipt,
)


PRODUCER_ACTIVATION_SCHEMA_VERSION = "pnc_rca_capacity_sample_producer_activation_v1"
PRODUCER_ACTIVATION_HMAC_DOMAIN = (
    b"hermes/rca_capacity_sample_producer_activation_v1\x00"
)
VM_TERMINAL_RECEIPT_SCHEMA_VERSION = "rca_prod_attempt_terminal_receipt_v1"
VM_TERMINAL_RECEIPT_HMAC_DOMAIN = b"hermes/rca_prod_attempt_terminal_receipt_v1\x00"
TERMINAL_HMAC_ENV = "HERMES_RCA_PROD_TERMINAL_HMAC_KEY"
VM_CAPACITY_MEASUREMENT_SCHEMA_VERSION = "rca_prod_capacity_measurement_v1"
VM_REMOTE_READ_ATTESTATION_SCHEMA_VERSION = "rca_prod_remote_read_attestation_v1"
HOST_SUCCESS_RECEIPT_SCHEMA_VERSION = "pnc_rca_capacity_host_success_v1"
HOST_SUCCESS_RECEIPT_HMAC_DOMAIN = b"hermes/rca_capacity_host_success_v1\x00"
PRODUCER_ACTIVATION_NAME = "sample-producer-activation.json"
HOST_SUCCESS_DIRECTORY = "host-success-receipts"
MAX_RECEIPT_BYTES = 512 * 1024
MAX_REMOTE_TIMEOUT_SECONDS = 30
MAX_REQUIRED_EFFECTS = 32
CREATE_ONCE_TEMP_SUFFIX = ".create-once.tmp"
HEX64_RE = re.compile(r"^[0-9a-f]{64}$")
IDENTITY_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$")

PRODUCER_ACTIVATION_FIELDS = {
    "schema_version",
    "receipt_id",
    "release_id",
    "bootstrap_epoch_id",
    "release_bom_sha256",
    "active_release_binding_sha256",
    "activated_at",
    "receipt_fingerprint",
    "hmac_sha256",
}
VM_TERMINAL_RECEIPT_FIELDS = {
    "schema_version",
    "receipt_id",
    "created_at",
    "task_id",
    "attempt_id",
    "terminal_state",
    "exit_code",
    "process_exit_code",
    "admission",
    "release",
    "worker",
    "measurement",
    "remote_read_attestation",
    "remote_read_attestation_sha256",
    "receipt_fingerprint",
    "hmac_sha256",
}
VM_ADMISSION_FIELDS = {
    "receipt_id",
    "receipt_raw_sha256",
    "receipt_fingerprint",
}
VM_RELEASE_FIELDS = {
    "capacity_mode",
    "release_id",
    "release_approval_id",
    "bootstrap_epoch_id",
    "release_bom_sha256",
    "active_release_binding_sha256",
}
VM_WORKER_FIELDS = {"commit", "source_files", "source_manifest_sha256"}
VM_SOURCE_FILE_FIELDS = {"path", "sha256"}
VM_MEASUREMENT_FIELDS = {
    "schema_version",
    "period_seconds",
    "max_gap_seconds",
    "max_gap_observed_seconds",
    "started_at",
    "finished_at",
    "sample_count",
    "coverage_ok",
    "reasons",
    "root",
    "delivery",
    "delivery_task_allocated_bytes",
    "delivery_task_node_count",
}
VM_FILESYSTEM_MEASUREMENT_FIELDS = {
    "path",
    "device",
    "filesystem",
    "initial_available_bytes",
    "final_available_bytes",
    "high_available_bytes",
    "minimum_available_bytes",
    "peak_free_drop_bytes",
}
VM_REMOTE_ATTESTATION_FIELDS = {
    "schema_version",
    "task_id",
    "attempt_id",
    "remote_read",
    "input_materialized",
    "mdi_download_attempted",
    "fallback_used",
    "manifest_sha256",
    "pipeline_sha256",
    "service_sha256",
    "attestation_fingerprint",
}
HOST_SUCCESS_RECEIPT_FIELDS = {
    "schema_version",
    "receipt_id",
    "created_at",
    "release_id",
    "bootstrap_epoch_id",
    "release_bom_sha256",
    "active_release_binding_sha256",
    "task_id",
    "attempt_id",
    "delivery_id",
    "source_kind",
    "admission_receipt_sha256",
    "admission_receipt_fingerprint",
    "producer_activation_receipt_sha256",
    "producer_activation_receipt_fingerprint",
    "vm_terminal_receipt_sha256",
    "vm_terminal_receipt_fingerprint",
    "delivery_snapshot_sha256",
    "job_outcome",
    "job_status",
    "required_effects",
    "receipt_fingerprint",
    "hmac_sha256",
}
HOST_REQUIRED_EFFECT_FIELDS = {
    "effect_key",
    "effect_kind",
    "target_key",
    "remote_id",
    "completed_at",
    "remote_receipt_sha256",
}


class CapacitySampleEvidenceError(RuntimeError):
    def __init__(self, code: str):
        self.code = str(code or "rca_capacity_sample_evidence_invalid")[:120]
        super().__init__(self.code)


@dataclass(frozen=True)
class VerifiedVmTerminalReceipt:
    receipt: dict[str, Any]
    raw_sha256: str
    fingerprint: str


@dataclass(frozen=True)
class CapacitySampleBuild:
    host_success_receipt: dict[str, Any]
    host_success_receipt_sha256: str
    sample: dict[str, Any]


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
        raise CapacitySampleEvidenceError(
            "rca_capacity_evidence_not_canonical"
        ) from exc


def _strict_json(raw: bytes, *, code: str) -> dict[str, Any]:
    def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate key")
            result[key] = value
        return result

    try:
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=pairs,
            parse_constant=lambda item: (_ for _ in ()).throw(ValueError(item)),
        )
    except (UnicodeDecodeError, ValueError, TypeError) as exc:
        raise CapacitySampleEvidenceError(code) from exc
    if not isinstance(value, dict):
        raise CapacitySampleEvidenceError(code)
    return value


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _hex(value: Any, code: str) -> str:
    candidate = str(value or "")
    if candidate != candidate.lower() or HEX64_RE.fullmatch(candidate) is None:
        raise CapacitySampleEvidenceError(code)
    return candidate


def _identity(value: Any, code: str) -> str:
    candidate = str(value or "")
    if candidate != candidate.strip() or IDENTITY_RE.fullmatch(candidate) is None:
        raise CapacitySampleEvidenceError(code)
    return candidate


def _integer(value: Any, code: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise CapacitySampleEvidenceError(code)
    return value


def _number(value: Any, code: str, *, minimum: float = 0) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise CapacitySampleEvidenceError(code)
    number = float(value)
    if not number >= minimum or number == float("inf"):
        raise CapacitySampleEvidenceError(code)
    return number


def _time(value: Any, code: str) -> datetime:
    if not isinstance(value, str) or value != value.strip():
        raise CapacitySampleEvidenceError(code)
    text = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(text)
    except (TypeError, ValueError, OverflowError) as exc:
        raise CapacitySampleEvidenceError(code) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise CapacitySampleEvidenceError(code)
    return parsed.astimezone(timezone.utc)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise CapacitySampleEvidenceError("rca_capacity_evidence_time_invalid")
    return value.astimezone(timezone.utc)


def _iso(value: datetime) -> str:
    normalized = _utc(value)
    timespec = "microseconds" if normalized.microsecond else "seconds"
    return normalized.isoformat(timespec=timespec).replace("+00:00", "Z")


def _signed(
    body: Mapping[str, Any],
    *,
    fingerprint_field: str,
    hmac_field: str,
    key: bytes,
    domain: bytes,
) -> dict[str, Any]:
    if len(key) < transition.MIN_HMAC_KEY_BYTES:
        raise CapacitySampleEvidenceError("rca_capacity_evidence_hmac_key_invalid")
    result = dict(body)
    result.pop(fingerprint_field, None)
    result.pop(hmac_field, None)
    raw = canonical_bytes(result)
    result[fingerprint_field] = _sha(raw)
    result[hmac_field] = hmac.new(key, domain + raw, hashlib.sha256).hexdigest()
    return result


def _validate_signed(
    value: Mapping[str, Any],
    *,
    fingerprint_field: str,
    hmac_field: str,
    key: bytes,
    domain: bytes,
    code: str,
) -> None:
    body = {
        name: item
        for name, item in value.items()
        if name not in {fingerprint_field, hmac_field}
    }
    raw = canonical_bytes(body)
    fingerprint = _hex(value.get(fingerprint_field), code)
    signature = _hex(value.get(hmac_field), code)
    if not hmac.compare_digest(fingerprint, _sha(raw)) or not hmac.compare_digest(
        signature, hmac.new(key, domain + raw, hashlib.sha256).hexdigest()
    ):
        raise CapacitySampleEvidenceError(code)


def issue_producer_activation_receipt(
    *,
    release_id: str,
    bootstrap_epoch_id: str,
    release_bom_sha256: str,
    active_release_binding_sha256: str,
    activated_at: datetime,
    hmac_key: bytes,
    receipt_id: str | None = None,
) -> dict[str, Any]:
    body = {
        "schema_version": PRODUCER_ACTIVATION_SCHEMA_VERSION,
        "receipt_id": receipt_id or f"producer-{secrets.token_hex(16)}",
        "release_id": release_id,
        "bootstrap_epoch_id": bootstrap_epoch_id,
        "release_bom_sha256": release_bom_sha256,
        "active_release_binding_sha256": active_release_binding_sha256,
        "activated_at": _iso(activated_at),
    }
    receipt = _signed(
        body,
        fingerprint_field="receipt_fingerprint",
        hmac_field="hmac_sha256",
        key=hmac_key,
        domain=PRODUCER_ACTIVATION_HMAC_DOMAIN,
    )
    return validate_producer_activation_receipt(receipt, hmac_key=hmac_key)


def validate_producer_activation_receipt(
    value: Any,
    *,
    hmac_key: bytes,
    expected_release_id: str | None = None,
    expected_bootstrap_epoch_id: str | None = None,
    expected_release_bom_sha256: str | None = None,
    expected_active_release_binding_sha256: str | None = None,
) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != PRODUCER_ACTIVATION_FIELDS:
        raise CapacitySampleEvidenceError(
            "rca_capacity_producer_receipt_schema_invalid"
        )
    if value.get("schema_version") != PRODUCER_ACTIVATION_SCHEMA_VERSION:
        raise CapacitySampleEvidenceError(
            "rca_capacity_producer_receipt_schema_invalid"
        )
    _identity(value.get("receipt_id"), "rca_capacity_producer_receipt_identity_invalid")
    release = _identity(
        value.get("release_id"), "rca_capacity_producer_receipt_identity_invalid"
    )
    epoch = _identity(
        value.get("bootstrap_epoch_id"),
        "rca_capacity_producer_receipt_identity_invalid",
    )
    bom = _hex(
        value.get("release_bom_sha256"), "rca_capacity_producer_receipt_binding_invalid"
    )
    binding = _hex(
        value.get("active_release_binding_sha256"),
        "rca_capacity_producer_receipt_binding_invalid",
    )
    _time(value.get("activated_at"), "rca_capacity_producer_receipt_time_invalid")
    if any((
        expected_release_id is not None and release != expected_release_id,
        expected_bootstrap_epoch_id is not None
        and epoch != expected_bootstrap_epoch_id,
        expected_release_bom_sha256 is not None and bom != expected_release_bom_sha256,
        expected_active_release_binding_sha256 is not None
        and binding != expected_active_release_binding_sha256,
    )):
        raise CapacitySampleEvidenceError(
            "rca_capacity_producer_receipt_binding_invalid"
        )
    _validate_signed(
        value,
        fingerprint_field="receipt_fingerprint",
        hmac_field="hmac_sha256",
        key=hmac_key,
        domain=PRODUCER_ACTIVATION_HMAC_DOMAIN,
        code="rca_capacity_producer_receipt_tampered",
    )
    return dict(value)


def producer_activation_path(state_root: str | Path) -> Path:
    return Path(state_root).expanduser().absolute() / PRODUCER_ACTIVATION_NAME


def host_success_receipt_path(
    state_root: str | Path, *, task_id: str, attempt_id: str
) -> Path:
    task = _identity(task_id, "rca_capacity_host_receipt_identity_invalid")
    attempt = _identity(attempt_id, "rca_capacity_host_receipt_identity_invalid")
    name = f"{_sha(task.encode())[:20]}-{_sha(attempt.encode())[:20]}.json"
    return Path(state_root).expanduser().absolute() / HOST_SUCCESS_DIRECTORY / name


def _fsync_directory(path: Path) -> None:
    descriptor = -1
    try:
        descriptor = os.open(
            path,
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_DIRECTORY", 0),
        )
        os.fsync(descriptor)
    except OSError as exc:
        raise CapacitySampleEvidenceError(
            "rca_capacity_receipt_write_failed"
        ) from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _read_interrupted_target_raw(target: Path, info: os.stat_result) -> bytes:
    if (
        not stat.S_ISREG(info.st_mode)
        or info.st_uid != os.getuid()
        or info.st_nlink != 2
        or stat.S_IMODE(info.st_mode) != 0o600
        or info.st_size < 2
        or info.st_size > MAX_RECEIPT_BYTES
    ):
        raise CapacitySampleEvidenceError("rca_capacity_receipt_conflict")
    identity = (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns)
    descriptor = -1
    try:
        descriptor = os.open(
            target,
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0),
        )
        opened = os.fstat(descriptor)
        if (opened.st_dev, opened.st_ino, opened.st_size, opened.st_mtime_ns) != identity:
            raise CapacitySampleEvidenceError("rca_capacity_receipt_conflict")
        raw = b""
        while len(raw) < opened.st_size:
            chunk = os.read(descriptor, opened.st_size - len(raw))
            if not chunk:
                raise CapacitySampleEvidenceError("rca_capacity_receipt_conflict")
            raw += chunk
        if os.read(descriptor, 1):
            raise CapacitySampleEvidenceError("rca_capacity_receipt_conflict")
        after = os.fstat(descriptor)
        current = target.lstat()
        if (
            (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
            != identity
            or (current.st_dev, current.st_ino, current.st_size, current.st_mtime_ns)
            != identity
        ):
            raise CapacitySampleEvidenceError("rca_capacity_receipt_conflict")
    except CapacitySampleEvidenceError:
        raise
    except OSError as exc:
        raise CapacitySampleEvidenceError("rca_capacity_receipt_conflict") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    try:
        value = _strict_json(raw, code="rca_capacity_receipt_json_invalid")
    except CapacitySampleEvidenceError as exc:
        raise CapacitySampleEvidenceError("rca_capacity_receipt_conflict") from exc
    if canonical_bytes(value) != raw:
        raise CapacitySampleEvidenceError("rca_capacity_receipt_conflict")
    return raw


def _recover_interrupted_create_once(target: Path, raw: bytes) -> bool:
    try:
        target_info = target.lstat()
    except FileNotFoundError:
        return False
    except OSError as exc:
        raise CapacitySampleEvidenceError("rca_capacity_receipt_conflict") from exc
    if target_info.st_nlink == 1:
        try:
            existing, existing_raw = read_owner_only_receipt(target)
        except CapacitySampleEvidenceError as exc:
            raise CapacitySampleEvidenceError(
                "rca_capacity_receipt_conflict"
            ) from exc
        if existing_raw != raw or canonical_bytes(existing) != raw:
            raise CapacitySampleEvidenceError("rca_capacity_receipt_conflict")
        return True
    if (
        not stat.S_ISREG(target_info.st_mode)
        or target_info.st_uid != os.getuid()
        or target_info.st_nlink != 2
        or stat.S_IMODE(target_info.st_mode) != 0o600
        or target_info.st_size != len(raw)
    ):
        raise CapacitySampleEvidenceError("rca_capacity_receipt_conflict")
    pattern = f".{target.name}.*{CREATE_ONCE_TEMP_SUFFIX}"
    try:
        candidates = list(target.parent.glob(pattern))
    except OSError as exc:
        raise CapacitySampleEvidenceError("rca_capacity_receipt_conflict") from exc
    if len(candidates) != 1:
        raise CapacitySampleEvidenceError("rca_capacity_receipt_conflict")
    temporary = candidates[0]
    try:
        temporary_info = temporary.lstat()
    except OSError as exc:
        raise CapacitySampleEvidenceError("rca_capacity_receipt_conflict") from exc
    identity = (
        target_info.st_dev,
        target_info.st_ino,
        target_info.st_size,
        target_info.st_mtime_ns,
    )
    if (
        not stat.S_ISREG(temporary_info.st_mode)
        or temporary_info.st_uid != os.getuid()
        or temporary_info.st_nlink != 2
        or stat.S_IMODE(temporary_info.st_mode) != 0o600
        or (
            temporary_info.st_dev,
            temporary_info.st_ino,
            temporary_info.st_size,
            temporary_info.st_mtime_ns,
        )
        != identity
    ):
        raise CapacitySampleEvidenceError("rca_capacity_receipt_conflict")
    descriptor = -1
    try:
        descriptor = os.open(
            temporary,
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0),
        )
        opened = os.fstat(descriptor)
        if (
            opened.st_dev,
            opened.st_ino,
            opened.st_size,
            opened.st_mtime_ns,
        ) != identity:
            raise CapacitySampleEvidenceError("rca_capacity_receipt_conflict")
        recovered = b""
        while len(recovered) < len(raw):
            chunk = os.read(descriptor, len(raw) - len(recovered))
            if not chunk:
                break
            recovered += chunk
        if recovered != raw or os.read(descriptor, 1):
            raise CapacitySampleEvidenceError("rca_capacity_receipt_conflict")
        after = os.fstat(descriptor)
        if (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns) != identity:
            raise CapacitySampleEvidenceError("rca_capacity_receipt_conflict")
        target_after = target.lstat()
        temporary_after = temporary.lstat()
        if (
            (
                target_after.st_dev,
                target_after.st_ino,
                target_after.st_size,
                target_after.st_mtime_ns,
            )
            != identity
            or (
                temporary_after.st_dev,
                temporary_after.st_ino,
                temporary_after.st_size,
                temporary_after.st_mtime_ns,
            )
            != identity
        ):
            raise CapacitySampleEvidenceError("rca_capacity_receipt_conflict")
    except CapacitySampleEvidenceError:
        raise
    except OSError as exc:
        raise CapacitySampleEvidenceError("rca_capacity_receipt_conflict") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    try:
        temporary.unlink()
        _fsync_directory(target.parent)
        repaired = target.lstat()
    except (OSError, CapacitySampleEvidenceError) as exc:
        raise CapacitySampleEvidenceError("rca_capacity_receipt_conflict") from exc
    if (
        repaired.st_dev != identity[0]
        or repaired.st_ino != identity[1]
        or repaired.st_size != identity[2]
        or repaired.st_mtime_ns != identity[3]
        or repaired.st_nlink != 1
        or repaired.st_uid != os.getuid()
        or stat.S_IMODE(repaired.st_mode) != 0o600
    ):
        raise CapacitySampleEvidenceError("rca_capacity_receipt_conflict")
    try:
        existing, existing_raw = read_owner_only_receipt(target)
    except CapacitySampleEvidenceError as exc:
        raise CapacitySampleEvidenceError("rca_capacity_receipt_conflict") from exc
    if existing_raw != raw or canonical_bytes(existing) != raw:
        raise CapacitySampleEvidenceError("rca_capacity_receipt_conflict")
    return True


def write_owner_only_create_once(path: str | Path, value: Mapping[str, Any]) -> str:
    target = Path(path).expanduser().absolute()
    raw = canonical_bytes(dict(value))
    if not raw or len(raw) > MAX_RECEIPT_BYTES:
        raise CapacitySampleEvidenceError("rca_capacity_receipt_size_invalid")
    target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    try:
        parent_stat = target.parent.lstat()
    except OSError as exc:
        raise CapacitySampleEvidenceError(
            "rca_capacity_receipt_parent_invalid"
        ) from exc
    if stat.S_ISLNK(parent_stat.st_mode) or not stat.S_ISDIR(parent_stat.st_mode):
        raise CapacitySampleEvidenceError("rca_capacity_receipt_parent_invalid")
    os.chmod(target.parent, 0o700)
    if _recover_interrupted_create_once(target, raw):
        return _sha(raw)
    temporary = target.with_name(
        f".{target.name}.{os.getpid()}.{secrets.token_hex(8)}"
        f"{CREATE_ONCE_TEMP_SUFFIX}"
    )
    descriptor = -1
    try:
        descriptor = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
            0o600,
        )
        view = memoryview(raw)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise OSError("short write")
            view = view[written:]
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = -1
        try:
            os.link(temporary, target, follow_symlinks=False)
        except FileExistsError:
            if _recover_interrupted_create_once(target, raw):
                return _sha(raw)
            raise CapacitySampleEvidenceError("rca_capacity_receipt_conflict")
        temporary.unlink()
        _fsync_directory(target.parent)
        written = target.lstat()
        if (
            not stat.S_ISREG(written.st_mode)
            or written.st_uid != os.getuid()
            or written.st_nlink != 1
            or stat.S_IMODE(written.st_mode) != 0o600
            or written.st_size != len(raw)
        ):
            raise CapacitySampleEvidenceError("rca_capacity_receipt_write_failed")
        return _sha(raw)
    except CapacitySampleEvidenceError:
        raise
    except OSError as exc:
        raise CapacitySampleEvidenceError("rca_capacity_receipt_write_failed") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def read_owner_only_receipt(path: str | Path) -> tuple[dict[str, Any], bytes]:
    target = Path(path).expanduser().absolute()
    descriptor = -1
    try:
        before = target.lstat()
        if before.st_nlink == 2:
            interrupted_raw = _read_interrupted_target_raw(target, before)
            _recover_interrupted_create_once(target, interrupted_raw)
            return read_owner_only_receipt(target)
        if (
            stat.S_ISLNK(before.st_mode)
            or not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
            or stat.S_IMODE(before.st_mode) != 0o600
            or before.st_uid != os.getuid()
            or before.st_size < 2
            or before.st_size > MAX_RECEIPT_BYTES
        ):
            raise CapacitySampleEvidenceError("rca_capacity_receipt_file_invalid")
        descriptor = os.open(target, os.O_RDONLY | os.O_NOFOLLOW)
        opened = os.fstat(descriptor)
        identity = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
        if (
            opened.st_dev,
            opened.st_ino,
            opened.st_size,
            opened.st_mtime_ns,
        ) != identity:
            raise CapacitySampleEvidenceError("rca_capacity_receipt_identity_changed")
        raw = b""
        while len(raw) < opened.st_size:
            chunk = os.read(descriptor, opened.st_size - len(raw))
            if not chunk:
                raise CapacitySampleEvidenceError("rca_capacity_receipt_read_failed")
            raw += chunk
        if os.read(descriptor, 1):
            raise CapacitySampleEvidenceError("rca_capacity_receipt_identity_changed")
        after = os.fstat(descriptor)
        if (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns) != identity:
            raise CapacitySampleEvidenceError("rca_capacity_receipt_identity_changed")
    except CapacitySampleEvidenceError:
        raise
    except OSError as exc:
        raise CapacitySampleEvidenceError("rca_capacity_receipt_read_failed") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    value = _strict_json(raw, code="rca_capacity_receipt_json_invalid")
    if canonical_bytes(value) != raw:
        raise CapacitySampleEvidenceError("rca_capacity_receipt_not_canonical")
    return value, raw


def ensure_owner_only_lock_file(path: str | Path) -> None:
    """Create an empty private lock inode once, then validate exact identity."""
    target = Path(path).expanduser().absolute()
    target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(target.parent, 0o700)
    descriptor = -1
    try:
        try:
            descriptor = os.open(
                target,
                os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                0o600,
            )
            os.fsync(descriptor)
        except FileExistsError:
            descriptor = os.open(target, os.O_RDWR | os.O_NOFOLLOW)
        opened = os.fstat(descriptor)
        observed = target.lstat()
        if (
            not stat.S_ISREG(opened.st_mode)
            or stat.S_ISLNK(observed.st_mode)
            or stat.S_IMODE(opened.st_mode) != 0o600
            or opened.st_uid != os.getuid()
            or opened.st_nlink != 1
            or (opened.st_dev, opened.st_ino) != (observed.st_dev, observed.st_ino)
        ):
            raise CapacitySampleEvidenceError("rca_capacity_lock_file_invalid")
    except CapacitySampleEvidenceError:
        raise
    except OSError as exc:
        raise CapacitySampleEvidenceError("rca_capacity_lock_file_invalid") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def read_and_validate_producer_activation(
    path: str | Path, *, hmac_key: bytes, **expected: Any
) -> tuple[dict[str, Any], str]:
    value, raw = read_owner_only_receipt(path)
    return validate_producer_activation_receipt(
        value, hmac_key=hmac_key, **expected
    ), _sha(raw)


def expected_vm_terminal_receipt_path(task_id: str, attempt_id: str) -> str:
    task = _identity(task_id, "rca_capacity_vm_receipt_identity_invalid")
    attempt = _identity(attempt_id, "rca_capacity_vm_receipt_identity_invalid")
    return (
        f"/home/mini/.hermes/worker-state/tasks/{task}/control/rca-prod/"
        f"attempts/{attempt}/terminal-receipt.json"
    )


def _remote_receipt_reader_script(expected_path: str) -> str:
    return f"""import base64, hashlib, json, os, stat
path = {expected_path!r}
limit = {MAX_RECEIPT_BYTES}
parts = path.split('/')[1:]
cursor = '/'
for part in parts[:-1]:
    cursor = os.path.join(cursor, part)
    info = os.lstat(cursor)
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
        raise RuntimeError('receipt_parent_invalid')
before = os.lstat(path)
if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
    raise RuntimeError('receipt_not_regular')
if stat.S_IMODE(before.st_mode) != 0o600 or before.st_uid != os.getuid() or before.st_nlink != 1:
    raise RuntimeError('receipt_not_owner_only')
if before.st_size < 2 or before.st_size > limit:
    raise RuntimeError('receipt_size_invalid')
fd = os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0))
try:
    opened = os.fstat(fd)
    identity = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
    if (opened.st_dev, opened.st_ino, opened.st_size, opened.st_mtime_ns) != identity:
        raise RuntimeError('receipt_identity_changed')
    raw = b''
    while len(raw) < opened.st_size:
        chunk = os.read(fd, opened.st_size - len(raw))
        if not chunk:
            raise RuntimeError('receipt_short_read')
        raw += chunk
    if os.read(fd, 1):
        raise RuntimeError('receipt_grew')
    after = os.fstat(fd)
    if (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns) != identity:
        raise RuntimeError('receipt_identity_changed')
finally:
    os.close(fd)
print(json.dumps({{'ok': True, 'path': path, 'size': len(raw), 'sha256': hashlib.sha256(raw).hexdigest(), 'raw_base64': base64.b64encode(raw).decode('ascii')}}, sort_keys=True, separators=(',', ':')))
"""


def read_remote_vm_terminal_receipt(
    *,
    ssh_mini_agent: str,
    task_id: str,
    attempt_id: str,
    timeout_seconds: int,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> bytes:
    if timeout_seconds < 1 or timeout_seconds > MAX_REMOTE_TIMEOUT_SECONDS:
        raise CapacitySampleEvidenceError("rca_capacity_vm_receipt_timeout_invalid")
    agent = str(ssh_mini_agent or "").strip()
    if not agent:
        raise CapacitySampleEvidenceError("rca_capacity_vm_receipt_reader_invalid")
    expected = expected_vm_terminal_receipt_path(task_id, attempt_id)
    child_env = dict(os.environ)
    child_env.pop("HERMES_RCA_PROD_ADMISSION_HMAC_KEY", None)
    child_env.pop(TERMINAL_HMAC_ENV, None)
    try:
        process = run(
            [agent, "run_py_json"],
            input=_remote_receipt_reader_script(expected),
            text=True,
            capture_output=True,
            timeout=timeout_seconds,
            check=False,
            env=child_env,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise CapacitySampleEvidenceError(
            "rca_capacity_vm_receipt_read_failed"
        ) from exc
    if (
        process.returncode != 0
        or len(process.stdout.encode("utf-8")) > MAX_RECEIPT_BYTES * 2
    ):
        raise CapacitySampleEvidenceError("rca_capacity_vm_receipt_read_failed")
    try:
        envelope = _strict_json(
            process.stdout.encode("utf-8"),
            code="rca_capacity_vm_receipt_reader_invalid",
        )
        raw = base64.b64decode(str(envelope.get("raw_base64") or ""), validate=True)
    except (ValueError, TypeError) as exc:
        raise CapacitySampleEvidenceError(
            "rca_capacity_vm_receipt_reader_invalid"
        ) from exc
    if (
        set(envelope) != {"ok", "path", "size", "sha256", "raw_base64"}
        or envelope.get("ok") is not True
        or envelope.get("path") != expected
        or envelope.get("size") != len(raw)
        or not 2 <= len(raw) <= MAX_RECEIPT_BYTES
        or envelope.get("sha256") != _sha(raw)
    ):
        raise CapacitySampleEvidenceError("rca_capacity_vm_receipt_reader_invalid")
    return raw


def _validate_filesystem_measurement(
    value: Any, *, expected_path: str
) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != VM_FILESYSTEM_MEASUREMENT_FIELDS:
        raise CapacitySampleEvidenceError("rca_capacity_vm_measurement_schema_invalid")
    if value.get("path") != expected_path:
        raise CapacitySampleEvidenceError("rca_capacity_vm_measurement_path_invalid")
    if not isinstance(value.get("device"), str) or not value["device"]:
        raise CapacitySampleEvidenceError("rca_capacity_vm_measurement_device_invalid")
    if not isinstance(value.get("filesystem"), str) or not value["filesystem"]:
        raise CapacitySampleEvidenceError("rca_capacity_vm_measurement_device_invalid")
    for field in (
        "initial_available_bytes",
        "final_available_bytes",
        "high_available_bytes",
        "minimum_available_bytes",
        "peak_free_drop_bytes",
    ):
        _integer(value.get(field), "rca_capacity_vm_measurement_value_invalid")
    if not (
        value["high_available_bytes"]
        >= value["initial_available_bytes"]
        >= value["minimum_available_bytes"]
        and value["high_available_bytes"]
        >= value["final_available_bytes"]
        >= value["minimum_available_bytes"]
        and value["peak_free_drop_bytes"]
        == value["high_available_bytes"] - value["minimum_available_bytes"]
    ):
        raise CapacitySampleEvidenceError("rca_capacity_vm_measurement_value_invalid")
    return dict(value)


def validate_vm_terminal_receipt(
    raw: bytes,
    *,
    task_meta: Mapping[str, Any],
    admission_receipt: Mapping[str, Any],
    producer_activation: Mapping[str, Any],
    admission_hmac_key: bytes,
    terminal_hmac_key: bytes | None = None,
) -> VerifiedVmTerminalReceipt:
    if not isinstance(raw, bytes) or not 2 <= len(raw) <= MAX_RECEIPT_BYTES:
        raise CapacitySampleEvidenceError("rca_capacity_vm_receipt_size_invalid")
    receipt = _strict_json(raw, code="rca_capacity_vm_receipt_json_invalid")
    if canonical_bytes(receipt) != raw or set(receipt) != VM_TERMINAL_RECEIPT_FIELDS:
        raise CapacitySampleEvidenceError("rca_capacity_vm_receipt_schema_invalid")
    if receipt.get("schema_version") != VM_TERMINAL_RECEIPT_SCHEMA_VERSION:
        raise CapacitySampleEvidenceError("rca_capacity_vm_receipt_schema_invalid")
    task_id = _identity(
        receipt.get("task_id"), "rca_capacity_vm_receipt_identity_invalid"
    )
    attempt_id = _identity(
        receipt.get("attempt_id"), "rca_capacity_vm_receipt_identity_invalid"
    )
    _identity(receipt.get("receipt_id"), "rca_capacity_vm_receipt_identity_invalid")
    created_at = _time(
        receipt.get("created_at"), "rca_capacity_vm_receipt_time_invalid"
    )
    if (
        receipt.get("terminal_state") != "completed"
        or receipt.get("exit_code") != 0
        or receipt.get("process_exit_code") != 0
    ):
        raise CapacitySampleEvidenceError("rca_capacity_vm_receipt_not_successful")
    if not isinstance(task_meta, Mapping):
        raise CapacitySampleEvidenceError("rca_capacity_vm_meta_invalid")
    expected_path = expected_vm_terminal_receipt_path(task_id, attempt_id)
    raw_sha = _sha(raw)
    if (
        task_meta.get("rca_prod_capacity_sample_eligible") is not True
        or task_meta.get("rca_prod_capacity_measurement_error") not in {None, ""}
        or task_meta.get("rca_prod_terminal_receipt_path") != expected_path
        or task_meta.get("rca_prod_terminal_receipt_sha256") != raw_sha
        or task_meta.get("rca_prod_terminal_receipt_fingerprint")
        != receipt.get("receipt_fingerprint")
        or task_meta.get("rca_prod_attempt_id") != attempt_id
        or task_meta.get("rca_prod_capacity_mode") != "bootstrap"
    ):
        raise CapacitySampleEvidenceError("rca_capacity_vm_meta_invalid")
    admission = receipt.get("admission")
    release = receipt.get("release")
    worker = receipt.get("worker")
    measurement = receipt.get("measurement")
    attestation = receipt.get("remote_read_attestation")
    if not isinstance(admission, Mapping) or set(admission) != VM_ADMISSION_FIELDS:
        raise CapacitySampleEvidenceError("rca_capacity_vm_receipt_schema_invalid")
    if not isinstance(release, Mapping) or set(release) != VM_RELEASE_FIELDS:
        raise CapacitySampleEvidenceError("rca_capacity_vm_receipt_schema_invalid")
    if not isinstance(worker, Mapping) or set(worker) != VM_WORKER_FIELDS:
        raise CapacitySampleEvidenceError("rca_capacity_vm_receipt_schema_invalid")
    if (
        not isinstance(measurement, Mapping)
        or set(measurement) != VM_MEASUREMENT_FIELDS
    ):
        raise CapacitySampleEvidenceError("rca_capacity_vm_measurement_schema_invalid")
    if (
        not isinstance(attestation, Mapping)
        or set(attestation) != VM_REMOTE_ATTESTATION_FIELDS
    ):
        raise CapacitySampleEvidenceError("rca_capacity_vm_attestation_schema_invalid")
    activation = validate_producer_activation_receipt(
        producer_activation,
        hmac_key=admission_hmac_key,
        expected_release_id=str(release.get("release_id") or ""),
        expected_bootstrap_epoch_id=str(release.get("bootstrap_epoch_id") or ""),
        expected_release_bom_sha256=str(release.get("release_bom_sha256") or ""),
        expected_active_release_binding_sha256=str(
            release.get("active_release_binding_sha256") or ""
        ),
    )
    if release.get("capacity_mode") != "bootstrap":
        raise CapacitySampleEvidenceError("rca_capacity_vm_release_binding_invalid")
    for field in ("release_bom_sha256", "active_release_binding_sha256"):
        _hex(release.get(field), "rca_capacity_vm_release_binding_invalid")
    _identity(release.get("release_id"), "rca_capacity_vm_release_binding_invalid")
    _identity(
        release.get("release_approval_id"), "rca_capacity_vm_release_binding_invalid"
    )
    _identity(
        release.get("bootstrap_epoch_id"), "rca_capacity_vm_release_binding_invalid"
    )
    bindings = admission_receipt.get("bindings")
    if not isinstance(bindings, Mapping):
        raise CapacitySampleEvidenceError("rca_capacity_admission_receipt_invalid")
    try:
        validated_admission = validate_rca_prod_bootstrap_receipt(
            admission_receipt,
            expected_bindings=bindings,
            expected_epoch_id=release["bootstrap_epoch_id"],
            expected_release_bom_sha256=release["release_bom_sha256"],
            expected_active_release_binding_sha256=release[
                "active_release_binding_sha256"
            ],
            hmac_key=admission_hmac_key,
            now=created_at,
            allow_historical=True,
        )
    except (RcaProdAdmissionError, TypeError, ValueError) as exc:
        raise CapacitySampleEvidenceError(
            "rca_capacity_admission_receipt_invalid"
        ) from exc
    admission_raw = canonical_bytes(validated_admission)
    bootstrap_authorization = validated_admission.get("bootstrap_authorization")
    if not isinstance(bootstrap_authorization, Mapping):
        raise CapacitySampleEvidenceError("rca_capacity_admission_receipt_invalid")
    if (
        bindings.get("task_id") != task_id
        or bindings.get("attempt_id") != attempt_id
        or admission.get("receipt_id") != validated_admission.get("receipt_id")
        or admission.get("receipt_raw_sha256") != _sha(admission_raw)
        or admission.get("receipt_fingerprint")
        != validated_admission.get("receipt_fingerprint")
        or task_meta.get("rca_prod_admission_key_fingerprint")
        != hashlib.sha256(admission_hmac_key).hexdigest()
        or task_meta.get("rca_prod_bootstrap_epoch_id") != release["bootstrap_epoch_id"]
        or task_meta.get("rca_prod_release_bom_sha256") != release["release_bom_sha256"]
        or task_meta.get("rca_prod_active_release_binding_sha256")
        != release["active_release_binding_sha256"]
        or task_meta.get("rca_prod_bootstrap_release_approval_id")
        != release["release_approval_id"]
        or task_meta.get("rca_prod_bootstrap_authorization_fingerprint")
        != bootstrap_authorization.get("receipt_fingerprint")
    ):
        raise CapacitySampleEvidenceError(
            "rca_capacity_admission_receipt_binding_invalid"
        )
    issued_at = _time(
        validated_admission.get("issued_at"),
        "rca_capacity_admission_receipt_time_invalid",
    )
    activated_at = _time(
        activation.get("activated_at"), "rca_capacity_producer_receipt_time_invalid"
    )
    if issued_at < activated_at or created_at < issued_at:
        raise CapacitySampleEvidenceError("rca_capacity_sample_history_fence_failed")
    if measurement.get("schema_version") != VM_CAPACITY_MEASUREMENT_SCHEMA_VERSION:
        raise CapacitySampleEvidenceError("rca_capacity_vm_measurement_schema_invalid")
    if measurement.get("coverage_ok") is not True:
        raise CapacitySampleEvidenceError("rca_capacity_vm_measurement_incomplete")
    reasons = measurement.get("reasons")
    if not isinstance(reasons, list) or reasons:
        raise CapacitySampleEvidenceError("rca_capacity_vm_measurement_incomplete")
    _number(
        measurement.get("period_seconds"), "rca_capacity_vm_measurement_value_invalid"
    )
    maximum_gap = _number(
        measurement.get("max_gap_seconds"), "rca_capacity_vm_measurement_value_invalid"
    )
    observed_gap = _number(
        measurement.get("max_gap_observed_seconds"),
        "rca_capacity_vm_measurement_value_invalid",
    )
    if observed_gap > maximum_gap:
        raise CapacitySampleEvidenceError("rca_capacity_vm_measurement_incomplete")
    started = _time(
        measurement.get("started_at"), "rca_capacity_vm_measurement_time_invalid"
    )
    finished = _time(
        measurement.get("finished_at"), "rca_capacity_vm_measurement_time_invalid"
    )
    if not issued_at <= started <= finished <= created_at:
        raise CapacitySampleEvidenceError("rca_capacity_vm_measurement_time_invalid")
    _integer(
        measurement.get("sample_count"),
        "rca_capacity_vm_measurement_value_invalid",
        minimum=2,
    )
    root = _validate_filesystem_measurement(measurement.get("root"), expected_path="/")
    delivery = _validate_filesystem_measurement(
        measurement.get("delivery"), expected_path="/mnt/tmp"
    )
    allocated = _integer(
        measurement.get("delivery_task_allocated_bytes"),
        "rca_capacity_vm_measurement_value_invalid",
    )
    _integer(
        measurement.get("delivery_task_node_count"),
        "rca_capacity_vm_measurement_value_invalid",
    )
    if allocated > delivery["peak_free_drop_bytes"]:
        raise CapacitySampleEvidenceError("rca_capacity_vm_measurement_value_invalid")
    if attestation.get("schema_version") != VM_REMOTE_READ_ATTESTATION_SCHEMA_VERSION:
        raise CapacitySampleEvidenceError("rca_capacity_vm_attestation_schema_invalid")
    if (
        attestation.get("task_id") != task_id
        or attestation.get("attempt_id") != attempt_id
    ):
        raise CapacitySampleEvidenceError("rca_capacity_vm_attestation_binding_invalid")
    if (
        attestation.get("remote_read") is not True
        or attestation.get("input_materialized") is not False
        or attestation.get("mdi_download_attempted") is not False
        or attestation.get("fallback_used") is not False
    ):
        raise CapacitySampleEvidenceError("rca_capacity_vm_attestation_policy_invalid")
    for field in ("manifest_sha256", "pipeline_sha256", "service_sha256"):
        _hex(attestation.get(field), "rca_capacity_vm_attestation_binding_invalid")
    attestation_body = {
        key: item
        for key, item in attestation.items()
        if key != "attestation_fingerprint"
    }
    attestation_fingerprint = _sha(canonical_bytes(attestation_body))
    if not hmac.compare_digest(
        _hex(
            attestation.get("attestation_fingerprint"),
            "rca_capacity_vm_attestation_tampered",
        ),
        attestation_fingerprint,
    ) or receipt.get("remote_read_attestation_sha256") != _sha(
        canonical_bytes(attestation)
    ):
        raise CapacitySampleEvidenceError("rca_capacity_vm_attestation_tampered")
    source_files = worker.get("source_files")
    if (
        not isinstance(worker.get("commit"), str)
        or not worker["commit"]
        or not isinstance(source_files, list)
        or not source_files
        or len(source_files) > 128
    ):
        raise CapacitySampleEvidenceError("rca_capacity_vm_worker_identity_invalid")
    normalized_files: list[dict[str, str]] = []
    for item in source_files:
        if not isinstance(item, Mapping) or set(item) != VM_SOURCE_FILE_FIELDS:
            raise CapacitySampleEvidenceError("rca_capacity_vm_worker_identity_invalid")
        path = str(item.get("path") or "")
        if not path or path.startswith("/") or ".." in Path(path).parts:
            raise CapacitySampleEvidenceError("rca_capacity_vm_worker_identity_invalid")
        normalized_files.append({
            "path": path,
            "sha256": _hex(
                item.get("sha256"), "rca_capacity_vm_worker_identity_invalid"
            ),
        })
    if worker.get("source_manifest_sha256") != _sha(canonical_bytes(normalized_files)):
        raise CapacitySampleEvidenceError("rca_capacity_vm_worker_identity_invalid")
    effective_key = (
        terminal_hmac_key
        or hmac.new(
            admission_hmac_key, VM_TERMINAL_RECEIPT_HMAC_DOMAIN, hashlib.sha256
        ).digest()
    )
    if len(effective_key) < transition.MIN_HMAC_KEY_BYTES:
        raise CapacitySampleEvidenceError("rca_capacity_vm_receipt_hmac_key_invalid")
    body = {
        key: item
        for key, item in receipt.items()
        if key not in {"receipt_fingerprint", "hmac_sha256"}
    }
    body_raw = canonical_bytes(body)
    fingerprint = _hex(
        receipt.get("receipt_fingerprint"), "rca_capacity_vm_receipt_tampered"
    )
    signature = _hex(receipt.get("hmac_sha256"), "rca_capacity_vm_receipt_tampered")
    if not hmac.compare_digest(fingerprint, _sha(body_raw)) or not hmac.compare_digest(
        signature, hmac.new(effective_key, body_raw, hashlib.sha256).hexdigest()
    ):
        raise CapacitySampleEvidenceError("rca_capacity_vm_receipt_tampered")
    return VerifiedVmTerminalReceipt(
        receipt=dict(receipt), raw_sha256=raw_sha, fingerprint=fingerprint
    )


def _delivery_success(
    snapshot: Mapping[str, Any], *, vm_created_at: datetime
) -> tuple[list[dict[str, str]], str, str, str, str]:
    if snapshot.get("schema_version") != "pnc_rca_delivery_capacity_snapshot_v1":
        raise CapacitySampleEvidenceError("rca_capacity_delivery_snapshot_invalid")
    job = snapshot.get("job")
    effects = snapshot.get("effects")
    subscriptions = snapshot.get("required_subscriptions")
    if (
        not isinstance(job, Mapping)
        or not isinstance(effects, list)
        or not isinstance(subscriptions, list)
    ):
        raise CapacitySampleEvidenceError("rca_capacity_delivery_snapshot_invalid")
    if job.get("outcome") != "success" or job.get("status") != "delivered":
        raise CapacitySampleEvidenceError("rca_capacity_delivery_not_successful")
    if len(effects) < 1 or len(effects) > MAX_REQUIRED_EFFECTS:
        raise CapacitySampleEvidenceError("rca_capacity_delivery_effects_invalid")
    if len(subscriptions) > MAX_REQUIRED_EFFECTS:
        raise CapacitySampleEvidenceError("rca_capacity_delivery_subscriptions_invalid")
    if any(
        not isinstance(item, Mapping)
        or item.get("required") is not True
        or item.get("status") != "succeeded"
        for item in effects
    ):
        raise CapacitySampleEvidenceError("rca_capacity_delivery_effects_invalid")
    if any(
        not isinstance(item, Mapping)
        or item.get("required") is not True
        or item.get("status") != "materialized"
        for item in subscriptions
    ):
        raise CapacitySampleEvidenceError("rca_capacity_delivery_subscriptions_invalid")
    required: list[dict[str, str]] = []
    kinds: set[str] = set()
    keys: set[str] = set()
    effects_by_key: dict[str, Mapping[str, Any]] = {}
    for effect in effects:
        effect_key = _identity(
            effect.get("effect_key"), "rca_capacity_delivery_effects_invalid"
        )
        if effect_key in keys:
            raise CapacitySampleEvidenceError("rca_capacity_delivery_effects_invalid")
        keys.add(effect_key)
        effects_by_key[effect_key] = effect
        kind = str(effect.get("effect_kind") or "")
        target = str(effect.get("target_key") or "")
        if not kind or not target:
            raise CapacitySampleEvidenceError("rca_capacity_delivery_effects_invalid")
        completed_at = _time(
            effect.get("completed_at"), "rca_capacity_delivery_effect_time_invalid"
        )
        if completed_at < vm_created_at:
            raise CapacitySampleEvidenceError(
                "rca_capacity_sample_history_fence_failed"
            )
        remote_receipt = effect.get("remote_receipt")
        remote_id = str(effect.get("remote_id") or "")
        if (
            not isinstance(remote_receipt, Mapping)
            or not remote_receipt
            or not remote_id
            or remote_receipt.get("remote_id") != remote_id
        ):
            raise CapacitySampleEvidenceError(
                "rca_capacity_delivery_remote_receipt_invalid"
            )
        remote_sha = _sha(canonical_bytes(dict(remote_receipt)))
        required.append({
            "effect_key": effect_key,
            "effect_kind": kind,
            "target_key": target,
            "remote_id": remote_id,
            "completed_at": str(effect["completed_at"]),
            "remote_receipt_sha256": remote_sha,
        })
        kinds.add(kind)
    subscription_keys: set[str] = set()
    for subscription in subscriptions:
        subscription_key = _identity(
            subscription.get("subscription_key"),
            "rca_capacity_delivery_subscriptions_invalid",
        )
        effect_key = _identity(
            subscription.get("effect_key"),
            "rca_capacity_delivery_subscriptions_invalid",
        )
        referenced_effect = effects_by_key.get(effect_key)
        if (
            subscription_key in subscription_keys
            or referenced_effect is None
            or subscription.get("delivery_id") != job.get("delivery_id")
            or subscription.get("effect_kind")
            != referenced_effect.get("effect_kind")
            or subscription.get("target_key") != referenced_effect.get("target_key")
        ):
            raise CapacitySampleEvidenceError(
                "rca_capacity_delivery_subscriptions_invalid"
            )
        subscription_keys.add(subscription_key)
        materialized_at = _time(
            subscription.get("materialized_at"),
            "rca_capacity_delivery_subscriptions_invalid",
        )
        completed_at = _time(
            referenced_effect.get("completed_at"),
            "rca_capacity_delivery_effect_time_invalid",
        )
        if materialized_at > completed_at:
            raise CapacitySampleEvidenceError(
                "rca_capacity_delivery_subscriptions_invalid"
            )
    source_kind = str(snapshot.get("source_kind") or "")
    if "feishu_issue_comment" not in kinds:
        raise CapacitySampleEvidenceError("rca_capacity_delivery_effects_invalid")
    if source_kind == "feishu_group_manual" and "feishu_thread_reply" not in kinds:
        raise CapacitySampleEvidenceError("rca_capacity_manual_thread_effect_missing")
    if source_kind not in {"feishu_group_manual", "kafka_workflow_event"}:
        raise CapacitySampleEvidenceError("rca_capacity_delivery_source_invalid")
    required.sort(key=lambda item: (item["effect_kind"], item["effect_key"]))
    return (
        required,
        _identity(job.get("delivery_id"), "rca_capacity_delivery_snapshot_invalid"),
        str(job["outcome"]),
        str(job["status"]),
        source_kind,
    )


def issue_host_success_receipt(
    *,
    snapshot: Mapping[str, Any],
    delivery_snapshot_sha256: str,
    admission_receipt: Mapping[str, Any],
    producer_activation: Mapping[str, Any],
    producer_activation_receipt_sha256: str,
    vm_terminal: VerifiedVmTerminalReceipt,
    created_at: datetime,
    hmac_key: bytes,
    receipt_id: str | None = None,
) -> dict[str, Any]:
    vm_receipt = vm_terminal.receipt
    release = vm_receipt["release"]
    admission = vm_receipt["admission"]
    if (
        _sha(canonical_bytes(dict(admission_receipt)))
        != admission["receipt_raw_sha256"]
        or admission_receipt.get("receipt_fingerprint")
        != admission["receipt_fingerprint"]
    ):
        raise CapacitySampleEvidenceError(
            "rca_capacity_host_receipt_admission_binding_invalid"
        )
    effects, delivery_id, outcome, status, source_kind = _delivery_success(
        snapshot,
        vm_created_at=_time(
            vm_receipt["created_at"], "rca_capacity_vm_receipt_time_invalid"
        ),
    )
    _hex(delivery_snapshot_sha256, "rca_capacity_delivery_snapshot_invalid")
    _hex(producer_activation_receipt_sha256, "rca_capacity_producer_receipt_tampered")
    validate_producer_activation_receipt(
        producer_activation,
        hmac_key=hmac_key,
        expected_release_id=release["release_id"],
        expected_bootstrap_epoch_id=release["bootstrap_epoch_id"],
        expected_release_bom_sha256=release["release_bom_sha256"],
        expected_active_release_binding_sha256=release["active_release_binding_sha256"],
    )
    body = {
        "schema_version": HOST_SUCCESS_RECEIPT_SCHEMA_VERSION,
        "receipt_id": receipt_id or f"host-success-{secrets.token_hex(16)}",
        "created_at": _iso(created_at),
        "release_id": release["release_id"],
        "bootstrap_epoch_id": release["bootstrap_epoch_id"],
        "release_bom_sha256": release["release_bom_sha256"],
        "active_release_binding_sha256": release["active_release_binding_sha256"],
        "task_id": vm_receipt["task_id"],
        "attempt_id": vm_receipt["attempt_id"],
        "delivery_id": delivery_id,
        "source_kind": source_kind,
        "admission_receipt_sha256": admission["receipt_raw_sha256"],
        "admission_receipt_fingerprint": admission["receipt_fingerprint"],
        "producer_activation_receipt_sha256": producer_activation_receipt_sha256,
        "producer_activation_receipt_fingerprint": producer_activation[
            "receipt_fingerprint"
        ],
        "vm_terminal_receipt_sha256": vm_terminal.raw_sha256,
        "vm_terminal_receipt_fingerprint": vm_terminal.fingerprint,
        "delivery_snapshot_sha256": delivery_snapshot_sha256,
        "job_outcome": outcome,
        "job_status": status,
        "required_effects": effects,
    }
    return validate_host_success_receipt(
        _signed(
            body,
            fingerprint_field="receipt_fingerprint",
            hmac_field="hmac_sha256",
            key=hmac_key,
            domain=HOST_SUCCESS_RECEIPT_HMAC_DOMAIN,
        ),
        hmac_key=hmac_key,
    )


def validate_host_success_receipt(value: Any, *, hmac_key: bytes) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != HOST_SUCCESS_RECEIPT_FIELDS:
        raise CapacitySampleEvidenceError("rca_capacity_host_receipt_schema_invalid")
    if value.get("schema_version") != HOST_SUCCESS_RECEIPT_SCHEMA_VERSION:
        raise CapacitySampleEvidenceError("rca_capacity_host_receipt_schema_invalid")
    for field in (
        "receipt_id",
        "release_id",
        "bootstrap_epoch_id",
        "task_id",
        "attempt_id",
        "delivery_id",
    ):
        _identity(value.get(field), "rca_capacity_host_receipt_identity_invalid")
    for field in (
        "release_bom_sha256",
        "active_release_binding_sha256",
        "admission_receipt_sha256",
        "admission_receipt_fingerprint",
        "producer_activation_receipt_sha256",
        "producer_activation_receipt_fingerprint",
        "vm_terminal_receipt_sha256",
        "vm_terminal_receipt_fingerprint",
        "delivery_snapshot_sha256",
    ):
        _hex(value.get(field), "rca_capacity_host_receipt_binding_invalid")
    created_at = _time(
        value.get("created_at"), "rca_capacity_host_receipt_time_invalid"
    )
    if value.get("job_outcome") != "success" or value.get("job_status") != "delivered":
        raise CapacitySampleEvidenceError("rca_capacity_host_receipt_outcome_invalid")
    effects = value.get("required_effects")
    if (
        not isinstance(effects, list)
        or not effects
        or len(effects) > MAX_REQUIRED_EFFECTS
    ):
        raise CapacitySampleEvidenceError("rca_capacity_host_receipt_effects_invalid")
    previous: tuple[str, str] | None = None
    kinds: set[str] = set()
    for effect in effects:
        if (
            not isinstance(effect, Mapping)
            or set(effect) != HOST_REQUIRED_EFFECT_FIELDS
        ):
            raise CapacitySampleEvidenceError(
                "rca_capacity_host_receipt_effects_invalid"
            )
        _identity(effect.get("effect_key"), "rca_capacity_host_receipt_effects_invalid")
        if (
            not str(effect.get("effect_kind") or "")
            or not str(effect.get("target_key") or "")
            or not str(effect.get("remote_id") or "")
        ):
            raise CapacitySampleEvidenceError(
                "rca_capacity_host_receipt_effects_invalid"
            )
        completed_at = _time(
            effect.get("completed_at"),
            "rca_capacity_host_receipt_effects_invalid",
        )
        if completed_at > created_at:
            raise CapacitySampleEvidenceError("rca_capacity_host_receipt_time_invalid")
        _hex(
            effect.get("remote_receipt_sha256"),
            "rca_capacity_host_receipt_effects_invalid",
        )
        current = (str(effect["effect_kind"]), str(effect["effect_key"]))
        kinds.add(str(effect["effect_kind"]))
        if previous is not None and current <= previous:
            raise CapacitySampleEvidenceError(
                "rca_capacity_host_receipt_effects_invalid"
            )
        previous = current
    source_kind = value.get("source_kind")
    if (
        source_kind not in {"feishu_group_manual", "kafka_workflow_event"}
        or "feishu_issue_comment" not in kinds
        or (source_kind == "feishu_group_manual" and "feishu_thread_reply" not in kinds)
    ):
        raise CapacitySampleEvidenceError("rca_capacity_host_receipt_effects_invalid")
    _validate_signed(
        value,
        fingerprint_field="receipt_fingerprint",
        hmac_field="hmac_sha256",
        key=hmac_key,
        domain=HOST_SUCCESS_RECEIPT_HMAC_DOMAIN,
        code="rca_capacity_host_receipt_tampered",
    )
    return dict(value)


def build_capacity_sample(
    *,
    snapshot: Mapping[str, Any],
    delivery_snapshot_sha256: str,
    task_meta: Mapping[str, Any],
    vm_terminal_raw: bytes,
    producer_activation: Mapping[str, Any],
    producer_activation_receipt_sha256: str,
    admission_hmac_key: bytes,
    terminal_hmac_key: bytes | None = None,
    observed_at: datetime,
    sample_id: str | None = None,
) -> CapacitySampleBuild:
    admission_receipt = task_meta.get("rca_prod_admission_receipt")
    if not isinstance(admission_receipt, Mapping):
        raise CapacitySampleEvidenceError("rca_capacity_admission_receipt_invalid")
    vm = validate_vm_terminal_receipt(
        vm_terminal_raw,
        task_meta=task_meta,
        admission_receipt=admission_receipt,
        producer_activation=producer_activation,
        admission_hmac_key=admission_hmac_key,
        terminal_hmac_key=terminal_hmac_key,
    )
    if snapshot.get("task_id") != vm.receipt["task_id"]:
        raise CapacitySampleEvidenceError(
            "rca_capacity_delivery_snapshot_identity_invalid"
        )
    deterministic_identity = _sha(
        canonical_bytes({
            "task_id": vm.receipt["task_id"],
            "attempt_id": vm.receipt["attempt_id"],
            "delivery_snapshot_sha256": delivery_snapshot_sha256,
            "vm_terminal_receipt_sha256": vm.raw_sha256,
            "producer_activation_receipt_sha256": (producer_activation_receipt_sha256),
        })
    )
    host = issue_host_success_receipt(
        snapshot=snapshot,
        delivery_snapshot_sha256=delivery_snapshot_sha256,
        admission_receipt=admission_receipt,
        producer_activation=producer_activation,
        producer_activation_receipt_sha256=producer_activation_receipt_sha256,
        vm_terminal=vm,
        created_at=observed_at,
        hmac_key=admission_hmac_key,
        receipt_id=f"host-success-{deterministic_identity[:32]}",
    )
    host_raw = canonical_bytes(host)
    receipt = vm.receipt
    measurement = receipt["measurement"]
    attestation = receipt["remote_read_attestation"]
    release = receipt["release"]
    sample = transition.issue_capacity_sample(
        sample_id=sample_id or f"sample-{deterministic_identity[:32]}",
        release_id=release["release_id"],
        bootstrap_epoch_id=release["bootstrap_epoch_id"],
        release_bom_sha256=release["release_bom_sha256"],
        active_release_binding_sha256=release["active_release_binding_sha256"],
        task_id=receipt["task_id"],
        attempt_id=receipt["attempt_id"],
        admission_receipt_sha256=receipt["admission"]["receipt_raw_sha256"],
        admission_receipt_fingerprint=receipt["admission"]["receipt_fingerprint"],
        task_manifest_sha256=attestation["manifest_sha256"],
        producer_activation_receipt_sha256=producer_activation_receipt_sha256,
        producer_activation_receipt_fingerprint=producer_activation[
            "receipt_fingerprint"
        ],
        vm_terminal_receipt_sha256=vm.raw_sha256,
        vm_terminal_receipt_fingerprint=vm.fingerprint,
        host_success_receipt_sha256=_sha(host_raw),
        host_success_receipt_fingerprint=host["receipt_fingerprint"],
        terminal_status="succeeded",
        root_peak_bytes=measurement["root"]["peak_free_drop_bytes"],
        delivery_peak_bytes=measurement["delivery"]["peak_free_drop_bytes"],
        delivery_used_bytes=measurement["delivery_task_allocated_bytes"],
        input_materialized_bytes=0,
        observed_at=observed_at,
        hmac_key=admission_hmac_key,
    )
    return CapacitySampleBuild(
        host_success_receipt=host,
        host_success_receipt_sha256=_sha(host_raw),
        sample=sample,
    )
