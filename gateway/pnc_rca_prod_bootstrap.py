"""Strict, release-bound bootstrap capacity authorization for RCA production."""

from __future__ import annotations

import hashlib
import json
import os
import re
import secrets
import stat
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping


SCHEMA_VERSION = "context-rca-bootstrap-capacity-authorization/v1"
CAPACITY_MODE = "bootstrap"
RESOURCE_CLASS = "rca_prod"
MAX_EPOCH_DURATION = timedelta(days=8)
MAX_CONCURRENCY = 1
DAILY_STARTED_ATTEMPT_QUOTA = 5
QUOTA_TIMEZONE = "UTC"
ROOT_RESERVE_BYTES = 400 * 1024**3
ROOT_PER_TASK_BYTES = 64 * 1024**3
ROOT_REQUIRED_AVAILABLE_BYTES = 464 * 1024**3
DELIVERY_RESERVE_BYTES = 512 * 1024**3
DELIVERY_PER_TASK_BYTES = 128 * 1024**3
DELIVERY_REQUIRED_AVAILABLE_BYTES = 640 * 1024**3
HEX64_RE = re.compile(r"^[0-9a-f]{64}$")
EPOCH_ID_RE = re.compile(r"^rca-bootstrap-[A-Za-z0-9._-]{1,96}$")
AUTHORITY_EPOCH_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
BOOTSTRAP_AUTHORIZATION_PATH = (
    Path.home() / ".ssh-mini" / "rca-bootstrap-capacity-authorization.json"
)
MAX_AUTHORIZATION_FILE_BYTES = 64 * 1024
ACTIVE_RELEASE_BINDING_NAME = "active-release-binding.json"
ACTIVE_RELEASE_BINDING_SCHEMA_VERSION = "pnc_rca_production_env_stage_receipt_v2"
MAX_ACTIVE_RELEASE_BINDING_BYTES = 256 * 1024
MAX_LIVE_ENV_BYTES = 1024 * 1024
ACTIVE_RELEASE_BINDING_FIELDS = {
    "schema_version",
    "release_id",
    "authority_sha256",
    "authority_epoch_id",
    "complete",
    "live_write_performed",
    "bindings",
    "policy",
    "side_effect_contract",
}

AUTHORIZATION_FIELDS = {
    "schema_version",
    "receipt_id",
    "issued_at",
    "expires_at",
    "resource_class",
    "capacity_mode",
    "bootstrap_epoch_id",
    "started_at",
    "deadline",
    "release_approval",
    "policy",
    "receipt_fingerprint",
}
RELEASE_APPROVAL_FIELDS = {
    "approval_id",
    "release_bom_sha256",
    "approval_evidence_sha256",
    "authorized_by",
    "authorized_role",
}
POLICY_FIELDS = {
    "max_concurrency",
    "daily_started_attempt_quota",
    "quota_timezone",
    "root_reserve_bytes",
    "root_per_task_bytes",
    "root_required_available_bytes",
    "delivery_reserve_bytes",
    "delivery_per_task_bytes",
    "delivery_required_available_bytes",
    "queue_if_blocked",
    "bypass_requested",
    "input_materialization",
}


class RcaBootstrapAuthorizationError(ValueError):
    def __init__(self, code: str):
        self.code = str(code or "rca_bootstrap_authorization_invalid")[:120]
        super().__init__(self.code)


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
        raise RcaBootstrapAuthorizationError(
            "rca_bootstrap_authorization_not_canonical"
        ) from exc


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
        raise RcaBootstrapAuthorizationError("rca_bootstrap_time_invalid")
    return current.astimezone(timezone.utc)


def _hex(value: Any, code: str) -> str:
    normalized = str(value or "").strip().lower()
    if not HEX64_RE.fullmatch(normalized):
        raise RcaBootstrapAuthorizationError(code)
    return normalized


def _policy() -> dict[str, Any]:
    return {
        "max_concurrency": MAX_CONCURRENCY,
        "daily_started_attempt_quota": DAILY_STARTED_ATTEMPT_QUOTA,
        "quota_timezone": QUOTA_TIMEZONE,
        "root_reserve_bytes": ROOT_RESERVE_BYTES,
        "root_per_task_bytes": ROOT_PER_TASK_BYTES,
        "root_required_available_bytes": ROOT_REQUIRED_AVAILABLE_BYTES,
        "delivery_reserve_bytes": DELIVERY_RESERVE_BYTES,
        "delivery_per_task_bytes": DELIVERY_PER_TASK_BYTES,
        "delivery_required_available_bytes": DELIVERY_REQUIRED_AVAILABLE_BYTES,
        "queue_if_blocked": False,
        "bypass_requested": False,
        "input_materialization": "forbidden",
    }


def authorization_fingerprint(receipt: Mapping[str, Any]) -> str:
    return hashlib.sha256(
        canonical_bytes(
            {key: value for key, value in receipt.items() if key != "receipt_fingerprint"}
        )
    ).hexdigest()


def validate_bootstrap_authorization(
    receipt: Any,
    *,
    now: datetime | None = None,
    expected_epoch_id: str | None = None,
    expected_release_bom_sha256: str | None = None,
    expected_release_approval_id: str | None = None,
    expected_approval_evidence_sha256: str | None = None,
    admission_expires_at: datetime | None = None,
    authorization_receipt_sha256: str | None = None,
) -> dict[str, Any]:
    if not isinstance(receipt, Mapping) or set(receipt) != AUTHORIZATION_FIELDS:
        raise RcaBootstrapAuthorizationError("rca_bootstrap_authorization_schema_invalid")
    release = receipt.get("release_approval")
    policy = receipt.get("policy")
    if not isinstance(release, Mapping) or set(release) != RELEASE_APPROVAL_FIELDS:
        raise RcaBootstrapAuthorizationError("rca_bootstrap_release_approval_invalid")
    if not isinstance(policy, Mapping) or set(policy) != POLICY_FIELDS:
        raise RcaBootstrapAuthorizationError("rca_bootstrap_policy_invalid")
    if (
        receipt.get("schema_version") != SCHEMA_VERSION
        or receipt.get("resource_class") != RESOURCE_CLASS
        or receipt.get("capacity_mode") != CAPACITY_MODE
        or dict(policy) != _policy()
    ):
        raise RcaBootstrapAuthorizationError("rca_bootstrap_policy_invalid")
    receipt_id = str(receipt.get("receipt_id") or "").strip()
    epoch_id = str(receipt.get("bootstrap_epoch_id") or "").strip()
    approval_id = str(release.get("approval_id") or "").strip()
    authorized_by = str(release.get("authorized_by") or "").strip()
    if (
        not receipt_id
        or len(receipt_id) > 128
        or not EPOCH_ID_RE.fullmatch(epoch_id)
        or not approval_id
        or len(approval_id) > 128
        or not authorized_by
        or len(authorized_by) > 128
    ):
        raise RcaBootstrapAuthorizationError("rca_bootstrap_identity_invalid")
    if release.get("authorized_role") != "owner":
        raise RcaBootstrapAuthorizationError("rca_bootstrap_owner_required")
    release_bom_sha256 = _hex(
        release.get("release_bom_sha256"), "rca_bootstrap_release_bom_invalid"
    )
    approval_evidence_sha256 = _hex(
        release.get("approval_evidence_sha256"),
        "rca_bootstrap_approval_evidence_invalid",
    )
    if expected_epoch_id is not None and epoch_id != expected_epoch_id:
        raise RcaBootstrapAuthorizationError("rca_bootstrap_epoch_binding_invalid")
    if expected_release_bom_sha256 is not None and (
        release_bom_sha256 != _hex(
            expected_release_bom_sha256, "rca_bootstrap_release_bom_invalid"
        )
    ):
        raise RcaBootstrapAuthorizationError("rca_bootstrap_release_bom_binding_invalid")
    if expected_release_approval_id is not None and (
        approval_id != str(expected_release_approval_id or "").strip()
    ):
        raise RcaBootstrapAuthorizationError(
            "rca_bootstrap_release_approval_binding_invalid"
        )
    if expected_approval_evidence_sha256 is not None and (
        approval_evidence_sha256
        != _hex(
            expected_approval_evidence_sha256,
            "rca_bootstrap_approval_evidence_invalid",
        )
    ):
        raise RcaBootstrapAuthorizationError(
            "rca_bootstrap_approval_evidence_binding_invalid"
        )
    current = _now(now)
    try:
        issued_at = _timestamp(receipt.get("issued_at"))
        started_at = _timestamp(receipt.get("started_at"))
        deadline = _timestamp(receipt.get("deadline"))
        expires_at = _timestamp(receipt.get("expires_at"))
    except (TypeError, ValueError, OverflowError) as exc:
        raise RcaBootstrapAuthorizationError("rca_bootstrap_time_invalid") from exc
    if (
        deadline <= started_at
        or deadline - started_at > MAX_EPOCH_DURATION
        or expires_at != deadline
        or issued_at < started_at
        or issued_at > deadline
        or issued_at > current + timedelta(seconds=5)
        or current < started_at - timedelta(seconds=5)
        or current > deadline
        or (admission_expires_at is not None and deadline < admission_expires_at)
    ):
        raise RcaBootstrapAuthorizationError("rca_bootstrap_expired_or_deadline_invalid")
    expected_fingerprint = authorization_fingerprint(receipt)
    if receipt.get("receipt_fingerprint") != expected_fingerprint:
        raise RcaBootstrapAuthorizationError("rca_bootstrap_fingerprint_invalid")
    raw_sha = (
        _hex(
            authorization_receipt_sha256,
            "rca_bootstrap_authorization_file_sha_invalid",
        )
        if authorization_receipt_sha256 is not None
        else None
    )
    return {
        "authorization_ready": True,
        "status": "valid",
        "reason_codes": [],
        "schema_version": SCHEMA_VERSION,
        "capacity_mode": CAPACITY_MODE,
        "receipt_id": receipt_id,
        "receipt_fingerprint": expected_fingerprint,
        "authorization_receipt_sha256": raw_sha,
        "bootstrap_epoch_id": epoch_id,
        "started_at": receipt["started_at"],
        "deadline": receipt["deadline"],
        "release_approval_id": approval_id,
        "release_bom_sha256": release_bom_sha256,
        "approval_evidence_sha256": approval_evidence_sha256,
        "authorized_by": authorized_by,
        **_policy(),
    }


def _read_authorization_file() -> tuple[bytes, str]:
    path = BOOTSTRAP_AUTHORIZATION_PATH.expanduser().absolute()
    if not hasattr(os, "O_NOFOLLOW"):
        raise RcaBootstrapAuthorizationError(
            "rca_bootstrap_authorization_no_follow_unavailable"
        )
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise RcaBootstrapAuthorizationError(
            "rca_bootstrap_authorization_file_unavailable"
        ) from exc
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or stat.S_IMODE(before.st_mode) != 0o600
            or before.st_uid != os.geteuid()
            or before.st_nlink != 1
            or before.st_size < 2
            or before.st_size > MAX_AUTHORIZATION_FILE_BYTES
        ):
            raise RcaBootstrapAuthorizationError(
                "rca_bootstrap_authorization_file_not_owner_only"
            )
        chunks: list[bytes] = []
        remaining = MAX_AUTHORIZATION_FILE_BYTES + 1
        while remaining > 0:
            chunk = os.read(descriptor, min(64 * 1024, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        raw = b"".join(chunks)
        after = os.fstat(descriptor)
        try:
            lexical = os.lstat(path)
        except OSError as exc:
            raise RcaBootstrapAuthorizationError(
                "rca_bootstrap_authorization_file_unstable"
            ) from exc
        identity = (
            before.st_dev,
            before.st_ino,
            before.st_mode,
            before.st_uid,
            before.st_nlink,
            before.st_size,
            before.st_mtime_ns,
            before.st_ctime_ns,
        )
        if (
            len(raw) > MAX_AUTHORIZATION_FILE_BYTES
            or stat.S_ISLNK(lexical.st_mode)
            or (after.st_dev, after.st_ino) != (lexical.st_dev, lexical.st_ino)
            or identity
            != (
                after.st_dev,
                after.st_ino,
                after.st_mode,
                after.st_uid,
                after.st_nlink,
                after.st_size,
                after.st_mtime_ns,
                after.st_ctime_ns,
            )
        ):
            raise RcaBootstrapAuthorizationError(
                "rca_bootstrap_authorization_file_unstable"
            )
        return raw, hashlib.sha256(raw).hexdigest()
    finally:
        os.close(descriptor)


def load_bootstrap_authorization(
    *,
    now: datetime | None = None,
    expected_epoch_id: str | None = None,
    expected_release_bom_sha256: str | None = None,
    expected_release_approval_id: str | None = None,
    expected_approval_evidence_sha256: str | None = None,
) -> dict[str, Any]:
    """Read and validate the one canonical bootstrap authority without caching."""

    raw, raw_sha256 = _read_authorization_file()

    def unique(items: list[tuple[str, Any]]) -> dict[str, Any]:
        value: dict[str, Any] = {}
        for key, item in items:
            if key in value:
                raise RcaBootstrapAuthorizationError(
                    "rca_bootstrap_authorization_file_duplicate_key"
                )
            value[key] = item
        return value

    try:
        receipt = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=unique,
            parse_constant=lambda _value: (_ for _ in ()).throw(
                RcaBootstrapAuthorizationError(
                    "rca_bootstrap_authorization_file_number_invalid"
                )
            ),
        )
    except RcaBootstrapAuthorizationError:
        raise
    except (UnicodeError, json.JSONDecodeError, RecursionError) as exc:
        raise RcaBootstrapAuthorizationError(
            "rca_bootstrap_authorization_file_json_invalid"
        ) from exc
    return validate_bootstrap_authorization(
        receipt,
        now=now,
        expected_epoch_id=expected_epoch_id,
        expected_release_bom_sha256=expected_release_bom_sha256,
        expected_release_approval_id=expected_release_approval_id,
        expected_approval_evidence_sha256=expected_approval_evidence_sha256,
        authorization_receipt_sha256=raw_sha256,
    )


def _read_bound_file(
    path: Path, *, artifact: str, maximum: int
) -> tuple[bytes, str]:
    selected = path.expanduser().absolute()
    if not hasattr(os, "O_NOFOLLOW"):
        raise RcaBootstrapAuthorizationError(f"{artifact}_no_follow_unavailable")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | os.O_NOFOLLOW
    try:
        descriptor = os.open(selected, flags)
    except OSError as exc:
        raise RcaBootstrapAuthorizationError(f"{artifact}_unavailable") from exc
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or stat.S_IMODE(before.st_mode) != 0o600
            or before.st_uid != os.geteuid()
            or before.st_nlink != 1
            or before.st_size < 1
            or before.st_size > maximum
        ):
            raise RcaBootstrapAuthorizationError(f"{artifact}_not_owner_only")
        chunks: list[bytes] = []
        total = 0
        while total <= maximum:
            chunk = os.read(descriptor, min(64 * 1024, maximum + 1 - total))
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
        after = os.fstat(descriptor)
        try:
            lexical = os.lstat(selected)
        except OSError as exc:
            raise RcaBootstrapAuthorizationError(f"{artifact}_unstable") from exc
        before_identity = (
            before.st_dev,
            before.st_ino,
            before.st_mode,
            before.st_uid,
            before.st_nlink,
            before.st_size,
            before.st_mtime_ns,
            before.st_ctime_ns,
        )
        after_identity = (
            after.st_dev,
            after.st_ino,
            after.st_mode,
            after.st_uid,
            after.st_nlink,
            after.st_size,
            after.st_mtime_ns,
            after.st_ctime_ns,
        )
        if (
            total > maximum
            or stat.S_ISLNK(lexical.st_mode)
            or (after.st_dev, after.st_ino) != (lexical.st_dev, lexical.st_ino)
            or before_identity != after_identity
        ):
            raise RcaBootstrapAuthorizationError(f"{artifact}_unstable")
        raw = b"".join(chunks)
        return raw, hashlib.sha256(raw).hexdigest()
    finally:
        os.close(descriptor)


def _strict_bound_json(raw: bytes, *, artifact: str) -> Mapping[str, Any]:
    def unique(items: list[tuple[str, Any]]) -> dict[str, Any]:
        value: dict[str, Any] = {}
        for key, item in items:
            if key in value:
                raise RcaBootstrapAuthorizationError(f"{artifact}_duplicate_key")
            value[key] = item
        return value

    try:
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=unique,
            parse_constant=lambda _value: (_ for _ in ()).throw(
                RcaBootstrapAuthorizationError(f"{artifact}_number_invalid")
            ),
        )
    except RcaBootstrapAuthorizationError:
        raise
    except (UnicodeError, json.JSONDecodeError, RecursionError) as exc:
        raise RcaBootstrapAuthorizationError(f"{artifact}_json_invalid") from exc
    if not isinstance(value, Mapping):
        raise RcaBootstrapAuthorizationError(f"{artifact}_shape_invalid")
    return value


def load_active_release_binding(
    *,
    path: Path,
    live_env_path: Path,
    expected_release_id: str,
    expected_epoch_id: str,
    expected_authority_sha256: str | None = None,
    expected_authority_epoch_id: str | None = None,
    verify_live_env: bool = True,
) -> dict[str, Any]:
    """Validate the post-BOM receipt installed beside the active RCA store."""

    if not isinstance(verify_live_env, bool):
        raise RcaBootstrapAuthorizationError(
            "rca_active_release_binding_verify_live_env_invalid"
        )

    raw, raw_sha256 = _read_bound_file(
        path,
        artifact="rca_active_release_binding",
        maximum=MAX_ACTIVE_RELEASE_BINDING_BYTES,
    )
    body = _strict_bound_json(raw, artifact="rca_active_release_binding")
    if set(body) != ACTIVE_RELEASE_BINDING_FIELDS:
        raise RcaBootstrapAuthorizationError("rca_active_release_binding_invalid")
    bindings = body.get("bindings")
    policy = body.get("policy")
    side_effect = body.get("side_effect_contract")
    capacity = policy.get("capacity_admission") if isinstance(policy, Mapping) else None
    release_approval = (
        bindings.get("release_approval") if isinstance(bindings, Mapping) else None
    )
    candidate_env = bindings.get("candidate_env") if isinstance(bindings, Mapping) else None
    bootstrap_binding = (
        bindings.get("bootstrap_authorization")
        if isinstance(bindings, Mapping)
        else None
    )
    authority_sha256 = _hex(
        body.get("authority_sha256"),
        "rca_active_release_binding_authority_invalid",
    )
    authority_epoch_id = str(body.get("authority_epoch_id") or "").strip()
    if (
        body.get("schema_version") != ACTIVE_RELEASE_BINDING_SCHEMA_VERSION
        or body.get("release_id") != expected_release_id
        or AUTHORITY_EPOCH_ID_RE.fullmatch(authority_epoch_id) is None
        or (
            expected_authority_sha256 is not None
            and authority_sha256
            != _hex(
                expected_authority_sha256,
                "rca_active_release_binding_authority_invalid",
            )
        )
        or (
            expected_authority_epoch_id is not None
            and authority_epoch_id != expected_authority_epoch_id
        )
        or body.get("complete") is not True
        or body.get("live_write_performed") is not False
        or not isinstance(side_effect, Mapping)
        or side_effect.get("canonical_active_release_binding")
        != str(path.expanduser().absolute())
        or side_effect.get("canonical_live_env")
        != str(live_env_path.expanduser().absolute())
        or not isinstance(capacity, Mapping)
        or not isinstance(release_approval, Mapping)
        or not isinstance(candidate_env, Mapping)
        or not isinstance(bootstrap_binding, Mapping)
        or capacity.get("capacity_mode") != "bootstrap"
        or capacity.get("bootstrap_epoch_id") != expected_epoch_id
        or capacity.get("release_approval_id") != expected_release_id
    ):
        raise RcaBootstrapAuthorizationError("rca_active_release_binding_invalid")
    release_bom_sha256 = _hex(
        capacity.get("release_bom_sha256"),
        "rca_active_release_binding_release_bom_invalid",
    )
    approval_evidence_sha256 = _hex(
        capacity.get("approval_evidence_sha256"),
        "rca_active_release_binding_approval_invalid",
    )
    authorization_sha256 = _hex(
        capacity.get("bootstrap_authorization_sha256"),
        "rca_active_release_binding_authorization_invalid",
    )
    authorization_fingerprint_value = _hex(
        capacity.get("bootstrap_authorization_fingerprint"),
        "rca_active_release_binding_authorization_invalid",
    )
    candidate_env_sha256 = _hex(
        candidate_env.get("sha256"),
        "rca_active_release_binding_candidate_env_invalid",
    )
    if (
        _hex(
            body.get("bindings", {}).get("release_bom_sha256"),
            "rca_active_release_binding_release_bom_invalid",
        )
        != release_bom_sha256
        or _hex(
            release_approval.get("sha256"),
            "rca_active_release_binding_approval_invalid",
        )
        != approval_evidence_sha256
        or _hex(
            bootstrap_binding.get("sha256"),
            "rca_active_release_binding_authorization_invalid",
        )
        != authorization_sha256
        or _hex(
            bootstrap_binding.get("receipt_fingerprint"),
            "rca_active_release_binding_authorization_invalid",
        )
        != authorization_fingerprint_value
    ):
        raise RcaBootstrapAuthorizationError(
            "rca_active_release_binding_cross_binding_invalid"
        )
    if verify_live_env:
        _live_env_raw, live_env_sha256 = _read_bound_file(
            live_env_path,
            artifact="rca_active_release_live_env",
            maximum=MAX_LIVE_ENV_BYTES,
        )
        if live_env_sha256 != candidate_env_sha256:
            raise RcaBootstrapAuthorizationError(
                "rca_active_release_binding_live_env_mismatch"
            )
    return {
        "binding_ready": True,
        "binding_receipt_sha256": raw_sha256,
        "release_id": expected_release_id,
        "authority_sha256": authority_sha256,
        "authority_epoch_id": authority_epoch_id,
        "bootstrap_epoch_id": expected_epoch_id,
        "release_bom_sha256": release_bom_sha256,
        "approval_evidence_sha256": approval_evidence_sha256,
        "authorization_receipt_sha256": authorization_sha256,
        "authorization_fingerprint": authorization_fingerprint_value,
        "candidate_env_sha256": candidate_env_sha256,
    }


def issue_bootstrap_authorization(
    *,
    bootstrap_epoch_id: str,
    started_at: datetime,
    deadline: datetime,
    release_approval_id: str,
    release_bom_sha256: str,
    approval_evidence_sha256: str,
    authorized_by: str,
    authorized_role: str,
    now: datetime | None = None,
    receipt_id: str | None = None,
) -> dict[str, Any]:
    current = _now(now)
    normalized_started = _now(started_at)
    normalized_deadline = _now(deadline)
    if current < normalized_started or current > normalized_deadline:
        raise RcaBootstrapAuthorizationError(
            "rca_bootstrap_issue_outside_epoch"
        )
    receipt: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "receipt_id": str(receipt_id or f"bootstrap-auth-{secrets.token_hex(16)}"),
        "issued_at": current.replace(microsecond=0).isoformat(),
        "expires_at": normalized_deadline.replace(microsecond=0).isoformat(),
        "resource_class": RESOURCE_CLASS,
        "capacity_mode": CAPACITY_MODE,
        "bootstrap_epoch_id": str(bootstrap_epoch_id),
        "started_at": normalized_started.replace(microsecond=0).isoformat(),
        "deadline": normalized_deadline.replace(microsecond=0).isoformat(),
        "release_approval": {
            "approval_id": str(release_approval_id),
            "release_bom_sha256": str(release_bom_sha256).lower(),
            "approval_evidence_sha256": str(approval_evidence_sha256).lower(),
            "authorized_by": str(authorized_by),
            "authorized_role": str(authorized_role),
        },
        "policy": _policy(),
    }
    receipt["receipt_fingerprint"] = authorization_fingerprint(receipt)
    validate_bootstrap_authorization(receipt, now=current)
    return receipt
