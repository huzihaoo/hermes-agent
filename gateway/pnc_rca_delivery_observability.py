"""Fail-closed, append-only runtime observations for RCA deliveries.

This module deliberately has no dependency on the delivery store or provider
clients.  It is a small boundary contract: callers must supply measured data,
and a malformed or contradictory row is rejected before it reaches the JSONL
receipt.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import re
import stat
from typing import Any, Iterator, Mapping


OBSERVATION_SCHEMA_VERSION = "pnc_rca_delivery_observation_v2"
OBSERVATION_LEVELS = frozenset({
    "L0_abstain",
    "L1_observation",
    "L2_attribution",
})
OBSERVATION_REQUIRED_FIELDS = frozenset({
    "observation_id",
    "work_item_id",
    "case_key",
    "delivered_at",
    "level",
    "has_attribution",
    "viz_published",
    "viz_bytes",
    "evidence_channel_msg_count",
    "evidence_refs_nonempty",
    "evaluator_hit_count",
    "pipeline_elapsed_seconds",
    "outcome_content_sha256",
    "remote_receipt_id",
    "release_id",
    "inventory_pin",
})
OBSERVATION_OPTIONAL_FIELDS = frozenset({
    "evidence_channel_msg_count_not_measured_reason",
    "evidence_refs_nonempty_not_measured_reason",
})
OBSERVATION_ALLOWED_FIELDS = frozenset({"schema_version"}).union(
    OBSERVATION_REQUIRED_FIELDS,
    OBSERVATION_OPTIONAL_FIELDS,
)
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


ObservationReceiptIdentity = tuple[int, int, int, int, int, int, int]


@dataclass(frozen=True)
class DeliveryObservationReceiptSnapshot:
    """A fully verified receipt plus the cheap identity used for cache hits."""

    identity: ObservationReceiptIdentity
    payload_sha256_by_id: dict[str, str]
    receipt_sha256: str


@dataclass(frozen=True)
class DeliveryObservationAppendResult:
    """The canonical row and the exact receipt snapshot that proved its append."""

    observation: dict[str, Any]
    receipt: DeliveryObservationReceiptSnapshot


@dataclass(frozen=True)
class _DeliveryObservationReceiptScan:
    snapshot: DeliveryObservationReceiptSnapshot
    complete_size: int
    torn_tail: bytes | None = None


class DeliveryObservationError(ValueError):
    """A row failed the runtime observation contract."""

    def __init__(self, code: str, detail: str = ""):
        self.code = str(code)
        self.detail = str(detail)
        super().__init__(self.code if not detail else f"{self.code}: {detail}")


def default_observation_path(hermes_home: str | Path | None = None) -> Path:
    home = Path(hermes_home or os.environ.get("HERMES_HOME", "~/.hermes"))
    return (
        home.expanduser()
        / "runtime"
        / "pnc_agent"
        / "feishu_issue_kafka_rca"
        / "delivery_observations.v2.jsonl"
    )


def _required_text(value: Any, field: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise DeliveryObservationError("observation_required_field_missing", field)
    return text


def _aware_iso(value: Any) -> str:
    text = _required_text(value, "delivered_at")
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise DeliveryObservationError("observation_timestamp_invalid", text) from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise DeliveryObservationError("observation_timestamp_not_utc", text)
    return parsed.astimezone(timezone.utc).isoformat()


def _nonnegative_number(value: Any, field: str) -> int | float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise DeliveryObservationError("observation_number_invalid", field)
    result = float(value)
    if not math.isfinite(result) or result < 0:
        raise DeliveryObservationError("observation_number_invalid", field)
    return int(value) if isinstance(value, int) else result


def _sha256_text(value: Any, field: str) -> str:
    digest = _required_text(value, field)
    if _SHA256_RE.fullmatch(digest) is None:
        raise DeliveryObservationError("observation_sha256_invalid", field)
    return digest


def delivery_observation_id(value: Mapping[str, Any]) -> str:
    """Return the content-bound identity for an otherwise complete row."""
    if not isinstance(value, Mapping):
        raise DeliveryObservationError("observation_not_an_object")
    identity_payload = dict(value)
    identity_payload.pop("observation_id", None)
    identity_payload.setdefault("schema_version", OBSERVATION_SCHEMA_VERSION)
    encoded = json.dumps(
        identity_payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def validate_delivery_observation(value: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise DeliveryObservationError("observation_not_an_object")
    missing = OBSERVATION_REQUIRED_FIELDS.difference(value)
    if missing:
        raise DeliveryObservationError(
            "observation_required_field_missing", ",".join(sorted(missing))
        )
    unexpected = set(value).difference(OBSERVATION_ALLOWED_FIELDS)
    if unexpected:
        raise DeliveryObservationError(
            "observation_unexpected_field", ",".join(sorted(map(str, unexpected)))
        )
    if value.get("schema_version") != OBSERVATION_SCHEMA_VERSION:
        raise DeliveryObservationError("observation_schema_version_invalid")
    level = value.get("level")
    if level not in OBSERVATION_LEVELS:
        raise DeliveryObservationError("observation_level_invalid", str(level))
    for field in ("has_attribution", "viz_published"):
        if not isinstance(value.get(field), bool):
            raise DeliveryObservationError("observation_boolean_invalid", field)
    viz_bytes = _nonnegative_number(value.get("viz_bytes"), "viz_bytes")
    if not isinstance(viz_bytes, int):
        raise DeliveryObservationError("observation_number_invalid", "viz_bytes")
    if value.get("viz_published") is False and viz_bytes != 0:
        raise DeliveryObservationError("observation_viz_bytes_without_publication")
    count = value.get("evidence_channel_msg_count")
    if count is None:
        _required_text(
            value.get("evidence_channel_msg_count_not_measured_reason"),
            "evidence_channel_msg_count_not_measured_reason",
        )
    elif isinstance(count, bool) or not isinstance(count, int) or count < 0:
        raise DeliveryObservationError("observation_number_invalid", "evidence_channel_msg_count")
    refs = value.get("evidence_refs_nonempty")
    if refs is None:
        _required_text(
            value.get("evidence_refs_nonempty_not_measured_reason"),
            "evidence_refs_nonempty_not_measured_reason",
        )
    elif not isinstance(refs, bool):
        raise DeliveryObservationError("observation_boolean_invalid", "evidence_refs_nonempty")
    evaluator_hit_count = value.get("evaluator_hit_count")
    if (
        isinstance(evaluator_hit_count, bool)
        or not isinstance(evaluator_hit_count, int)
        or evaluator_hit_count < 0
    ):
        raise DeliveryObservationError("observation_number_invalid", "evaluator_hit_count")
    has_attribution = value.get("has_attribution") is True
    if has_attribution:
        if refs is not True:
            raise DeliveryObservationError("observation_attribution_without_evidence_refs")
        if evaluator_hit_count <= 0:
            raise DeliveryObservationError(
                "observation_attribution_without_evaluator_hit"
            )
    _nonnegative_number(value.get("pipeline_elapsed_seconds"), "pipeline_elapsed_seconds")
    for field in ("work_item_id", "case_key", "remote_receipt_id", "release_id"):
        _required_text(value.get(field), field)
    delivered_at = _aware_iso(value.get("delivered_at"))
    for field in ("observation_id", "inventory_pin", "outcome_content_sha256"):
        _sha256_text(value.get(field), field)
    normalized = dict(value)
    normalized["schema_version"] = OBSERVATION_SCHEMA_VERSION
    normalized["delivered_at"] = delivered_at
    normalized["viz_bytes"] = viz_bytes
    if normalized["observation_id"] != delivery_observation_id(normalized):
        raise DeliveryObservationError("observation_identity_mismatch")
    return normalized


def build_delivery_observation(fields: Mapping[str, Any]) -> dict[str, Any]:
    """Validate and return a detached canonical observation row."""
    if not isinstance(fields, Mapping):
        raise DeliveryObservationError("observation_not_an_object")
    value = dict(fields)
    value.setdefault("schema_version", OBSERVATION_SCHEMA_VERSION)
    return validate_delivery_observation(value)


def _canonical_observation_bytes(fields: Mapping[str, Any]) -> bytes:
    observation = build_delivery_observation(fields)
    return json.dumps(
        observation,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def delivery_observation_payload_sha256(fields: Mapping[str, Any]) -> str:
    """Return the exact payload hash persisted by the outbox and JSONL."""
    return hashlib.sha256(_canonical_observation_bytes(fields)).hexdigest()


def _receipt_identity(info: os.stat_result) -> ObservationReceiptIdentity:
    return (
        int(info.st_dev),
        int(info.st_ino),
        int(info.st_mode),
        int(info.st_nlink),
        int(info.st_size),
        int(info.st_mtime_ns),
        int(info.st_ctime_ns),
    )


def _validate_receipt_stat(info: os.stat_result) -> None:
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
        raise DeliveryObservationError("observation_path_not_regular")
    if info.st_mode & 0o022:
        raise DeliveryObservationError("observation_path_permissions_unsafe")


def delivery_observation_receipt_identity(
    path: str | Path,
) -> ObservationReceiptIdentity | None:
    """Return a cheap tamper-sensitive identity for an already verified file."""
    source = Path(path).expanduser()
    try:
        info = os.lstat(source)
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise DeliveryObservationError(
            "observation_read_open_failed", type(exc).__name__
        ) from exc
    _validate_receipt_stat(info)
    return _receipt_identity(info)


@contextmanager
def delivery_observation_file_lock(path: str | Path) -> Iterator[None]:
    """Serialize cooperative flushers through a secure sibling lock file."""
    destination = Path(path).expanduser()
    destination.parent.mkdir(parents=True, exist_ok=True)
    lock_path = destination.with_name(f"{destination.name}.lock")
    flags = os.O_RDWR | os.O_CREAT
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(lock_path, flags, 0o600)
    except OSError as exc:
        raise DeliveryObservationError(
            "observation_lock_open_failed", type(exc).__name__
        ) from exc
    locked_acquired = False
    try:
        try:
            initial = os.fstat(descriptor)
            _validate_receipt_stat(initial)
            os.fchmod(descriptor, 0o600)
            opened = os.fstat(descriptor)
            current = os.lstat(lock_path)
            if (current.st_dev, current.st_ino) != (opened.st_dev, opened.st_ino):
                raise DeliveryObservationError("observation_lock_identity_changed")
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX)
                locked_acquired = True
            except OSError as exc:
                raise DeliveryObservationError(
                    "observation_lock_failed", type(exc).__name__
                ) from exc
            locked = os.fstat(descriptor)
            current = os.lstat(lock_path)
            if (
                _receipt_identity(locked) != _receipt_identity(opened)
                or (current.st_dev, current.st_ino) != (locked.st_dev, locked.st_ino)
            ):
                raise DeliveryObservationError("observation_lock_identity_changed")
        except OSError as exc:
            raise DeliveryObservationError(
                "observation_lock_validation_failed", type(exc).__name__
            ) from exc
        yield
    finally:
        try:
            if locked_acquired:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)


def ensure_delivery_observation_path(path: str | Path) -> Path:
    """Create or validate the receipt before an external write is admitted."""
    destination = Path(path).expanduser()
    destination.parent.mkdir(parents=True, exist_ok=True)
    try:
        _validate_receipt_stat(os.lstat(destination))
        return destination
    except FileNotFoundError:
        pass
    flags = os.O_WRONLY | os.O_APPEND | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(destination, flags, 0o600)
    except FileExistsError:
        _validate_receipt_stat(os.lstat(destination))
        return destination
    except OSError as exc:
        raise DeliveryObservationError(
            "observation_append_open_failed", type(exc).__name__
        ) from exc
    try:
        os.fchmod(descriptor, 0o600)
        os.fsync(descriptor)
    except OSError as exc:
        raise DeliveryObservationError(
            "observation_append_failed", type(exc).__name__
        ) from exc
    finally:
        os.close(descriptor)
    return destination


def _bound_receipt_stat(
    descriptor: int,
    source: Path,
) -> os.stat_result:
    opened = os.fstat(descriptor)
    _validate_receipt_stat(opened)
    try:
        current = os.lstat(source)
    except OSError as exc:
        raise DeliveryObservationError(
            "observation_receipt_identity_changed", type(exc).__name__
        ) from exc
    _validate_receipt_stat(current)
    if _receipt_identity(current) != _receipt_identity(opened):
        raise DeliveryObservationError("observation_receipt_identity_changed")
    return opened


def _scan_delivery_observation_receipt(
    descriptor: int,
    source: Path,
    *,
    recoverable_frame: bytes | None = None,
) -> _DeliveryObservationReceiptScan:
    """Read and validate the complete descriptor while it remains path-bound."""
    opened = _bound_receipt_stat(descriptor, source)
    receipt_digest = hashlib.sha256()
    identities: dict[str, str] = {}
    complete_size = 0
    torn_tail: bytes | None = None
    try:
        os.lseek(descriptor, 0, os.SEEK_SET)
        with os.fdopen(os.dup(descriptor), "rb") as stream:
            for line_number, raw_line in enumerate(stream, start=1):
                if raw_line == b"\n":
                    raise DeliveryObservationError(
                        "observation_receipt_line_invalid", str(line_number)
                    )
                if not raw_line.endswith(b"\n"):
                    if (
                        recoverable_frame is None
                        or not raw_line
                        or len(raw_line) >= len(recoverable_frame)
                        or not recoverable_frame.startswith(raw_line)
                    ):
                        raise DeliveryObservationError(
                            "observation_receipt_line_invalid", str(line_number)
                        )
                    torn_tail = raw_line
                    break
                try:
                    payload = json.loads(raw_line[:-1].decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                    raise DeliveryObservationError(
                        "observation_receipt_line_invalid", str(line_number)
                    ) from exc
                observation = validate_delivery_observation(payload)
                canonical = _canonical_observation_bytes(observation)
                if raw_line[:-1] != canonical:
                    raise DeliveryObservationError(
                        "observation_receipt_line_noncanonical", str(line_number)
                    )
                observation_id = observation["observation_id"]
                if observation_id in identities:
                    raise DeliveryObservationError(
                        "observation_receipt_duplicate_id", observation_id
                    )
                identities[observation_id] = hashlib.sha256(canonical).hexdigest()
                receipt_digest.update(raw_line)
                complete_size += len(raw_line)
        after = _bound_receipt_stat(descriptor, source)
        if _receipt_identity(after) != _receipt_identity(opened):
            raise DeliveryObservationError("observation_receipt_identity_changed")
    except OSError as exc:
        raise DeliveryObservationError(
            "observation_read_failed", type(exc).__name__
        ) from exc
    return _DeliveryObservationReceiptScan(
        snapshot=DeliveryObservationReceiptSnapshot(
            identity=_receipt_identity(after),
            payload_sha256_by_id=identities,
            receipt_sha256=receipt_digest.hexdigest(),
        ),
        complete_size=complete_size,
        torn_tail=torn_tail,
    )


def append_delivery_observation_verified(
    path: str | Path,
    fields: Mapping[str, Any],
) -> DeliveryObservationAppendResult:
    """Append one row and return its path-bound, fully reread receipt snapshot."""
    observation = build_delivery_observation(fields)
    destination = ensure_delivery_observation_path(path)
    payload = _canonical_observation_bytes(observation)
    framed_payload = payload + b"\n"
    expected_payload_sha256 = hashlib.sha256(payload).hexdigest()
    observation_id = observation["observation_id"]
    flags = os.O_RDWR | os.O_APPEND
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(destination, flags)
    except OSError as exc:
        raise DeliveryObservationError(
            "observation_append_open_failed", type(exc).__name__
        ) from exc
    try:
        _bound_receipt_stat(descriptor, destination)
        os.fchmod(descriptor, 0o600)
        before = _scan_delivery_observation_receipt(
            descriptor,
            destination,
            recoverable_frame=framed_payload,
        )
        if before.torn_tail is not None:
            os.ftruncate(descriptor, before.complete_size)
            os.fsync(descriptor)
            before = _scan_delivery_observation_receipt(descriptor, destination)
        if observation_id in before.snapshot.payload_sha256_by_id:
            raise DeliveryObservationError(
                "observation_receipt_duplicate_id", observation_id
            )

        written = os.write(descriptor, framed_payload)
        if written != len(framed_payload):
            raise DeliveryObservationError(
                "observation_append_short_write", f"{written}/{len(framed_payload)}"
            )
        os.fsync(descriptor)

        after = _scan_delivery_observation_receipt(descriptor, destination)
        expected_hashes = dict(before.snapshot.payload_sha256_by_id)
        expected_hashes[observation_id] = expected_payload_sha256
        if (
            after.snapshot.payload_sha256_by_id != expected_hashes
            or after.snapshot.identity[4]
            != before.snapshot.identity[4] + len(framed_payload)
        ):
            raise DeliveryObservationError("observation_append_verification_failed")
    except OSError as exc:
        raise DeliveryObservationError(
            "observation_append_failed", type(exc).__name__
        ) from exc
    finally:
        os.close(descriptor)
    return DeliveryObservationAppendResult(
        observation=observation,
        receipt=after.snapshot,
    )


def append_delivery_observation(
    path: str | Path,
    fields: Mapping[str, Any],
) -> dict[str, Any]:
    """Compatibility wrapper returning the verified canonical observation row."""
    return append_delivery_observation_verified(path, fields).observation


def read_delivery_observation_receipt(
    path: str | Path,
) -> DeliveryObservationReceiptSnapshot:
    """Validate canonical JSONL and bind every ID to its exact payload hash."""
    source = Path(path).expanduser()
    try:
        path_info = os.lstat(source)
    except FileNotFoundError:
        raise DeliveryObservationError("observation_path_missing")
    _validate_receipt_stat(path_info)
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(source, flags)
    except OSError as exc:
        raise DeliveryObservationError(
            "observation_read_open_failed", type(exc).__name__
        ) from exc
    try:
        opened = os.fstat(descriptor)
        _validate_receipt_stat(opened)
        if (path_info.st_dev, path_info.st_ino) != (opened.st_dev, opened.st_ino):
            raise DeliveryObservationError("observation_receipt_identity_changed")
        return _scan_delivery_observation_receipt(
            descriptor,
            source,
        ).snapshot
    finally:
        os.close(descriptor)


def read_delivery_observation_ids(path: str | Path) -> set[str]:
    """Validate the append-only receipt and return its unique identities."""
    source = Path(path).expanduser()
    if delivery_observation_receipt_identity(source) is None:
        return set()
    return set(read_delivery_observation_receipt(source).payload_sha256_by_id)
