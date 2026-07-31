"""Fail-closed, append-only runtime observations for RCA deliveries.

This module deliberately has no dependency on the delivery store or provider
clients.  It is a small boundary contract: callers must supply measured data,
and a malformed or contradictory row is rejected before it reaches the JSONL
receipt.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import math
import os
from pathlib import Path
import re
from typing import Any, Mapping


OBSERVATION_SCHEMA_VERSION = "pnc_rca_delivery_observation_v1"
OBSERVATION_LEVELS = frozenset({
    "L0_abstain",
    "L1_observation",
    "L2_attribution",
})
OBSERVATION_REQUIRED_FIELDS = frozenset({
    "work_item_id",
    "case_key",
    "delivered_at",
    "level",
    "has_attribution",
    "viz_published",
    "viz_bytes",
    "evidence_channel_msg_count",
    "evidence_refs_nonempty",
    "pipeline_elapsed_seconds",
    "outcome_content_sha256",
    "remote_receipt_id",
    "release_id",
    "inventory_pin",
})
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class DeliveryObservationError(ValueError):
    """A row failed the runtime observation contract."""

    def __init__(self, code: str, detail: str = ""):
        self.code = str(code)
        self.detail = str(detail)
        super().__init__(self.code if not detail else f"{self.code}: {detail}")


def default_observation_path(hermes_home: str | Path | None = None) -> Path:
    home = Path(hermes_home or os.environ.get("HERMES_HOME", "~/.hermes"))
    return home.expanduser() / "runtime" / "pnc_agent" / "feishu_issue_kafka_rca" / "delivery_observations.jsonl"


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


def validate_delivery_observation(value: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise DeliveryObservationError("observation_not_an_object")
    missing = OBSERVATION_REQUIRED_FIELDS.difference(value)
    if missing:
        raise DeliveryObservationError(
            "observation_required_field_missing", ",".join(sorted(missing))
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
    if value.get("has_attribution") is True and count == 0:
        raise DeliveryObservationError("observation_attribution_without_evidence")
    _nonnegative_number(value.get("pipeline_elapsed_seconds"), "pipeline_elapsed_seconds")
    for field in ("work_item_id", "case_key", "remote_receipt_id", "release_id", "inventory_pin"):
        _required_text(value.get(field), field)
    delivered_at = _aware_iso(value.get("delivered_at"))
    digest = _required_text(value.get("outcome_content_sha256"), "outcome_content_sha256")
    if _SHA256_RE.fullmatch(digest) is None:
        raise DeliveryObservationError("observation_sha256_invalid", "outcome_content_sha256")
    normalized = dict(value)
    normalized["schema_version"] = OBSERVATION_SCHEMA_VERSION
    normalized["delivered_at"] = delivered_at
    normalized["viz_bytes"] = viz_bytes
    return normalized


def build_delivery_observation(fields: Mapping[str, Any]) -> dict[str, Any]:
    """Validate and return a detached canonical observation row."""
    if not isinstance(fields, Mapping):
        raise DeliveryObservationError("observation_not_an_object")
    value = dict(fields)
    value.setdefault("schema_version", OBSERVATION_SCHEMA_VERSION)
    return validate_delivery_observation(value)


def append_delivery_observation(path: str | Path, fields: Mapping[str, Any]) -> dict[str, Any]:
    """Append one validated row without replacing or truncating prior rows."""
    observation = build_delivery_observation(fields)
    destination = Path(path).expanduser()
    destination.parent.mkdir(parents=True, exist_ok=True)
    try:
        mode = os.lstat(destination).st_mode
        if not os.path.isfile(destination) or os.path.islink(destination):
            raise DeliveryObservationError("observation_path_not_regular")
        if mode & 0o022:
            raise DeliveryObservationError("observation_path_permissions_unsafe")
    except FileNotFoundError:
        pass
    payload = json.dumps(observation, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
    flags = os.O_WRONLY | os.O_APPEND | os.O_CREAT
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        fd = os.open(destination, flags, 0o600)
    except OSError as exc:
        raise DeliveryObservationError("observation_append_open_failed", type(exc).__name__) from exc
    try:
        os.fchmod(fd, 0o600)
        encoded = payload.encode("utf-8")
        offset = 0
        while offset < len(encoded):
            written = os.write(fd, encoded[offset:])
            if written <= 0:
                raise OSError("short observation append")
            offset += written
        os.fsync(fd)
    except OSError as exc:
        raise DeliveryObservationError("observation_append_failed", type(exc).__name__) from exc
    finally:
        os.close(fd)
    return observation
