"""Deterministic issue-time to front-camera frame resolution for G1Q3 RCA."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import re
from typing import Any, Mapping, Sequence


FRAME_FIELD_NAME = "问题发生frame_id"
FRAME_TIMEZONE_NAME = "Asia/Shanghai"
FRAME_TIMEZONE = timezone(timedelta(hours=8), FRAME_TIMEZONE_NAME)
MANAGEMENT_TIMESTAMP_UNIT = "microseconds_since_unix_epoch"
# Feishu marker times are second-precision, so the corresponding camera frame
# may be up to one second away after timestamp quantization.
DEFAULT_MAX_FRAME_DELTA_US = 1_000_000
DEFAULT_FRONT_CAMERA_TOPIC_PRIORITY = (
    "front_120",
    "camera1",
    "front_190",
    "camera4",
    "front_30",
    "avm_front",
)
_DATE_TIME_RE = re.compile(
    r"^(?P<year>\d{4})(?:-(?P<month_dash>\d{2})-(?P<day_dash>\d{2})|"
    r"(?P<month_compact>\d{2})(?P<day_compact>\d{2})\s*,)\s*"
    r"(?P<hour>\d{2}):(?P<minute>\d{2}):(?P<second>\d{2})"
    r"(?P<fraction>\.\d{1,6})?$"
)
_MIN_TIMESTAMP_US = 946_684_800_000_000  # 2000-01-01T00:00:00Z
_MAX_TIMESTAMP_US = 4_102_444_800_000_000  # 2100-01-01T00:00:00Z


class FrameReferenceError(ValueError):
    """A fail-closed frame reference parsing or resolution error."""

    def __init__(self, code: str, detail: str = ""):
        self.code = str(code or "frame_reference_invalid")[:120]
        self.detail = str(detail or self.code)[:500]
        super().__init__(self.detail)


def parse_frame_reference(value: Any) -> dict[str, Any]:
    """Normalize a numeric frame id or an exact local marker time."""
    text = str(value or "").strip()
    if not text:
        return {}
    if text.isascii() and text.isdigit():
        frame_id = int(text)
        if frame_id <= 0:
            raise FrameReferenceError("frame_id_not_positive")
        return {
            "kind": "frame_id",
            "frame_id": str(frame_id),
            "source_field": FRAME_FIELD_NAME,
        }

    match = _DATE_TIME_RE.fullmatch(text)
    if match is None:
        raise FrameReferenceError("frame_reference_format_invalid")
    parts = match.groupdict()
    fraction = parts.get("fraction") or ""
    try:
        marker_time = datetime(
            int(parts["year"]),
            int(parts["month_dash"] or parts["month_compact"]),
            int(parts["day_dash"] or parts["day_compact"]),
            int(parts["hour"]),
            int(parts["minute"]),
            int(parts["second"]),
            int(fraction[1:].ljust(6, "0")) if fraction else 0,
            tzinfo=FRAME_TIMEZONE,
        )
    except ValueError as exc:
        raise FrameReferenceError("frame_reference_datetime_invalid") from exc
    utc_marker = marker_time.astimezone(timezone.utc)
    epoch_delta = utc_marker - datetime(1970, 1, 1, tzinfo=timezone.utc)
    management_timestamp_us = (
        (epoch_delta.days * 86_400 + epoch_delta.seconds) * 1_000_000
        + epoch_delta.microseconds
    )
    return {
        "kind": "front_camera_timestamp",
        "source_field": FRAME_FIELD_NAME,
        "marker_time": marker_time.isoformat(),
        "timezone": FRAME_TIMEZONE_NAME,
        "management_timestamp": management_timestamp_us,
        "management_timestamp_unit": MANAGEMENT_TIMESTAMP_UNIT,
        "camera_scope": "front_view",
        "selection": "nearest_timestamp",
        "max_delta_us": DEFAULT_MAX_FRAME_DELTA_US,
        "topic_priority": list(DEFAULT_FRONT_CAMERA_TOPIC_PRIORITY),
    }


def normalize_management_timestamp_us(value: Any) -> int:
    """Normalize integer epoch seconds/milliseconds/microseconds/nanoseconds."""
    if isinstance(value, bool):
        raise FrameReferenceError("management_timestamp_invalid")
    text = str(value).strip()
    if not text.isascii() or not text.isdigit():
        raise FrameReferenceError("management_timestamp_invalid")
    raw = int(text)
    if raw >= 100_000_000_000_000_000:
        normalized = raw // 1_000
    elif raw >= 100_000_000_000_000:
        normalized = raw
    elif raw >= 100_000_000_000:
        normalized = raw * 1_000
    elif raw >= 100_000_000:
        normalized = raw * 1_000_000
    else:
        raise FrameReferenceError("management_timestamp_unit_unknown")
    if not _MIN_TIMESTAMP_US <= normalized < _MAX_TIMESTAMP_US:
        raise FrameReferenceError("management_timestamp_out_of_range")
    return normalized


def _topic_priority(topic: str, priority: Sequence[str]) -> int | None:
    lowered = topic.lower()
    for index, token in enumerate(priority):
        if token.lower() in lowered:
            return index
    return None


def resolve_front_camera_frame(
    *,
    frame_lookup: Mapping[str, Any],
    index_payloads: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    """Resolve the nearest front-camera ``frame_id`` from L2 index payloads."""
    if frame_lookup.get("kind") != "front_camera_timestamp":
        raise FrameReferenceError("frame_lookup_kind_invalid")
    if frame_lookup.get("management_timestamp_unit") != MANAGEMENT_TIMESTAMP_UNIT:
        raise FrameReferenceError("frame_lookup_timestamp_unit_invalid")
    target_us = normalize_management_timestamp_us(
        frame_lookup.get("management_timestamp")
    )
    max_delta_us = frame_lookup.get("max_delta_us")
    if isinstance(max_delta_us, bool) or not isinstance(max_delta_us, int):
        raise FrameReferenceError("frame_lookup_max_delta_invalid")
    if not 1 <= max_delta_us <= DEFAULT_MAX_FRAME_DELTA_US:
        raise FrameReferenceError("frame_lookup_max_delta_invalid")
    raw_priority = frame_lookup.get("topic_priority")
    if (
        not isinstance(raw_priority, list)
        or not raw_priority
        or any(not isinstance(item, str) or not item for item in raw_priority)
    ):
        raise FrameReferenceError("frame_lookup_topic_priority_invalid")

    candidates: list[tuple[int, int, int, int, str, int]] = []
    for topic, payload in index_payloads.items():
        rank = _topic_priority(str(topic), raw_priority)
        if rank is None or not isinstance(payload, Mapping):
            continue
        fields = payload.get("fields")
        rows = payload.get("index")
        if not isinstance(fields, Mapping) or not isinstance(rows, list):
            continue
        timestamp_index = fields.get("timestamp")
        frame_id_index = fields.get("frame_id")
        if (
            isinstance(timestamp_index, bool)
            or not isinstance(timestamp_index, int)
            or isinstance(frame_id_index, bool)
            or not isinstance(frame_id_index, int)
        ):
            continue
        for row_index, row in enumerate(rows):
            if not isinstance(row, list):
                continue
            try:
                timestamp_us = normalize_management_timestamp_us(
                    row[timestamp_index]
                )
                frame_id = int(row[frame_id_index])
            except (FrameReferenceError, IndexError, TypeError, ValueError):
                continue
            if frame_id <= 0:
                continue
            delta_us = abs(timestamp_us - target_us)
            candidates.append(
                (rank, delta_us, timestamp_us, row_index, str(topic), frame_id)
            )
    if not candidates:
        raise FrameReferenceError("front_camera_index_unavailable")

    # Topic preference is authoritative; within that topic use nearest time,
    # preferring the earlier frame and then its stable row position on ties.
    rank = min(item[0] for item in candidates)
    selected = min(
        (item for item in candidates if item[0] == rank),
        key=lambda item: (item[1], item[2] > target_us, item[2], item[3]),
    )
    _, delta_us, matched_us, row_index, topic, frame_id = selected
    if delta_us > max_delta_us:
        raise FrameReferenceError("front_camera_frame_outside_tolerance")
    return {
        "status": "resolved",
        "frame_id": str(frame_id),
        "topic": topic,
        "target_management_timestamp": target_us,
        "matched_management_timestamp": matched_us,
        "management_timestamp_unit": MANAGEMENT_TIMESTAMP_UNIT,
        "delta_us": delta_us,
        "row_index": row_index,
        "selection": "nearest_timestamp",
        "camera_scope": "front_view",
    }
