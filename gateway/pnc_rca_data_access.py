"""Fail-closed remote data access contract for G1Q3 RCA inputs."""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Mapping

from gateway.pnc_pdcl_contract import is_placeholder_reference_value, parse_pdcl_command


RCA_REMOTE_DATA_ACCESS_SCHEMA_VERSION = "g1q3_rca_remote_data_access_v1"
RCA_REMOTE_DATA_ACCESS_MODE = "remote_read"
RCA_REMOTE_DATA_TRANSPORT = "pdcl_pyclip"
RCA_REMOTE_READER_DISTRIBUTION = "pdcl_pyclip"
RCA_REMOTE_READER_REQUIRED_VERSION = "0.1.6+rca.2"
RCA_REMOTE_DATA_SOURCE_FIELD = "问题数据地址_PDCL"
MAX_REMOTE_REFERENCES = 16
MAX_REMOTE_REFERENCE_LENGTH = 512
MAX_REMOTE_SOURCE_LENGTH = 16 * 1024
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class RemoteDataAccessError(ValueError):
    """The issue data field cannot be represented by the remote-reader ABI."""

    def __init__(self, code: str, detail: str):
        super().__init__(detail)
        self.code = code
        self.detail = detail


def _dedupe(values: list[str]) -> list[str]:
    return list(dict.fromkeys(str(value).strip() for value in values if str(value).strip()))


def _references_from_parsed(parsed: dict[str, Any]) -> list[dict[str, str]]:
    verb = str(parsed.get("verb") or "").strip().lower()
    event_ids = _dedupe(list(parsed.get("event_ids") or []))
    address_ids = _dedupe(list(parsed.get("clip_ukeys") or []))
    references: list[dict[str, str]] = []

    if verb == "event":
        event_ids = _dedupe(event_ids + address_ids)
        address_ids = []
    elif verb == "clip":
        pass
    elif verb in {"refresh", "refresh2"}:
        # A refresh card normally carries an explicit event id.  A -u value is
        # still a clip locator and can be consumed by RemoteClipReader.
        pass
    else:
        raise RemoteDataAccessError(
            "remote_data_reference_kind_unsupported",
            f"PDCL resource kind {verb or 'unknown'} has no production remote reader",
        )

    references.extend(
        {
            "kind": "event",
            "event_uuid": value,
            "reader_class": "RemoteEventReader",
        }
        for value in event_ids
    )
    references.extend(
        {
            "kind": "clip",
            "clip_uuid": value,
            "reader_class": "RemoteClipReader",
        }
        for value in address_ids
    )
    if not references:
        if parsed.get("ticket_ids"):
            raise RemoteDataAccessError(
                "remote_data_reference_resolution_required",
                "ticket-only data addresses must be resolved to an event or clip UUID",
            )
        raise RemoteDataAccessError(
            "remote_data_reference_missing",
            "the issue data field has no event or clip UUID",
        )
    if len(references) > MAX_REMOTE_REFERENCES:
        raise RemoteDataAccessError(
            "remote_data_reference_limit_exceeded",
            f"at most {MAX_REMOTE_REFERENCES} remote references are allowed per issue",
        )
    return references


def build_remote_data_access(source_value: str) -> dict[str, Any]:
    """Convert the legacy issue field syntax into a non-executable reader ABI.

    The source field historically contains an MDI command.  It is parsed only
    as an address envelope.  The command itself never crosses the VM boundary.
    """
    normalized = str(source_value or "").strip()
    if not normalized:
        raise RemoteDataAccessError(
            "issue_field_missing_remote_data_reference",
            "the issue field 问题数据地址_PDCL is empty",
        )
    if len(normalized.encode("utf-8")) > MAX_REMOTE_SOURCE_LENGTH:
        raise RemoteDataAccessError(
            "remote_data_source_limit_exceeded",
            "the issue data field exceeds the remote-reader input limit",
        )
    parsed = parse_pdcl_command(normalized)
    if parsed is None:
        raise RemoteDataAccessError(
            "remote_data_reference_invalid",
            "the issue data field is not a supported read-only address envelope",
        )
    references = _references_from_parsed(parsed)
    contract = {
        "schema_version": RCA_REMOTE_DATA_ACCESS_SCHEMA_VERSION,
        "mode": RCA_REMOTE_DATA_ACCESS_MODE,
        "transport": RCA_REMOTE_DATA_TRANSPORT,
        "references": references,
        "source": {
            "field": RCA_REMOTE_DATA_SOURCE_FIELD,
            "value_sha256": hashlib.sha256(normalized.encode("utf-8")).hexdigest(),
        },
        "reader_contract": {
            "distribution": RCA_REMOTE_READER_DISTRIBUTION,
            "required_version": RCA_REMOTE_READER_REQUIRED_VERSION,
            "mdi_download_allowed": False,
            "fallback": "forbidden",
            "completeness": "full_requested_scope",
        },
    }
    return validate_remote_data_access(contract)


def build_blocked_remote_data_access(
    source_value: str, error: RemoteDataAccessError
) -> dict[str, Any]:
    """Represent a host-proven field blocker without restoring an MDI path."""
    normalized = str(source_value or "").strip()
    return {
        "schema_version": RCA_REMOTE_DATA_ACCESS_SCHEMA_VERSION,
        "mode": RCA_REMOTE_DATA_ACCESS_MODE,
        "transport": RCA_REMOTE_DATA_TRANSPORT,
        "status": "blocked",
        "references": [],
        "source": {
            "field": RCA_REMOTE_DATA_SOURCE_FIELD,
            "value_sha256": hashlib.sha256(normalized.encode("utf-8")).hexdigest(),
        },
        "reader_contract": {
            "distribution": RCA_REMOTE_READER_DISTRIBUTION,
            "required_version": RCA_REMOTE_READER_REQUIRED_VERSION,
            "mdi_download_allowed": False,
            "fallback": "forbidden",
            "completeness": "full_requested_scope",
        },
        "blocker": {
            "kind": error.code,
            "retryable": True,
        },
    }


def validate_remote_data_access(value: Any) -> dict[str, Any]:
    """Strictly validate and detach the active VM remote-reader ABI."""
    if not isinstance(value, Mapping):
        raise RemoteDataAccessError(
            "remote_data_access_missing", "data.data_access is required"
        )
    access = dict(value)
    if access.get("status") == "blocked":
        blocker = access.get("blocker")
        kind = (
            str(blocker.get("kind") or "remote_data_access_blocked")
            if isinstance(blocker, Mapping)
            else "remote_data_access_blocked"
        )
        raise RemoteDataAccessError(kind, "host marked remote data access blocked")
    if set(access) != {
        "schema_version",
        "mode",
        "transport",
        "references",
        "source",
        "reader_contract",
    }:
        raise RemoteDataAccessError(
            "remote_data_access_shape_invalid",
            "remote data access fields do not match the active ABI",
        )
    if access.get("schema_version") != RCA_REMOTE_DATA_ACCESS_SCHEMA_VERSION:
        raise RemoteDataAccessError(
            "remote_data_access_schema_invalid", "unexpected remote data access schema"
        )
    if (
        access.get("mode") != RCA_REMOTE_DATA_ACCESS_MODE
        or access.get("transport") != RCA_REMOTE_DATA_TRANSPORT
    ):
        raise RemoteDataAccessError(
            "remote_data_access_mode_invalid", "only pdcl_pyclip remote_read is allowed"
        )
    source = access.get("source")
    if not isinstance(source, Mapping) or set(source) != {"field", "value_sha256"}:
        raise RemoteDataAccessError(
            "remote_data_source_invalid", "remote data source binding is invalid"
        )
    if (
        source.get("field") != RCA_REMOTE_DATA_SOURCE_FIELD
        or not _SHA256_RE.fullmatch(str(source.get("value_sha256") or ""))
    ):
        raise RemoteDataAccessError(
            "remote_data_source_invalid", "remote data source binding is invalid"
        )
    expected_reader_contract = {
        "distribution": RCA_REMOTE_READER_DISTRIBUTION,
        "required_version": RCA_REMOTE_READER_REQUIRED_VERSION,
        "mdi_download_allowed": False,
        "fallback": "forbidden",
        "completeness": "full_requested_scope",
    }
    if access.get("reader_contract") != expected_reader_contract:
        raise RemoteDataAccessError(
            "remote_reader_contract_invalid", "remote reader contract mismatch"
        )
    references = access.get("references")
    if (
        not isinstance(references, list)
        or not 1 <= len(references) <= MAX_REMOTE_REFERENCES
    ):
        raise RemoteDataAccessError(
            "remote_data_reference_count_invalid", "remote reference count is invalid"
        )
    normalized: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for item in references:
        if not isinstance(item, Mapping):
            raise RemoteDataAccessError(
                "remote_data_reference_invalid", "remote reference must be an object"
            )
        kind = str(item.get("kind") or "")
        if kind == "clip":
            locator_key, reader_class = "clip_uuid", "RemoteClipReader"
        elif kind == "event":
            locator_key, reader_class = "event_uuid", "RemoteEventReader"
        else:
            raise RemoteDataAccessError(
                "remote_data_reference_kind_unsupported",
                "only event and clip references are supported",
            )
        if set(item) != {"kind", locator_key, "reader_class"}:
            raise RemoteDataAccessError(
                "remote_data_reference_invalid", "remote reference fields mismatch"
            )
        raw_locator = item.get(locator_key)
        locator = raw_locator.strip() if isinstance(raw_locator, str) else ""
        if (
            not locator
            or len(locator) > MAX_REMOTE_REFERENCE_LENGTH
            or raw_locator != locator
            or "\x00" in locator
            or is_placeholder_reference_value(locator)
            or item.get("reader_class") != reader_class
        ):
            raise RemoteDataAccessError(
                "remote_data_reference_invalid",
                "remote reference locator/class mismatch",
            )
        identity = (kind, locator)
        if identity in seen:
            raise RemoteDataAccessError(
                "remote_data_reference_duplicate", "remote reference is duplicated"
            )
        seen.add(identity)
        normalized.append(
            {"kind": kind, locator_key: locator, "reader_class": reader_class}
        )
    detached = {**access, "references": normalized}
    return json.loads(
        json.dumps(
            detached,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    )


def redacted_data_access(value: dict[str, Any]) -> dict[str, Any]:
    """Return a health-safe summary without opaque clip/event identifiers."""
    references = value.get("references") if isinstance(value, dict) else []
    references = references if isinstance(references, list) else []
    return {
        "schema_version": value.get("schema_version"),
        "mode": value.get("mode"),
        "transport": value.get("transport"),
        "reference_count": len(references),
        "reference_kinds": sorted(
            {
                str(item.get("kind") or "")
                for item in references
                if isinstance(item, dict) and item.get("kind")
            }
        ),
        "source": dict(value.get("source") or {}),
        "reader_contract": dict(value.get("reader_contract") or {}),
    }
