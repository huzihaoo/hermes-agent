#!/usr/bin/env python3
"""Consume the Stage-B record-only launch binding from inherited FD 198."""

from __future__ import annotations

import errno
import fcntl
import hashlib
import json
import os
import re
import stat
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from types import MappingProxyType
from typing import Any, Callable, Mapping


EXTERNAL_CENSUS_BINDING_FD = 198
MAX_BINDING_BYTES = 64 * 1024
BINDING_SCHEMA = "hermes_record_only_external_census_binding_v1"
CAPTURE_SCHEMA = "hermes_record_only_external_census_capture_v1"
CONSUMER_OUTPUT_SCHEMA = "hermes_record_only_external_census_consumer_output_v1"
AUTHORITY_SCOPE = "RECORD_ONLY_GENERIC_CANDIDATE_SERVICE_LAUNCH"
ISSUER = "STAGE_B_SEALED_OPERATOR"
MAX_LIFETIME_SECONDS = 120

_HEX40 = re.compile(r"^[0-9a-f]{40}$")
_HEX64 = re.compile(r"^[0-9a-f]{64}$")
_TOKEN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,159}$")
_NONCE = re.compile(r"^[A-Za-z0-9._-]{32,160}$")
_UTC_TEXT = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?Z$"
)

_BINDING_FIELDS = {
    "schema_version",
    "decision",
    "authority_scope",
    "record_only_launch_authorized",
    "candidate_execution_authorized",
    "cutover_authorized",
    "real_outbound_authorized",
    "production_state_write_authorized",
    "boot_session",
    "capture_id",
    "nonce",
    "generated_at",
    "expires_at",
    "candidate_commit",
    "candidate_tree",
    "candidate_source_seal_sha256",
    "global_census_sha256",
    "route_index_sha256",
    "source_manifest_sha256",
    "prototype_status_sha256",
    "total_routes",
    "unclassified_routes",
    "overlay_paths_total",
    "overlay_paths_covered",
    "global_route_closure",
    "overlay_coverage_complete",
    "issuer",
    "launch_session_id",
}


class ExternalCensusBindingError(RuntimeError):
    """Raised when the inherited Stage-B binding fails closed."""


def canonical_json(value: Any) -> bytes:
    return (
        json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        + "\n"
    ).encode("utf-8")


def capture_id_for(binding: Mapping[str, Any]) -> str:
    material = {
        "schema_version": CAPTURE_SCHEMA,
        "candidate_commit": binding["candidate_commit"],
        "candidate_tree": binding["candidate_tree"],
        "candidate_source_seal_sha256": binding["candidate_source_seal_sha256"],
        "global_census_sha256": binding["global_census_sha256"],
        "route_index_sha256": binding["route_index_sha256"],
        "source_manifest_sha256": binding["source_manifest_sha256"],
        "prototype_status_sha256": binding["prototype_status_sha256"],
        "total_routes": binding["total_routes"],
        "overlay_paths_total": binding["overlay_paths_total"],
        "overlay_paths_covered": binding["overlay_paths_covered"],
    }
    return hashlib.sha256(canonical_json(material)).hexdigest()


def _strict_json_loads(raw: bytes) -> Any:
    def reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ExternalCensusBindingError(f"duplicate binding key: {key}")
            result[key] = value
        return result

    def reject_constant(value: str) -> None:
        raise ExternalCensusBindingError(f"non-finite binding value: {value}")

    try:
        return json.loads(
            raw,
            object_pairs_hook=reject_duplicates,
            parse_constant=reject_constant,
        )
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ExternalCensusBindingError(f"invalid binding JSON: {exc}") from exc


def _parse_utc(value: Any, field: str) -> datetime:
    if not isinstance(value, str) or not _UTC_TEXT.fullmatch(value):
        raise ExternalCensusBindingError(f"{field} must be canonical UTC text")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise ExternalCensusBindingError(f"{field} is invalid") from exc
    if parsed.tzinfo != timezone.utc:
        raise ExternalCensusBindingError(f"{field} must be UTC")
    return parsed


def _identity(st: os.stat_result) -> dict[str, int]:
    return {
        "dev": st.st_dev,
        "ino": st.st_ino,
        "size": st.st_size,
        "mode": f"{stat.S_IMODE(st.st_mode):04o}",
        "uid": st.st_uid,
        "nlink": st.st_nlink,
        "mtime_ns": st.st_mtime_ns,
        "ctime_ns": st.st_ctime_ns,
    }


def _validate_binding(value: Any, *, now: datetime) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != _BINDING_FIELDS:
        raise ExternalCensusBindingError("external census binding fields differ")
    expected = {
        "schema_version": BINDING_SCHEMA,
        "decision": "PASS",
        "authority_scope": AUTHORITY_SCOPE,
        "record_only_launch_authorized": True,
        "candidate_execution_authorized": False,
        "cutover_authorized": False,
        "real_outbound_authorized": False,
        "production_state_write_authorized": False,
        "unclassified_routes": 0,
        "global_route_closure": True,
        "overlay_coverage_complete": True,
        "issuer": ISSUER,
    }
    if any(value.get(key) != expected_value for key, expected_value in expected.items()):
        raise ExternalCensusBindingError("external census binding authority differs")

    for field in ("candidate_commit", "candidate_tree"):
        item = value[field]
        if not isinstance(item, str) or not _HEX40.fullmatch(item):
            raise ExternalCensusBindingError(f"{field} must be a Git object id")
    for field in (
        "candidate_source_seal_sha256",
        "global_census_sha256",
        "route_index_sha256",
        "source_manifest_sha256",
        "prototype_status_sha256",
    ):
        item = value[field]
        if not isinstance(item, str) or not _HEX64.fullmatch(item):
            raise ExternalCensusBindingError(f"{field} must be a SHA-256")
    boot_session = value["boot_session"]
    if not isinstance(boot_session, str) or not _TOKEN.fullmatch(boot_session):
        raise ExternalCensusBindingError("boot_session is invalid")
    launch_session_id = value["launch_session_id"]
    if not isinstance(launch_session_id, str) or not _NONCE.fullmatch(
        launch_session_id
    ):
        raise ExternalCensusBindingError("launch_session_id is invalid")
    nonce = value["nonce"]
    if not isinstance(nonce, str) or not _NONCE.fullmatch(nonce):
        raise ExternalCensusBindingError("nonce is invalid")

    total_routes = value["total_routes"]
    unclassified_routes = value["unclassified_routes"]
    overlay_total = value["overlay_paths_total"]
    overlay_covered = value["overlay_paths_covered"]
    if (
        isinstance(total_routes, bool)
        or not isinstance(total_routes, int)
        or total_routes < 1
        or isinstance(unclassified_routes, bool)
        or not isinstance(unclassified_routes, int)
        or unclassified_routes != 0
        or isinstance(overlay_total, bool)
        or not isinstance(overlay_total, int)
        or overlay_total < 1
        or isinstance(overlay_covered, bool)
        or not isinstance(overlay_covered, int)
        or overlay_covered != overlay_total
    ):
        raise ExternalCensusBindingError("route or overlay closure differs")

    generated_at = _parse_utc(value["generated_at"], "generated_at")
    expires_at = _parse_utc(value["expires_at"], "expires_at")
    lifetime = (expires_at - generated_at).total_seconds()
    if lifetime <= 0 or lifetime > MAX_LIFETIME_SECONDS:
        raise ExternalCensusBindingError("binding lifetime exceeds policy")
    if now.tzinfo is None or now.utcoffset() != timezone.utc.utcoffset(now):
        raise ExternalCensusBindingError("consumer clock must be UTC-aware")
    if now < generated_at or now >= expires_at:
        raise ExternalCensusBindingError("binding is not currently valid")

    capture_id = value["capture_id"]
    if (
        not isinstance(capture_id, str)
        or not _HEX64.fullmatch(capture_id)
        or capture_id != capture_id_for(value)
    ):
        raise ExternalCensusBindingError("capture_id differs")
    return value


@dataclass(frozen=True)
class ConsumedExternalCensusBinding:
    binding: Mapping[str, Any]
    consumer_output: Mapping[str, Any]

    @property
    def index_sha256(self) -> str:
        return str(self.binding["route_index_sha256"])

    @property
    def artifact_name(self) -> str:
        return "stage-b-global-census"

    @property
    def artifact_sha256(self) -> str:
        return str(self.binding["global_census_sha256"])

    @property
    def status(self) -> str:
        return "SEALED_EXTERNAL_CENSUS_RECORD_ONLY_LAUNCH"

    def as_ledger_binding(self) -> dict[str, Any]:
        return {
            "schema_version": BINDING_SCHEMA,
            "binding_sha256": self.consumer_output["binding_sha256"],
            "capture_id": self.binding["capture_id"],
            "boot_session": self.binding["boot_session"],
            "launch_session_id": self.binding["launch_session_id"],
            "candidate_commit": self.binding["candidate_commit"],
            "candidate_tree": self.binding["candidate_tree"],
            "candidate_source_seal_sha256": self.binding[
                "candidate_source_seal_sha256"
            ],
            "global_census_sha256": self.binding["global_census_sha256"],
            "route_index_sha256": self.binding["route_index_sha256"],
            "source_manifest_sha256": self.binding["source_manifest_sha256"],
            "prototype_status_sha256": self.binding["prototype_status_sha256"],
            "total_routes": self.binding["total_routes"],
            "unclassified_routes": self.binding["unclassified_routes"],
            "overlay_paths_total": self.binding["overlay_paths_total"],
            "overlay_paths_covered": self.binding["overlay_paths_covered"],
            "record_only_launch_authorized": self.binding[
                "record_only_launch_authorized"
            ],
            "candidate_execution_authorized": self.binding[
                "candidate_execution_authorized"
            ],
            "cutover_authorized": self.binding["cutover_authorized"],
            "real_outbound_authorized": self.binding["real_outbound_authorized"],
            "production_state_write_authorized": self.binding[
                "production_state_write_authorized"
            ],
        }

    def as_status(self) -> dict[str, Any]:
        return {
            "schema_version": BINDING_SCHEMA,
            "status": self.status,
            "gate_decision": "PASS",
            "external_binding_consumed": True,
            "binding_sha256": self.consumer_output["binding_sha256"],
            "capture_id": self.binding["capture_id"],
            "source_commit": self.binding["candidate_commit"],
            "source_tree": self.binding["candidate_tree"],
            "candidate_source_seal_sha256": self.binding[
                "candidate_source_seal_sha256"
            ],
            "global_census_sha256": self.binding["global_census_sha256"],
            "route_index_sha256": self.binding["route_index_sha256"],
            "source_manifest_sha256": self.binding["source_manifest_sha256"],
            "prototype_status_sha256": self.binding["prototype_status_sha256"],
            "total_routes": self.binding["total_routes"],
            "unclassified_routes": self.binding["unclassified_routes"],
            "overlay_paths_total": self.binding["overlay_paths_total"],
            "overlay_paths_covered": self.binding["overlay_paths_covered"],
            "global_route_closure": self.binding["global_route_closure"],
            "overlay_coverage_complete": self.binding[
                "overlay_coverage_complete"
            ],
            "record_only_launch_authorized": self.binding[
                "record_only_launch_authorized"
            ],
            "candidate_execution_authorized": self.binding[
                "candidate_execution_authorized"
            ],
            "cutover_authorized": self.binding["cutover_authorized"],
            "real_outbound_authorized": self.binding["real_outbound_authorized"],
            "production_state_write_authorized": self.binding[
                "production_state_write_authorized"
            ],
            "external_delivery_attempted": False,
            "external_delivery_verified": False,
        }


def consume_external_census_binding(
    *,
    _now: datetime | None = None,
    _after_read_hook: Callable[[], None] | None = None,
) -> ConsumedExternalCensusBinding:
    """Read, validate, and close the one inherited Stage-B binding FD."""
    fd = EXTERNAL_CENSUS_BINDING_FD
    fd_open = False
    try:
        before = os.fstat(fd)
        fd_open = True
    except OSError as exc:
        raise ExternalCensusBindingError(
            f"required inherited FD {fd} is unavailable"
        ) from exc

    try:
        try:
            initial_offset = os.lseek(fd, 0, os.SEEK_CUR)
            access_mode = fcntl.fcntl(fd, fcntl.F_GETFL) & os.O_ACCMODE
            descriptor_flags = fcntl.fcntl(fd, fcntl.F_GETFD)
        except OSError as exc:
            raise ExternalCensusBindingError("binding FD metadata is unavailable") from exc
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != os.getuid()
            or stat.S_IMODE(before.st_mode) != 0o400
            or before.st_nlink != 1
            or before.st_size < 1
            or before.st_size > MAX_BINDING_BYTES
            or initial_offset != 0
            or access_mode != os.O_RDONLY
            or descriptor_flags & fcntl.FD_CLOEXEC
        ):
            raise ExternalCensusBindingError(
                "binding FD type/owner/mode/link/size/offset/access differs"
            )

        chunks: list[bytes] = []
        remaining = MAX_BINDING_BYTES + 1
        while remaining > 0:
            block = os.read(fd, min(65536, remaining))
            if not block:
                break
            chunks.append(block)
            remaining -= len(block)
        raw = b"".join(chunks)
        eof = os.read(fd, 1) == b""
        final_offset = os.lseek(fd, 0, os.SEEK_CUR)
        if _after_read_hook is not None:
            _after_read_hook()
        after = os.fstat(fd)
        if (
            len(raw) != before.st_size
            or len(raw) > MAX_BINDING_BYTES
            or not eof
            or final_offset != before.st_size
            or _identity(after) != _identity(before)
        ):
            raise ExternalCensusBindingError("binding FD changed during read")

        parsed = _strict_json_loads(raw)
        if raw != canonical_json(parsed):
            raise ExternalCensusBindingError("binding JSON is not canonical")
        binding = _validate_binding(parsed, now=_now or datetime.now(timezone.utc))
        binding_sha256 = hashlib.sha256(raw).hexdigest()
        fd_open = False
        try:
            os.close(fd)
        except OSError as exc:
            raise ExternalCensusBindingError("binding FD could not be closed") from exc
        try:
            os.fstat(fd)
        except OSError as exc:
            if exc.errno != errno.EBADF:
                raise ExternalCensusBindingError(
                    "binding FD closure could not be verified"
                ) from exc
        else:
            raise ExternalCensusBindingError("binding FD remained open after close")
        output = {
            "schema_version": CONSUMER_OUTPUT_SCHEMA,
            "decision": "PASS",
            "boot_session": binding["boot_session"],
            "candidate_commit": binding["candidate_commit"],
            "candidate_tree": binding["candidate_tree"],
            "launch_session_id": binding["launch_session_id"],
            "binding_sha256": binding_sha256,
            "fd_number": fd,
            "pre_identity": _identity(before),
            "post_identity": _identity(after),
            "bytes_sha256": binding_sha256,
            "initial_offset": initial_offset,
            "final_offset": final_offset,
            "eof_observed": eof,
            "fd_closed": True,
            "env_override_used": False,
            "argv_override_used": False,
            "path_open_attempts": 0,
            "record_only_launch_authorized": binding[
                "record_only_launch_authorized"
            ],
            "candidate_execution_authorized": binding[
                "candidate_execution_authorized"
            ],
            "cutover_authorized": binding["cutover_authorized"],
            "real_outbound_authorized": binding["real_outbound_authorized"],
            "production_state_write_authorized": binding[
                "production_state_write_authorized"
            ],
            "consumer_generated_authority": False,
        }
        return ConsumedExternalCensusBinding(
            binding=MappingProxyType(dict(binding)),
            consumer_output=MappingProxyType(output),
        )
    finally:
        if fd_open:
            try:
                os.close(fd)
            except OSError:
                pass


def main() -> int:
    try:
        consumed = consume_external_census_binding()
    except ExternalCensusBindingError as exc:
        sys.stderr.write(f"external census binding rejected: {exc}\n")
        return 3
    sys.stdout.buffer.write(canonical_json(dict(consumed.consumer_output)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
