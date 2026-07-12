#!/usr/bin/env python3
"""Unified, fail-closed record-only outbound transport for candidate smoke."""

from __future__ import annotations

import fcntl
import hashlib
import hmac
import json
import os
import pwd
import re
import secrets
import stat
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from types import MappingProxyType
from typing import Any, Callable, Iterator, Mapping
from urllib.parse import urlsplit

from gateway.record_only.census_binding import AUTHORITATIVE_CENSUS_BINDING


SCHEMA_VERSION = 2
LEDGER_FORMAT = "record-only-authenticated-jsonl-v2"
MAX_LEDGER_BYTES = 16 * 1024 * 1024
MAX_VALUE_BYTES = 2 * 1024 * 1024
MAX_IDENTIFIER_BYTES = 4096
TARGET_OUTBOUND_CENSUS_INDEX_SHA256 = AUTHORITATIVE_CENSUS_BINDING.index_sha256
TARGET_OUTBOUND_CENSUS_ARTIFACT = AUTHORITATIVE_CENSUS_BINDING.artifact_name
TARGET_OUTBOUND_CENSUS_SHA256 = AUTHORITATIVE_CENSUS_BINDING.artifact_sha256
TARGET_OUTBOUND_CENSUS_STATUS = AUTHORITATIVE_CENSUS_BINDING.status
TARGET_OUTBOUND_CENSUS_BINDING = MappingProxyType(
    {
        "index_sha256": AUTHORITATIVE_CENSUS_BINDING.index_sha256,
        "canonical_artifact": AUTHORITATIVE_CENSUS_BINDING.artifact_name,
        "canonical_artifact_sha256": AUTHORITATIVE_CENSUS_BINDING.artifact_sha256,
        "status": AUTHORITATIVE_CENSUS_BINDING.status,
        "gate_decision": AUTHORITATIVE_CENSUS_BINDING.gate_decision,
        "source_commit": AUTHORITATIVE_CENSUS_BINDING.source_commit,
        "source_tree": AUTHORITATIVE_CENSUS_BINDING.source_tree,
        "source_sha256_manifest_sha256": (
            AUTHORITATIVE_CENSUS_BINDING.source_sha256_manifest_sha256
        ),
        "source_tree_manifest_sha256": (
            AUTHORITATIVE_CENSUS_BINDING.source_tree_manifest_sha256
        ),
        "manifest_files": AUTHORITATIVE_CENSUS_BINDING.manifest_files,
        "scanned_files": AUTHORITATIVE_CENSUS_BINDING.scanned_files,
        "total_rows": AUTHORITATIVE_CENSUS_BINDING.total_rows,
        "runtime_rows": AUTHORITATIVE_CENSUS_BINDING.runtime_rows,
        "test_rows": AUTHORITATIVE_CENSUS_BINDING.test_rows,
        "pending_rows": AUTHORITATIVE_CENSUS_BINDING.pending_rows,
        "unverified_rows": AUTHORITATIVE_CENSUS_BINDING.unverified_rows,
        "unclassified_executable_mode_count": len(
            AUTHORITATIVE_CENSUS_BINDING.unclassified_executable_modes
        ),
        "all_runtime_rows_classified": False,
        "runtime_egress_trace_complete": False,
        "dynamic_import_trace_complete": False,
        "skill_trace_complete": False,
        "subprocess_descendant_trace_complete": False,
        "record_only_coverage_complete": False,
        "production_ready": False,
        "promotion_authorized": False,
        "candidate_execution_authorized": False,
        "cutover_authorized": False,
        "external_delivery_attempted": False,
        "external_delivery_verified": False,
    }
)
OPERATIONS = {
    "text_send",
    "text_reply",
    "text_update",
    "card_send",
    "card_reply",
    "card_update",
    "file_send",
    "file_reply",
    "reaction_add",
    "reaction_remove",
    "startup_notice",
    "shutdown_notice",
    "error_notice",
}
NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
TASK_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$")
FEISHU_ID_RE = re.compile(r"\b(?:ou|oc|om|on)_[A-Za-z0-9_-]+\b")
HTTP_LINK_RE = re.compile(r"https?://[^\s<>'\"\]\)]+")
CIFS_LINK_RE = re.compile(r"(?<!:)//(?:hfs1?|[^/\s]+)/[^\s<>'\"\]\)]+")
SECRET_TEXT_RE = re.compile(
    r"(?i)[\"']?(?:api[_-]?key|access[_-]?token|refresh[_-]?token|authorization|"
    r"password|passwd|client[_-]?secret|app[_-]?secret|private[_-]?key|credential|cookie)"
    r"[\"']?\s*[:=]\s*[\"']?[^\s,;}\"']+"
)
AUTH_HEADER_RE = re.compile(r"(?i)authorization\s*:\s*(?:bearer|basic)\s+\S+")
AUTH_TOKEN_RE = re.compile(r"(?i)\b(?:bearer|basic)\s+[A-Za-z0-9._~+/=-]{6,}")
URL_CREDENTIAL_RE = re.compile(r"(?i)https?://[^:/\s]+:[^@/\s]+@")
PRIVATE_KEY_RE = re.compile(r"-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----")
SECRET_KEYS = {
    "api_key",
    "apikey",
    "access_token",
    "refresh_token",
    "authorization",
    "password",
    "secret",
    "client_secret",
    "app_secret",
    "token",
    "credential",
    "credentials",
    "cookie",
    "set_cookie",
    "private_key",
    "signing_key",
    "webhook_secret",
    "id_hash_key",
}
IDENTIFIER_KEYS = {
    "open_id",
    "user_id",
    "chat_id",
    "message_id",
    "reaction_id",
    "thread_id",
    "receive_id",
}

BLOCKERS = (
    "external_outbound_census_not_verified",
    "unclassified_executable_modes_not_resolved",
    "runtime_egress_trace_not_complete",
    "dynamic_import_trace_not_complete",
    "skill_trace_not_complete",
    "subprocess_descendant_trace_not_complete",
    "candidate_integration_not_verified",
    "deny_network_containment_not_verified",
    "credential_stripping_not_verified",
    "trusted_record_key_provisioning_not_verified",
    "durable_external_ledger_anchor_not_implemented",
    "record_root_filesystem_semantics_not_attested",
    "trusted_clock_not_integrated",
)

PROTOTYPE_SAFETY_STATUS = MappingProxyType(
    {
        "provisional_target_only": True,
        "production_ready": False,
        "promotion_authorized": False,
        "candidate_execution_authorized": False,
        "cutover_authorized": False,
        "record_only": True,
        "simulated": True,
        "success_scope": "record_persisted_not_delivered",
        "external_delivery_attempted": False,
        "external_delivery_verified": False,
        "caller_claims_verified": False,
        "record_only_coverage_complete": False,
        "external_outbound_census": MappingProxyType(
            AUTHORITATIVE_CENSUS_BINDING.as_status()
        ),
        "blockers": BLOCKERS,
    }
)

HEADER_FIELDS = {
    "schema_version",
    "kind",
    "ledger_format",
    "canonicalization",
    "generation",
    "record_count",
    "chain_head_hmac_sha256",
    "integrity_hmac_sha256",
    "provisional_target_only",
    "production_ready",
    "promotion_authorized",
    "candidate_execution_authorized",
    "cutover_authorized",
    "external_delivery_attempted",
    "external_delivery_verified",
    "runtime_egress_trace_complete",
    "blockers",
    "target_outbound_census_binding",
    "target_outbound_census_sha256",
    "target_outbound_census_status",
    "record_only_coverage_complete",
}
RECORD_FIELDS = {
    "schema_version",
    "kind",
    "sequence",
    "previous_integrity_hmac_sha256",
    "integrity_hmac_sha256",
    "record_id",
    "recorded_at",
    "first_recorded_at",
    "last_recorded_at",
    "attempt_count",
    "source_component",
    "operation",
    "platform",
    "destination",
    "payload_type",
    "payload",
    "payload_hmac_sha256",
    "recorded_payload_hash",
    "mentions",
    "links",
    "task_id_hash",
    "terminal_state",
    "reply_mode",
    "update_mode",
    "dedupe_key",
    "caller_dedupe_key_hash",
    "metadata",
    "metadata_hmac_sha256",
    "simulated_message_id",
    "record_only",
    "simulated",
    "success_scope",
    "external_delivery_attempted",
    "external_delivery_verified",
    "caller_claims_verified",
    "provisional_target_only",
    "production_ready",
    "promotion_authorized",
    "candidate_execution_authorized",
    "cutover_authorized",
    "runtime_egress_trace_complete",
    "blockers",
    "target_outbound_census_binding",
    "target_outbound_census_sha256",
    "target_outbound_census_status",
    "record_only_coverage_complete",
}


class RecordOnlyError(RuntimeError):
    """Raised when record-only safety or ledger integrity fails."""


@dataclass(frozen=True)
class RecordResult:
    success: bool
    record_id: str
    message_id: str
    duplicate: bool
    dedupe_key: str
    attempt_count: int
    record_only: bool = True
    simulated: bool = True
    success_scope: str = "record_persisted_not_delivered"
    external_delivery_attempted: bool = False
    external_delivery_verified: bool = False
    caller_claims_verified: bool = False
    record_only_coverage_complete: bool = False
    promotion_authorized: bool = False
    candidate_execution_authorized: bool = False
    cutover_authorized: bool = False
    runtime_egress_trace_complete: bool = False
    target_outbound_census_index_sha256: str = TARGET_OUTBOUND_CENSUS_INDEX_SHA256
    target_outbound_census_artifact: str = TARGET_OUTBOUND_CENSUS_ARTIFACT
    target_outbound_census_sha256: str = TARGET_OUTBOUND_CENSUS_SHA256
    target_outbound_census_status: str = TARGET_OUTBOUND_CENSUS_STATUS
    provisional_target_only: bool = True
    production_ready: bool = False
    blockers: tuple[str, ...] = BLOCKERS


@dataclass(frozen=True)
class RecordSendResult:
    success: bool
    message_id: str | None = None
    error: str | None = None
    record_only: bool = True
    simulated: bool = True
    success_scope: str = "record_persisted_not_delivered"
    external_delivery_attempted: bool = False
    external_delivery_verified: bool = False
    caller_claims_verified: bool = False
    record_only_coverage_complete: bool = False
    promotion_authorized: bool = False
    candidate_execution_authorized: bool = False
    cutover_authorized: bool = False
    runtime_egress_trace_complete: bool = False
    target_outbound_census_index_sha256: str = TARGET_OUTBOUND_CENSUS_INDEX_SHA256
    target_outbound_census_artifact: str = TARGET_OUTBOUND_CENSUS_ARTIFACT
    target_outbound_census_sha256: str = TARGET_OUTBOUND_CENSUS_SHA256
    target_outbound_census_status: str = TARGET_OUTBOUND_CENSUS_STATUS
    provisional_target_only: bool = True
    production_ready: bool = False
    blockers: tuple[str, ...] = BLOCKERS


def _canonical(value: Any) -> bytes:
    try:
        return json.dumps(
            value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise RecordOnlyError(f"payload is not canonical JSON: {exc}") from exc


def _strict_json_loads(raw: bytes, *, context: str) -> Any:
    def reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise RecordOnlyError(f"duplicate JSON key in {context}: {key}")
            result[key] = value
        return result

    def reject_constant(value: str) -> None:
        raise RecordOnlyError(f"non-finite JSON value in {context}: {value}")

    try:
        return json.loads(
            raw,
            object_pairs_hook=reject_duplicate_keys,
            parse_constant=reject_constant,
        )
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise RecordOnlyError(f"invalid JSON in {context}: {exc}") from exc


def _sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _target_census_ledger_binding() -> dict[str, Any]:
    return dict(TARGET_OUTBOUND_CENSUS_BINDING)


def _safe_name(value: Any, *, field: str, pattern: re.Pattern[str] = NAME_RE) -> str:
    if not isinstance(value, str) or not pattern.fullmatch(value):
        raise RecordOnlyError(f"invalid {field}")
    return value


def _normalized_key(value: Any) -> str:
    text = unicodedata.normalize("NFKC", str(value)).lower()
    text = "".join(character for character in text if unicodedata.category(character) != "Cf")
    return re.sub(r"[^a-z0-9]+", "_", text).strip("_")


def _is_secret_key(value: Any) -> bool:
    normalized = _normalized_key(value)
    if normalized in SECRET_KEYS:
        return True
    return normalized.endswith(
        (
            "_api_key",
            "_access_token",
            "_refresh_token",
            "_auth_token",
            "_authorization",
            "_password",
            "_passwd",
            "_secret",
            "_credential",
            "_credentials",
            "_cookie",
            "_private_key",
            "_signing_key",
            "_webhook_url",
        )
    )


def _check_bounded_string(value: str, *, field: str) -> None:
    if len(value.encode("utf-8")) > MAX_IDENTIFIER_BYTES:
        raise RecordOnlyError(f"{field} exceeds bounded record-only size")


def _walk_strings(value: Any) -> Iterator[str]:
    if isinstance(value, str):
        yield value
    elif isinstance(value, (list, tuple)):
        for item in value:
            yield from _walk_strings(item)
    elif isinstance(value, dict):
        for key, item in value.items():
            if _is_secret_key(key) and item is not None and item != "":
                raise RecordOnlyError(f"secret-bearing payload key refused: {key}")
            yield str(key)
            yield from _walk_strings(item)


def _reject_secrets(value: Any) -> None:
    for text in _walk_strings(value):
        if (
            SECRET_TEXT_RE.search(text)
            or AUTH_HEADER_RE.search(text)
            or AUTH_TOKEN_RE.search(text)
            or URL_CREDENTIAL_RE.search(text)
            or PRIVATE_KEY_RE.search(text)
        ):
            raise RecordOnlyError("secret-like payload text refused")
        stripped = text.strip()
        if stripped.startswith(("{", "[")):
            try:
                nested = _strict_json_loads(stripped.encode("utf-8"), context="nested JSON text")
            except RecordOnlyError:
                continue
            _reject_secrets(nested)


def _validate_json_shape(value: Any, *, field: str) -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                raise RecordOnlyError(f"{field} object keys must be strings")
            _validate_json_shape(item, field=field)
    elif isinstance(value, (list, tuple)):
        for item in value:
            _validate_json_shape(item, field=field)


def _reject_credential_links(links: list[str]) -> None:
    sensitive_query_key = re.compile(
        r"(?i)(?:^|[?&])(?:token|key|sig|signature|credential|code|auth|password|secret)="
    )
    for link in links:
        if link.lower().startswith(("http://", "https://")):
            parsed = urlsplit(link)
            if parsed.username is not None or parsed.password is not None:
                raise RecordOnlyError("credential-bearing HTTP link refused")
            if sensitive_query_key.search("?" + parsed.query):
                raise RecordOnlyError("credential-like HTTP query refused")
            if parsed.query or parsed.fragment:
                raise RecordOnlyError("HTTP links with query or fragment are not recordable")


def _validate_routing_claims(
    *,
    operation: str,
    payload_type: str,
    thread_id: str | None,
    message_id: str | None,
    reply_mode: str,
    update_mode: str,
) -> None:
    if operation in {"text_send", "text_reply", "text_update", "startup_notice", "shutdown_notice", "error_notice"}:
        if payload_type != "text":
            raise RecordOnlyError("text and notice operations require payload_type=text")
    if operation in {"card_send", "card_reply", "card_update"} and payload_type != "interactive_card":
        raise RecordOnlyError("card operations require payload_type=interactive_card")
    if operation in {"file_send", "file_reply"} and payload_type not in {"file", "image", "audio", "video", "animation"}:
        raise RecordOnlyError("file operations require a supported media payload_type")
    if operation in {"reaction_add", "reaction_remove"} and payload_type != "reaction":
        raise RecordOnlyError("reaction operations require payload_type=reaction")
    if operation in {"text_send", "startup_notice", "shutdown_notice", "error_notice"}:
        if thread_id is not None or message_id is not None or reply_mode != "none" or update_mode != "none":
            raise RecordOnlyError("send/notice routing claims are contradictory")
    elif operation == "text_reply":
        if (thread_id is None and message_id is None) or reply_mode == "none" or update_mode != "none":
            raise RecordOnlyError("text reply routing claims are contradictory")
    elif operation == "text_update":
        if message_id is None or reply_mode != "none" or update_mode not in {"patch", "replace"}:
            raise RecordOnlyError("text update routing claims are contradictory")
    elif operation == "card_send":
        expected_reply = "thread" if thread_id is not None else "none"
        if message_id is not None or reply_mode != expected_reply or update_mode != "create":
            raise RecordOnlyError("card send routing claims are contradictory")
    elif operation == "card_reply":
        if (thread_id is None and message_id is None) or reply_mode == "none" or update_mode != "create":
            raise RecordOnlyError("card reply routing claims are contradictory")
    elif operation == "card_update":
        expected_reply = "thread" if thread_id is not None else "none"
        if message_id is None or reply_mode != expected_reply or update_mode not in {"patch", "replace"}:
            raise RecordOnlyError("card update routing claims are contradictory")
    elif operation == "file_send":
        if thread_id is not None or message_id is not None or reply_mode != "none" or update_mode != "none":
            raise RecordOnlyError("file send routing claims are contradictory")
    elif operation == "file_reply":
        if (thread_id is None and message_id is None) or reply_mode == "none" or update_mode != "none":
            raise RecordOnlyError("file reply routing claims are contradictory")
    elif operation == "reaction_add":
        if thread_id is not None or message_id is None or reply_mode != "none" or update_mode != "create":
            raise RecordOnlyError("reaction add routing claims are contradictory")
    elif operation == "reaction_remove":
        if thread_id is not None or message_id is None or reply_mode != "none" or update_mode != "delete":
            raise RecordOnlyError("reaction remove routing claims are contradictory")


def _default_forbidden_roots() -> tuple[Path, ...]:
    homes = {Path.home().resolve()}
    try:
        homes.add(Path(pwd.getpwuid(os.getuid()).pw_dir).resolve())
    except (KeyError, OSError):
        pass
    blocked: set[Path] = {Path("/mnt"), Path("/Volumes")}
    for home in homes:
        blocked.update(
            {
                home / ".hermes",
                home / ".openclaw",
                home / ".codex",
                home / ".claude",
                home / ".ssh",
                home / ".ssh-mini",
                home / "Mounts",
                home / "Library" / "LaunchAgents",
            }
        )
    return tuple(sorted(blocked, key=str))


class RecordOnlyOutboundTransport:
    def __init__(
        self,
        root: Path,
        *,
        id_hash_key: bytes,
        source_component: str,
        clock: Callable[[], datetime] | None = None,
        crash_hook: Callable[[str], None] | None = None,
        forbidden_roots: tuple[Path, ...] | None = None,
    ) -> None:
        if not isinstance(id_hash_key, bytes) or len(id_hash_key) < 32:
            raise RecordOnlyError("id_hash_key must contain at least 32 bytes")
        self.source_component = _safe_name(source_component, field="source_component")
        self._id_key = hmac.new(
            id_hash_key, b"hermes-record-only/id-hash/v1", hashlib.sha256
        ).digest()
        self._integrity_key = hmac.new(
            id_hash_key, b"hermes-record-only/ledger-integrity/v2", hashlib.sha256
        ).digest()
        self._content_key = hmac.new(
            id_hash_key, b"hermes-record-only/content-hash/v1", hashlib.sha256
        ).digest()
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._crash_hook = crash_hook
        self._last_seen_generation = 0
        self._last_seen_head: str | None = None
        forbidden = (*_default_forbidden_roots(), *(forbidden_roots or ()))
        self.root = self._prepare_root(
            root, tuple(item.resolve() for item in forbidden)
        )
        self.ledger = self.root / "outbound-records.jsonl"
        self.lock = self.root / ".outbound-records.lock"
        flags = (
            os.O_RDONLY
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
        )
        self._root_fd = os.open(self.root, flags)
        try:
            self._root_identity = self._validated_root_identity(self._root_fd)
        except Exception:
            os.close(self._root_fd)
            self._root_fd = -1
            raise

    @staticmethod
    def _validated_root_identity(fd: int) -> tuple[int, int]:
        info = os.fstat(fd)
        if not stat.S_ISDIR(info.st_mode):
            raise RecordOnlyError("record root fd must reference a directory")
        if info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise RecordOnlyError("record root must be user-owned mode 0700")
        return info.st_dev, info.st_ino

    def _assert_root_identity(self) -> None:
        if self._root_fd < 0:
            raise RecordOnlyError("record transport is closed")
        fd_identity = self._validated_root_identity(self._root_fd)
        try:
            path_info = self.root.lstat()
        except OSError as exc:
            raise RecordOnlyError(f"record root path is unavailable: {exc}") from exc
        if stat.S_ISLNK(path_info.st_mode) or not stat.S_ISDIR(path_info.st_mode):
            raise RecordOnlyError("record root path identity changed")
        path_identity = (path_info.st_dev, path_info.st_ino)
        if fd_identity != self._root_identity or path_identity != self._root_identity:
            raise RecordOnlyError("record root path was replaced after initialization")

    def close(self) -> None:
        if getattr(self, "_root_fd", -1) >= 0:
            os.close(self._root_fd)
            self._root_fd = -1

    def __del__(self) -> None:
        try:
            self.close()
        except OSError:
            pass

    @staticmethod
    def _prepare_root(path: Path, forbidden: tuple[Path, ...]) -> Path:
        if not path.is_absolute():
            raise RecordOnlyError("record root must be absolute")
        if path.exists():
            if path.is_symlink() or not path.is_dir():
                raise RecordOnlyError("record root must be a real directory")
            resolved = path.resolve(strict=True)
            if resolved != path:
                raise RecordOnlyError("record root path must be canonical")
        else:
            parent = path.parent.resolve(strict=True)
            if parent != path.parent:
                raise RecordOnlyError("record root parent must be canonical")
            resolved = parent / path.name
        for blocked in forbidden:
            try:
                resolved.relative_to(blocked)
            except ValueError:
                continue
            raise RecordOnlyError(f"record root is under forbidden root: {blocked}")
        if not path.exists():
            path.mkdir(mode=0o700)
            resolved = path.resolve(strict=True)
            if resolved != path:
                raise RecordOnlyError("record root path changed while it was created")
            for blocked in forbidden:
                try:
                    resolved.relative_to(blocked)
                except ValueError:
                    continue
                raise RecordOnlyError(f"record root is under forbidden root: {blocked}")
        info = resolved.lstat()
        if info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise RecordOnlyError("record root must be user-owned mode 0700")
        return resolved

    def _hash_id(self, value: str | None) -> str | None:
        if value is None or value == "":
            return None
        digest = hmac.new(self._id_key, value.encode("utf-8"), hashlib.sha256).hexdigest()
        return f"hmac-sha256:{digest}"

    def _integrity_hmac(self, value: Mapping[str, Any]) -> str:
        digest = hmac.new(self._integrity_key, _canonical(dict(value)), hashlib.sha256).hexdigest()
        return f"hmac-sha256:{digest}"

    def _redact_value(self, value: Any, explicit_ids: set[str]) -> Any:
        if isinstance(value, dict):
            result: dict[Any, Any] = {}
            for key, item in value.items():
                normalized_key = _normalized_key(key)
                redacted_key = self._redact_value(str(key), explicit_ids)
                if redacted_key in result:
                    raise RecordOnlyError("redacted object keys collide")
                if normalized_key in IDENTIFIER_KEYS:
                    if isinstance(item, str):
                        result[redacted_key] = self._hash_id(item)
                    elif isinstance(item, list):
                        result[redacted_key] = [
                            self._hash_id(entry) if isinstance(entry, str) else self._redact_value(entry, explicit_ids)
                            for entry in item
                        ]
                    else:
                        result[redacted_key] = self._redact_value(item, explicit_ids)
                else:
                    result[redacted_key] = self._redact_value(item, explicit_ids)
            return result
        if isinstance(value, (list, tuple)):
            return [self._redact_value(item, explicit_ids) for item in value]
        if isinstance(value, str):
            redacted = value
            for identifier in sorted(explicit_ids, key=len, reverse=True):
                if identifier:
                    redacted = redacted.replace(identifier, self._hash_id(identifier) or "")
            return FEISHU_ID_RE.sub(lambda match: self._hash_id(match.group(0)) or "", redacted)
        return value

    @staticmethod
    def _extract_links(value: Any) -> list[str]:
        links: set[str] = set()
        for text in _walk_strings(value):
            links.update(HTTP_LINK_RE.findall(text))
            links.update(CIFS_LINK_RE.findall(text))
        return sorted(links)

    @staticmethod
    def _extract_mention_ids(value: Any) -> list[str]:
        mentions: set[str] = set()
        for text in _walk_strings(value):
            mentions.update(
                match.group(0)
                for match in FEISHU_ID_RE.finditer(text)
                if match.group(0).startswith("ou_")
            )
        return sorted(mentions)

    def _encode_ledger(
        self, rows: list[dict[str, Any]], *, generation: int
    ) -> tuple[list[dict[str, Any]], bytes, str | None]:
        if isinstance(generation, bool) or generation < 1:
            raise RecordOnlyError("invalid ledger generation")
        sealed_rows: list[dict[str, Any]] = []
        previous_hmac: str | None = None
        seal_fields = {
            "sequence",
            "previous_integrity_hmac_sha256",
            "integrity_hmac_sha256",
        }
        expected_unsealed = RECORD_FIELDS - seal_fields
        for sequence, source in enumerate(rows, 1):
            row = {key: value for key, value in source.items() if key not in seal_fields}
            if set(row) != expected_unsealed:
                raise RecordOnlyError("record fields do not match authenticated ledger schema")
            row["sequence"] = sequence
            row["previous_integrity_hmac_sha256"] = previous_hmac
            row["integrity_hmac_sha256"] = self._integrity_hmac(row)
            previous_hmac = row["integrity_hmac_sha256"]
            sealed_rows.append(row)
        header = {
            "schema_version": SCHEMA_VERSION,
            "kind": "ledger_header",
            "ledger_format": LEDGER_FORMAT,
            "canonicalization": "python-json-sort-keys-no-nan-v1",
            "generation": generation,
            "record_count": len(sealed_rows),
            "chain_head_hmac_sha256": previous_hmac,
            "provisional_target_only": True,
            "production_ready": False,
            "promotion_authorized": False,
            "candidate_execution_authorized": False,
            "cutover_authorized": False,
            "external_delivery_attempted": False,
            "external_delivery_verified": False,
            "runtime_egress_trace_complete": False,
            "target_outbound_census_binding": _target_census_ledger_binding(),
            "target_outbound_census_sha256": TARGET_OUTBOUND_CENSUS_SHA256,
            "target_outbound_census_status": TARGET_OUTBOUND_CENSUS_STATUS,
            "record_only_coverage_complete": False,
            "blockers": list(BLOCKERS),
        }
        header["integrity_hmac_sha256"] = self._integrity_hmac(header)
        payload = _canonical(header) + b"\n" + b"".join(
            _canonical(row) + b"\n" for row in sealed_rows
        )
        if len(payload) > MAX_LEDGER_BYTES:
            raise RecordOnlyError("record ledger would exceed bounded smoke size")
        return sealed_rows, payload, previous_hmac

    def _read_records(
        self,
    ) -> tuple[list[dict[str, Any]], bytes, int, str | None]:
        self._assert_root_identity()
        flags = (
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_NONBLOCK", 0)
        )
        try:
            fd = os.open(self.ledger.name, flags, dir_fd=self._root_fd)
        except FileNotFoundError:
            if self._last_seen_generation:
                raise RecordOnlyError("record ledger disappeared after it was observed")
            return [], b"", 0, None
        except OSError as exc:
            raise RecordOnlyError(f"record ledger cannot be safely opened: {exc}") from exc
        try:
            info = os.fstat(fd)
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_uid != os.getuid()
                or info.st_mode & 0o077
                or info.st_nlink != 1
            ):
                raise RecordOnlyError("record ledger ownership/type/link check failed")
            if info.st_size > MAX_LEDGER_BYTES:
                raise RecordOnlyError("record ledger exceeds bounded smoke size")
            buffer = bytearray()
            while True:
                block = os.read(fd, 65536)
                if not block:
                    break
                buffer.extend(block)
                if len(buffer) > MAX_LEDGER_BYTES:
                    raise RecordOnlyError("record ledger exceeds bounded smoke size")
            data = bytes(buffer)
        finally:
            os.close(fd)
        if data and not data.endswith(b"\n"):
            raise RecordOnlyError("partial record ledger line detected")
        if not data:
            raise RecordOnlyError("empty record ledger detected")
        raw_lines = data.splitlines()
        header = _strict_json_loads(raw_lines[0], context="ledger header")
        if not isinstance(header, dict) or set(header) != HEADER_FIELDS:
            raise RecordOnlyError("invalid authenticated ledger header fields")
        if _canonical(header) != raw_lines[0]:
            raise RecordOnlyError("ledger header is not canonical JSON")
        header_without_hmac = dict(header)
        header_hmac = header_without_hmac.pop("integrity_hmac_sha256")
        expected_header_hmac = self._integrity_hmac(header_without_hmac)
        if not isinstance(header_hmac, str) or not hmac.compare_digest(
            header_hmac, expected_header_hmac
        ):
            raise RecordOnlyError("ledger header integrity check failed")
        generation = header.get("generation")
        record_count = header.get("record_count")
        if (
            header.get("schema_version") != SCHEMA_VERSION
            or header.get("kind") != "ledger_header"
            or header.get("ledger_format") != LEDGER_FORMAT
            or header.get("canonicalization") != "python-json-sort-keys-no-nan-v1"
            or isinstance(generation, bool)
            or not isinstance(generation, int)
            or generation < 1
            or isinstance(record_count, bool)
            or not isinstance(record_count, int)
            or record_count < 1
            or header.get("provisional_target_only") is not True
            or header.get("production_ready") is not False
            or header.get("promotion_authorized") is not False
            or header.get("candidate_execution_authorized") is not False
            or header.get("cutover_authorized") is not False
            or header.get("external_delivery_attempted") is not False
            or header.get("external_delivery_verified") is not False
            or header.get("runtime_egress_trace_complete") is not False
            or header.get("target_outbound_census_binding")
            != _target_census_ledger_binding()
            or header.get("target_outbound_census_sha256")
            != TARGET_OUTBOUND_CENSUS_SHA256
            or header.get("target_outbound_census_status") != TARGET_OUTBOUND_CENSUS_STATUS
            or header.get("record_only_coverage_complete") is not False
            or header.get("blockers") != list(BLOCKERS)
        ):
            raise RecordOnlyError("invalid authenticated ledger header invariants")
        if record_count != len(raw_lines) - 1:
            raise RecordOnlyError("ledger record count does not match authenticated header")

        rows: list[dict[str, Any]] = []
        record_ids: dict[str, str] = {}
        dedupe_keys: set[str] = set()
        previous_hmac: str | None = None
        for sequence, raw in enumerate(raw_lines[1:], 1):
            line_number = sequence + 1
            row = _strict_json_loads(raw, context=f"ledger line {line_number}")
            if not isinstance(row, dict) or set(row) != RECORD_FIELDS:
                raise RecordOnlyError(f"invalid record fields at line {line_number}")
            if _canonical(row) != raw:
                raise RecordOnlyError(f"record line {line_number} is not canonical JSON")
            row_without_hmac = dict(row)
            row_hmac = row_without_hmac.pop("integrity_hmac_sha256")
            expected_row_hmac = self._integrity_hmac(row_without_hmac)
            if not isinstance(row_hmac, str) or not hmac.compare_digest(
                row_hmac, expected_row_hmac
            ):
                raise RecordOnlyError(f"record integrity check failed at line {line_number}")
            if (
                row.get("schema_version") != SCHEMA_VERSION
                or row.get("kind") != "outbound_record"
                or isinstance(row.get("sequence"), bool)
                or not isinstance(row.get("sequence"), int)
                or row.get("sequence") != sequence
                or row.get("previous_integrity_hmac_sha256") != previous_hmac
                or row.get("record_only") is not True
                or row.get("simulated") is not True
                or row.get("success_scope") != "record_persisted_not_delivered"
                or row.get("external_delivery_attempted") is not False
                or row.get("external_delivery_verified") is not False
                or row.get("caller_claims_verified") is not False
                or row.get("provisional_target_only") is not True
                or row.get("production_ready") is not False
                or row.get("promotion_authorized") is not False
                or row.get("candidate_execution_authorized") is not False
                or row.get("cutover_authorized") is not False
                or row.get("runtime_egress_trace_complete") is not False
                or row.get("target_outbound_census_binding")
                != _target_census_ledger_binding()
                or row.get("target_outbound_census_sha256")
                != TARGET_OUTBOUND_CENSUS_SHA256
                or row.get("target_outbound_census_status")
                != TARGET_OUTBOUND_CENSUS_STATUS
                or row.get("record_only_coverage_complete") is not False
                or row.get("blockers") != list(BLOCKERS)
            ):
                raise RecordOnlyError(f"invalid record safety invariant at line {line_number}")
            previous_hmac = row_hmac
            key = row.get("dedupe_key")
            record_id = row.get("record_id")
            if (
                not isinstance(key, str)
                or not re.fullmatch(r"dedupe:[0-9a-f]{64}", key)
                or not isinstance(record_id, str)
                or record_id != f"record:{key.split(':', 1)[1][:32]}"
                or row.get("simulated_message_id")
                != f"rec_{key.split(':', 1)[1][:24]}"
            ):
                raise RecordOnlyError(f"invalid record identity at line {line_number}")
            if record_id in record_ids and record_ids[record_id] != key:
                raise RecordOnlyError(f"record id collision at line {line_number}")
            if key in dedupe_keys:
                raise RecordOnlyError(f"duplicate dedupe identity at line {line_number}")
            record_ids[record_id] = key
            dedupe_keys.add(key)
            attempt_count = row.get("attempt_count")
            if isinstance(attempt_count, bool) or not isinstance(attempt_count, int) or attempt_count < 1:
                raise RecordOnlyError(f"invalid attempt count at line {line_number}")
            if row.get("recorded_payload_hash") != _sha(_canonical(row.get("payload"))):
                raise RecordOnlyError(f"recorded payload hash mismatch at line {line_number}")
            destination = row.get("destination")
            if not isinstance(destination, dict) or set(destination) != {
                "kind",
                "id_hash",
                "thread_id_hash",
                "message_id_hash",
            }:
                raise RecordOnlyError(f"invalid destination envelope at line {line_number}")
            for hash_field in (
                destination.get("id_hash"),
                destination.get("thread_id_hash"),
                destination.get("message_id_hash"),
                row.get("task_id_hash"),
                row.get("caller_dedupe_key_hash"),
            ):
                if hash_field is not None and not (
                    isinstance(hash_field, str)
                    and re.fullmatch(r"hmac-sha256:[0-9a-f]{64}", hash_field)
                ):
                    raise RecordOnlyError(f"invalid HMAC identifier at line {line_number}")
            recorded_content = {
                "payload": row.get("payload"),
                "metadata": row.get("metadata"),
                "links": row.get("links"),
            }
            _reject_secrets(recorded_content)
            link_values = row.get("links")
            if not isinstance(link_values, list) or not all(
                isinstance(link, str) for link in link_values
            ):
                raise RecordOnlyError(f"invalid links at line {line_number}")
            _reject_credential_links(link_values)
            if FEISHU_ID_RE.search(_canonical(recorded_content).decode("utf-8")):
                raise RecordOnlyError(f"raw Feishu identifier persisted at line {line_number}")
            rows.append(row)
        if header.get("chain_head_hmac_sha256") != previous_hmac:
            raise RecordOnlyError("ledger chain head does not match authenticated header")
        if generation < self._last_seen_generation:
            raise RecordOnlyError("record ledger generation rolled back in this process")
        if (
            generation == self._last_seen_generation
            and self._last_seen_head is not None
            and previous_hmac != self._last_seen_head
        ):
            raise RecordOnlyError("record ledger fork detected in this process")
        self._last_seen_generation = generation
        self._last_seen_head = previous_hmac
        return rows, data, generation, previous_hmac

    def _atomic_records_write(
        self, rows: list[dict[str, Any]], *, generation: int
    ) -> None:
        self._assert_root_identity()
        _, payload, expected_head = self._encode_ledger(rows, generation=generation)
        temp_name = f".outbound-records.tmp.{os.getpid()}.{secrets.token_hex(6)}"
        flags = (
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
        )
        fd = -1
        try:
            fd = os.open(temp_name, flags, 0o600, dir_fd=self._root_fd)
            offset = 0
            while offset < len(payload):
                written = os.write(fd, payload[offset:])
                if written < 1:
                    raise RecordOnlyError("short write while persisting record ledger")
                offset += written
            os.fsync(fd)
            temp_info = os.fstat(fd)
            if (
                not stat.S_ISREG(temp_info.st_mode)
                or temp_info.st_uid != os.getuid()
                or temp_info.st_mode & 0o077
                or temp_info.st_nlink != 1
            ):
                raise RecordOnlyError("temporary ledger ownership/type/link check failed")
            if self._crash_hook:
                self._crash_hook("before_replace")
            self._assert_root_identity()
            temp_info = os.fstat(fd)
            if temp_info.st_nlink != 1:
                raise RecordOnlyError("temporary ledger was linked before replace")
            os.replace(
                temp_name,
                self.ledger.name,
                src_dir_fd=self._root_fd,
                dst_dir_fd=self._root_fd,
            )
            ledger_info = os.stat(self.ledger.name, dir_fd=self._root_fd, follow_symlinks=False)
            temp_info = os.fstat(fd)
            if (
                (ledger_info.st_dev, ledger_info.st_ino)
                != (temp_info.st_dev, temp_info.st_ino)
                or temp_info.st_nlink != 1
            ):
                raise RecordOnlyError("ledger path changed during atomic replace")
            os.fsync(self._root_fd)
            self._assert_root_identity()
            if self._crash_hook:
                self._crash_hook("after_replace")
            _, persisted, persisted_generation, persisted_head = self._read_records()
            if (
                persisted != payload
                or persisted_generation != generation
                or persisted_head != expected_head
            ):
                raise RecordOnlyError("persisted ledger failed post-write verification")
        finally:
            if fd >= 0:
                os.close(fd)
            try:
                os.unlink(temp_name, dir_fd=self._root_fd)
            except FileNotFoundError:
                pass

    def _open_transaction_lock(self) -> int:
        flags = (
            os.O_RDONLY
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
        )
        try:
            transaction_fd = os.open(".", flags, dir_fd=self._root_fd)
        except OSError as exc:
            raise RecordOnlyError(f"record root transaction lock cannot be opened: {exc}") from exc
        try:
            if self._validated_root_identity(transaction_fd) != self._root_identity:
                raise RecordOnlyError("record root transaction lock identity changed")
            fcntl.flock(transaction_fd, fcntl.LOCK_EX)
            self._assert_root_identity()
            return transaction_fd
        except Exception:
            os.close(transaction_fd)
            raise

    def _open_lock_sentinel(self) -> int:
        try:
            lock_fd = os.open(
                self.lock.name,
                os.O_RDWR
                | os.O_CREAT
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0)
                | getattr(os, "O_NONBLOCK", 0),
                0o600,
                dir_fd=self._root_fd,
            )
        except OSError as exc:
            raise RecordOnlyError(f"record lock sentinel cannot be safely opened: {exc}") from exc
        try:
            self._assert_lock_sentinel(lock_fd)
            return lock_fd
        except Exception:
            os.close(lock_fd)
            raise

    def _assert_lock_sentinel(self, lock_fd: int) -> None:
        self._assert_root_identity()
        lock_info = os.fstat(lock_fd)
        try:
            path_info = os.stat(self.lock.name, dir_fd=self._root_fd, follow_symlinks=False)
        except OSError as exc:
            raise RecordOnlyError(f"record lock sentinel path changed: {exc}") from exc
        if (
            not stat.S_ISREG(lock_info.st_mode)
            or lock_info.st_uid != os.getuid()
            or lock_info.st_mode & 0o077
            or lock_info.st_nlink != 1
            or lock_info.st_size != 0
            or not stat.S_ISREG(path_info.st_mode)
            or (path_info.st_dev, path_info.st_ino) != (lock_info.st_dev, lock_info.st_ino)
        ):
            raise RecordOnlyError("record lock ownership/mode/link/identity check failed")

    def record(
        self,
        *,
        operation: str,
        platform: str,
        destination_kind: str,
        destination_id: str,
        payload_type: str,
        payload: Any,
        task_id: str | None = None,
        thread_id: str | None = None,
        message_id: str | None = None,
        mention_ids: list[str] | None = None,
        link_values: list[str] | None = None,
        terminal_state: str | None = None,
        reply_mode: str = "none",
        update_mode: str = "none",
        caller_dedupe_key: str | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> RecordResult:
        if operation not in OPERATIONS:
            raise RecordOnlyError(f"unsupported outbound operation: {operation}")
        platform = _safe_name(platform, field="platform")
        destination_kind = _safe_name(destination_kind, field="destination_kind")
        payload_type = _safe_name(payload_type, field="payload_type")
        reply_mode = _safe_name(reply_mode, field="reply_mode")
        update_mode = _safe_name(update_mode, field="update_mode")
        if not isinstance(destination_id, str) or not destination_id:
            raise RecordOnlyError("destination_id must be a non-empty string")
        _check_bounded_string(destination_id, field="destination_id")
        for name, value in (("thread_id", thread_id), ("message_id", message_id)):
            if value is not None and not isinstance(value, str):
                raise RecordOnlyError(f"{name} must be a string or null")
            if value is not None:
                if not value:
                    raise RecordOnlyError(f"{name} must not be empty when provided")
                _check_bounded_string(value, field=name)
        if caller_dedupe_key is not None and not isinstance(caller_dedupe_key, str):
            raise RecordOnlyError("caller_dedupe_key must be a string or null")
        if caller_dedupe_key is not None:
            if not caller_dedupe_key:
                raise RecordOnlyError("caller_dedupe_key must not be empty when provided")
            _check_bounded_string(caller_dedupe_key, field="caller_dedupe_key")
        if task_id is not None:
            task_id = _safe_name(task_id, field="task_id", pattern=TASK_ID_RE)
        if terminal_state is not None:
            terminal_state = _safe_name(terminal_state, field="terminal_state")
        if mention_ids is not None and not isinstance(mention_ids, list):
            raise RecordOnlyError("mention_ids must be a list")
        if not all(isinstance(item, str) and item for item in (mention_ids or [])):
            raise RecordOnlyError("mention_ids must contain non-empty strings")
        mentions = sorted(set(mention_ids or []) | set(self._extract_mention_ids(payload)))
        for mention in mentions:
            _check_bounded_string(mention, field="mention_id")
        if link_values is not None and not isinstance(link_values, list):
            raise RecordOnlyError("link_values must be a list")
        explicit_links = link_values or []
        if not all(isinstance(item, str) and item for item in explicit_links):
            raise RecordOnlyError("link_values must contain non-empty strings")
        for link in explicit_links:
            _check_bounded_string(link, field="link_value")
        if metadata is not None and not isinstance(metadata, dict):
            raise RecordOnlyError("metadata must be an object")
        metadata_value = dict(metadata or {})
        mentions = sorted(set(mentions) | set(self._extract_mention_ids(metadata_value)))
        for mention in mentions:
            _check_bounded_string(mention, field="mention_id")
        _validate_routing_claims(
            operation=operation,
            payload_type=payload_type,
            thread_id=thread_id,
            message_id=message_id,
            reply_mode=reply_mode,
            update_mode=update_mode,
        )
        _validate_json_shape(payload, field="payload")
        _validate_json_shape(metadata_value, field="metadata")
        raw_payload = _canonical(payload)
        raw_metadata = _canonical(metadata_value)
        if len(raw_payload) > MAX_VALUE_BYTES or len(raw_metadata) > MAX_VALUE_BYTES:
            raise RecordOnlyError("payload or metadata exceeds bounded record-only size")
        _reject_secrets(payload)
        _reject_secrets(metadata_value)
        _reject_secrets(explicit_links)

        explicit_ids = {
            value
            for value in [destination_id, thread_id, message_id, *mentions]
            if isinstance(value, str) and value
        }
        raw_payload_hash = hmac.new(self._content_key, raw_payload, hashlib.sha256).hexdigest()
        raw_metadata_hash = hmac.new(
            self._content_key, raw_metadata, hashlib.sha256
        ).hexdigest()
        redacted_payload = self._redact_value(payload, explicit_ids)
        redacted_metadata = self._redact_value(metadata_value, explicit_ids)
        links = [
            self._redact_value(link, explicit_ids)
            for link in sorted(
                set(self._extract_links(payload))
                | set(self._extract_links(metadata_value))
                | set(explicit_links)
            )
        ]
        _reject_credential_links(links)
        dedupe_material = {
            "source_component": self.source_component,
            "operation": operation,
            "platform": platform,
            "destination_kind": destination_kind,
            "destination_id_hash": self._hash_id(destination_id),
            "thread_id_hash": self._hash_id(thread_id),
            "message_id_hash": self._hash_id(message_id),
            "task_id_hash": self._hash_id(task_id),
            "payload_type": payload_type,
            "payload_hash": raw_payload_hash,
            "metadata_hash": raw_metadata_hash,
            "mention_id_hashes": [self._hash_id(item) for item in mentions],
            "links": links,
            "terminal_state": terminal_state,
            "reply_mode": reply_mode,
            "update_mode": update_mode,
            "caller_dedupe_key_hash": self._hash_id(caller_dedupe_key),
        }
        dedupe_key = f"dedupe:{_sha(_canonical(dedupe_material))}"
        record_id = f"record:{dedupe_key.split(':', 1)[1][:32]}"
        synthetic_message_id = f"rec_{dedupe_key.split(':', 1)[1][:24]}"
        observed_time = self._clock()
        if not isinstance(observed_time, datetime) or observed_time.tzinfo is None:
            raise RecordOnlyError("record-only clock must return a timezone-aware datetime")
        recorded_at = observed_time.astimezone(timezone.utc).isoformat()
        record = {
            "schema_version": SCHEMA_VERSION,
            "kind": "outbound_record",
            "record_id": record_id,
            "recorded_at": recorded_at,
            "first_recorded_at": recorded_at,
            "last_recorded_at": recorded_at,
            "attempt_count": 1,
            "source_component": self.source_component,
            "operation": operation,
            "platform": platform,
            "destination": {
                "kind": destination_kind,
                "id_hash": self._hash_id(destination_id),
                "thread_id_hash": self._hash_id(thread_id),
                "message_id_hash": self._hash_id(message_id),
            },
            "payload_type": payload_type,
            "payload": redacted_payload,
            "payload_hmac_sha256": raw_payload_hash,
            "recorded_payload_hash": _sha(_canonical(redacted_payload)),
            "mentions": [{"id_hash": self._hash_id(item)} for item in mentions],
            "links": links,
            "task_id_hash": self._hash_id(task_id),
            "terminal_state": terminal_state,
            "reply_mode": reply_mode,
            "update_mode": update_mode,
            "dedupe_key": dedupe_key,
            "caller_dedupe_key_hash": self._hash_id(caller_dedupe_key),
            "metadata": redacted_metadata,
            "metadata_hmac_sha256": raw_metadata_hash,
            "simulated_message_id": synthetic_message_id,
            "record_only": True,
            "simulated": True,
            "success_scope": "record_persisted_not_delivered",
            "external_delivery_attempted": False,
            "external_delivery_verified": False,
            "caller_claims_verified": False,
            "provisional_target_only": True,
            "production_ready": False,
            "promotion_authorized": False,
            "candidate_execution_authorized": False,
            "cutover_authorized": False,
            "runtime_egress_trace_complete": False,
            "target_outbound_census_binding": _target_census_ledger_binding(),
            "target_outbound_census_sha256": TARGET_OUTBOUND_CENSUS_SHA256,
            "target_outbound_census_status": TARGET_OUTBOUND_CENSUS_STATUS,
            "record_only_coverage_complete": False,
            "blockers": list(BLOCKERS),
        }

        transaction_fd = self._open_transaction_lock()
        lock_fd = -1
        try:
            lock_fd = self._open_lock_sentinel()
            rows, _, generation, _ = self._read_records()
            caller_key_hash = self._hash_id(caller_dedupe_key)
            for index, existing in enumerate(rows):
                if (
                    caller_key_hash is not None
                    and existing.get("source_component") == self.source_component
                    and existing.get("caller_dedupe_key_hash") == caller_key_hash
                    and existing.get("dedupe_key") != dedupe_key
                ):
                    raise RecordOnlyError("caller dedupe key reused for a different outbound envelope")
                if existing["dedupe_key"] == dedupe_key:
                    updated = dict(existing)
                    updated["attempt_count"] += 1
                    updated["last_recorded_at"] = recorded_at
                    rows[index] = updated
                    self._atomic_records_write(rows, generation=generation + 1)
                    self._assert_lock_sentinel(lock_fd)
                    return RecordResult(
                        success=True,
                        record_id=existing["record_id"],
                        message_id=existing["simulated_message_id"],
                        duplicate=True,
                        dedupe_key=dedupe_key,
                        attempt_count=updated["attempt_count"],
                    )
                if (
                    existing.get("record_id") == record_id
                    or existing.get("simulated_message_id") == synthetic_message_id
                ):
                    raise RecordOnlyError("synthetic record identity collision")
            rows.append(record)
            self._atomic_records_write(rows, generation=generation + 1)
            self._assert_lock_sentinel(lock_fd)
        finally:
            try:
                if lock_fd >= 0:
                    os.close(lock_fd)
            finally:
                try:
                    fcntl.flock(transaction_fd, fcntl.LOCK_UN)
                finally:
                    os.close(transaction_fd)
        return RecordResult(
            success=True,
            record_id=record_id,
            message_id=synthetic_message_id,
            duplicate=False,
            dedupe_key=dedupe_key,
            attempt_count=1,
        )

    def read_all(self) -> list[dict[str, Any]]:
        rows, _, _, _ = self._read_records()
        return rows

    @staticmethod
    def safety_status() -> dict[str, Any]:
        return {
            "provisional_target_only": True,
            "production_ready": False,
            "promotion_authorized": False,
            "candidate_execution_authorized": False,
            "cutover_authorized": False,
            "record_only": True,
            "simulated": True,
            "success_scope": "record_persisted_not_delivered",
            "external_delivery_attempted": False,
            "external_delivery_verified": False,
            "caller_claims_verified": False,
            "record_only_coverage_complete": False,
            "external_outbound_census": AUTHORITATIVE_CENSUS_BINDING.as_status(),
            "blockers": list(BLOCKERS),
        }


def _parse_target(target: str) -> tuple[str, str, str | None]:
    if not isinstance(target, str):
        raise RecordOnlyError("target must be a string")
    parts = target.split(":", 2)
    if len(parts) < 2 or not parts[0] or not parts[1]:
        raise RecordOnlyError("target must be platform:id[:thread]")
    return parts[0].lower(), parts[1], parts[2] if len(parts) == 3 and parts[2] else None


class RecordOnlyRelaySender:
    """Drop-in record sender for relay text and task-card call sites."""

    def __init__(self, transport: RecordOnlyOutboundTransport) -> None:
        self.transport = transport

    def send(self, args: dict[str, Any]) -> str:
        if not isinstance(args, dict) or args.get("action", "send") != "send":
            return json.dumps(
                {
                    "success": False,
                    "error": "record-only sender supports action=send only",
                    **RecordOnlyOutboundTransport.safety_status(),
                },
                sort_keys=True,
            )
        if not isinstance(args.get("message"), str):
            raise RecordOnlyError("relay message must be a string")
        target = args.get("target")
        if not isinstance(target, str):
            raise RecordOnlyError("relay target must be a string")
        platform, destination_id, thread_id = _parse_target(target)
        result = self.transport.record(
            operation="text_reply" if thread_id else "text_send",
            platform=platform,
            destination_kind="thread" if thread_id else "chat",
            destination_id=destination_id,
            thread_id=thread_id,
            payload_type="text",
            payload=args["message"],
            task_id=args.get("task_id"),
            terminal_state=args.get("terminal_state"),
            reply_mode="thread" if thread_id else "none",
            caller_dedupe_key=args.get("dedupe_key"),
        )
        return json.dumps(
            {
                "success": True,
                "platform": platform,
                "message_id": result.message_id,
                "record_id": result.record_id,
                "duplicate": result.duplicate,
                **self.transport.safety_status(),
            },
            sort_keys=True,
        )

    def send_task_card(
        self, target: str, card_payload: dict[str, Any], message_id: str | None = None
    ) -> dict[str, Any]:
        platform, destination_id, thread_id = _parse_target(target)
        result = self.transport.record(
            operation="card_update" if message_id else "card_send",
            platform=platform,
            destination_kind="message" if message_id else ("thread" if thread_id else "chat"),
            destination_id=destination_id,
            thread_id=thread_id,
            message_id=message_id,
            payload_type="interactive_card",
            payload=card_payload,
            reply_mode="thread" if thread_id else "none",
            update_mode="patch" if message_id else "create",
        )
        return {
            "success": True,
            "message_id": result.message_id,
            "record_id": result.record_id,
            "duplicate": result.duplicate,
            "updated": False,
            "simulated_update_recorded": bool(message_id),
            **self.transport.safety_status(),
        }


class GatewayRecordAdapter:
    """Minimal gateway adapter surface for offline send/reply/update smoke."""

    def __init__(self, transport: RecordOnlyOutboundTransport, platform: str = "feishu") -> None:
        self.transport = transport
        self.platform = _safe_name(platform, field="platform")

    async def send(
        self, chat_id: str, content: str, *, metadata: dict[str, Any] | None = None
    ) -> RecordSendResult:
        if not isinstance(chat_id, str) or not chat_id:
            raise RecordOnlyError("gateway chat_id must be a non-empty string")
        if not isinstance(content, str):
            raise RecordOnlyError("gateway content must be a string")
        if metadata is not None and not isinstance(metadata, dict):
            raise RecordOnlyError("gateway metadata must be an object")
        meta = metadata or {}
        thread_id = meta.get("thread_id") or None
        message_id = meta.get("reply_to_message_id") or None
        if thread_id is not None and not isinstance(thread_id, str):
            raise RecordOnlyError("gateway thread_id must be a string")
        if message_id is not None and not isinstance(message_id, str):
            raise RecordOnlyError("gateway reply_to_message_id must be a string")
        result = self.transport.record(
            operation="text_reply" if (thread_id or message_id) else "text_send",
            platform=self.platform,
            destination_kind="thread" if thread_id else "chat",
            destination_id=chat_id,
            thread_id=thread_id,
            message_id=message_id,
            payload_type="text",
            payload=content,
            reply_mode="message" if message_id else ("thread" if thread_id else "none"),
            metadata=meta,
        )
        return RecordSendResult(success=True, message_id=result.message_id)

    async def edit_message(
        self,
        chat_id: str,
        message_id: str,
        text: str,
        *,
        metadata: dict[str, Any] | None = None,
    ) -> RecordSendResult:
        if not isinstance(chat_id, str) or not chat_id:
            raise RecordOnlyError("gateway chat_id must be a non-empty string")
        if not isinstance(message_id, str) or not message_id:
            raise RecordOnlyError("gateway message_id must be a non-empty string")
        if not isinstance(text, str):
            raise RecordOnlyError("gateway text must be a string")
        if metadata is not None and not isinstance(metadata, dict):
            raise RecordOnlyError("gateway metadata must be an object")
        result = self.transport.record(
            operation="text_update",
            platform=self.platform,
            destination_kind="message",
            destination_id=chat_id,
            message_id=message_id,
            payload_type="text",
            payload=text,
            update_mode="patch",
            metadata=metadata or {},
        )
        return RecordSendResult(success=True, message_id=result.message_id)
