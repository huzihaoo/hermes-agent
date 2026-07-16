#!/usr/bin/env python3
"""Stage Feishu API-poll ownership across a Gateway cutover without live writes."""

from __future__ import annotations

import argparse
import ast
import contextlib
import fcntl
import hashlib
import json
import math
import os
import platform
import pwd
import re
import stat
import subprocess
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from io import StringIO
from pathlib import Path
from typing import Any, Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from dotenv import dotenv_values

from gateway.platforms.feishu import (
    FeishuAdapter,
    _API_POLL_SIDECAR_SCHEMA,
    _MAX_API_POLL_PENDING_PER_CHAT,
    _MAX_API_POLL_PER_CHAT_BYTES,
    _MAX_API_POLL_TOTAL_BYTES,
)
from scripts import pnc_rca_cutover_guard as cutover_guard


PLAN_SCHEMA_VERSION = "pnc_rca_feishu_ingress_hold_plan_v1"
RUN_IDENTITY_SCHEMA_VERSION = "pnc_rca_feishu_ingress_hold_run_identity_v1"
CHAT_SNAPSHOT_SCHEMA_VERSION = "pnc_rca_feishu_readonly_chat_snapshot_v1"
SIDECAR_IDENTITY_SCHEMA_VERSION = "pnc_rca_feishu_sidecar_identity_v1"
ADAPTER_IDENTITY_SCHEMA_VERSION = "pnc_rca_feishu_adapter_identity_v1"
APPROVAL_SCHEMA_VERSION = "pnc_rca_feishu_ingress_hold_approval_v1"
APPROVAL_IDENTITY_SCHEMA_VERSION = "pnc_rca_feishu_ingress_hold_identity_v1"
CUTOVER_BINDING_SCHEMA_VERSION = "pnc_rca_feishu_ingress_hold_cutover_v2"
GATE_VALIDATION_SCHEMA_VERSION = "pnc_rca_feishu_ingress_hold_gate_validation_v2"
APPLY_RECEIPT_SCHEMA_VERSION = "pnc_rca_feishu_ingress_hold_apply_receipt_v1"
APPLY_MANIFEST_SCHEMA_VERSION = "pnc_rca_feishu_ingress_hold_manifest_v1"

PLAN_FILENAME = "feishu_ingress_hold_plan.json"
STAGED_SIDECAR_FILENAME = "feishu_api_poll_state_v1.staged.json"
APPLY_RECEIPT_FILENAME = "feishu_ingress_hold_apply_receipt.json"
APPLY_MANIFEST_FILENAME = "feishu_ingress_hold_manifest.json"
RUN_IDENTITY_FILENAME = "run_identity.json"
RUN_LOCK_FILENAME = ".ingress-hold.lock"

# Hermes 0.18.2 keeps the historical gateway module as a compatibility alias.
# Bind the hold contract to the committed implementation that owns the sidecar.
ADAPTER_RELATIVE_PATH = "plugins/platforms/feishu/adapter.py"
DEFAULT_CANONICAL_GATEWAY_ROOT = Path("/Users/songying/.hermes/runtime/hermes-live")
EMPTY_SHA256 = hashlib.sha256(b"").hexdigest()
MAX_FILE_BYTES = 20 * 1024 * 1024
MAX_PLAN_BYTES = 20 * 1024 * 1024
MAX_API_RESPONSE_BYTES = 8 * 1024 * 1024
MAX_APPROVAL_VALIDITY_SECONDS = 24 * 60 * 60
MAX_CUTOVER_WINDOW_SECONDS = 60 * 60
MAX_FUTURE_SKEW_SECONDS = 300
CHAT_ID_RE = re.compile(r"oc_[A-Za-z0-9_-]{16,255}\Z")
HOLD_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{7,127}\Z")
RELEASE_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{7,127}\Z")
COMMIT_SHA_RE = re.compile(r"[0-9a-f]{40}\Z")
SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
NONCE_RE = re.compile(r"[A-Za-z0-9_-]{16,128}\Z")
SENSITIVE_KEY_RE = re.compile(
    r"(?:password|passwd|secret|token|credential|private[_-]?key)",
    re.IGNORECASE,
)

APPLY_ACTION_SET = (
    "stage_feishu_ingress_hold_sidecar",
    "install_staged_sidecar_during_gateway_cutover",
)


class IngressHoldError(ValueError):
    """One immutable ingress-hold or sidecar invariant failed closed."""

    def __init__(self, code: str, detail: str = ""):
        super().__init__(detail or code)
        self.code = code
        self.detail = detail or code


@dataclass(frozen=True)
class HoldInputs:
    env_file: Path
    host_candidate: Path
    live_sidecar: Path
    chat_ids: tuple[str, ...]
    hold_id: str
    run_root: Path
    canonical_gateway_root: Path = DEFAULT_CANONICAL_GATEWAY_ROOT
    approval_receipt: Path | None = None
    cutover_binding: Path | None = None
    page_size: int = 50
    max_pages: int = 200


@dataclass(frozen=True)
class HoldResult:
    phase: str
    run_root: Path
    artifact_path: Path
    body: Mapping[str, Any]
    resumed: bool


@dataclass(frozen=True)
class _OwnedFile:
    path: Path
    raw: bytes
    stat_result: os.stat_result

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.raw).hexdigest()


@dataclass(frozen=True)
class _OwnedJson(_OwnedFile):
    body: Mapping[str, Any]


class ReadOnlyMessageApi(Protocol):
    def snapshot_chat(
        self,
        chat_id: str,
        *,
        floor_ms: int,
        page_size: int,
        max_pages: int,
    ) -> Mapping[str, Any]: ...


def _canonical_json(value: Any) -> bytes:
    try:
        return (
            json.dumps(
                value,
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
            + "\n"
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise IngressHoldError("ingress_hold_json_invalid") from exc


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).rstrip(b"\n")).hexdigest()


def _require_sha256(value: Any, *, code: str) -> str:
    if not isinstance(value, str) or SHA256_RE.fullmatch(value) is None:
        raise IngressHoldError(code)
    return value


def _strict_json(raw: bytes, *, artifact: str) -> Mapping[str, Any]:
    if not raw or len(raw) > MAX_FILE_BYTES:
        raise IngressHoldError(f"{artifact}_size_invalid")

    def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in items:
            if key in result:
                raise IngressHoldError(f"{artifact}_duplicate_key")
            result[key] = value
        return result

    try:
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=pairs,
            parse_constant=lambda _value: (_ for _ in ()).throw(
                IngressHoldError(f"{artifact}_number_invalid")
            ),
        )
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise IngressHoldError(f"{artifact}_json_invalid") from exc
    if not isinstance(value, dict):
        raise IngressHoldError(f"{artifact}_shape_invalid")
    return value


def _read_stable_file(
    path: Path,
    *,
    artifact: str,
    require_owner_only: bool,
) -> _OwnedFile:
    absolute = path.expanduser().absolute()
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(absolute, flags)
    except OSError as exc:
        raise IngressHoldError(f"{artifact}_unavailable") from exc
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
            or before.st_size < 0
            or before.st_size > MAX_FILE_BYTES
            or (require_owner_only and before.st_uid != os.geteuid())
            or (require_owner_only and stat.S_IMODE(before.st_mode) != 0o600)
        ):
            raise IngressHoldError(f"{artifact}_identity_invalid")
        chunks: list[bytes] = []
        remaining = before.st_size
        while remaining:
            chunk = os.read(descriptor, min(1024 * 1024, remaining))
            if not chunk:
                raise IngressHoldError(f"{artifact}_unstable")
            chunks.append(chunk)
            remaining -= len(chunk)
        if os.read(descriptor, 1):
            raise IngressHoldError(f"{artifact}_unstable")
        after = os.fstat(descriptor)
        lexical = os.lstat(absolute)
        stable_fields = (
            "st_dev",
            "st_ino",
            "st_mode",
            "st_nlink",
            "st_uid",
            "st_size",
            "st_mtime_ns",
            "st_ctime_ns",
        )
        if stat.S_ISLNK(lexical.st_mode) or any(
            getattr(before, field) != getattr(after, field)
            or getattr(before, field) != getattr(lexical, field)
            for field in stable_fields
        ):
            raise IngressHoldError(f"{artifact}_unstable")
        return _OwnedFile(absolute, b"".join(chunks), after)
    except OSError as exc:
        raise IngressHoldError(f"{artifact}_unstable") from exc
    finally:
        os.close(descriptor)


def _read_owned_json(path: Path, *, artifact: str) -> _OwnedJson:
    owned = _read_stable_file(path, artifact=artifact, require_owner_only=True)
    return _OwnedJson(
        path=owned.path,
        raw=owned.raw,
        stat_result=owned.stat_result,
        body=_strict_json(owned.raw, artifact=artifact),
    )


def _parse_timestamp(value: Any, *, artifact: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise IngressHoldError(f"{artifact}_timestamp_invalid")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise IngressHoldError(f"{artifact}_timestamp_invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise IngressHoldError(f"{artifact}_timestamp_invalid")
    return parsed.astimezone(timezone.utc)


def _read_env(path: Path) -> tuple[_OwnedFile, Mapping[str, str]]:
    owned = _read_stable_file(
        path,
        artifact="feishu_ingress_hold_env",
        require_owner_only=True,
    )
    try:
        parsed = dotenv_values(
            stream=StringIO(owned.raw.decode("utf-8")),
            interpolate=False,
        )
    except (UnicodeError, ValueError) as exc:
        raise IngressHoldError("feishu_ingress_hold_env_invalid") from exc
    values = {str(key): str(value) for key, value in parsed.items() if value is not None}
    for key in ("FEISHU_APP_ID", "FEISHU_APP_SECRET"):
        if not values.get(key, "").strip():
            raise IngressHoldError("feishu_ingress_hold_credentials_missing")
    domain = values.get("FEISHU_DOMAIN", "feishu").strip().lower()
    if domain not in {"feishu", "lark"}:
        raise IngressHoldError("feishu_ingress_hold_domain_invalid")
    return owned, values


def _sensitive_values(environment: Mapping[str, str]) -> tuple[str, ...]:
    return tuple(
        sorted(
            {
                value
                for key, value in environment.items()
                if len(value) >= 4 and SENSITIVE_KEY_RE.search(key)
            },
            key=len,
            reverse=True,
        )
    )


def _assert_redacted(value: Any, *, secrets: Sequence[str]) -> None:
    raw = _canonical_json(value)
    for secret in secrets:
        if secret.encode("utf-8") in raw:
            raise IngressHoldError("feishu_ingress_hold_credential_leak")


def _fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    descriptor = os.open(path, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _read_published(path: Path) -> bytes:
    owned = _read_stable_file(
        path,
        artifact="feishu_ingress_hold_artifact",
        require_owner_only=True,
    )
    return owned.raw


def _publish_no_clobber(path: Path, body: Mapping[str, Any]) -> bool:
    payload = _canonical_json(body)
    if len(payload) > MAX_PLAN_BYTES:
        raise IngressHoldError("feishu_ingress_hold_artifact_too_large")
    digest = hashlib.sha256(payload).hexdigest()
    temporary = path.parent / f".{path.name}.{digest}.tmp"
    if path.exists():
        if _read_published(path) != payload:
            raise IngressHoldError("feishu_ingress_hold_artifact_conflict", path.name)
        if temporary.exists():
            if _read_published(temporary) != payload:
                raise IngressHoldError("feishu_ingress_hold_temporary_conflict")
            temporary.unlink()
            _fsync_directory(path.parent)
        return True
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0)
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(temporary, flags, 0o600)
    except FileExistsError:
        if _read_published(temporary) != payload:
            raise IngressHoldError("feishu_ingress_hold_temporary_conflict")
    else:
        try:
            view = memoryview(payload)
            while view:
                written = os.write(descriptor, view)
                if written <= 0:
                    raise IngressHoldError("feishu_ingress_hold_write_failed")
                view = view[written:]
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        _fsync_directory(path.parent)
    try:
        os.link(temporary, path, follow_symlinks=False)
    except FileExistsError:
        if _read_published(path) != payload:
            raise IngressHoldError("feishu_ingress_hold_artifact_conflict", path.name)
    _fsync_directory(path.parent)
    temporary.unlink()
    _fsync_directory(path.parent)
    return False


def _ensure_run_root(path: Path) -> tuple[Path, bool]:
    root = path.expanduser().absolute()
    if not root.parent.is_dir() or root.parent.is_symlink():
        raise IngressHoldError("feishu_ingress_hold_run_parent_invalid")
    try:
        os.mkdir(root, 0o700)
        created = True
        _fsync_directory(root.parent)
    except FileExistsError:
        created = False
    info = root.lstat()
    if (
        not stat.S_ISDIR(info.st_mode)
        or stat.S_IMODE(info.st_mode) != 0o700
        or info.st_uid != os.geteuid()
        or root.is_symlink()
    ):
        raise IngressHoldError("feishu_ingress_hold_run_root_invalid")
    return root, created


@contextlib.contextmanager
def _run_lock(root: Path):
    path = root / RUN_LOCK_FILENAME
    descriptor = os.open(
        path,
        os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0),
        0o600,
    )
    try:
        info = os.fstat(descriptor)
        if (
            not stat.S_ISREG(info.st_mode)
            or stat.S_IMODE(info.st_mode) != 0o600
            or info.st_uid != os.geteuid()
            or info.st_nlink != 1
        ):
            raise IngressHoldError("feishu_ingress_hold_lock_invalid")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise IngressHoldError("feishu_ingress_hold_in_progress") from exc
        yield
    finally:
        with contextlib.suppress(OSError):
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def _git(repo: Path, *arguments: str, binary: bool = False) -> str | bytes:
    try:
        result = subprocess.run(
            ["git", "-C", str(repo), *arguments],
            check=False,
            capture_output=True,
            text=not binary,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise IngressHoldError("feishu_ingress_hold_git_unavailable") from exc
    if result.returncode != 0:
        raise IngressHoldError("feishu_ingress_hold_git_failed")
    return result.stdout if binary else str(result.stdout).strip()


def _adapter_schema_from_source(raw: bytes) -> str:
    try:
        tree = ast.parse(raw.decode("utf-8"))
    except (UnicodeError, SyntaxError) as exc:
        raise IngressHoldError("feishu_ingress_hold_adapter_source_invalid") from exc
    values = []
    for node in tree.body:
        if (
            isinstance(node, ast.Assign)
            and any(
                isinstance(target, ast.Name)
                and target.id == "_API_POLL_SIDECAR_SCHEMA"
                for target in node.targets
            )
            and isinstance(node.value, ast.Constant)
            and isinstance(node.value.value, str)
        ):
            values.append(node.value.value)
    if values != [_API_POLL_SIDECAR_SCHEMA]:
        raise IngressHoldError("feishu_ingress_hold_adapter_schema_unverified")
    return values[0]


def _host_adapter_identity(root: Path) -> Mapping[str, Any]:
    repo = root.expanduser().resolve()
    if not repo.is_dir():
        raise IngressHoldError("feishu_ingress_hold_host_candidate_missing")
    git_root = Path(str(_git(repo, "rev-parse", "--show-toplevel"))).resolve()
    if git_root != repo:
        raise IngressHoldError("feishu_ingress_hold_host_repo_root_mismatch")
    before = str(_git(repo, "rev-parse", "--verify", "HEAD"))
    if COMMIT_SHA_RE.fullmatch(before) is None:
        raise IngressHoldError("feishu_ingress_hold_host_commit_invalid")
    status = str(
        _git(repo, "status", "--porcelain=v1", "--untracked-files=all")
    )
    if status:
        raise IngressHoldError("feishu_ingress_hold_host_tree_dirty")
    path = repo / ADAPTER_RELATIVE_PATH
    owned = _read_stable_file(
        path,
        artifact="feishu_ingress_hold_adapter",
        require_owner_only=False,
    )
    tracked = str(
        _git(repo, "ls-files", "--error-unmatch", "--", ADAPTER_RELATIVE_PATH)
    )
    committed = _git(
        repo,
        "cat-file",
        "blob",
        f"{before}:{ADAPTER_RELATIVE_PATH}",
        binary=True,
    )
    after = str(_git(repo, "rev-parse", "--verify", "HEAD"))
    if tracked != ADAPTER_RELATIVE_PATH or committed != owned.raw or after != before:
        raise IngressHoldError("feishu_ingress_hold_adapter_not_at_commit")
    return {
        "schema_version": ADAPTER_IDENTITY_SCHEMA_VERSION,
        "repo_root": str(repo),
        "host_commit": before,
        "tree_clean": True,
        "status_sha256": EMPTY_SHA256,
        "adapter_relative_path": ADAPTER_RELATIVE_PATH,
        "adapter_sha256": owned.sha256,
        "adapter_sidecar_schema": _adapter_schema_from_source(owned.raw),
    }


def _empty_state() -> dict[str, Any]:
    return {
        "pending": {},
        "baselined_chat_ids": [],
        "last_seen_create_time_ms": {},
        "cursor_message_ids": {},
        "discovery_floor_ms": {},
        "scan_state": {},
        "terminal_holes": [],
        "seen_message_ids": [],
    }


def _normalize_state(
    value: Any,
    *,
    revision: int,
    app_scope: str,
) -> Mapping[str, Any]:
    probe = object.__new__(FeishuAdapter)
    probe._api_poll_app_scope = app_scope
    probe._dedup_cache_size = 1000
    probe._api_poll_state_error = None
    probe._api_poll_raw_state = None
    probe._api_poll_revision = revision
    probe._api_poll_pending_items = {}
    probe._api_poll_baselined_chat_ids = set()
    probe._api_poll_seen_message_ids = set()
    probe._api_poll_seen_message_order = []
    probe._api_poll_last_seen_create_time_ms = {}
    probe._api_poll_cursor_message_ids = {}
    probe._api_poll_discovery_floor_ms = {}
    probe._api_poll_scan_state = {}
    probe._api_poll_terminal_holes = []
    if not FeishuAdapter._load_api_poll_state(probe, value):
        raise IngressHoldError("feishu_ingress_hold_sidecar_state_invalid")
    return FeishuAdapter._api_poll_state_snapshot(probe)


def _normalized_sidecar_payload(
    value: Mapping[str, Any],
    *,
    expected_app_scope: str,
) -> Mapping[str, Any]:
    if set(value) != {
        "schema_version",
        "app_scope",
        "revision",
        "updated_at",
        "rollback_readiness",
        "state",
    }:
        raise IngressHoldError("feishu_ingress_hold_sidecar_shape_invalid")
    if value.get("schema_version") != _API_POLL_SIDECAR_SCHEMA:
        raise IngressHoldError("feishu_ingress_hold_sidecar_schema_unsupported")
    if value.get("app_scope") != expected_app_scope:
        raise IngressHoldError("feishu_ingress_hold_sidecar_app_scope_mismatch")
    revision = value.get("revision")
    updated_at = value.get("updated_at")
    if (
        isinstance(revision, bool)
        or not isinstance(revision, int)
        or revision < 0
        or isinstance(updated_at, bool)
        or not isinstance(updated_at, (int, float))
        or not math.isfinite(float(updated_at))
        or updated_at < 0
    ):
        raise IngressHoldError("feishu_ingress_hold_sidecar_metadata_invalid")
    state = _normalize_state(
        value.get("state"),
        revision=revision,
        app_scope=expected_app_scope,
    )
    pending_count = sum(len(items) for items in state["pending"].values())
    continuation_count = len(state["scan_state"])
    expected_readiness = {
        "ready": pending_count == 0 and continuation_count == 0,
        "pending_count": pending_count,
        "scan_continuation_count": continuation_count,
    }
    if value.get("rollback_readiness") != expected_readiness:
        raise IngressHoldError("feishu_ingress_hold_sidecar_readiness_invalid")
    normalized = {
        "schema_version": _API_POLL_SIDECAR_SCHEMA,
        "app_scope": expected_app_scope,
        "revision": revision,
        "updated_at": float(updated_at),
        "rollback_readiness": expected_readiness,
        "state": state,
    }
    if len(_canonical_json(normalized)) > _MAX_API_POLL_TOTAL_BYTES:
        raise IngressHoldError("feishu_ingress_hold_sidecar_capacity_exceeded")
    return normalized


def _sidecar_observation(path: Path, *, app_scope: str) -> tuple[Mapping[str, Any], Mapping[str, Any]]:
    absolute = path.expanduser().absolute()
    try:
        lexical = absolute.lstat()
    except FileNotFoundError:
        return (
            {
                "schema_version": SIDECAR_IDENTITY_SCHEMA_VERSION,
                "state": "absent",
                "path": str(absolute),
                "sha256": EMPTY_SHA256,
                "size_bytes": 0,
                "revision": 0,
                "semantic_sha256": _sha256_json(_empty_state()),
            },
            {
                "schema_version": _API_POLL_SIDECAR_SCHEMA,
                "app_scope": app_scope,
                "revision": 0,
                "updated_at": 0.0,
                "rollback_readiness": {
                    "ready": True,
                    "pending_count": 0,
                    "scan_continuation_count": 0,
                },
                "state": _empty_state(),
            },
        )
    if stat.S_ISLNK(lexical.st_mode):
        raise IngressHoldError("feishu_ingress_hold_live_sidecar_symlink")
    owned = _read_stable_file(
        absolute,
        artifact="feishu_ingress_hold_live_sidecar",
        require_owner_only=True,
    )
    payload = _normalized_sidecar_payload(
        _strict_json(owned.raw, artifact="feishu_ingress_hold_live_sidecar"),
        expected_app_scope=app_scope,
    )
    return (
        {
            "schema_version": SIDECAR_IDENTITY_SCHEMA_VERSION,
            "state": "present",
            "path": str(absolute),
            "sha256": owned.sha256,
            "size_bytes": len(owned.raw),
            "revision": payload["revision"],
            "semantic_sha256": _sha256_json(payload),
        },
        payload,
    )


def _message_time_ms(item: Mapping[str, Any]) -> int | None:
    return FeishuAdapter._api_poll_item_create_time_ms(dict(item))


def _normalize_snapshot(
    value: Mapping[str, Any],
    *,
    chat_id: str,
    floor_ms: int,
) -> Mapping[str, Any]:
    if set(value) != {
        "schema_version",
        "chat_id",
        "floor_ms",
        "complete",
        "started_at_ms",
        "completed_at_ms",
        "pages",
        "items",
    }:
        raise IngressHoldError("feishu_ingress_hold_snapshot_shape_invalid")
    if (
        value.get("schema_version") != CHAT_SNAPSHOT_SCHEMA_VERSION
        or value.get("chat_id") != chat_id
        or value.get("floor_ms") != floor_ms
        or value.get("complete") is not True
    ):
        raise IngressHoldError("feishu_ingress_hold_snapshot_identity_invalid")
    started = value.get("started_at_ms")
    completed = value.get("completed_at_ms")
    if (
        isinstance(started, bool)
        or not isinstance(started, int)
        or isinstance(completed, bool)
        or not isinstance(completed, int)
        or started < 0
        or completed < started
    ):
        raise IngressHoldError("feishu_ingress_hold_snapshot_time_invalid")
    pages = value.get("pages")
    items = value.get("items")
    if not isinstance(pages, list) or not pages or not isinstance(items, list):
        raise IngressHoldError("feishu_ingress_hold_snapshot_collection_invalid")
    normalized_pages = []
    for index, page in enumerate(pages):
        if not isinstance(page, Mapping) or set(page) != {
            "page_index",
            "request_cursor_sha256",
            "response_cursor_sha256",
            "item_count",
            "accepted_count",
            "has_more",
            "stopped_at_floor",
        }:
            raise IngressHoldError("feishu_ingress_hold_page_trace_invalid")
        if (
            page.get("page_index") != index
            or isinstance(page.get("item_count"), bool)
            or not isinstance(page.get("item_count"), int)
            or page["item_count"] < 0
            or isinstance(page.get("accepted_count"), bool)
            or not isinstance(page.get("accepted_count"), int)
            or page["accepted_count"] < 0
            or page["accepted_count"] > page["item_count"]
            or not isinstance(page.get("has_more"), bool)
            or not isinstance(page.get("stopped_at_floor"), bool)
        ):
            raise IngressHoldError("feishu_ingress_hold_page_trace_invalid")
        _require_sha256(
            page.get("request_cursor_sha256"),
            code="feishu_ingress_hold_page_cursor_invalid",
        )
        _require_sha256(
            page.get("response_cursor_sha256"),
            code="feishu_ingress_hold_page_cursor_invalid",
        )
        normalized_pages.append(dict(page))
    normalized_items: dict[str, Mapping[str, Any]] = {}
    for raw in items:
        item = FeishuAdapter._validated_api_poll_pending_item(
            raw,
            expected_chat_id=chat_id,
        )
        message_id = str(item.get("message_id") or "").strip()
        observed = _message_time_ms(item)
        if observed is not None and observed < floor_ms:
            continue
        existing = normalized_items.get(message_id)
        if existing is not None and existing != item:
            raise IngressHoldError("feishu_ingress_hold_duplicate_message_conflict")
        normalized_items[message_id] = item
    ordered = sorted(
        normalized_items.values(),
        key=lambda item: (_message_time_ms(item) or 0, str(item["message_id"])),
    )
    if len(ordered) > _MAX_API_POLL_PENDING_PER_CHAT:
        raise IngressHoldError("feishu_ingress_hold_snapshot_capacity_exceeded")
    normalized = {
        "schema_version": CHAT_SNAPSHOT_SCHEMA_VERSION,
        "chat_id": chat_id,
        "floor_ms": floor_ms,
        "complete": True,
        "started_at_ms": started,
        "completed_at_ms": completed,
        "pages": normalized_pages,
        "items": ordered,
    }
    if len(_canonical_json(normalized)) > _MAX_API_POLL_PER_CHAT_BYTES:
        raise IngressHoldError("feishu_ingress_hold_snapshot_capacity_exceeded")
    return normalized


class FeishuReadOnlyMessageApi:
    """Minimal auth + GET-only message pagination; never calls message write APIs."""

    def __init__(
        self,
        environment: Mapping[str, str],
        *,
        opener: Callable[..., Any] = urlopen,
        clock_ms: Callable[[], int] | None = None,
    ):
        self._app_id = environment["FEISHU_APP_ID"]
        self._app_secret = environment["FEISHU_APP_SECRET"]
        self._domain = environment.get("FEISHU_DOMAIN", "feishu").lower()
        self._opener = opener
        self._clock_ms = clock_ms or (lambda: time.time_ns() // 1_000_000)
        self._tenant_token: str | None = None

    @property
    def _base_url(self) -> str:
        return (
            "https://open.larksuite.com"
            if self._domain == "lark"
            else "https://open.feishu.cn"
        )

    def _read_json(self, request: Request, *, timeout: int) -> Mapping[str, Any]:
        try:
            with self._opener(request, timeout=timeout) as response:
                raw = response.read(MAX_API_RESPONSE_BYTES + 1)
        except (HTTPError, URLError, TimeoutError, OSError) as exc:
            raise IngressHoldError("feishu_ingress_hold_read_api_failed") from exc
        if len(raw) > MAX_API_RESPONSE_BYTES:
            raise IngressHoldError("feishu_ingress_hold_api_response_too_large")
        return _strict_json(raw, artifact="feishu_ingress_hold_api_response")

    def _token(self) -> str:
        if self._tenant_token:
            return self._tenant_token
        request = Request(
            f"{self._base_url}/open-apis/auth/v3/tenant_access_token/internal",
            data=json.dumps({
                "app_id": self._app_id,
                "app_secret": self._app_secret,
            }).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        payload = self._read_json(request, timeout=15)
        token = str(payload.get("tenant_access_token") or "").strip()
        if payload.get("code") != 0 or not token:
            raise IngressHoldError("feishu_ingress_hold_auth_failed")
        self._tenant_token = token
        return token

    def snapshot_chat(
        self,
        chat_id: str,
        *,
        floor_ms: int,
        page_size: int,
        max_pages: int,
    ) -> Mapping[str, Any]:
        started_at_ms = self._clock_ms()
        token = self._token()
        page_token = ""
        seen_tokens: set[str] = set()
        messages: dict[str, Mapping[str, Any]] = {}
        traces = []
        for page_index in range(max_pages):
            query = {
                "container_id_type": "chat",
                "container_id": chat_id,
                "sort_type": "ByCreateTimeDesc",
                "page_size": str(page_size),
            }
            if page_token:
                query["page_token"] = page_token
            request = Request(
                f"{self._base_url}/open-apis/im/v1/messages?{urlencode(query)}",
                headers={"Authorization": f"Bearer {token}"},
                method="GET",
            )
            payload = self._read_json(request, timeout=20)
            if payload.get("code") != 0:
                raise IngressHoldError("feishu_ingress_hold_message_list_failed")
            data = payload.get("data")
            if not isinstance(data, Mapping) or not isinstance(data.get("items", []), list):
                raise IngressHoldError("feishu_ingress_hold_message_list_invalid")
            page_items = data.get("items", [])
            accepted = 0
            stopped_at_floor = False
            for raw in page_items:
                item = FeishuAdapter._validated_api_poll_pending_item(
                    raw,
                    expected_chat_id=chat_id,
                )
                observed_ms = _message_time_ms(item)
                if observed_ms is not None and observed_ms < floor_ms:
                    stopped_at_floor = True
                    continue
                message_id = str(item.get("message_id") or "")
                existing = messages.get(message_id)
                if existing is not None and existing != item:
                    raise IngressHoldError(
                        "feishu_ingress_hold_duplicate_message_conflict"
                    )
                messages[message_id] = item
                accepted += existing is None
            has_more = data.get("has_more") is True
            next_token = str(data.get("page_token") or "").strip()
            traces.append({
                "page_index": page_index,
                "request_cursor_sha256": hashlib.sha256(
                    page_token.encode("utf-8")
                ).hexdigest(),
                "response_cursor_sha256": hashlib.sha256(
                    next_token.encode("utf-8")
                ).hexdigest(),
                "item_count": len(page_items),
                "accepted_count": accepted,
                "has_more": has_more,
                "stopped_at_floor": stopped_at_floor,
            })
            if stopped_at_floor or not has_more:
                break
            if not next_token or next_token == page_token or next_token in seen_tokens:
                raise IngressHoldError("feishu_ingress_hold_pagination_cycle")
            seen_tokens.add(next_token)
            page_token = next_token
        else:
            raise IngressHoldError("feishu_ingress_hold_pagination_incomplete")
        result = {
            "schema_version": CHAT_SNAPSHOT_SCHEMA_VERSION,
            "chat_id": chat_id,
            "floor_ms": floor_ms,
            "complete": True,
            "started_at_ms": started_at_ms,
            "completed_at_ms": self._clock_ms(),
            "pages": traces,
            "items": sorted(
                messages.values(),
                key=lambda item: (
                    _message_time_ms(item) or 0,
                    str(item["message_id"]),
                ),
            ),
        }
        return _normalize_snapshot(result, chat_id=chat_id, floor_ms=floor_ms)


def _chat_ids(values: Sequence[str]) -> tuple[str, ...]:
    normalized = tuple(sorted({str(value or "").strip() for value in values}))
    if not normalized or len(normalized) > 32 or any(
        CHAT_ID_RE.fullmatch(value) is None for value in normalized
    ):
        raise IngressHoldError("feishu_ingress_hold_chat_set_invalid")
    return normalized


def _validate_inputs(inputs: HoldInputs, *, phase: str) -> HoldInputs:
    if phase not in {"plan", "apply"}:
        raise IngressHoldError("feishu_ingress_hold_phase_invalid")
    if HOLD_ID_RE.fullmatch(inputs.hold_id) is None:
        raise IngressHoldError("feishu_ingress_hold_id_invalid")
    chats = _chat_ids(inputs.chat_ids)
    if inputs.page_size < 1 or inputs.page_size > 50:
        raise IngressHoldError("feishu_ingress_hold_page_size_invalid")
    if inputs.max_pages < 1 or inputs.max_pages > 1000:
        raise IngressHoldError("feishu_ingress_hold_max_pages_invalid")
    canonical = inputs.canonical_gateway_root.expanduser().absolute()
    if canonical != DEFAULT_CANONICAL_GATEWAY_ROOT:
        raise IngressHoldError("feishu_ingress_hold_canonical_root_invalid")
    run_root = inputs.run_root.expanduser().absolute()
    source = inputs.host_candidate.expanduser().resolve()
    for forbidden in (source, canonical):
        try:
            run_root.relative_to(forbidden)
        except ValueError:
            pass
        else:
            raise IngressHoldError("feishu_ingress_hold_run_root_inside_runtime")
    if phase == "apply" and (
        inputs.approval_receipt is None or inputs.cutover_binding is None
    ):
        raise IngressHoldError("feishu_ingress_hold_apply_authorization_required")
    return HoldInputs(
        env_file=inputs.env_file,
        host_candidate=inputs.host_candidate,
        live_sidecar=inputs.live_sidecar,
        chat_ids=chats,
        hold_id=inputs.hold_id,
        run_root=inputs.run_root,
        canonical_gateway_root=canonical,
        approval_receipt=inputs.approval_receipt,
        cutover_binding=inputs.cutover_binding,
        page_size=inputs.page_size,
        max_pages=inputs.max_pages,
    )


def _app_scope(environment: Mapping[str, str]) -> str:
    return hashlib.sha256(
        f"{environment.get('FEISHU_DOMAIN', 'feishu').lower()}\0"
        f"{environment['FEISHU_APP_ID']}".encode("utf-8")
    ).hexdigest()[:32]


def _floor_by_chat(
    state: Mapping[str, Any],
    *,
    chat_ids: Sequence[str],
    hold_start_ms: int,
) -> Mapping[str, int]:
    cursors = state.get("last_seen_create_time_ms", {})
    floors = state.get("discovery_floor_ms", {})
    result = {}
    for chat_id in chat_ids:
        values = [hold_start_ms]
        for source in (cursors, floors):
            value = source.get(chat_id) if isinstance(source, Mapping) else None
            if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
                values.append(value)
        result[chat_id] = min(values)
    return result


def _run_identity(
    inputs: HoldInputs,
    *,
    env_sha256: str,
    created_at: str,
    hold_start_ms: int,
) -> Mapping[str, Any]:
    return {
        "schema_version": RUN_IDENTITY_SCHEMA_VERSION,
        "hold_id": inputs.hold_id,
        "created_at": created_at,
        "hold_start_ms": hold_start_ms,
        "inputs": {
            "env_file": {
                "path": str(inputs.env_file.expanduser().absolute()),
                "sha256": env_sha256,
            },
            "host_candidate": str(inputs.host_candidate.expanduser().resolve()),
            "live_sidecar": str(inputs.live_sidecar.expanduser().absolute()),
            "canonical_gateway_root": str(inputs.canonical_gateway_root),
            "chat_ids": list(inputs.chat_ids),
            "chat_set_sha256": _sha256_json(list(inputs.chat_ids)),
            "page_size": inputs.page_size,
            "max_pages": inputs.max_pages,
        },
        "side_effect_contract": {
            "feishu_message_writes": False,
            "live_sidecar_writes": False,
            "gateway_process_changes": False,
            "launchctl_invoked": False,
            "auth_token_exchange": True,
            "message_api": "GET_only",
            "output_scope": "unique_owner_only_run_root",
        },
    }


def _snapshot_all(
    reader: ReadOnlyMessageApi,
    *,
    chat_ids: Sequence[str],
    floors: Mapping[str, int],
    page_size: int,
    max_pages: int,
) -> Mapping[str, Any]:
    snapshots = {}
    for chat_id in chat_ids:
        try:
            raw = reader.snapshot_chat(
                chat_id,
                floor_ms=floors[chat_id],
                page_size=page_size,
                max_pages=max_pages,
            )
        except IngressHoldError:
            raise
        except Exception as exc:
            raise IngressHoldError(
                "feishu_ingress_hold_read_api_failed", chat_id
            ) from exc
        snapshots[chat_id] = _normalize_snapshot(
            raw,
            chat_id=chat_id,
            floor_ms=floors[chat_id],
        )
    return snapshots


def _plan_body(
    *,
    inputs: HoldInputs,
    identity: Mapping[str, Any],
    host: Mapping[str, Any],
    sidecar_identity: Mapping[str, Any],
    app_scope: str,
    floors: Mapping[str, int],
    snapshots: Mapping[str, Any],
) -> Mapping[str, Any]:
    body = {
        "schema_version": PLAN_SCHEMA_VERSION,
        "hold_id": inputs.hold_id,
        "created_at": identity["created_at"],
        "production_effects_executed": False,
        "phase": "plan",
        "chat_ids": list(inputs.chat_ids),
        "chat_set_sha256": _sha256_json(list(inputs.chat_ids)),
        "app_scope": app_scope,
        "run_identity_sha256": _sha256_json(identity),
        "host_adapter_identity": host,
        "live_sidecar_identity": sidecar_identity,
        "window": {
            "hold_start_ms": identity["hold_start_ms"],
            "floor_by_chat": dict(floors),
            "snapshot_completed_at_ms": max(
                snapshot["completed_at_ms"] for snapshot in snapshots.values()
            ),
        },
        "api_snapshot": {"chats": snapshots},
        "apply_contract": {
            "approval_schema_version": APPROVAL_SCHEMA_VERSION,
            "cutover_binding_schema_version": CUTOVER_BINDING_SCHEMA_VERSION,
            "gate_validator_required": (
                "validate_feishu_ingress_hold_cutover_binding"
            ),
            "watermark_policy": "preserve_or_increase_never_advance_for_pending",
            "live_install_performed_by_this_tool": False,
        },
        "future_install": {
            "performed": False,
            "requires_separate_cutover": True,
            "canonical_gateway_root": str(inputs.canonical_gateway_root),
            "canonical_sidecar_path": str(inputs.live_sidecar.expanduser().absolute()),
            "procedure": (
                "after verified Gateway writer-stop, copy staged sidecar to an "
                "owner-only same-filesystem sibling temp, fsync and re-hash it, "
                "atomically rename it to the canonical sidecar path, fsync the "
                "directory, then start the exact bound Gateway commit and verify "
                "revision, pending ownership and non-regressing watermarks"
            ),
        },
        "side_effect_contract": identity["side_effect_contract"],
    }
    if len(_canonical_json(body)) > MAX_PLAN_BYTES:
        raise IngressHoldError("feishu_ingress_hold_plan_capacity_exceeded")
    return body


def _validate_plan(
    plan: Mapping[str, Any],
    *,
    inputs: HoldInputs,
    identity: Mapping[str, Any],
    host: Mapping[str, Any],
    sidecar_identity: Mapping[str, Any],
    app_scope: str,
) -> None:
    if (
        plan.get("schema_version") != PLAN_SCHEMA_VERSION
        or plan.get("hold_id") != inputs.hold_id
        or plan.get("production_effects_executed") is not False
        or plan.get("phase") != "plan"
        or plan.get("chat_ids") != list(inputs.chat_ids)
        or plan.get("chat_set_sha256") != _sha256_json(list(inputs.chat_ids))
        or plan.get("app_scope") != app_scope
        or plan.get("run_identity_sha256") != _sha256_json(identity)
        or plan.get("host_adapter_identity") != host
        or plan.get("live_sidecar_identity") != sidecar_identity
    ):
        raise IngressHoldError("feishu_ingress_hold_plan_binding_invalid")
    window = plan.get("window")
    snapshot = plan.get("api_snapshot")
    if not isinstance(window, Mapping) or not isinstance(snapshot, Mapping):
        raise IngressHoldError("feishu_ingress_hold_plan_shape_invalid")
    floors = window.get("floor_by_chat")
    chats = snapshot.get("chats")
    if (
        window.get("hold_start_ms") != identity["hold_start_ms"]
        or not isinstance(floors, Mapping)
        or set(floors) != set(inputs.chat_ids)
        or not isinstance(chats, Mapping)
        or set(chats) != set(inputs.chat_ids)
    ):
        raise IngressHoldError("feishu_ingress_hold_plan_window_invalid")
    for chat_id in inputs.chat_ids:
        _normalize_snapshot(chats[chat_id], chat_id=chat_id, floor_ms=floors[chat_id])


def _machine_identity() -> Mapping[str, str]:
    for source, path in (
        ("etc_machine_id", Path("/etc/machine-id")),
        ("dbus_machine_id", Path("/var/lib/dbus/machine-id")),
    ):
        try:
            value = path.read_text(encoding="ascii").strip()
        except (OSError, UnicodeError):
            continue
        if re.fullmatch(r"[A-Za-z0-9-]{16,128}", value):
            return {
                "source": source,
                "sha256": hashlib.sha256(f"{source}\0{value}".encode()).hexdigest(),
            }
    if platform.system() == "Darwin":
        try:
            result = subprocess.run(
                ["/usr/sbin/ioreg", "-rd1", "-c", "IOPlatformExpertDevice"],
                check=False,
                capture_output=True,
                text=True,
                timeout=3,
            )
        except (OSError, subprocess.SubprocessError):
            result = None
        if result is not None and result.returncode == 0:
            match = re.search(
                r'"IOPlatformUUID"\s*=\s*"([A-Fa-f0-9-]{16,64})"',
                result.stdout,
            )
            if match:
                source = "darwin_ioplatformuuid"
                return {
                    "source": source,
                    "sha256": hashlib.sha256(
                        f"{source}\0{match.group(1).lower()}".encode()
                    ).hexdigest(),
                }
    raise IngressHoldError("feishu_ingress_hold_machine_identity_unavailable")


def _approval_identity(machine: Mapping[str, str]) -> Mapping[str, Any]:
    source = machine.get("source")
    digest = machine.get("sha256")
    if not isinstance(source, str) or not source or SHA256_RE.fullmatch(str(digest)) is None:
        raise IngressHoldError("feishu_ingress_hold_machine_identity_invalid")
    try:
        username = pwd.getpwuid(os.geteuid()).pw_name
    except KeyError as exc:
        raise IngressHoldError("feishu_ingress_hold_local_identity_unavailable") from exc
    return {
        "schema_version": APPROVAL_IDENTITY_SCHEMA_VERSION,
        "method": "kernel_owner_and_machine_binding",
        "uid": os.geteuid(),
        "username": username,
        "machine_identity_source": source,
        "machine_identity_sha256": digest,
    }


def _validate_approval(
    owned: _OwnedJson,
    *,
    plan: Mapping[str, Any],
    plan_sha256: str,
    machine: Mapping[str, str],
    now: datetime,
) -> Mapping[str, Any]:
    body = owned.body
    if set(body) != {
        "schema_version",
        "hold_id",
        "decision",
        "created_at",
        "expires_at",
        "nonce",
        "plan_sha256",
        "chat_set_sha256",
        "host_commit",
        "adapter_sha256",
        "adapter_sidecar_schema",
        "live_sidecar_identity_sha256",
        "app_scope",
        "action_set",
        "action_set_sha256",
        "identity",
    }:
        raise IngressHoldError("feishu_ingress_hold_approval_shape_invalid")
    host = plan["host_adapter_identity"]
    expected = {
        "schema_version": APPROVAL_SCHEMA_VERSION,
        "hold_id": plan["hold_id"],
        "decision": "authorize_feishu_ingress_hold_staging",
        "plan_sha256": plan_sha256,
        "chat_set_sha256": plan["chat_set_sha256"],
        "host_commit": host["host_commit"],
        "adapter_sha256": host["adapter_sha256"],
        "adapter_sidecar_schema": host["adapter_sidecar_schema"],
        "live_sidecar_identity_sha256": _sha256_json(
            plan["live_sidecar_identity"]
        ),
        "app_scope": plan["app_scope"],
        "action_set": list(APPLY_ACTION_SET),
        "action_set_sha256": _sha256_json(list(APPLY_ACTION_SET)),
        "identity": _approval_identity(machine),
    }
    for key, value in expected.items():
        if body.get(key) != value:
            raise IngressHoldError(f"feishu_ingress_hold_approval_{key}_mismatch")
    if owned.stat_result.st_uid != body["identity"]["uid"]:
        raise IngressHoldError("feishu_ingress_hold_approval_identity_mismatch")
    nonce = body.get("nonce")
    if not isinstance(nonce, str) or NONCE_RE.fullmatch(nonce) is None:
        raise IngressHoldError("feishu_ingress_hold_approval_nonce_invalid")
    created = _parse_timestamp(body.get("created_at"), artifact="approval_created")
    expires = _parse_timestamp(body.get("expires_at"), artifact="approval_expires")
    current = now.astimezone(timezone.utc)
    if (
        expires <= created
        or (expires - created).total_seconds() > MAX_APPROVAL_VALIDITY_SECONDS
        or created - current > timedelta(seconds=MAX_FUTURE_SKEW_SECONDS)
        or current >= expires
    ):
        raise IngressHoldError("feishu_ingress_hold_approval_time_invalid")
    return {
        "schema_version": APPROVAL_SCHEMA_VERSION,
        "decision": body["decision"],
        "created_at": created.isoformat(),
        "expires_at": expires.isoformat(),
        "receipt_sha256": owned.sha256,
        "identity": body["identity"],
        "nonce_sha256": hashlib.sha256(nonce.encode()).hexdigest(),
    }


def _validate_cutover(
    owned: _OwnedJson,
    *,
    inputs: HoldInputs,
    plan: Mapping[str, Any],
    plan_sha256: str,
    now: datetime,
) -> Mapping[str, Any]:
    body = owned.body
    if set(body) != {
        "schema_version",
        "hold_id",
        "release_id",
        "plan_sha256",
        "canonical_gateway_root",
        "canonical_sidecar_path",
        "host_commit",
        "adapter_sha256",
        "chat_set_sha256",
        "live_sidecar_identity_sha256",
        "gateway_writer_state",
        "writer_stop_receipt_path",
        "writer_stop_receipt_sha256",
        "cutover_lease_fingerprint",
        "release_prepare_manifest_sha256",
        "release_approval_receipt_sha256",
        "old_gateway_runtime_identity_sha256",
        "window_started_at",
        "window_expires_at",
    }:
        raise IngressHoldError("feishu_ingress_hold_cutover_shape_invalid")
    host = plan["host_adapter_identity"]
    expected = {
        "schema_version": CUTOVER_BINDING_SCHEMA_VERSION,
        "hold_id": inputs.hold_id,
        "plan_sha256": plan_sha256,
        "canonical_gateway_root": str(inputs.canonical_gateway_root),
        "canonical_sidecar_path": str(inputs.live_sidecar.expanduser().absolute()),
        "host_commit": host["host_commit"],
        "adapter_sha256": host["adapter_sha256"],
        "chat_set_sha256": plan["chat_set_sha256"],
        "live_sidecar_identity_sha256": _sha256_json(
            plan["live_sidecar_identity"]
        ),
        "gateway_writer_state": "stopped",
    }
    for key, value in expected.items():
        if body.get(key) != value:
            raise IngressHoldError(f"feishu_ingress_hold_cutover_{key}_mismatch")
    if not isinstance(body.get("release_id"), str) or RELEASE_ID_RE.fullmatch(
        body["release_id"]
    ) is None:
        raise IngressHoldError("feishu_ingress_hold_cutover_release_id_invalid")
    _require_sha256(
        body.get("writer_stop_receipt_sha256"),
        code="feishu_ingress_hold_writer_stop_receipt_invalid",
    )
    writer_stop_path = Path(str(body.get("writer_stop_receipt_path") or ""))
    if (
        not writer_stop_path.is_absolute()
        or writer_stop_path != writer_stop_path.expanduser().absolute()
    ):
        raise IngressHoldError("feishu_ingress_hold_writer_stop_receipt_path_invalid")
    for field in (
        "cutover_lease_fingerprint",
        "release_prepare_manifest_sha256",
        "release_approval_receipt_sha256",
        "old_gateway_runtime_identity_sha256",
    ):
        _require_sha256(
            body.get(field),
            code=f"feishu_ingress_hold_{field}_invalid",
        )
    started = _parse_timestamp(
        body.get("window_started_at"), artifact="cutover_started"
    )
    expires = _parse_timestamp(
        body.get("window_expires_at"), artifact="cutover_expires"
    )
    current = now.astimezone(timezone.utc)
    if (
        expires <= started
        or (expires - started).total_seconds() > MAX_CUTOVER_WINDOW_SECONDS
        or started - current > timedelta(seconds=MAX_FUTURE_SKEW_SECONDS)
        or current >= expires
    ):
        raise IngressHoldError("feishu_ingress_hold_cutover_window_invalid")
    return {
        "schema_version": CUTOVER_BINDING_SCHEMA_VERSION,
        "release_id": body["release_id"],
        "gateway_writer_state": "stopped",
        "writer_stop_receipt_path": str(writer_stop_path),
        "writer_stop_receipt_sha256": body["writer_stop_receipt_sha256"],
        "cutover_lease_fingerprint": body["cutover_lease_fingerprint"],
        "release_prepare_manifest_sha256": body[
            "release_prepare_manifest_sha256"
        ],
        "release_approval_receipt_sha256": body[
            "release_approval_receipt_sha256"
        ],
        "old_gateway_runtime_identity_sha256": body[
            "old_gateway_runtime_identity_sha256"
        ],
        "window_started_at": started.isoformat(),
        "window_expires_at": expires.isoformat(),
        "binding_sha256": owned.sha256,
    }


def build_cutover_binding(
    inputs: HoldInputs,
    *,
    release_id: str,
    writer_stop_receipt: Path,
    lease: cutover_guard.CutoverLease,
    output_path: Path,
    now: datetime | None = None,
) -> Mapping[str, Any]:
    """Publish the exact dynamic hold binding under the active cutover lease."""
    selected = _validate_inputs(inputs, phase="plan")
    if RELEASE_ID_RE.fullmatch(release_id) is None:
        raise IngressHoldError("feishu_ingress_hold_cutover_release_id_invalid")
    destination = output_path.expanduser()
    if not destination.is_absolute() or ".." in destination.parts:
        raise IngressHoldError("feishu_ingress_hold_cutover_output_path_invalid")
    destination = destination.absolute()
    lease.assert_active()
    current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    plan_path = selected.run_root.expanduser().absolute() / PLAN_FILENAME
    plan_owned = _read_owned_json(plan_path, artifact="feishu_ingress_hold_plan")
    plan = plan_owned.body
    if (
        plan.get("schema_version") != PLAN_SCHEMA_VERSION
        or plan.get("hold_id") != selected.hold_id
        or plan.get("phase") != "plan"
        or plan.get("production_effects_executed") is not False
    ):
        raise IngressHoldError("feishu_ingress_hold_plan_binding_invalid")
    try:
        writer_owned, writer = cutover_guard.read_writer_stop_receipt(
            writer_stop_receipt,
            now=current,
        )
    except cutover_guard.CutoverGuardError as exc:
        raise IngressHoldError(exc.code, exc.detail) from exc
    if (
        lease.fingerprint != writer.get("lease_fingerprint")
        or lease.body.get("release_id") != release_id
        or writer.get("release_id") != release_id
        or writer.get("hold_id") != selected.hold_id
        or writer.get("plan_sha256") != plan_owned.sha256
        or writer.get("release_prepare_manifest_sha256")
        != lease.body.get("release_prepare_manifest", {}).get("sha256")
        or writer.get("approval_receipt_sha256")
        != lease.body.get("approval_receipt", {}).get("sha256")
    ):
        raise IngressHoldError("feishu_ingress_hold_cutover_writer_stop_mismatch")
    expires = min(
        _parse_timestamp(lease.body.get("expires_at"), artifact="cutover_expires"),
        current + timedelta(seconds=MAX_CUTOVER_WINDOW_SECONDS),
    )
    if expires <= current:
        raise IngressHoldError("feishu_ingress_hold_cutover_window_invalid")
    host = plan.get("host_adapter_identity")
    if not isinstance(host, Mapping):
        raise IngressHoldError("feishu_ingress_hold_plan_binding_invalid")
    body = {
        "schema_version": CUTOVER_BINDING_SCHEMA_VERSION,
        "hold_id": selected.hold_id,
        "release_id": release_id,
        "plan_sha256": plan_owned.sha256,
        "canonical_gateway_root": str(selected.canonical_gateway_root),
        "canonical_sidecar_path": str(selected.live_sidecar.expanduser().absolute()),
        "host_commit": host.get("host_commit"),
        "adapter_sha256": host.get("adapter_sha256"),
        "chat_set_sha256": plan.get("chat_set_sha256"),
        "live_sidecar_identity_sha256": _sha256_json(
            plan.get("live_sidecar_identity")
        ),
        "gateway_writer_state": "stopped",
        "writer_stop_receipt_path": str(writer_owned.path),
        "writer_stop_receipt_sha256": writer_owned.sha256,
        "cutover_lease_fingerprint": lease.fingerprint,
        "release_prepare_manifest_sha256": writer[
            "release_prepare_manifest_sha256"
        ],
        "release_approval_receipt_sha256": writer["approval_receipt_sha256"],
        "old_gateway_runtime_identity_sha256": writer[
            "old_gateway_runtime_identity_sha256"
        ],
        "window_started_at": current.isoformat(),
        "window_expires_at": expires.isoformat(),
    }
    _validate_cutover(
        _OwnedJson(
            path=destination,
            raw=_canonical_json(body),
            stat_result=writer_owned.stat_result,
            body=body,
        ),
        inputs=selected,
        plan=plan,
        plan_sha256=plan_owned.sha256,
        now=current,
    )
    _publish_no_clobber(destination, body)
    lease.assert_active()
    published = _read_owned_json(
        destination,
        artifact="feishu_ingress_hold_cutover",
    )
    if published.body != body:
        raise IngressHoldError("feishu_ingress_hold_cutover_publication_invalid")
    return body


def _validate_writer_stop_receipt(
    *,
    cutover: _OwnedJson,
    cutover_projection: Mapping[str, Any],
    plan: Mapping[str, Any],
    plan_sha256: str,
    now: datetime,
    live_observer: Callable[[], Mapping[str, Any]] | None,
    live_sidecar: Path,
) -> tuple[Any, Mapping[str, Any], Mapping[str, Any]]:
    receipt_path = Path(str(cutover_projection["writer_stop_receipt_path"]))
    try:
        owned, receipt = cutover_guard.read_writer_stop_receipt(
            receipt_path,
            now=now,
        )
    except cutover_guard.CutoverGuardError as exc:
        raise IngressHoldError(exc.code, exc.detail) from exc
    expected = {
        "release_id": cutover_projection["release_id"],
        "hold_id": plan["hold_id"],
        "plan_sha256": plan_sha256,
        "lease_fingerprint": cutover_projection["cutover_lease_fingerprint"],
        "release_prepare_manifest_sha256": cutover_projection[
            "release_prepare_manifest_sha256"
        ],
        "approval_receipt_sha256": cutover_projection[
            "release_approval_receipt_sha256"
        ],
        "old_gateway_runtime_identity_sha256": cutover_projection[
            "old_gateway_runtime_identity_sha256"
        ],
        "live_sidecar_identity": plan["live_sidecar_identity"],
        "live_sidecar_identity_sha256": _sha256_json(
            plan["live_sidecar_identity"]
        ),
    }
    if owned.path != receipt_path or owned.sha256 != cutover_projection[
        "writer_stop_receipt_sha256"
    ]:
        raise IngressHoldError("feishu_ingress_hold_writer_stop_receipt_mismatch")
    for field, value in expected.items():
        if receipt.get(field) != value:
            raise IngressHoldError(
                f"feishu_ingress_hold_writer_stop_{field}_mismatch"
            )
    old_runtime = receipt["old_gateway_runtime_identity"]
    expected_sidecar = receipt["live_sidecar_identity"]
    if live_observer is None:
        def sidecar_observer() -> Mapping[str, Any]:
            identity, _payload = _sidecar_observation(
                live_sidecar,
                app_scope=str(plan["app_scope"]),
            )
            return identity

        def observe_live() -> Mapping[str, Any]:
            return cutover_guard.observe_gateway_writer_stopped(
                expected_live_runtime_identity=old_runtime,
                expected_live_sidecar_identity=expected_sidecar,
                sidecar_observer=sidecar_observer,
            )

        selected_observer = observe_live
    else:
        selected_observer = live_observer
    try:
        live = cutover_guard.validate_writer_stop_observation(
            selected_observer(),
            expected_live_runtime_identity=old_runtime,
            expected_live_sidecar_identity=expected_sidecar,
        )
    except cutover_guard.CutoverGuardError as exc:
        raise IngressHoldError(exc.code, exc.detail) from exc
    if live != receipt.get("writer_stop_observation"):
        raise IngressHoldError("feishu_ingress_hold_writer_stop_live_drift")
    return owned, receipt, live


def _validate_gate_support(
    *,
    plan: Mapping[str, Any],
    plan_sha256: str,
    approval: _OwnedJson,
    cutover: _OwnedJson,
    writer_stop_receipt: Mapping[str, Any],
    writer_stop_receipt_sha256: str,
) -> Mapping[str, Any]:
    try:
        from scripts import pnc_rca_release_gate as gate
    except Exception as exc:
        raise IngressHoldError(
            "feishu_ingress_hold_gate_validator_unsupported"
        ) from exc
    validator = getattr(
        gate,
        "validate_feishu_ingress_hold_cutover_binding",
        None,
    )
    if not callable(validator):
        raise IngressHoldError("feishu_ingress_hold_gate_validator_unsupported")
    try:
        result = validator(
            plan=plan,
            plan_sha256=plan_sha256,
            approval_receipt=approval.body,
            approval_receipt_sha256=approval.sha256,
            cutover_binding=cutover.body,
            cutover_binding_sha256=cutover.sha256,
            writer_stop_receipt=writer_stop_receipt,
            writer_stop_receipt_sha256=writer_stop_receipt_sha256,
        )
    except TypeError as exc:
        raise IngressHoldError(
            "feishu_ingress_hold_gate_validator_unsupported"
        ) from exc
    except Exception as exc:
        raise IngressHoldError("feishu_ingress_hold_gate_validation_failed") from exc
    expected = {
        "schema_version": GATE_VALIDATION_SCHEMA_VERSION,
        "ok": True,
        "plan_sha256": plan_sha256,
        "approval_receipt_sha256": approval.sha256,
        "cutover_binding_sha256": cutover.sha256,
        "writer_stop_receipt_sha256": writer_stop_receipt_sha256,
        "cutover_lease_fingerprint": writer_stop_receipt["lease_fingerprint"],
        "old_gateway_runtime_identity_sha256": writer_stop_receipt[
            "old_gateway_runtime_identity_sha256"
        ],
        "gateway_writer_state": "stopped",
    }
    if result != expected:
        raise IngressHoldError("feishu_ingress_hold_gate_validation_failed")
    return expected


def _merge_staged_sidecar(
    *,
    base: Mapping[str, Any],
    plan: Mapping[str, Any],
    apply_snapshots: Mapping[str, Any],
    updated_at: float,
) -> tuple[Mapping[str, Any], Mapping[str, Any]]:
    state = json.loads(json.dumps(base["state"], ensure_ascii=False))
    pending = state["pending"]
    seen = set(state["seen_message_ids"])
    cursor_ids = state["cursor_message_ids"]
    before_watermarks = dict(state["last_seen_create_time_ms"])
    plan_snapshots = plan["api_snapshot"]["chats"]
    added_by_chat: dict[str, list[str]] = {}
    for chat_id in plan["chat_ids"]:
        existing = {
            str(item.get("message_id") or ""): item
            for item in pending.get(chat_id, [])
        }
        already_delivered = seen | set(cursor_ids.get(chat_id, []))
        added = []
        for source in (plan_snapshots[chat_id], apply_snapshots[chat_id]):
            for raw in source["items"]:
                item = FeishuAdapter._validated_api_poll_pending_item(
                    raw,
                    expected_chat_id=chat_id,
                )
                message_id = str(item.get("message_id") or "")
                if message_id in already_delivered or message_id in existing:
                    continue
                existing[message_id] = item
                added.append(message_id)
        values = sorted(
            existing.values(),
            key=lambda item: (
                _message_time_ms(item) or 0,
                str(item["message_id"]),
            ),
        )
        if len(values) > _MAX_API_POLL_PENDING_PER_CHAT:
            raise IngressHoldError("feishu_ingress_hold_pending_capacity_exceeded")
        if values:
            pending[chat_id] = values
        state["baselined_chat_ids"] = sorted(
            set(state["baselined_chat_ids"]) | {chat_id}
        )
        state["discovery_floor_ms"].setdefault(
            chat_id,
            plan["window"]["floor_by_chat"][chat_id],
        )
        added_by_chat[chat_id] = sorted(set(added))
    after_watermarks = dict(state["last_seen_create_time_ms"])
    for chat_id, before in before_watermarks.items():
        after = after_watermarks.get(chat_id)
        if after is None or after < before:
            raise IngressHoldError("feishu_ingress_hold_watermark_regressed")
    pending_count = sum(len(items) for items in pending.values())
    continuation_count = len(state["scan_state"])
    staged = {
        "schema_version": _API_POLL_SIDECAR_SCHEMA,
        "app_scope": plan["app_scope"],
        "revision": int(base["revision"]) + 1,
        "updated_at": updated_at,
        "rollback_readiness": {
            "ready": pending_count == 0 and continuation_count == 0,
            "pending_count": pending_count,
            "scan_continuation_count": continuation_count,
        },
        "state": state,
    }
    staged = _normalized_sidecar_payload(
        staged,
        expected_app_scope=plan["app_scope"],
    )
    return staged, {
        "policy": "preserve_or_increase_never_advance_for_pending",
        "before": before_watermarks,
        "after": after_watermarks,
        "non_regressing": True,
        "added_message_ids_by_chat": added_by_chat,
    }


def _static_context(
    inputs: HoldInputs,
    *,
    env_owned: _OwnedFile,
    environment: Mapping[str, str],
    identity: Mapping[str, Any],
) -> tuple[Mapping[str, Any], Mapping[str, Any], Mapping[str, Any], Mapping[str, int]]:
    host = _host_adapter_identity(inputs.host_candidate)
    app_scope = _app_scope(environment)
    sidecar_identity, base = _sidecar_observation(
        inputs.live_sidecar,
        app_scope=app_scope,
    )
    if identity["inputs"]["env_file"]["sha256"] != env_owned.sha256:
        raise IngressHoldError("feishu_ingress_hold_env_changed")
    floors = _floor_by_chat(
        base["state"],
        chat_ids=inputs.chat_ids,
        hold_start_ms=identity["hold_start_ms"],
    )
    return host, sidecar_identity, base, floors


def run_ingress_hold(
    inputs: HoldInputs,
    *,
    phase: str = "plan",
    reader: ReadOnlyMessageApi | None = None,
    now: datetime | None = None,
    clock_ms: Callable[[], int] | None = None,
    machine_identity_observer: Callable[[], Mapping[str, str]] = _machine_identity,
    writer_stop_observer: Callable[[], Mapping[str, Any]] | None = None,
) -> HoldResult:
    selected = _validate_inputs(inputs, phase=phase)
    current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    now_ms = clock_ms or (lambda: time.time_ns() // 1_000_000)
    env_owned, environment = _read_env(selected.env_file)
    secrets = _sensitive_values(environment)
    if phase == "apply" and not (
        selected.run_root.expanduser().absolute() / PLAN_FILENAME
    ).is_file():
        raise IngressHoldError("feishu_ingress_hold_plan_required")
    run_root, created = _ensure_run_root(selected.run_root)
    resumed = not created
    with _run_lock(run_root):
        identity_path = run_root / RUN_IDENTITY_FILENAME
        if identity_path.exists():
            owned_identity = _read_owned_json(
                identity_path,
                artifact="feishu_ingress_hold_run_identity",
            )
            identity = owned_identity.body
            created_at = str(identity.get("created_at") or "")
            hold_start_ms = identity.get("hold_start_ms")
            if not isinstance(hold_start_ms, int) or isinstance(hold_start_ms, bool):
                raise IngressHoldError("feishu_ingress_hold_run_identity_invalid")
            expected = _run_identity(
                selected,
                env_sha256=env_owned.sha256,
                created_at=created_at,
                hold_start_ms=hold_start_ms,
            )
            if identity != expected:
                raise IngressHoldError("feishu_ingress_hold_run_identity_conflict")
            resumed = True
        else:
            unexpected = {
                path.name
                for path in run_root.iterdir()
                if path.name != RUN_LOCK_FILENAME
                and not path.name.startswith(f".{RUN_IDENTITY_FILENAME}.")
            }
            if unexpected:
                raise IngressHoldError("feishu_ingress_hold_run_root_not_empty")
            identity = _run_identity(
                selected,
                env_sha256=env_owned.sha256,
                created_at=current.isoformat(),
                hold_start_ms=now_ms(),
            )
            resumed |= _publish_no_clobber(identity_path, identity)

        host, sidecar_identity, base, floors = _static_context(
            selected,
            env_owned=env_owned,
            environment=environment,
            identity=identity,
        )
        plan_path = run_root / PLAN_FILENAME
        if plan_path.exists():
            owned_plan = _read_owned_json(
                plan_path,
                artifact="feishu_ingress_hold_plan",
            )
            plan = owned_plan.body
            _validate_plan(
                plan,
                inputs=selected,
                identity=identity,
                host=host,
                sidecar_identity=sidecar_identity,
                app_scope=_app_scope(environment),
            )
        else:
            if phase != "plan":
                raise IngressHoldError("feishu_ingress_hold_plan_required")
            active_reader = reader or FeishuReadOnlyMessageApi(
                environment,
                clock_ms=now_ms,
            )
            snapshots = _snapshot_all(
                active_reader,
                chat_ids=selected.chat_ids,
                floors=floors,
                page_size=selected.page_size,
                max_pages=selected.max_pages,
            )
            after_identity, _after_base = _sidecar_observation(
                selected.live_sidecar,
                app_scope=_app_scope(environment),
            )
            if after_identity != sidecar_identity:
                raise IngressHoldError("feishu_ingress_hold_live_sidecar_changed")
            plan = _plan_body(
                inputs=selected,
                identity=identity,
                host=host,
                sidecar_identity=sidecar_identity,
                app_scope=_app_scope(environment),
                floors=floors,
                snapshots=snapshots,
            )
            _assert_redacted(plan, secrets=secrets)
            if _read_stable_file(
                selected.env_file,
                artifact="feishu_ingress_hold_env",
                require_owner_only=True,
            ).raw != env_owned.raw:
                raise IngressHoldError("feishu_ingress_hold_env_changed")
            resumed |= _publish_no_clobber(plan_path, plan)
            owned_plan = _read_owned_json(
                plan_path,
                artifact="feishu_ingress_hold_plan",
            )

        if phase == "plan":
            return HoldResult("plan", run_root, plan_path, plan, resumed)

        if selected.approval_receipt is None or selected.cutover_binding is None:
            raise IngressHoldError("feishu_ingress_hold_apply_authorization_required")
        approval_owned = _read_owned_json(
            selected.approval_receipt,
            artifact="feishu_ingress_hold_approval",
        )
        cutover_owned = _read_owned_json(
            selected.cutover_binding,
            artifact="feishu_ingress_hold_cutover",
        )
        plan_sha256 = owned_plan.sha256
        approval_projection = _validate_approval(
            approval_owned,
            plan=plan,
            plan_sha256=plan_sha256,
            machine=machine_identity_observer(),
            now=current,
        )
        cutover_projection = _validate_cutover(
            cutover_owned,
            inputs=selected,
            plan=plan,
            plan_sha256=plan_sha256,
            now=current,
        )
        (
            writer_stop_owned,
            writer_stop_receipt,
            live_writer_stop,
        ) = _validate_writer_stop_receipt(
            cutover=cutover_owned,
            cutover_projection=cutover_projection,
            plan=plan,
            plan_sha256=plan_sha256,
            now=current,
            live_observer=writer_stop_observer,
            live_sidecar=selected.live_sidecar,
        )
        gate_validation = _validate_gate_support(
            plan=plan,
            plan_sha256=plan_sha256,
            approval=approval_owned,
            cutover=cutover_owned,
            writer_stop_receipt=writer_stop_receipt,
            writer_stop_receipt_sha256=writer_stop_owned.sha256,
        )
        if _read_stable_file(
            writer_stop_owned.path,
            artifact="feishu_ingress_hold_writer_stop_receipt",
            require_owner_only=True,
        ).raw != writer_stop_owned.raw:
            raise IngressHoldError("feishu_ingress_hold_writer_stop_receipt_changed")
        active_reader = reader or FeishuReadOnlyMessageApi(
            environment,
            clock_ms=now_ms,
        )
        apply_snapshots = _snapshot_all(
            active_reader,
            chat_ids=selected.chat_ids,
            floors=floors,
            page_size=selected.page_size,
            max_pages=selected.max_pages,
        )
        after_identity, after_base = _sidecar_observation(
            selected.live_sidecar,
            app_scope=_app_scope(environment),
        )
        if after_identity != sidecar_identity or after_base != base:
            raise IngressHoldError("feishu_ingress_hold_live_sidecar_changed")
        staged, watermark_proof = _merge_staged_sidecar(
            base=base,
            plan=plan,
            apply_snapshots=apply_snapshots,
            updated_at=current.timestamp(),
        )
        _assert_redacted(staged, secrets=secrets)
        staged_sha256 = hashlib.sha256(_canonical_json(staged)).hexdigest()
        receipt = {
            "schema_version": APPLY_RECEIPT_SCHEMA_VERSION,
            "hold_id": selected.hold_id,
            "created_at": current.isoformat(),
            "ok": True,
            "production_effects_executed": False,
            "live_sidecar_written": False,
            "plan_sha256": plan_sha256,
            "approval": approval_projection,
            "cutover": cutover_projection,
            "writer_stop": {
                "receipt_path": str(writer_stop_owned.path),
                "receipt_sha256": writer_stop_owned.sha256,
                "lease_fingerprint": writer_stop_receipt["lease_fingerprint"],
                "old_gateway_runtime_identity_sha256": writer_stop_receipt[
                    "old_gateway_runtime_identity_sha256"
                ],
                "live_observation": live_writer_stop,
            },
            "gate_validation": gate_validation,
            "staged_sidecar": {
                "filename": STAGED_SIDECAR_FILENAME,
                "sha256": staged_sha256,
                "revision": staged["revision"],
                "app_scope": staged["app_scope"],
                "pending_count": staged["rollback_readiness"]["pending_count"],
            },
            "apply_snapshot_sha256": _sha256_json(apply_snapshots),
            "watermark_proof": watermark_proof,
            "future_install": {
                **plan["future_install"],
                "staged_source": str(run_root / STAGED_SIDECAR_FILENAME),
                "staged_sha256": staged_sha256,
            },
            "side_effect_contract": identity["side_effect_contract"],
        }
        _assert_redacted(receipt, secrets=secrets)
        final_inputs = (
            (selected.env_file, env_owned.raw, "feishu_ingress_hold_env"),
            (
                selected.approval_receipt,
                approval_owned.raw,
                "feishu_ingress_hold_approval",
            ),
            (
                selected.cutover_binding,
                cutover_owned.raw,
                "feishu_ingress_hold_cutover",
            ),
            (
                writer_stop_owned.path,
                writer_stop_owned.raw,
                "feishu_ingress_hold_writer_stop_receipt",
            ),
        )
        for path, raw, artifact in final_inputs:
            if _read_stable_file(
                path,
                artifact=artifact,
                require_owner_only=True,
            ).raw != raw:
                raise IngressHoldError("feishu_ingress_hold_input_changed")
        artifacts = {
            STAGED_SIDECAR_FILENAME: staged,
            APPLY_RECEIPT_FILENAME: receipt,
        }
        for filename in (STAGED_SIDECAR_FILENAME, APPLY_RECEIPT_FILENAME):
            resumed |= _publish_no_clobber(run_root / filename, artifacts[filename])
        manifest = {
            "schema_version": APPLY_MANIFEST_SCHEMA_VERSION,
            "hold_id": selected.hold_id,
            "created_at": current.isoformat(),
            "complete": True,
            "production_effects_executed": False,
            "plan_sha256": plan_sha256,
            "artifacts": {
                filename: {
                    "sha256": hashlib.sha256(
                        _canonical_json(artifacts[filename])
                    ).hexdigest(),
                    "size_bytes": len(_canonical_json(artifacts[filename])),
                    "schema_version": artifacts[filename]["schema_version"],
                }
                for filename in (STAGED_SIDECAR_FILENAME, APPLY_RECEIPT_FILENAME)
            },
        }
        resumed |= _publish_no_clobber(run_root / APPLY_MANIFEST_FILENAME, manifest)
        return HoldResult(
            "apply",
            run_root,
            run_root / APPLY_MANIFEST_FILENAME,
            manifest,
            resumed,
        )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", choices=("plan", "apply"), default="plan")
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--host-candidate", type=Path, required=True)
    parser.add_argument("--live-sidecar", type=Path, required=True)
    parser.add_argument("--chat-id", action="append", required=True)
    parser.add_argument("--hold-id", required=True)
    parser.add_argument("--run-root", type=Path, required=True)
    parser.add_argument(
        "--canonical-gateway-root",
        type=Path,
        default=DEFAULT_CANONICAL_GATEWAY_ROOT,
    )
    parser.add_argument("--approval-receipt", type=Path)
    parser.add_argument("--cutover-binding", type=Path)
    parser.add_argument("--page-size", type=int, default=50)
    parser.add_argument("--max-pages", type=int, default=200)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    inputs = HoldInputs(
        env_file=args.env_file,
        host_candidate=args.host_candidate,
        live_sidecar=args.live_sidecar,
        chat_ids=tuple(args.chat_id),
        hold_id=args.hold_id,
        run_root=args.run_root,
        canonical_gateway_root=args.canonical_gateway_root,
        approval_receipt=args.approval_receipt,
        cutover_binding=args.cutover_binding,
        page_size=args.page_size,
        max_pages=args.max_pages,
    )
    try:
        result = run_ingress_hold(inputs, phase=args.phase)
    except (OSError, ValueError, IngressHoldError) as exc:
        code = exc.code if isinstance(exc, IngressHoldError) else "ingress_hold_failed"
        print(json.dumps({"ok": False, "code": code}, sort_keys=True))
        return 2
    print(
        json.dumps(
            {
                "ok": True,
                "phase": result.phase,
                "artifact": str(result.artifact_path),
                "resumed": result.resumed,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
