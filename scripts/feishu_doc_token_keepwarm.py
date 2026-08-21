#!/usr/bin/env python3
"""Keep the feishu-doc OAuth credential warm through the Feishu Open API.

The previous implementation spawned the interactive feishu-doc MCP server.
LaunchAgents do not have that server's interactive environment, and the old
working directory may not exist.  This path uses the same documented OAuth
endpoints as the provider, refreshing only when the access token is near
expiry and then making a read-only ``user_info`` request.

This script never prints token/secret values.  It only reports non-secret
metadata: expiresAt, owner, health, and an error class/message when needed.
"""
from __future__ import annotations

import argparse
import contextlib
import fcntl
import hashlib
import hmac
import json
import os
import re
import secrets
import stat
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Callable, Iterator

APP_ID = "cli_a99b38e0a29b500b"
AUTH_PATH = Path(
    "/Users/songying/.hermes/mcp-storage/feishu-doc/feishu-service/feishu/auth"
) / APP_ID
TOKEN_URL = "https://open.feishu.cn/open-apis/authen/v2/oauth/token"
USER_INFO_URL = "https://open.feishu.cn/open-apis/authen/v1/user_info"
AUTHORIZE_URL = "https://open.feishu.cn/open-apis/authen/v1/authorize"
OAUTH_REDIRECT_URI = "http://localhost:3010/oauth/feishu/callback"
OAUTH_SCOPES = "contact:user.base:readonly docx:document drive:drive wiki:wiki offline_access"
OAUTH_STATE_PATH = Path(
    "/Users/songying/.hermes/runtime/shared-state/feishu_doc_oauth_state.json"
)
OAUTH_STATE_TTL_SECONDS = 15 * 60
OAUTH_STATE_MIN_TTL_SECONDS = 10 * 60
OAUTH_STATE_MAX_TTL_SECONDS = 15 * 60
EXPECTED_OWNER_NAME = "胡子豪"
EXPECTED_OWNER_USER_ID = "fefb829e"
EXPECTED_OWNER_OPEN_ID = "ou_d1d3cfeba1be0a22faa36aaf4fb3907d"
OAUTH_STATE_SCHEMA_VERSION = 1
_OAUTH_STATE_MAX_BYTES = 16 * 1024
_AUTH_MAX_BYTES = 1 << 20
_STATE_RE = re.compile(r"^[A-Za-z0-9_-]{43,128}$")
REFRESH_MARGIN_MS = 5 * 60 * 1000
TOKEN_ERROR_CODES = {1, 20005, 99991663, 99991664, 99991665, 99991666}

REAUTH_PATTERNS = (
    "99991663",
    "99991664",
    "99991665",
    "99991666",
    "Token 刷新失败",
    "refresh_token",
    "refresh token",
    "invalid refresh",
)
SECRET_KEY_RE = re.compile(r"(token|secret|password|authorization|app_secret)", re.I)
SECRET_VALUE_RE = re.compile(r"\b(?:u-|t-|m-)[A-Za-z0-9_-]{12,}\b|Bearer\s+\S+", re.I)
SECRET_ASSIGN_RE = re.compile(
    r"(?i)(?:^|[^A-Za-z0-9])[\"']?"
    r"(?:token|access[_-]?token|refresh[_-]?token|(?:app|client)[_-]?secret|"
    r"secret|password|authorization)[\"']?\s*[:=]\s*"
    r"(?:\"[^\"]*\"|'[^']*'|\S+)"
)


class KeepwarmError(Exception):
    def __init__(self, message: str, *, error_class: str = "KeepwarmError") -> None:
        super().__init__(message)
        self.error_class = error_class


def _redact_text(text: str) -> str:
    return SECRET_ASSIGN_RE.sub("<redacted>", SECRET_VALUE_RE.sub("<redacted>", text))


def _safe_error_message(text: str, limit: int = 240) -> str:
    raw = str(text)
    try:
        parsed = json.loads(raw)
    except (TypeError, ValueError, json.JSONDecodeError):
        rendered = _redact_text(raw)
    else:
        rendered = json.dumps(_scrub(parsed), ensure_ascii=False, separators=(",", ":"))
    return rendered.replace("\n", " ")[:limit]


def _scrub(value: Any) -> Any:
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for key, item in value.items():
            if SECRET_KEY_RE.search(str(key)):
                out[key] = "<redacted>"
            else:
                out[key] = _scrub(item)
        return out
    if isinstance(value, list):
        return [_scrub(item) for item in value]
    if isinstance(value, str):
        return _redact_text(value)
    return value


def _owned_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    st = path.lstat()
    if not stat.S_ISDIR(st.st_mode) or stat.S_ISLNK(st.st_mode) or st.st_uid != os.getuid():
        raise KeepwarmError(
            "OAuth storage parent is not an owned directory",
            error_class="AUTH_STORAGE_ERROR",
        )


def _safe_existing_private_file(path: Path, *, max_bytes: int, exact_mode: bool) -> os.stat_result | None:
    try:
        st = path.lstat()
    except FileNotFoundError:
        return None
    if (
        not stat.S_ISREG(st.st_mode)
        or stat.S_ISLNK(st.st_mode)
        or st.st_uid != os.getuid()
        or st.st_nlink != 1
        or st.st_size > max_bytes
        or (exact_mode and stat.S_IMODE(st.st_mode) != 0o600)
    ):
        raise KeepwarmError(
            "OAuth storage path is not a private regular file",
            error_class="AUTH_STORAGE_ERROR",
        )
    return st


def _fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY
    if hasattr(os, "O_DIRECTORY"):
        flags |= os.O_DIRECTORY
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags)
    try:
        opened = os.fstat(descriptor)
        if not stat.S_ISDIR(opened.st_mode) or opened.st_uid != os.getuid():
            raise KeepwarmError(
                "OAuth storage parent changed during fsync",
                error_class="AUTH_STORAGE_ERROR",
            )
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _atomic_private_json(path: Path, payload: dict[str, Any], *, max_bytes: int) -> None:
    """Atomically replace one owned JSON file and durably persist its directory entry."""
    _owned_directory(path.parent)
    _safe_existing_private_file(path, max_bytes=max_bytes, exact_mode=True)
    raw = (json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")
    if len(raw) > max_bytes:
        raise KeepwarmError("OAuth storage payload is too large", error_class="AUTH_STORAGE_ERROR")
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "wb") as stream:
            fd = -1
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        os.chmod(path, 0o600)
        _fsync_directory(path.parent)
        _safe_existing_private_file(path, max_bytes=max_bytes, exact_mode=True)
    finally:
        if fd >= 0:
            os.close(fd)
        try:
            os.unlink(temporary)
        except OSError:
            pass


@contextlib.contextmanager
def _oauth_state_lock(state_path: Path) -> Iterator[None]:
    _owned_directory(state_path.parent)
    lock_path = state_path.with_name(f".{state_path.name}.lock")
    flags = os.O_CREAT | os.O_RDWR
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(lock_path, flags, 0o600)
    except OSError as exc:
        raise KeepwarmError(
            "OAuth state lock is unavailable",
            error_class="AUTH_STORAGE_ERROR",
        ) from exc
    try:
        st = os.fstat(descriptor)
        if not stat.S_ISREG(st.st_mode) or st.st_uid != os.getuid() or st.st_nlink != 1:
            raise KeepwarmError(
                "OAuth state lock is not an owned regular file",
                error_class="AUTH_STORAGE_ERROR",
            )
        os.fchmod(descriptor, 0o600)
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)


@contextlib.contextmanager
def _oauth_auth_lock(auth_path: Path) -> Iterator[None]:
    """Serialize the direct callback and keep-warm writers we control.

    This advisory lock is a Python-writer contract. The retired Node/MCP
    FileSystemProvider does not cooperate with it, so that writer must remain
    disabled; snapshot checks are only a final guard against accidental drift.
    """

    _owned_directory(auth_path.parent)
    lock_path = auth_path.with_name(f".{auth_path.name}.lock")
    flags = os.O_CREAT | os.O_RDWR
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(lock_path, flags, 0o600)
    except OSError as exc:
        raise KeepwarmError(
            "OAuth auth lock is unavailable",
            error_class="AUTH_STORAGE_ERROR",
        ) from exc
    try:
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_uid != os.getuid()
            or opened.st_nlink != 1
        ):
            raise KeepwarmError(
                "OAuth auth lock is not an owned regular file",
                error_class="AUTH_STORAGE_ERROR",
            )
        os.fchmod(descriptor, 0o600)
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)


def _read_bounded_owned_file(path: Path, *, max_bytes: int, exact_mode: bool) -> tuple[bytes, os.stat_result]:
    before = _safe_existing_private_file(path, max_bytes=max_bytes, exact_mode=exact_mode)
    if before is None:
        raise FileNotFoundError(path)
    flags = os.O_RDONLY
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags)
    try:
        opened = os.fstat(descriptor)
        if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
            raise KeepwarmError("OAuth storage changed during read", error_class="AUTH_STORAGE_ERROR")
        chunks: list[bytes] = []
        remaining = max_bytes + 1
        while remaining > 0:
            chunk = os.read(descriptor, min(64 * 1024, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        raw = b"".join(chunks)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    if len(raw) > max_bytes or len(raw) != opened.st_size or (
        opened.st_dev,
        opened.st_ino,
        opened.st_size,
        opened.st_mtime_ns,
    ) != (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
    ):
        raise KeepwarmError("OAuth storage changed during read", error_class="AUTH_STORAGE_ERROR")
    return raw, after


def _auth_snapshot(auth_path: Path) -> dict[str, Any]:
    try:
        raw, st = _read_bounded_owned_file(
            auth_path,
            max_bytes=_AUTH_MAX_BYTES,
            exact_mode=True,
        )
    except FileNotFoundError:
        return {"exists": False}
    return {
        "exists": True,
        "device": st.st_dev,
        "inode": st.st_ino,
        "size": st.st_size,
        "mtime_ns": st.st_mtime_ns,
        "sha256": hashlib.sha256(raw).hexdigest(),
    }


def _valid_auth_snapshot(value: Any) -> bool:
    if value == {"exists": False}:
        return True
    if not isinstance(value, dict) or set(value) != {
        "exists",
        "device",
        "inode",
        "size",
        "mtime_ns",
        "sha256",
    }:
        return False
    return (
        value.get("exists") is True
        and all(type(value.get(key)) is int and value[key] >= 0 for key in ("device", "inode", "size", "mtime_ns"))
        and isinstance(value.get("sha256"), str)
        and bool(re.fullmatch(r"[0-9a-f]{64}", value["sha256"]))
    )


def _same_auth_snapshot(left: dict[str, Any], right: dict[str, Any]) -> bool:
    if not _valid_auth_snapshot(left) or not _valid_auth_snapshot(right):
        return False
    if left.get("exists") is not right.get("exists"):
        return False
    if left.get("exists") is False:
        return True
    if any(left.get(key) != right.get(key) for key in ("device", "inode", "size", "mtime_ns")):
        return False
    return hmac.compare_digest(str(left.get("sha256")), str(right.get("sha256")))


def _read_oauth_state(state_path: Path, *, now: int) -> dict[str, Any]:
    try:
        raw, _ = _read_bounded_owned_file(
            state_path,
            max_bytes=_OAUTH_STATE_MAX_BYTES,
            exact_mode=True,
        )
    except FileNotFoundError as exc:
        raise KeepwarmError("OAuth state is missing", error_class="OAUTH_STATE_MISSING") from exc
    try:
        ledger = json.loads(raw.decode("utf-8"))
    except Exception as exc:
        raise KeepwarmError("OAuth state is unreadable", error_class="OAUTH_STATE_INVALID") from exc
    if not isinstance(ledger, dict) or ledger.get("status") != "active":
        error_class = "OAUTH_STATE_REPLAY" if isinstance(ledger, dict) and ledger.get("status") == "consumed" else "OAUTH_STATE_INVALID"
        raise KeepwarmError("OAuth state is not active", error_class=error_class)
    if set(ledger) != {
        "schema_version",
        "status",
        "app_id",
        "redirect_uri",
        "state",
        "created_at",
        "expires_at",
        "auth_snapshot",
    }:
        raise KeepwarmError("OAuth state schema is invalid", error_class="OAUTH_STATE_INVALID")
    created_at = ledger.get("created_at")
    expires_at = ledger.get("expires_at")
    ttl = expires_at - created_at if type(created_at) is int and type(expires_at) is int else -1
    if (
        ledger.get("schema_version") != OAUTH_STATE_SCHEMA_VERSION
        or ledger.get("app_id") != APP_ID
        or ledger.get("redirect_uri") != OAUTH_REDIRECT_URI
        or not isinstance(ledger.get("state"), str)
        or not _STATE_RE.fullmatch(ledger["state"])
        or not _valid_auth_snapshot(ledger.get("auth_snapshot"))
        or not OAUTH_STATE_MIN_TTL_SECONDS <= ttl <= OAUTH_STATE_MAX_TTL_SECONDS
        or now < created_at - 5
    ):
        raise KeepwarmError("OAuth state schema is invalid", error_class="OAUTH_STATE_INVALID")
    if now >= expires_at:
        raise KeepwarmError("OAuth state has expired", error_class="OAUTH_STATE_EXPIRED")
    return ledger


def _fixed_oauth_inputs(app_id: str, redirect_uri: str) -> None:
    if app_id != APP_ID:
        raise KeepwarmError("OAuth app ID is not allowed", error_class="OAUTH_INPUT_INVALID")
    if redirect_uri != OAUTH_REDIRECT_URI:
        raise KeepwarmError("OAuth redirect URI is not allowed", error_class="OAUTH_INPUT_INVALID")


def start_doc_oauth(
    *,
    app_id: str = APP_ID,
    redirect_uri: str = OAUTH_REDIRECT_URI,
    ttl_seconds: int = OAUTH_STATE_TTL_SECONDS,
    state_path: Path | None = None,
    auth_path: Path | None = None,
    now: int | None = None,
    state_factory: Callable[[int], str] | None = None,
) -> dict[str, Any]:
    """Create one direct OAuth request without reading app secrets or using MCP."""
    _fixed_oauth_inputs(app_id, redirect_uri)
    if type(ttl_seconds) is not int or not OAUTH_STATE_MIN_TTL_SECONDS <= ttl_seconds <= OAUTH_STATE_MAX_TTL_SECONDS:
        raise KeepwarmError("OAuth state TTL must be 10-15 minutes", error_class="OAUTH_INPUT_INVALID")
    state_path = OAUTH_STATE_PATH if state_path is None else state_path
    auth_path = AUTH_PATH if auth_path is None else auth_path
    created_at = int(time.time()) if now is None else now
    if type(created_at) is not int or created_at < 0:
        raise KeepwarmError("OAuth clock value is invalid", error_class="OAUTH_INPUT_INVALID")
    reused = False
    with _oauth_state_lock(state_path):
        snapshot = _auth_snapshot(auth_path)
        try:
            ledger = _read_oauth_state(state_path, now=created_at)
        except KeepwarmError as exc:
            if exc.error_class not in {
                "OAUTH_STATE_MISSING",
                "OAUTH_STATE_EXPIRED",
                "OAUTH_STATE_REPLAY",
            }:
                raise
            ledger = {}
        if (
            ledger
            and ledger["expires_at"] - created_at >= 60
            and _same_auth_snapshot(ledger["auth_snapshot"], snapshot)
        ):
            state = ledger["state"]
            expires_in = ledger["expires_at"] - created_at
            reused = True
        else:
            state = (state_factory or secrets.token_urlsafe)(32)
            if not isinstance(state, str) or not _STATE_RE.fullmatch(state):
                raise KeepwarmError("OAuth state generator returned an invalid value", error_class="OAUTH_STATE_INVALID")
            ledger = {
                "schema_version": OAUTH_STATE_SCHEMA_VERSION,
                "status": "active",
                "app_id": app_id,
                "redirect_uri": redirect_uri,
                "state": state,
                "created_at": created_at,
                "expires_at": created_at + ttl_seconds,
                "auth_snapshot": snapshot,
            }
            _atomic_private_json(state_path, ledger, max_bytes=_OAUTH_STATE_MAX_BYTES)
            persisted = _read_oauth_state(state_path, now=created_at)
            if persisted != ledger:
                raise KeepwarmError("OAuth state verification failed", error_class="AUTH_STORAGE_ERROR")
            expires_in = ttl_seconds
    query = urllib.parse.urlencode(
        {
            "client_id": app_id,
            "redirect_uri": redirect_uri,
            "scope": OAUTH_SCOPES,
            "state": state,
            "response_type": "code",
        }
    )
    return {
        "ok": True,
        "auth_url": f"{AUTHORIZE_URL}?{query}",
        "state": state,
        "appId": app_id,
        "redirect_uri": redirect_uri,
        "expires_in": expires_in,
        "reused": reused,
    }


def _valid_callback_value(value: str, *, is_state: bool) -> bool:
    if not isinstance(value, str):
        return False
    if is_state:
        return bool(_STATE_RE.fullmatch(value))
    return 1 <= len(value) <= 4096 and all(0x21 <= ord(char) <= 0x7E for char in value)


def _exchange_authorization_code(
    *,
    code: str,
    app_id: str,
    app_secret: str,
    redirect_uri: str,
    timeout_seconds: int,
) -> dict[str, Any]:
    return _http_json(
        "POST",
        TOKEN_URL,
        payload={
            "grant_type": "authorization_code",
            "client_id": app_id,
            "client_secret": app_secret,
            "code": code,
            "redirect_uri": redirect_uri,
        },
        timeout_seconds=timeout_seconds,
    )


def _fetch_oauth_user_info(*, access_token: str, timeout_seconds: int) -> dict[str, Any]:
    return _user_info(access_token, timeout_seconds=timeout_seconds)


def _oauth_token_value(response: dict[str, Any], *, now_ms: int) -> dict[str, Any]:
    if not isinstance(response, dict) or _response_code(response) != 0:
        code = _response_code(response) if isinstance(response, dict) else None
        raise KeepwarmError(
            f"OAuth code exchange failed ({code})",
            error_class="OAUTH_EXCHANGE_FAILED",
        )
    payload = _token_payload(response)
    access_token = payload.get("access_token")
    refresh_token = payload.get("refresh_token")
    raw_expires_in = payload.get("expires_in")
    try:
        expires_in = int(raw_expires_in) if raw_expires_in is not None else 0
    except (TypeError, ValueError):
        expires_in = 0
    if (
        not isinstance(access_token, str)
        or not 1 <= len(access_token) <= 8192
        or any(ord(char) < 32 for char in access_token)
        or not isinstance(refresh_token, str)
        or not 1 <= len(refresh_token) <= 8192
        or any(ord(char) < 32 for char in refresh_token)
        or not 1 <= expires_in <= 31 * 24 * 3600
    ):
        raise KeepwarmError("OAuth token response is invalid", error_class="OAUTH_EXCHANGE_FAILED")
    return {
        "appId": APP_ID,
        "accessToken": access_token,
        "refreshToken": refresh_token,
        "expiresAt": now_ms + expires_in * 1000,
    }


def _validated_owner(user: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(user, dict):
        raise KeepwarmError("OAuth user_info is invalid", error_class="OAUTH_OWNER_MISMATCH")
    name = user.get("name")
    user_id = user.get("user_id") or user.get("userId")
    open_id = user.get("open_id") or user.get("openId")
    if (
        name != EXPECTED_OWNER_NAME
        or user_id != EXPECTED_OWNER_USER_ID
        or open_id != EXPECTED_OWNER_OPEN_ID
    ):
        raise KeepwarmError("OAuth owner does not match the expected user", error_class="OAUTH_OWNER_MISMATCH")
    normalized = {"name": name, "userId": user_id, "openId": open_id}
    for source, target in (("email", "email"), ("avatar_url", "avatarUrl"), ("avatarUrl", "avatarUrl")):
        item = user.get(source)
        if isinstance(item, str) and item:
            normalized[target] = item
    return normalized


def _replace_oauth_auth(
    value: dict[str, Any],
    *,
    expected_snapshot: dict[str, Any],
    auth_path: Path,
) -> None:
    if not _same_auth_snapshot(_auth_snapshot(auth_path), expected_snapshot):
        raise KeepwarmError("OAuth credential changed during authorization", error_class="AUTH_STORAGE_ERROR")
    payload = {"__mcp": {"v": 1}, "value": value}
    _owned_directory(auth_path.parent)
    raw = (json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")
    if len(raw) > _AUTH_MAX_BYTES:
        raise KeepwarmError("OAuth credential payload is too large", error_class="AUTH_STORAGE_ERROR")
    fd, temporary = tempfile.mkstemp(prefix=f".{auth_path.name}.", dir=str(auth_path.parent))
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "wb") as stream:
            fd = -1
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        if not _same_auth_snapshot(_auth_snapshot(auth_path), expected_snapshot):
            raise KeepwarmError("OAuth credential changed during authorization", error_class="AUTH_STORAGE_ERROR")
        os.replace(temporary, auth_path)
        _fsync_directory(auth_path.parent)
        _safe_existing_private_file(auth_path, max_bytes=_AUTH_MAX_BYTES, exact_mode=True)
    finally:
        if fd >= 0:
            os.close(fd)
        try:
            os.unlink(temporary)
        except OSError:
            pass


def complete_doc_oauth(
    *,
    code: str,
    state: str,
    app_id: str = APP_ID,
    redirect_uri: str = OAUTH_REDIRECT_URI,
    state_path: Path | None = None,
    auth_path: Path | None = None,
    now: int | None = None,
    timeout_seconds: int = 45,
    secret_loader: Callable[[], str] | None = None,
    exchange_code: Callable[..., dict[str, Any]] | None = None,
    fetch_user_info: Callable[..., dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Validate and consume one direct OAuth callback, then atomically rotate auth."""
    _fixed_oauth_inputs(app_id, redirect_uri)
    if not _valid_callback_value(code, is_state=False) or not _valid_callback_value(state, is_state=True):
        raise KeepwarmError("OAuth callback parameters are invalid", error_class="OAUTH_INPUT_INVALID")
    if type(timeout_seconds) is not int or not 1 <= timeout_seconds <= 120:
        raise KeepwarmError("OAuth timeout is invalid", error_class="OAUTH_INPUT_INVALID")
    state_path = OAUTH_STATE_PATH if state_path is None else state_path
    auth_path = AUTH_PATH if auth_path is None else auth_path
    completed_at = int(time.time()) if now is None else now
    if type(completed_at) is not int or completed_at < 0:
        raise KeepwarmError("OAuth clock value is invalid", error_class="OAUTH_INPUT_INVALID")
    with _oauth_state_lock(state_path):
        ledger = _read_oauth_state(state_path, now=completed_at)
        if not hmac.compare_digest(state, ledger["state"]):
            raise KeepwarmError("OAuth state does not match", error_class="OAUTH_STATE_MISMATCH")
        expected_snapshot = ledger["auth_snapshot"]
        with _oauth_auth_lock(auth_path):
            if not _same_auth_snapshot(_auth_snapshot(auth_path), expected_snapshot):
                raise KeepwarmError(
                    "OAuth credential changed during authorization",
                    error_class="AUTH_STORAGE_ERROR",
                )
            app_secret = (secret_loader or _load_oauth_app_secret)()
            if (
                not isinstance(app_secret, str)
                or not 1 <= len(app_secret) <= 4096
                or any(ord(char) < 32 for char in app_secret)
            ):
                raise KeepwarmError(
                    "OAuth app secret is unavailable",
                    error_class="REAUTH_REQUIRED",
                )
            exchange = exchange_code or _exchange_authorization_code
            try:
                response = exchange(
                    code=code,
                    app_id=app_id,
                    app_secret=app_secret,
                    redirect_uri=redirect_uri,
                    timeout_seconds=timeout_seconds,
                )
            except Exception as exc:
                error_class = str(
                    getattr(exc, "error_class", "OAUTH_EXCHANGE_FAILED")
                )
                raise KeepwarmError(
                    "OAuth code exchange request failed",
                    error_class=error_class,
                ) from exc
            if not _same_auth_snapshot(_auth_snapshot(auth_path), expected_snapshot):
                raise KeepwarmError(
                    "OAuth credential changed during authorization",
                    error_class="AUTH_STORAGE_ERROR",
                )
            token_value = _oauth_token_value(response, now_ms=completed_at * 1000)
            try:
                user = (fetch_user_info or _fetch_oauth_user_info)(
                    access_token=token_value["accessToken"],
                    timeout_seconds=timeout_seconds,
                )
            except Exception as exc:
                error_class = str(
                    getattr(exc, "error_class", "OAUTH_USER_INFO_FAILED")
                )
                raise KeepwarmError(
                    "OAuth user_info request failed",
                    error_class=error_class,
                ) from exc
            owner = _validated_owner(user)
            token_value["userInfo"] = owner
            _replace_oauth_auth(
                token_value,
                expected_snapshot=expected_snapshot,
                auth_path=auth_path,
            )
        consumed = {
            "schema_version": OAUTH_STATE_SCHEMA_VERSION,
            "status": "consumed",
            "app_id": APP_ID,
            "redirect_uri": OAUTH_REDIRECT_URI,
            "consumed_at": completed_at,
        }
        _atomic_private_json(state_path, consumed, max_bytes=_OAUTH_STATE_MAX_BYTES)
    return {
        "success": True,
        "appId": APP_ID,
        "expiresAt": token_value["expiresAt"],
        "userInfo": owner,
        "message": "OAuth authorization completed",
    }


def read_auth_metadata(auth_path: Path | None = None) -> dict[str, Any]:
    auth_path = AUTH_PATH if auth_path is None else auth_path
    try:
        raw, _ = _read_bounded_owned_file(
            auth_path,
            max_bytes=_AUTH_MAX_BYTES,
            exact_mode=True,
        )
    except FileNotFoundError:
        return {"exists": False, "expiresAt": None, "owner": None}
    data = json.loads(raw.decode("utf-8"))
    value = data.get("value", data) if isinstance(data, dict) else {}
    user = value.get("userInfo", {}) if isinstance(value, dict) else {}
    owner = user.get("name") or user.get("userId")
    return {
        "exists": True,
        "expiresAt": value.get("expiresAt"),
        "owner": owner,
    }


def classify_error(message: str) -> str:
    if any(pattern.lower() in message.lower() for pattern in REAUTH_PATTERNS):
        return "REAUTH_REQUIRED"
    return "PROBE_FAILED"


def _now_ms() -> int:
    return int(time.time() * 1000)


def _auth_value(auth_path: Path | None = None) -> tuple[dict[str, Any], dict[str, Any]]:
    path = AUTH_PATH if auth_path is None else auth_path
    try:
        raw, _ = _read_bounded_owned_file(
            path,
            max_bytes=_AUTH_MAX_BYTES,
            exact_mode=True,
        )
    except FileNotFoundError:
        raise KeepwarmError("OAuth credential is missing", error_class="REAUTH_REQUIRED")
    try:
        data = json.loads(raw.decode("utf-8"))
    except Exception as exc:
        raise KeepwarmError("OAuth credential metadata is unreadable", error_class="REAUTH_REQUIRED") from exc
    if not isinstance(data, dict):
        raise KeepwarmError("OAuth credential metadata is invalid", error_class="REAUTH_REQUIRED")
    value = data.get("value", data)
    if not isinstance(value, dict):
        raise KeepwarmError("OAuth credential value is invalid", error_class="REAUTH_REQUIRED")
    return data, value


def _app_secret() -> str:
    # Keep the lookup compatible with both the local MCP and Hermes .env names.
    return (
        os.getenv("FEISHU_DEFAULT_APP_SECRET", "").strip()
        or os.getenv("FEISHU_APP_SECRET", "").strip()
    )


def _load_oauth_app_secret() -> str:
    """Load the canonical Hermes dotenv only after callback validation."""

    try:
        from hermes_cli.env_loader import load_hermes_dotenv

        load_hermes_dotenv()
    except Exception:
        return ""
    return _app_secret()


def _http_json(
    method: str,
    url: str,
    *,
    payload: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    timeout_seconds: int = 45,
) -> dict[str, Any]:
    request_headers = {"Accept": "application/json", **(headers or {})}
    body = None
    if payload is not None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        request_headers.setdefault("Content-Type", "application/json")
    request = urllib.request.Request(url, data=body, headers=request_headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            raw = response.read()
    except urllib.error.HTTPError as exc:
        try:
            raw = exc.read()
        except Exception:
            raw = b""
        detail = _safe_error_message(raw.decode("utf-8", errors="replace"))
        raise KeepwarmError(
            f"Feishu HTTP {exc.code}: {detail or 'request failed'}",
            error_class="HTTP_ERROR",
        ) from exc
    except Exception as exc:
        raise KeepwarmError(_safe_error_message(str(exc)), error_class="NETWORK_ERROR") from exc
    try:
        parsed = json.loads(raw.decode("utf-8") if isinstance(raw, bytes) else raw)
    except Exception as exc:
        raise KeepwarmError("Feishu response was not valid JSON", error_class="INVALID_RESPONSE") from exc
    if not isinstance(parsed, dict):
        raise KeepwarmError("Feishu response was not an object", error_class="INVALID_RESPONSE")
    return parsed


def _response_code(response: dict[str, Any]) -> int | None:
    code = response.get("code")
    try:
        return int(code) if code is not None else None
    except (TypeError, ValueError):
        return None


def _response_message(response: dict[str, Any]) -> str:
    return _safe_error_message(str(response.get("msg") or response.get("message") or response.get("error_description") or ""))


def _token_payload(response: dict[str, Any]) -> dict[str, Any]:
    data = response.get("data")
    return data if isinstance(data, dict) else response


def _write_auth_value(data: dict[str, Any], value: dict[str, Any], auth_path: Path | None = None) -> None:
    path = AUTH_PATH if auth_path is None else auth_path
    try:
        st = path.lstat()
    except FileNotFoundError as exc:
        raise KeepwarmError("OAuth credential disappeared", error_class="REAUTH_REQUIRED") from exc
    if not stat_is_regular_owned(st):
        raise KeepwarmError("OAuth credential file is not an owned regular file", error_class="AUTH_STORAGE_ERROR")
    # Refuse to overwrite a credential rotation that happened after the
    # refresh request began.  The refresh response is tied to the snapshot
    # read by _auth_value(); blindly replacing a newer file can lose tokens.
    try:
        raw, _ = _read_bounded_owned_file(
            path,
            max_bytes=_AUTH_MAX_BYTES,
            exact_mode=True,
        )
        current = json.loads(raw.decode("utf-8"))
    except Exception as exc:
        raise KeepwarmError("OAuth credential changed during refresh", error_class="AUTH_STORAGE_ERROR") from exc
    if current != data:
        raise KeepwarmError("OAuth credential changed during refresh", error_class="AUTH_STORAGE_ERROR")
    updated = dict(data)
    if "value" in updated:
        updated["value"] = value
    else:
        updated = value
    _owned_directory(path.parent)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            fd = -1
            json.dump(updated, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        try:
            latest_raw, _ = _read_bounded_owned_file(
                path,
                max_bytes=_AUTH_MAX_BYTES,
                exact_mode=True,
            )
            latest = json.loads(latest_raw.decode("utf-8"))
        except Exception as exc:
            raise KeepwarmError(
                "OAuth credential changed during refresh",
                error_class="AUTH_STORAGE_ERROR",
            ) from exc
        if latest != data:
            raise KeepwarmError(
                "OAuth credential changed during refresh",
                error_class="AUTH_STORAGE_ERROR",
            )
        os.replace(tmp_name, path)
        _fsync_directory(path.parent)
        _safe_existing_private_file(path, max_bytes=_AUTH_MAX_BYTES, exact_mode=True)
    finally:
        if fd >= 0:
            os.close(fd)
        try:
            os.unlink(tmp_name)
        except OSError:
            pass


def stat_is_regular_owned(st: os.stat_result) -> bool:
    return (
        stat.S_ISREG(st.st_mode)
        and st.st_uid == os.getuid()
        and st.st_nlink == 1
        and stat.S_IMODE(st.st_mode) == 0o600
    )


def _refresh_access_token(
    data: dict[str, Any],
    value: dict[str, Any],
    *,
    timeout_seconds: int,
    auth_path: Path | None = None,
) -> tuple[str, int]:
    path = AUTH_PATH if auth_path is None else auth_path
    # Re-read after acquiring the writer lock. The caller's values may have
    # become stale while another controlled keep-warm/callback writer ran.
    del data, value
    with _oauth_auth_lock(path):
        locked_data, locked_value = _auth_value(path)
        refresh_token = str(locked_value.get("refreshToken") or "")
        app_id = str(locked_value.get("appId") or APP_ID)
        app_secret = _load_oauth_app_secret()
        if not refresh_token or not app_secret:
            raise KeepwarmError(
                "OAuth refresh requires owner reauthorization",
                error_class="REAUTH_REQUIRED",
            )
        response = _http_json(
            "POST",
            TOKEN_URL,
            payload={
                "grant_type": "refresh_token",
                "client_id": app_id,
                "client_secret": app_secret,
                "refresh_token": refresh_token,
            },
            timeout_seconds=timeout_seconds,
        )
        code = _response_code(response)
        if code not in (None, 0):
            message = _response_message(response) or f"OAuth refresh failed ({code})"
            error_class = (
                "REAUTH_REQUIRED"
                if code in TOKEN_ERROR_CODES
                else "TOKEN_REFRESH_FAILED"
            )
            raise KeepwarmError(message, error_class=error_class)
        payload = _token_payload(response)
        access_token = str(payload.get("access_token") or "")
        if not access_token:
            raise KeepwarmError(
                "OAuth refresh returned no access token",
                error_class="REAUTH_REQUIRED",
            )
        new_refresh = str(payload.get("refresh_token") or refresh_token)
        try:
            expires_in = max(1, int(payload.get("expires_in") or 7200))
        except (TypeError, ValueError):
            expires_in = 7200
        expires_at = _now_ms() + expires_in * 1000
        updated_value = dict(locked_value)
        updated_value.update(
            {
                "appId": app_id,
                "accessToken": access_token,
                "refreshToken": new_refresh,
                "expiresAt": expires_at,
            }
        )
        _write_auth_value(locked_data, updated_value, path)
        return access_token, expires_at


def _user_info(access_token: str, *, timeout_seconds: int) -> dict[str, Any]:
    response = _http_json(
        "GET",
        USER_INFO_URL,
        headers={"Authorization": f"Bearer {access_token}"},
        timeout_seconds=timeout_seconds,
    )
    code = _response_code(response)
    if code not in (None, 0):
        message = _response_message(response) or f"user_info failed ({code})"
        error_class = "TOKEN_EXPIRED" if code in TOKEN_ERROR_CODES else "API_ERROR"
        raise KeepwarmError(message, error_class=error_class)
    payload = response.get("data") if isinstance(response.get("data"), dict) else response
    if not isinstance(payload, dict) or not payload:
        raise KeepwarmError(
            "Feishu user_info response missing user data",
            error_class="INVALID_RESPONSE",
        )
    return payload


def call_feishu_get_user_info(timeout_seconds: int = 45) -> dict[str, Any]:
    """Perform a direct, read-only Feishu user-info probe without MCP."""
    data, value = _auth_value()
    access_token = str(value.get("accessToken") or "")
    if not access_token:
        raise KeepwarmError("OAuth access token is missing", error_class="REAUTH_REQUIRED")
    try:
        expires_at = int(value.get("expiresAt") or 0)
    except (TypeError, ValueError):
        expires_at = 0
    refreshed = False
    if expires_at <= _now_ms() + REFRESH_MARGIN_MS:
        access_token, _ = _refresh_access_token(
            data, value, timeout_seconds=timeout_seconds
        )
        refreshed = True
    try:
        user = _user_info(access_token, timeout_seconds=timeout_seconds)
    except KeepwarmError as exc:
        if exc.error_class != "TOKEN_EXPIRED" or refreshed:
            raise
        access_token, _ = _refresh_access_token(
            data, value, timeout_seconds=timeout_seconds
        )
        user = _user_info(access_token, timeout_seconds=timeout_seconds)
        refreshed = True
    return _scrub({"structuredContent": user, "refreshed": refreshed})


def _metadata_or_empty() -> dict[str, Any]:
    try:
        return read_auth_metadata()
    except Exception:
        return {"exists": False, "expiresAt": None, "owner": None}


def keepwarm() -> tuple[int, dict[str, Any]]:
    before = _metadata_or_empty()
    owner = before.get("owner")
    try:
        call_feishu_get_user_info()
        after = _metadata_or_empty()
        owner = after.get("owner") or owner
        before_exp = before.get("expiresAt")
        after_exp = after.get("expiresAt")
        rotated = bool(
            isinstance(before_exp, (int, float))
            and isinstance(after_exp, (int, float))
            and after_exp > before_exp
        )
        return 0, {
            "before_expiresAt": before_exp,
            "after_expiresAt": after_exp,
            "rotated": rotated,
            "owner": owner,
            "health": "OK",
        }
    except Exception as exc:  # noqa: BLE001 - CLI must classify all failures.
        msg = _safe_error_message(str(exc))
        error_class = getattr(exc, "error_class", "")
        # Preserve an explicit auth classification even when provider text is
        # localized or does not contain one of the known error patterns.
        health = "REAUTH_REQUIRED" if error_class == "REAUTH_REQUIRED" else classify_error(msg)
        after = _metadata_or_empty()
        return 2, {
            "before_expiresAt": before.get("expiresAt"),
            "after_expiresAt": after.get("expiresAt"),
            "rotated": False,
            "owner": after.get("owner") or owner,
            "health": health,
            "error_class": error_class or type(exc).__name__,
            "error": msg,
        }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Keep feishu-doc OAuth token warm via the Feishu Open API")
    parser.add_argument("--json", action="store_true", help="Print compact JSON result")
    args = parser.parse_args(argv)
    rc, result = keepwarm()
    if args.json:
        print(json.dumps(result, ensure_ascii=False, separators=(",", ":")))
    else:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
