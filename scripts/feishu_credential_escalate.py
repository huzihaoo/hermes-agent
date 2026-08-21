#!/usr/bin/env python3
"""Feishu credential health escalation router.

Consumes batch-2 health rows and renders/sends one fresh plain-text Feishu @ DM
per credential-health transition.  Default is dry-run.  Real sends require
``--send`` and use the existing relay/send_message_tool path, with p2p open_id
as the explicit target.  If the owner open_id or an explicit target is missing,
this module refuses to send and never falls back to a group/home channel.
"""
from __future__ import annotations

import argparse
import datetime as dt
import contextlib
import fcntl
import getpass
import hmac
import json
import math
import os
import re
import select
import stat
import subprocess
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse
from typing import Any, Callable

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from gateway.feishu_mention import (  # noqa: E402
    _load_user_id_mapping,
    _plain,
    build_at_mention,
    compute_notify_key,
    resolve_display_name,
)
from tools.send_message_tool import send_message_tool  # noqa: E402
from agent.redact import redact_sensitive_text  # noqa: E402
from scripts.feishu_doc_token_keepwarm import (  # noqa: E402
    OAUTH_STATE_MAX_TTL_SECONDS,
    OAUTH_STATE_MIN_TTL_SECONDS,
    OAUTH_STATE_TTL_SECONDS,
    complete_doc_oauth,
    start_doc_oauth,
)

DEFAULT_OWNER_OPEN_ID = os.getenv(
    "FEISHU_CREDENTIAL_DEFAULT_OWNER_OPEN_ID",
    "ou_d1d3cfeba1be0a22faa36aaf4fb3907d",
).strip()
STATE_PATH = Path("/Users/songying/.hermes/runtime/shared-state/feishu_credential_escalation_state.json")
FALLBACK_PATH = Path(
    "/Users/songying/.hermes/runtime/shared-state/feishu_credential_alert_fallback.json"
)
DEFAULT_HEALTH_PATH = Path(
    "/Users/songying/.hermes/workspace-work/knowledge/outputs/feishu-credential-health/latest.json"
)
RUNBOOK_PATH = "/Users/songying/.hermes/workspace-work/knowledge/wiki/runbooks/feishu-credential-runbook.md"
ESCALATE_HEALTH = {"REAUTH_REQUIRED", "EXPIRED", "EXPIRING(<7d)", "PROBE_FAILED"}
EXPECTED_HEALTH_SURFACES = frozenset({"doc", "project", "meegle_cli"})
VALID_HEALTH_STATES = frozenset({"OK", *ESCALATE_HEALTH})
DEFAULT_COOLDOWN_SECONDS = 24 * 3600
APP_ID = "cli_a99b38e0a29b500b"
CALLBACK_HOST = "127.0.0.1"
CALLBACK_PORT = 3010
AUTH_URL_TTL_SECONDS = OAUTH_STATE_TTL_SECONDS
CALLBACK_LOG_PATH = Path("/Users/songying/.hermes/logs/feishu-credential-callback-listener.log")
_FALLBACK_MAX_ALERTS = 100
_FALLBACK_MAX_BYTES = 1 << 20
_FALLBACK_SECRET_RE = re.compile(
    r"(?i)(?:https?://\S+|[\"']?(?:token|access[_-]?token|refresh[_-]?token|"
    r"(?:app|client)[_-]?secret|authorization|secret|password|credential|"
    r"device[_-]?code|(?:oauth[_-]?)?state|(?:(?:oauth|auth)[_-]?)?code)"
    r"[\"']?\s*[:=]\s*(?:\"[^\"]*\"|'[^']*'|\S+))"
)
_FEISHU_TOPIC_TARGET_RE = re.compile(
    r"^feishu:oc_[-A-Za-z0-9]+:om_[-A-Za-z0-9_]+$"
)
_FEISHU_OPEN_ID_RE = re.compile(r"^ou_[-A-Za-z0-9]+$")
_FEISHU_AUTH_HOSTS = frozenset({"open.feishu.cn"})
_OAUTH_STATE_RE = re.compile(r"^[A-Za-z0-9_-]{43,128}$")
_OAUTH_CALLBACK_MAX_BYTES = 8192
_CALLBACK_READY_TIMEOUT_SECONDS = 2.0
_CALLBACK_SOCKET_TIMEOUT_SECONDS = 5.0
_STATE_MAX_BYTES = 1 << 20
_SUPPORTED_OUTBOUND_MODES = frozenset({"", "live", "record-only"})
_LIVE_TRANSPORT_LOCK = threading.RLock()
_LISTENER_SECRET_ENV_KEY_RE = re.compile(
    r"(?i)(secret|token|password|passwd|authorization|credential|cookie|"
    r"api[_-]?key|private[_-]?key|ssh_auth_sock)"
)


@contextlib.contextmanager
def credential_live_transport():
    """Select the real transport only for this credential-alert send."""
    with _LIVE_TRANSPORT_LOCK:
        previous = os.environ.get("HERMES_OUTBOUND_MODE")
        normalized = str(previous or "").strip().lower()
        if normalized not in _SUPPORTED_OUTBOUND_MODES:
            raise RuntimeError(f"unsupported HERMES_OUTBOUND_MODE: {normalized!r}")
        os.environ["HERMES_OUTBOUND_MODE"] = "live"
        try:
            yield
        finally:
            if previous is None:
                os.environ.pop("HERMES_OUTBOUND_MODE", None)
            else:
                os.environ["HERMES_OUTBOUND_MODE"] = previous


def _fallback_text(value: Any, limit: int = 240) -> str:
    # Fallback data crosses a security boundary, so it must remain redacted
    # even when the process-wide diagnostic redaction preference is disabled.
    text = redact_sensitive_text(str(value or ""), force=True)
    text = _FALLBACK_SECRET_RE.sub("<redacted>", text)
    return " ".join(text.split())[:limit]


def _fallback_alert(row: dict[str, Any], *, reason: str, now: float | None = None) -> dict[str, Any]:
    """Build a secret-free, durable representation of one undelivered alert."""
    expires_at = row.get("expires_at")
    try:
        valid_expiry = (
            isinstance(expires_at, (int, float))
            and not isinstance(expires_at, bool)
            and math.isfinite(float(expires_at))
        )
    except (OverflowError, ValueError, TypeError):
        valid_expiry = False
    if not valid_expiry:
        expires_at = _fallback_text(expires_at, 64) or None
    try:
        recorded_at = float(now) if now is not None else time.time()
        if not math.isfinite(recorded_at):
            raise ValueError
    except (TypeError, ValueError, OverflowError):
        recorded_at = time.time()
    return {
        "surface": _fallback_text(row.get("surface"), 64),
        "health": _fallback_text(row.get("health"), 64),
        "owner": _fallback_text(row.get("owner"), 128) or None,
        "expires_at": expires_at,
        "checked_at": _fallback_text(row.get("checked_at"), 64) or None,
        "reason": _fallback_text(reason),
        "recorded_at": dt.datetime.fromtimestamp(
            recorded_at, dt.timezone.utc
        ).isoformat(timespec="seconds"),
    }


def _safe_send_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            str(key): (
                "<redacted>"
                if re.search(
                    r"(?i)(token|secret|password|authorization|credential|cookie|"
                    r"(?:auth|oauth|callback)[_-]?url|^(?:oauth[_-]?)?state$|"
                    r"^(?:(?:oauth|auth)[_-]?)?code$)",
                    str(key),
                )
                else _safe_send_value(item)
            )
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_safe_send_value(item) for item in value]
    if isinstance(value, str):
        if _OAUTH_STATE_RE.fullmatch(value):
            return "<redacted>"
        return _fallback_text(value, 500)
    if value is None or isinstance(value, (bool, int, float)):
        return value
    return _fallback_text(repr(value), 500)


def _strict_success_payload(value: Any) -> bool:
    """Accept delivery/auth success only from an explicit, non-contradictory payload."""
    if not isinstance(value, dict) or value.get("success") is not True:
        return False
    is_error = value.get("isError")
    if is_error is not None and is_error is not False:
        return False
    error = value.get("error")
    return error is None or (isinstance(error, str) and not error.strip())


def _safe_existing_fallback(path: Path) -> None:
    try:
        st = path.lstat()
    except FileNotFoundError:
        return
    if (
        not stat.S_ISREG(st.st_mode)
        or st.st_uid != os.getuid()
        or st.st_nlink != 1
        or stat.S_ISLNK(st.st_mode)
        or (st.st_mode & 0o077)
        or st.st_size > _FALLBACK_MAX_BYTES
    ):
        raise RuntimeError("fallback ledger path is not a private regular file")


def _write_fallback_alert(
    alert: dict[str, Any], path: Path | None = None
) -> dict[str, Any]:
    """Atomically persist the fallback ledger with owner-only permissions."""
    path = FALLBACK_PATH if path is None else path
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    parent_stat = path.parent.lstat()
    if not stat.S_ISDIR(parent_stat.st_mode) or parent_stat.st_uid != os.getuid():
        raise RuntimeError("fallback ledger parent is not an owned directory")
    _safe_existing_fallback(path)
    lock_path = path.with_name(f".{path.name}.lock")
    lock_flags = os.O_CREAT | os.O_RDWR
    if hasattr(os, "O_NOFOLLOW"):
        lock_flags |= os.O_NOFOLLOW
    lock_fd = os.open(lock_path, lock_flags, 0o600)
    try:
        lock_stat = os.fstat(lock_fd)
        if not stat.S_ISREG(lock_stat.st_mode) or lock_stat.st_uid != os.getuid() or lock_stat.st_nlink != 1:
            raise RuntimeError("fallback ledger lock is not an owned regular file")
        os.fchmod(lock_fd, 0o600)
        fcntl.flock(lock_fd, fcntl.LOCK_EX)
        _safe_existing_fallback(path)
        current = load_json(path)
        alerts = current.get("alerts") if isinstance(current, dict) else None
        alerts = [item for item in alerts if isinstance(item, dict)] if isinstance(alerts, list) else []
        identity = (alert.get("surface"), alert.get("health"), alert.get("expires_at"))
        alerts = [
            item for item in alerts
            if (item.get("surface"), item.get("health"), item.get("expires_at")) != identity
        ]
        alerts.append(alert)
        payload = {
            "schema_version": 1,
            "updated_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
            "alerts": alerts[-_FALLBACK_MAX_ALERTS:],
        }
        fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
        try:
            os.fchmod(fd, 0o600)
            raw = (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
            with os.fdopen(fd, "wb") as handle:
                fd = -1
                handle.write(raw)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_name, path)
            _safe_existing_fallback(path)
            try:
                dir_fd = os.open(path.parent, os.O_RDONLY)
                try:
                    os.fsync(dir_fd)
                finally:
                    os.close(dir_fd)
            except OSError:
                pass
        finally:
            if fd >= 0:
                os.close(fd)
            try:
                os.unlink(tmp_name)
            except OSError:
                pass
    finally:
        fcntl.flock(lock_fd, fcntl.LOCK_UN)
        os.close(lock_fd)
    return alert


def _resolve_fallback_alert(row: dict[str, Any], path: Path | None = None) -> None:
    """Remove recovered surfaces from the current fallback ledger."""
    path = FALLBACK_PATH if path is None else path
    if not path.exists():
        return
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    parent_stat = path.parent.lstat()
    if not stat.S_ISDIR(parent_stat.st_mode) or parent_stat.st_uid != os.getuid():
        raise RuntimeError("fallback ledger parent is not an owned directory")
    lock_path = path.with_name(f".{path.name}.lock")
    lock_flags = os.O_CREAT | os.O_RDWR
    if hasattr(os, "O_NOFOLLOW"):
        lock_flags |= os.O_NOFOLLOW
    lock_fd = os.open(lock_path, lock_flags, 0o600)
    try:
        lock_stat = os.fstat(lock_fd)
        if not stat.S_ISREG(lock_stat.st_mode) or lock_stat.st_uid != os.getuid() or lock_stat.st_nlink != 1:
            raise RuntimeError("fallback ledger lock is not an owned regular file")
        os.fchmod(lock_fd, 0o600)
        fcntl.flock(lock_fd, fcntl.LOCK_EX)
        _safe_existing_fallback(path)
        current = load_json(path)
        alerts = current.get("alerts") if isinstance(current, dict) else None
        if not isinstance(alerts, list):
            return
        surface = str(row.get("surface") or "")
        remaining = [
            item for item in alerts
            if not isinstance(item, dict) or item.get("surface") != surface
        ]
        if len(remaining) == len(alerts):
            return
        payload = {
            "schema_version": 1,
            "updated_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
            "alerts": remaining,
        }
        fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
        try:
            os.fchmod(fd, 0o600)
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                fd = -1
                json.dump(payload, handle, ensure_ascii=False, indent=2)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_name, path)
            _safe_existing_fallback(path)
        finally:
            if fd >= 0:
                os.close(fd)
            try:
                os.unlink(tmp_name)
            except OSError:
                pass
    finally:
        fcntl.flock(lock_fd, fcntl.LOCK_UN)
        os.close(lock_fd)


def _apple_script_string(value: str) -> str:
    """Quote a value for one argv-contained AppleScript expression."""
    return '"' + str(value).replace("\\", "\\\\").replace('"', '\\"').replace("\n", " ") + '"'


def _notify_fallback(alert: dict[str, Any], *, runner: Callable[..., Any] | None = None) -> dict[str, Any]:
    """Best-effort local notification; persistence remains the source of truth."""
    title = "Feishu credential alert"
    body = f"{alert.get('surface') or 'credential'}: {alert.get('health') or 'unhealthy'}"
    script = (
        f"display notification {_apple_script_string(body)} with title "
        f"{_apple_script_string(title)}"
    )
    invoke = runner or subprocess.run
    try:
        proc = invoke(
            ["/usr/bin/osascript", "-e", script],
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
        ok = getattr(proc, "returncode", 1) == 0
        return {"attempted": True, "ok": ok, "error": None if ok else "osascript_failed"}
    except Exception as exc:  # noqa: BLE001 - fallback must never hide the alert.
        return {"attempted": True, "ok": False, "error": type(exc).__name__}


def persist_fallback(row: dict[str, Any], *, reason: str, now: float | None = None) -> dict[str, Any]:
    alert = _fallback_alert(row, reason=reason, now=now)
    persisted_alert: dict[str, Any] | None = None
    persistence_error: str | None = None
    try:
        persisted_alert = _write_fallback_alert(alert)
    except Exception as exc:  # notification is an independent best-effort leg.
        persistence_error = f"{type(exc).__name__}: {_fallback_text(exc)}"
    notification = _notify_fallback(persisted_alert or alert)
    # The first write is the durable sensing guarantee.  A second best-effort
    # write records whether the local UI notification also succeeded.
    if persisted_alert is not None:
        try:
            _write_fallback_alert({**persisted_alert, "notification": notification})
        except Exception as exc:
            persistence_error = persistence_error or f"{type(exc).__name__}: {_fallback_text(exc)}"
    result: dict[str, Any] = {
        "persisted": persisted_alert is not None,
        "path": str(FALLBACK_PATH),
        "notification": notification,
    }
    if persistence_error:
        result["persistence_error"] = persistence_error
    return result


def _persist_fallback_safely(
    row: dict[str, Any], *, reason: str, now: float | None = None
) -> dict[str, Any]:
    try:
        return persist_fallback(row, reason=reason, now=now)
    except Exception as exc:  # noqa: BLE001 - retain the delivery failure result.
        return {
            "persisted": False,
            "path": str(FALLBACK_PATH),
            "error": f"{type(exc).__name__}: {_fallback_text(exc)}",
        }


@contextlib.contextmanager
def escalation_state_lock(path: Path | None = None):
    """Serialize send/dedup decisions across cron and manual processes."""
    path = STATE_PATH if path is None else path
    path.parent.mkdir(parents=True, exist_ok=True)
    parent_stat = path.parent.lstat()
    if not stat.S_ISDIR(parent_stat.st_mode) or parent_stat.st_uid != os.getuid():
        raise RuntimeError("escalation state parent is not an owned directory")
    lock_path = path.with_name(f".{path.name}.lock")
    flags = os.O_CREAT | os.O_RDWR
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    lock_fd = os.open(lock_path, flags, 0o600)
    try:
        lock_stat = os.fstat(lock_fd)
        if (
            not stat.S_ISREG(lock_stat.st_mode)
            or lock_stat.st_uid != os.getuid()
            or lock_stat.st_nlink != 1
        ):
            raise RuntimeError("escalation state lock is not an owned regular file")
        os.fchmod(lock_fd, 0o600)
        fcntl.flock(lock_fd, fcntl.LOCK_EX)
        yield
    finally:
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
        finally:
            os.close(lock_fd)


def _validated_escalation_state(value: Any) -> tuple[dict[str, Any], str | None]:
    """Normalize an empty ledger and fail closed on malformed dedup state."""
    if not isinstance(value, dict):
        return {"sent": {}}, "state_payload_invalid"
    state = dict(value)
    sent = state.get("sent")
    if sent is None:
        state["sent"] = {}
    elif not isinstance(sent, dict):
        return {"sent": {}}, "state_sent_ledger_invalid"
    else:
        for key, timestamp in sent.items():
            key_valid = (
                isinstance(key, str)
                and len(key) <= 512
                and key.count("|") == 3
                and key.split("|", 1)[0] in ESCALATE_HEALTH
                and bool(key.split("|", 2)[1])
                and not any(ord(char) < 32 for char in key)
            )
            timestamp_valid = (
                isinstance(timestamp, (int, float))
                and not isinstance(timestamp, bool)
                and math.isfinite(float(timestamp))
                and float(timestamp) > 0
            )
            if not key_valid or not timestamp_valid:
                return {"sent": {}}, "state_sent_entry_invalid"
    return state, None


def _validate_state_path(path: Path) -> None:
    """Reject symlinked/untrusted state files before reading or replacing them."""
    try:
        st = path.lstat()
    except FileNotFoundError:
        return
    if (
        not stat.S_ISREG(st.st_mode)
        or stat.S_ISLNK(st.st_mode)
        or st.st_uid != os.getuid()
        or st.st_nlink != 1
        or st.st_size > _STATE_MAX_BYTES
        or stat.S_IMODE(st.st_mode) != 0o600
    ):
        raise RuntimeError("escalation state path is not a private regular file")


def _read_bounded_state_file(path: Path) -> bytes:
    """Read one owner state file without following a swapped symlink."""

    _validate_state_path(path)
    before = path.lstat()
    flags = os.O_RDONLY
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags)
    try:
        opened = os.fstat(descriptor)
        if (
            (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino)
            or not stat.S_ISREG(opened.st_mode)
            or opened.st_uid != os.getuid()
            or opened.st_nlink != 1
        ):
            raise RuntimeError("escalation state changed during read")
        chunks: list[bytes] = []
        remaining = _STATE_MAX_BYTES + 1
        while remaining:
            chunk = os.read(descriptor, min(64 * 1024, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        raw = b"".join(chunks)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    if len(raw) > _STATE_MAX_BYTES or len(raw) != opened.st_size or (
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
        raise RuntimeError("escalation state changed during read")
    return raw


def _write_escalation_state(state: dict[str, Any]) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    parent_stat = STATE_PATH.parent.lstat()
    if not stat.S_ISDIR(parent_stat.st_mode) or parent_stat.st_uid != os.getuid():
        raise RuntimeError("escalation state parent is not an owned directory")
    _validate_state_path(STATE_PATH)
    raw = (json.dumps(state, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
    if len(raw) > _STATE_MAX_BYTES:
        raise RuntimeError("escalation state payload is too large")
    descriptor, temporary = tempfile.mkstemp(
        prefix=f".{STATE_PATH.name}.",
        dir=str(STATE_PATH.parent),
    )
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            descriptor = -1
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        _validate_state_path(STATE_PATH)
        os.replace(temporary, STATE_PATH)
        directory_flags = os.O_RDONLY
        if hasattr(os, "O_DIRECTORY"):
            directory_flags |= os.O_DIRECTORY
        if hasattr(os, "O_CLOEXEC"):
            directory_flags |= os.O_CLOEXEC
        if hasattr(os, "O_NOFOLLOW"):
            directory_flags |= os.O_NOFOLLOW
        directory = os.open(STATE_PATH.parent, directory_flags)
        try:
            opened_parent = os.fstat(directory)
            if (
                not stat.S_ISDIR(opened_parent.st_mode)
                or opened_parent.st_uid != os.getuid()
            ):
                raise RuntimeError("escalation state parent changed during fsync")
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        try:
            os.unlink(temporary)
        except OSError:
            pass
    _validate_state_path(STATE_PATH)


def load_escalation_state(path: Path | None = None) -> tuple[dict[str, Any], str | None]:
    """Load dedup state while preserving missing-vs-corrupt semantics."""
    path = STATE_PATH if path is None else path
    try:
        raw = _read_bounded_state_file(path).decode("utf-8")
    except FileNotFoundError:
        return {"sent": {}}, None
    except Exception:
        return {"sent": {}}, "state_unreadable"
    try:
        value = json.loads(raw)
    except Exception:
        return {"sent": {}}, "state_json_invalid"
    return _validated_escalation_state(value)


def load_send_environment() -> list[Path]:
    """Load ~/.hermes/.env before standalone --send uses send_message_tool.

    Gateway processes load Hermes dotenv at startup; cron/escalate are standalone
    processes, so they must mirror the canonical CLI send path without putting
    secrets into plist or logs.
    """
    try:
        from hermes_cli.env_loader import load_hermes_dotenv

        return load_hermes_dotenv()
    except Exception:
        return []



def _node_mcp_call_script(tool_name: str, arguments: dict[str, Any]) -> Path:
    payload = json.dumps({"tool": tool_name, "arguments": arguments}, ensure_ascii=False)
    source = r"""
import { Client } from '/Users/songying/.hermes/local-mcp/feishu-doc/node_modules/@modelcontextprotocol/sdk/dist/esm/client/index.js';
import { StdioClientTransport } from '/Users/songying/.hermes/local-mcp/feishu-doc/node_modules/@modelcontextprotocol/sdk/dist/esm/client/stdio.js';
import fs from 'fs';
import yaml from '/Users/songying/.hermes/local-mcp/feishu-doc/node_modules/js-yaml/dist/js-yaml.mjs';
const req = __PAYLOAD__;
const configPath = process.env.HERMES_CONFIG_PATH || '/Users/songying/.hermes/config.yaml';
const config = yaml.load(fs.readFileSync(configPath,'utf8'));
const s = config.mcp_servers['feishu-doc'];
const transport = new StdioClientTransport({command:s.command,args:s.args,env:{...process.env,...s.env}});
const client = new Client({name:'hermes-feishu-credential-reauth',version:'0.1.0'});
function scrub(value) {
  if (Array.isArray(value)) return value.map(scrub);
  if (value && typeof value === 'object') {
    const out = {};
    for (const [k, v] of Object.entries(value)) {
      const kl = k.toLowerCase();
      if (kl.includes('token') || kl.includes('secret') || kl.includes('password') || kl.includes('authorization')) out[k] = '<redacted>';
      else out[k] = scrub(v);
    }
    return out;
  }
  if (typeof value === 'string') return value.replace(/\b(?:u-|t-|m-)[A-Za-z0-9_-]{12,}\b|Bearer\s+\S+/gi, '<redacted>');
  return value;
}
await client.connect(transport);
try {
  const result = await client.callTool({name:req.tool, arguments:req.arguments});
  console.log(JSON.stringify(scrub(result)));
} finally {
  await client.close();
}
""".replace("__PAYLOAD__", payload)
    fd, path = tempfile.mkstemp(prefix="feishu_credential_mcp_", suffix=".mjs")
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        fh.write(source)
    return Path(path)


def _call_feishu_doc_tool(tool_name: str, arguments: dict[str, Any], timeout_seconds: int = 45) -> dict[str, Any]:
    script = _node_mcp_call_script(tool_name, arguments)
    try:
        proc = subprocess.run(
            ["node", str(script)],
            cwd=str(REPO_ROOT),
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout_seconds,
            check=False,
        )
        if proc.returncode != 0:
            return {"isError": True, "error": (proc.stderr or proc.stdout or f"node exited {proc.returncode}")[:240]}
        return json.loads(proc.stdout.strip() or "{}")
    finally:
        try:
            script.unlink()
        except OSError:
            pass


def get_doc_auth_url(
    call_tool: Callable[[str, dict[str, Any]], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    try:
        if call_tool is None:
            sc = start_doc_oauth(ttl_seconds=AUTH_URL_TTL_SECONDS)
        else:
            # Explicit injection remains for old result-shape compatibility in
            # tests. Production never selects or starts the retired MCP path.
            result = call_tool("feishu_auth_url", {"appId": APP_ID})
            sc = result.get("structuredContent") if isinstance(result, dict) else None
    except Exception as exc:  # noqa: BLE001 - alert generation must fail closed.
        return {
            "ok": False,
            "error": _fallback_text(str(exc)),
            "error_class": str(getattr(exc, "error_class", type(exc).__name__)),
        }
    if not isinstance(sc, dict):
        return {"ok": False, "error": "auth_url_failed"}
    auth_url = sc.get("auth_url") or sc.get("authUrl")
    state = sc.get("state")
    try:
        parsed = urlparse(auth_url)
        valid_url = (
            isinstance(auth_url, str)
            and len(auth_url) <= 4096
            and parsed.scheme == "https"
            and parsed.hostname in _FEISHU_AUTH_HOSTS
        )
    except (TypeError, ValueError):
        valid_url = False
    valid_state = isinstance(state, str) and bool(_OAUTH_STATE_RE.fullmatch(state))
    if not valid_url or not valid_state:
        return {"ok": False, "error": "auth_url_invalid"}
    return {"ok": True, "auth_url": auth_url, "state": state, "appId": sc.get("appId") or APP_ID}


def call_doc_auth_callback(
    *,
    code: str,
    state: str,
    call_tool: Callable[[str, dict[str, Any]], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    if call_tool is None:
        try:
            result = complete_doc_oauth(code=code, state=state)
        except Exception as exc:  # noqa: BLE001 - never expose auth payloads.
            return {
                "success": False,
                "isError": True,
                "error": _fallback_text(str(exc)),
                "error_class": str(getattr(exc, "error_class", type(exc).__name__)),
            }
        return {"structuredContent": result, "isError": False}
    return call_tool(
        "feishu_auth_callback",
        {"appId": APP_ID, "code": code, "state": state},
    )


def _parse_oauth_callback_input(raw: str) -> tuple[str, str]:
    value = str(raw or "").strip()
    if not value or len(value.encode("utf-8")) > _OAUTH_CALLBACK_MAX_BYTES:
        raise ValueError("OAuth callback input is missing or too large")
    if value.startswith("{"):
        try:
            payload = json.loads(value)
        except json.JSONDecodeError as exc:
            raise ValueError("OAuth callback JSON is invalid") from exc
        if not isinstance(payload, dict) or set(payload) != {"code", "state"}:
            raise ValueError("OAuth callback JSON schema is invalid")
        code = payload.get("code")
        state = payload.get("state")
    else:
        try:
            parsed = urlparse(value)
            allowed_origin = (
                parsed.scheme == "http"
                and parsed.netloc == f"localhost:{CALLBACK_PORT}"
                and parsed.hostname == "localhost"
                and parsed.port == CALLBACK_PORT
            )
        except ValueError:
            allowed_origin = False
            parsed = None
        if not allowed_origin or parsed is None or (
            parsed.path != "/oauth/feishu/callback"
            or parsed.params
            or parsed.fragment
        ):
            raise ValueError("OAuth callback URL is not allowed")
        query = parse_qs(parsed.query, keep_blank_values=True)
        if (
            set(query) != {"code", "state"}
            or len(query.get("code", [])) != 1
            or len(query.get("state", [])) != 1
        ):
            raise ValueError("OAuth callback URL parameters are invalid")
        code = query["code"][0]
        state = query["state"][0]
    if (
        not isinstance(code, str)
        or not 1 <= len(code) <= 4096
        or not all(0x21 <= ord(char) <= 0x7E for char in code)
        or not isinstance(state, str)
        or not _OAUTH_STATE_RE.fullmatch(state)
    ):
        raise ValueError("OAuth callback code or state is invalid")
    return code, state


def _read_oauth_callback_input(
    *,
    stream: Any | None = None,
    prompt_func: Callable[[str], str] | None = None,
) -> tuple[str, str]:
    """Read a callback URL/JSON without placing code or state in argv."""

    selected = sys.stdin if stream is None else stream
    if hasattr(selected, "isatty") and selected.isatty():
        raw = (prompt_func or getpass.getpass)("Paste OAuth callback URL or JSON: ")
    else:
        raw = selected.read(_OAUTH_CALLBACK_MAX_BYTES + 1)
    return _parse_oauth_callback_input(raw)


def _callback_listener_environment() -> dict[str, str]:
    """Build the pre-validation listener environment without inherited secrets."""

    return {
        key: value
        for key, value in os.environ.items()
        if not _LISTENER_SECRET_ENV_KEY_RE.search(key)
    }


def in_quiet_hours(now_dt: dt.datetime | None = None) -> bool:
    start = os.getenv("FEISHU_CREDENTIAL_QUIET_START", "22:00")
    end = os.getenv("FEISHU_CREDENTIAL_QUIET_END", "08:00")
    if str(os.getenv("FEISHU_CREDENTIAL_QUIET_HOURS", "1")).lower() in {"0", "false", "off", "no"}:
        return False
    now_dt = now_dt or dt.datetime.now().astimezone()
    def parse(value: str) -> tuple[int, int]:
        hh, mm = str(value or "").split(":", 1)
        return int(hh), int(mm)
    try:
        sh, sm = parse(start); eh, em = parse(end)
    except Exception:
        return False
    cur = now_dt.hour * 60 + now_dt.minute
    smin = sh * 60 + sm
    emin = eh * 60 + em
    if smin == emin:
        return False
    if smin < emin:
        return smin <= cur < emin
    return cur >= smin or cur < emin


def start_callback_listener(
    expected_state: str,
    *,
    ttl_seconds: int = AUTH_URL_TTL_SECONDS,
    readiness_timeout_seconds: float = _CALLBACK_READY_TIMEOUT_SECONDS,
    popen_factory: Callable[..., subprocess.Popen[Any]] | None = None,
    select_fn: Callable[..., tuple[list[int], list[int], list[int]]] | None = None,
    _return_process: bool = False,
) -> dict[str, Any]:
    if not isinstance(expected_state, str) or not _OAUTH_STATE_RE.fullmatch(expected_state):
        return {"started": False, "reason": "missing_state"}
    if type(ttl_seconds) is not int or not OAUTH_STATE_MIN_TTL_SECONDS <= ttl_seconds <= OAUTH_STATE_MAX_TTL_SECONDS:
        return {"started": False, "reason": "invalid_ttl"}
    if (
        isinstance(readiness_timeout_seconds, bool)
        or not isinstance(readiness_timeout_seconds, (int, float))
        or not math.isfinite(float(readiness_timeout_seconds))
        or not 0 < readiness_timeout_seconds <= 10
    ):
        return {"started": False, "reason": "invalid_readiness_timeout", "returncode": 2}
    CALLBACK_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    ready_read, ready_write = os.pipe()
    os.set_inheritable(ready_read, False)
    os.set_inheritable(ready_write, False)
    cmd = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--callback-listener",
        "--ttl-seconds",
        str(ttl_seconds),
        "--ready-fd",
        str(ready_write),
    ]
    proc: subprocess.Popen[Any] | None = None
    try:
        with CALLBACK_LOG_PATH.open("a", encoding="utf-8") as log:
            proc = (popen_factory or subprocess.Popen)(
                cmd,
                stdout=log,
                stderr=log,
                start_new_session=True,
                pass_fds=(ready_write,),
                env=_callback_listener_environment(),
            )
        os.close(ready_write)
        ready_write = -1
        readable, _, _ = (select_fn or select.select)(
            [ready_read], [], [], readiness_timeout_seconds
        )
        signal = os.read(ready_read, 64) if readable else b""
        if signal != b"READY\n":
            _terminate_callback_listener(proc)
            return {
                "started": False,
                "reason": "listener_not_ready" if readable else "readiness_timeout",
                "returncode": 2,
            }
        result: dict[str, Any] = {
            "started": True,
            "pid": proc.pid,
            "host": CALLBACK_HOST,
            "port": CALLBACK_PORT,
            "ttl_seconds": ttl_seconds,
        }
        if _return_process:
            result["_process"] = proc
        return result
    except Exception as exc:
        _terminate_callback_listener(proc)
        return {
            "started": False,
            "reason": _fallback_text(f"{type(exc).__name__}: {exc}"),
            "returncode": 2,
        }
    finally:
        if ready_write >= 0:
            os.close(ready_write)
        os.close(ready_read)


def _terminate_callback_listener(proc: Any | None) -> None:
    """Best-effort cleanup for a READY listener whose alert was not delivered."""

    if proc is None:
        return
    try:
        if proc.poll() is not None:
            return
        proc.terminate()
        try:
            proc.wait(timeout=2)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=2)
    except Exception:
        return


def _signal_listener_ready(ready_fd: int | None, payload: bytes) -> None:
    if ready_fd is None or ready_fd < 0:
        return
    try:
        os.write(ready_fd, payload)
    except OSError:
        pass
    finally:
        try:
            os.close(ready_fd)
        except OSError:
            pass


def run_callback_listener(
    *,
    ttl_seconds: int = AUTH_URL_TTL_SECONDS,
    ready_fd: int | None = None,
    server_factory: Callable[..., Any] | None = None,
    clock: Callable[[], float] | None = None,
) -> int:
    if type(ttl_seconds) is not int or not OAUTH_STATE_MIN_TTL_SECONDS <= ttl_seconds <= OAUTH_STATE_MAX_TTL_SECONDS:
        _signal_listener_ready(ready_fd, b"ERROR\n")
        print(json.dumps({"ok": False, "error_class": "OAUTH_INPUT_INVALID", "error": "invalid callback TTL"}), flush=True)
        return 2
    clock = time.time if clock is None else clock
    deadline = clock() + max(1, ttl_seconds)
    handled = {"done": False}

    class Handler(BaseHTTPRequestHandler):
        def setup(self) -> None:
            remaining = max(0.1, deadline - clock())
            self.request.settimeout(
                min(_CALLBACK_SOCKET_TIMEOUT_SECONDS, remaining)
            )
            super().setup()

        def do_GET(self):  # noqa: N802
            if urlparse(self.path).path != "/oauth/feishu/callback":
                self.send_response(404); self.end_headers(); return
            try:
                code, state = _parse_oauth_callback_input(
                    f"http://localhost:{CALLBACK_PORT}{self.path}"
                )
            except ValueError:
                self.send_response(400)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.end_headers()
                self.wfile.write("<html><body>授权失败：state 不匹配或缺少 code。请回到 runbook 走 paste-code。</body></html>".encode("utf-8"))
                return
            result = call_doc_auth_callback(code=code, state=state)
            structured = result.get("structuredContent") if isinstance(result, dict) else None
            success = _strict_success_payload(
                {
                    **(structured if isinstance(structured, dict) else {}),
                    **(
                        {key: result[key] for key in ("isError", "error") if key in result}
                        if isinstance(result, dict)
                        else {}
                    ),
                }
            )
            self.send_response(200 if success else 500)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            body = "<html><body><h2>飞书授权成功</h2><p>可以关闭此页面。</p></body></html>" if success else "<html><body><h2>飞书授权未完成</h2><p>请回到 runbook 走 paste-code。</p></body></html>"
            self.wfile.write(body.encode("utf-8"))
            handled["done"] = success

        def log_message(self, format: str, *args: Any) -> None:
            return

    try:
        server = (server_factory or HTTPServer)((CALLBACK_HOST, CALLBACK_PORT), Handler)
    except Exception as exc:  # noqa: BLE001 - always complete the readiness handshake.
        _signal_listener_ready(ready_fd, b"ERROR\n")
        print(
            json.dumps(
                {
                    "ok": False,
                    "error_class": type(exc).__name__,
                    "error": _fallback_text(exc),
                },
                ensure_ascii=False,
            ),
            flush=True,
        )
        return 2
    _signal_listener_ready(ready_fd, b"READY\n")
    server.timeout = 1
    print(json.dumps({"ok": True, "listening": f"{CALLBACK_HOST}:{CALLBACK_PORT}", "ttl_seconds": ttl_seconds}, ensure_ascii=False), flush=True)
    try:
        while clock() < deadline and not handled["done"]:
            server.handle_request()
    finally:
        server.server_close()
    print(
        json.dumps(
            {"ok": handled["done"], "closed": True, "handled": handled["done"]},
            ensure_ascii=False,
        ),
        flush=True,
    )
    return 0 if handled["done"] else 2


def now_ts() -> float:
    return time.time()


def today_bucket() -> str:
    return dt.datetime.now(dt.timezone.utc).astimezone().strftime("%Y-%m-%d")


def load_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def load_health_rows(path: Path = DEFAULT_HEALTH_PATH) -> list[dict[str, Any]]:
    body = load_json(path)
    rows = body.get("rows") if isinstance(body, dict) else None
    return rows if isinstance(rows, list) else []


def _health_rows_error(rows: Any) -> str | None:
    if not isinstance(rows, list) or not rows:
        return "HEALTH_ROWS_EMPTY"
    if any(not isinstance(row, dict) for row in rows):
        return "HEALTH_ROWS_INVALID"
    surfaces = [row.get("surface") for row in rows]
    if (
        len(surfaces) != len(EXPECTED_HEALTH_SURFACES)
        or any(not isinstance(surface, str) for surface in surfaces)
        or set(surfaces) != EXPECTED_HEALTH_SURFACES
        or any(
            not isinstance(row.get("health"), str)
            or row.get("health") not in VALID_HEALTH_STATES
            for row in rows
        )
    ):
        return "HEALTH_ROWS_INVALID"
    return None


def open_id_for_owner(owner: str | None, *, default_open_id: str = DEFAULT_OWNER_OPEN_ID) -> str:
    owner = str(owner or "").strip()
    mapping = _load_user_id_mapping()
    if owner:
        matches: list[str] = []
        for open_id, name in mapping.items():
            if str(name).strip() == owner:
                candidate = str(open_id).strip()
                if _FEISHU_OPEN_ID_RE.fullmatch(candidate):
                    matches.append(candidate)
        if len(matches) == 1:
            return matches[0]
        if len(matches) > 1:
            return ""
        if _FEISHU_OPEN_ID_RE.fullmatch(owner):
            return owner
        # Do NOT accept doc token userId (e.g. fefb829e) as a mention target.
        return ""
    candidate = str(default_open_id or "").strip()
    return candidate if _FEISHU_OPEN_ID_RE.fullmatch(candidate) else ""


def target_for_open_id(open_id: str, *, mode: str = "dm", topic_target: str = "") -> str:
    open_id = str(open_id or "").strip()
    if mode == "dm":
        return f"feishu:{open_id}" if _FEISHU_OPEN_ID_RE.fullmatch(open_id) else ""
    if mode == "topic":
        # A topic escalation must name both the Feishu group and message/thread.
        # Bare platforms, DMs, and groups can otherwise fall through to a home
        # channel or main-group send in send_message_tool.
        target = str(topic_target or "").strip()
        return target if _FEISHU_TOPIC_TARGET_RE.fullmatch(target) else ""
    return ""


def notify_key_for_row(row: dict[str, Any]) -> str:
    surface = str(row.get("surface") or "")
    health = str(row.get("health") or "")
    expires = row.get("expires_at")
    extra = str(expires if expires not in (None, "") else today_bucket())
    return compute_notify_key(user_state=health, transition_marker=surface, extra=extra)


def _notify_key_surface(notify_key: str) -> str:
    parts = str(notify_key or "").split("|", 3)
    return parts[1] if len(parts) >= 2 else ""


def recovered_surface_ledger_keys(row: dict[str, Any], state: dict[str, Any]) -> list[str]:
    """Return stale escalation keys for a surface that has recovered.

    A recovered/OK credential surface must clear its old abnormal ledger entries;
    otherwise a future real REAUTH_REQUIRED/EXPIRED event with the same notify
    key can be swallowed by the cooldown/cap state left from the previous
    outage.  Only same-surface entries are removed.
    """
    surface = str(row.get("surface") or "").strip()
    health = str(row.get("health") or "").strip()
    if not surface or health in ESCALATE_HEALTH:
        return []
    raw_sent = state.get("sent")
    sent: dict[str, Any] = raw_sent if isinstance(raw_sent, dict) else {}
    return sorted(
        key for key in sent
        if _notify_key_surface(str(key)) == surface
        and str(key).split("|", 1)[0] in ESCALATE_HEALTH
    )


def clear_recovered_surface_ledger(row: dict[str, Any], state: dict[str, Any]) -> list[str]:
    keys = recovered_surface_ledger_keys(row, state)
    raw_sent = state.get("sent")
    sent: dict[str, Any] = raw_sent if isinstance(raw_sent, dict) else {}
    for key in keys:
        sent.pop(key, None)
    if keys:
        state["sent"] = sent
    return keys


def build_message(
    row: dict[str, Any],
    open_id: str,
    *,
    auth_url: str = "",
    callback_listener_ready: bool = False,
) -> str:
    surface = str(row.get("surface") or "")
    health = str(row.get("health") or "")
    result: dict[str, Any]
    name = resolve_display_name(open_id) or str(row.get("owner") or "")
    mention = build_at_mention(open_id, name)
    cn_surface = "文档" if surface == "doc" else "项目" if surface == "project" else surface
    if health == "REAUTH_REQUIRED":
        state_text = "失效需重新授权" if surface == "doc" else "失效需人工再签发"
    elif health == "EXPIRING(<7d)":
        state_text = "临期"
    elif health == "EXPIRED":
        state_text = "已过期"
    else:
        state_text = "探针失败需排查"
    section = "1 文档 OAuth 重新授权" if surface == "doc" else "3 Meegle 用户凭据处置"
    # Keep plain text: no markdown bullets/backticks; _plain strips accidental markdown from components.
    if surface == "doc":
        if auth_url:
            if callback_listener_ready:
                callback_flow = (
                    "如果在这台 Mac 的浏览器打开并授权，会自动回调落库；"
                    "如果在手机或异地设备打开，请从连接被拒页地址栏复制完整 callback URL，走 runbook paste-code。"
                )
            else:
                callback_flow = (
                    "本机自动回调当前不可用；授权后请从地址栏复制完整 callback URL，"
                    "走 runbook paste-code。"
                )
            flow = (
                f"授权链接：{auth_url}。"
                "链接约 10 到 15 分钟内有效，过期后回复即可重发。"
                f"{callback_flow}"
            )
        else:
            flow = "当前授权入口未启用，请按 runbook §1 的阻断说明处理；不要恢复历史 MCP 配置。"
    else:
        flow = "项目侧使用 Meegle 用户访问凭据，请由 owner 走 device-code 重新授权后复测。"
    text = (
        f"{mention} 飞书{_plain(cn_surface)}凭证{_plain(state_text)}，请按 runbook 处理："
        f"{RUNBOOK_PATH} §{section}。"
        f"{flow}"
    )
    return _plain(text).replace("；", " ") if "<at" not in text else text


def should_suppress(state: dict[str, Any], notify_key: str, *, now: float | None = None, cooldown_seconds: int = DEFAULT_COOLDOWN_SECONDS) -> bool:
    now = now_ts() if now is None else now
    try:
        last = float((state.get("sent") or {}).get(notify_key) or 0)
    except Exception:
        last = 0
    return bool(last and now - last < cooldown_seconds)


def row_escalation(
    row: dict[str, Any],
    *,
    send: bool = False,
    state: dict[str, Any] | None = None,
    send_func: Callable[[dict[str, Any]], str] | None = None,
    target_mode: str = "dm",
    topic_target: str = "",
    default_open_id: str = DEFAULT_OWNER_OPEN_ID,
    cooldown_seconds: int = DEFAULT_COOLDOWN_SECONDS,
    now: float | None = None,
    state_load_error: str | None = None,
) -> dict[str, Any]:
    surface = str(row.get("surface") or "")
    health = str(row.get("health") or "")
    if state is None:
        state, loaded_error = load_escalation_state()
    else:
        state, loaded_error = _validated_escalation_state(state)
    state_load_error = state_load_error or loaded_error
    if health not in ESCALATE_HEALTH:
        cleared = clear_recovered_surface_ledger(row, state) if send else recovered_surface_ledger_keys(row, state)
        state_write_error: str | None = None
        if send and cleared:
            try:
                _write_escalation_state(state)
            except Exception as exc:  # recovery must still clear local fallback state
                state_write_error = _fallback_text(f"{type(exc).__name__}: {exc}")
        fallback_error: str | None = None
        if send:
            try:
                _resolve_fallback_alert(row)
            except Exception as exc:  # best effort; retain an observable result
                fallback_error = _fallback_text(f"{type(exc).__name__}: {exc}")
        result = {
            "surface": surface,
            "health": health,
            "skipped": True,
            "reason": "health_ok_or_not_escalating",
            "recovery_cleared_count": len(cleared),
            "recovery_cleared_keys": cleared,
        }
        state_error = state_load_error or state_write_error
        if state_error:
            result["state_error"] = state_error
        if fallback_error:
            result["fallback_error"] = fallback_error
        return result
    if state_load_error:
        state_refusal: dict[str, Any] = {
            "surface": surface,
            "health": health,
            "refused": True,
            "reason": state_load_error,
            "state_error": state_load_error,
        }
        if send:
            state_refusal["fallback"] = _persist_fallback_safely(
                row, reason=state_load_error, now=now
            )
        return state_refusal
    open_id = open_id_for_owner(row.get("owner"), default_open_id=default_open_id if surface == "project" else "")
    if not open_id or not open_id.startswith("ou_"):
        owner_refusal: dict[str, Any] = {
            "surface": surface,
            "health": health,
            "refused": True,
            "reason": "owner_open_id_unresolved",
            "open_id": open_id,
        }
        if send:
            owner_refusal["fallback"] = _persist_fallback_safely(
                row, reason=owner_refusal["reason"], now=now
            )
        return owner_refusal
    target = target_for_open_id(open_id, mode=target_mode, topic_target=topic_target)
    if not target:
        target_refusal: dict[str, Any] = {
            "surface": surface,
            "health": health,
            "refused": True,
            "reason": "no_explicit_target",
            "open_id": open_id,
        }
        if send:
            target_refusal["fallback"] = _persist_fallback_safely(
                row, reason=target_refusal["reason"], now=now
            )
        return target_refusal
    notify_key = notify_key_for_row(row)
    suppressed = should_suppress(state, notify_key, now=now, cooldown_seconds=cooldown_seconds)
    quiet_suppressed = bool(surface == "doc" and in_quiet_hours())
    callback_listener = {"started": False, "reason": "dry_run_or_no_auth_url"}
    message = build_message(row, open_id)
    result = {
        "surface": surface,
        "health": health,
        "target": target,
        "open_id": open_id,
        "has_mention": bool(build_at_mention(open_id, resolve_display_name(open_id))),
        "notify_key": notify_key,
        "suppressed": suppressed,
        "quiet_hours_suppressed": quiet_suppressed,
        "dry_run": not send,
        "callback_listener": callback_listener,
        "preview": message,
    }
    if suppressed:
        return result
    if quiet_suppressed:
        # Quiet hours are an intentional policy decision, not an external
        # delivery failure. Leave the retry eligible for the next run and do
        # not create a local notification that defeats the quiet-hours policy.
        return result
    auth_info: dict[str, Any] = {}
    if send and surface == "doc" and health in {"REAUTH_REQUIRED", "EXPIRED", "EXPIRING(<7d)"}:
        try:
            auth_info = get_doc_auth_url()
        except Exception as exc:  # noqa: BLE001 - sending the alert must continue.
            auth_info = {
                "ok": False,
                "error": f"{type(exc).__name__}: {_fallback_text(exc)}",
            }
    auth_url = str(auth_info.get("auth_url") or "")
    if not send:
        return result
    listener_process: Any | None = None
    if auth_url and auth_info.get("state"):
        callback_listener = start_callback_listener(
            str(auth_info.get("state")),
            _return_process=True,
        )
        listener_process = callback_listener.pop("_process", None)
        result["callback_listener"] = callback_listener
    message = build_message(
        row,
        open_id,
        auth_url=auth_url,
        callback_listener_ready=callback_listener.get("started") is True,
    )
    sender = send_func or send_message_tool
    try:
        with credential_live_transport():
            raw = sender({"action": "send", "target": target, "message": message})
    except Exception as exc:  # noqa: BLE001 - persist the alert before returning.
        _terminate_callback_listener(listener_process)
        reason = f"{type(exc).__name__}: {exc}"
        result.update({"sent": False, "send_result": {"error": _fallback_text(reason)}})
        result["fallback"] = _persist_fallback_safely(row, reason=reason, now=now)
        return result
    try:
        send_result = raw if isinstance(raw, dict) else json.loads(raw)
    except Exception:
        send_result = {"raw": _fallback_text(raw, 300)}
    if not isinstance(send_result, dict):
        send_result = {"raw": _safe_send_value(send_result)}
    else:
        send_result = _safe_send_value(send_result)
    # Treat only the sender's explicit boolean success as delivery.  A string
    # such as ``"false"`` is truthy in Python and must not suppress fallback.
    ok = _strict_success_payload(send_result)
    result.update({"sent": ok, "send_result": send_result})
    if ok:
        state["sent"][notify_key] = now_ts() if now is None else now
        try:
            _write_escalation_state(state)
        except Exception as exc:  # delivery succeeded; expose state durability separately.
            result["state_error"] = _fallback_text(f"{type(exc).__name__}: {exc}")
    else:
        _terminate_callback_listener(listener_process)
        reason = str(send_result.get("error") or send_result.get("raw") or "sender_returned_failure")
        result["fallback"] = _persist_fallback_safely(row, reason=reason, now=now)
    return result


def run(rows: list[dict[str, Any]], **kwargs: Any) -> dict[str, Any]:
    health_rows_error = _health_rows_error(rows)
    if health_rows_error:
        return {
            "ok": False,
            "error_class": health_rows_error,
            "results": [],
        }
    options = dict(kwargs)
    supplied_state = options.pop("state", None)
    supplied_state_error = options.pop("state_load_error", None)
    send = bool(options.get("send"))
    now = options.get("now")

    def result_failed(item: dict[str, Any]) -> bool:
        send_result = item.get("send_result")
        send_error = (
            send_result.get("error") if isinstance(send_result, dict) else None
        )
        return bool(
            item.get("refused")
            or item.get("sent") is False
            or send_error
            or item.get("state_error")
            or item.get("fallback_error")
        )

    def _render_locked() -> dict[str, Any]:
        if supplied_state is None:
            state, state_load_error = load_escalation_state()
        else:
            state, state_load_error = _validated_escalation_state(supplied_state)
        state_load_error = supplied_state_error or state_load_error
        rendered: list[dict[str, Any]] = []
        for row in rows:
            try:
                rendered.append(
                    row_escalation(
                        row,
                        state=state,
                        state_load_error=state_load_error,
                        **options,
                    )
                )
            except Exception as exc:  # noqa: BLE001 - one surface must not hide another.
                reason = f"{type(exc).__name__}: {exc}"
                failed: dict[str, Any] = {
                    "surface": _fallback_text(row.get("surface"), 64),
                    "health": _fallback_text(row.get("health"), 64),
                    "sent": False,
                    "send_result": {"error": _fallback_text(reason)},
                }
                if send and row.get("health") in ESCALATE_HEALTH:
                    failed["fallback"] = _persist_fallback_safely(
                        row, reason=reason, now=now
                    )
                rendered.append(failed)
        return {
            "ok": not any(result_failed(item) for item in rendered),
            "results": rendered,
        }

    try:
        with escalation_state_lock() if send else contextlib.nullcontext():
            return _render_locked()
    except Exception as exc:  # lock failure must fail closed before any send.
        reason = f"{type(exc).__name__}: {exc}"
        rendered = []
        for row in rows:
            surface = _fallback_text(row.get("surface"), 64)
            health = _fallback_text(row.get("health"), 64)
            failed: dict[str, Any]
            if row.get("health") in ESCALATE_HEALTH:
                failed = {
                    "surface": surface,
                    "health": health,
                    "sent": False,
                    "send_result": {"error": _fallback_text(reason)},
                }
            else:
                failed = {
                    "surface": surface,
                    "health": health,
                    "skipped": True,
                    "reason": "health_ok_or_not_escalating",
                    "state_error": _fallback_text(reason),
                }
            if row.get("health") in ESCALATE_HEALTH and send:
                failed["fallback"] = _persist_fallback_safely(
                    row, reason=reason, now=now
                )
            rendered.append(failed)
        return {"ok": False, "results": rendered}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Dry-run or send Feishu credential health escalations")
    parser.add_argument("--health-json", default=str(DEFAULT_HEALTH_PATH))
    parser.add_argument("--send", action="store_true", help="Actually send messages; default is dry-run")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--target-mode", choices=["dm", "topic"], default=os.getenv("FEISHU_CREDENTIAL_ESCALATION_TARGET_MODE", "dm"))
    parser.add_argument("--topic-target", default=os.getenv("FEISHU_CREDENTIAL_ESCALATION_TOPIC_TARGET", ""))
    parser.add_argument("--cooldown-seconds", type=int, default=DEFAULT_COOLDOWN_SECONDS)
    oauth_action = parser.add_mutually_exclusive_group()
    oauth_action.add_argument("--oauth-start", action="store_true", help="Create a direct Feishu doc OAuth URL")
    oauth_action.add_argument(
        "--oauth-callback",
        action="store_true",
        help="Complete direct Feishu doc OAuth from a callback URL/JSON read from stdin",
    )
    oauth_action.add_argument("--callback-listener", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--ttl-seconds", type=int, default=AUTH_URL_TTL_SECONDS, help=argparse.SUPPRESS)
    parser.add_argument("--ready-fd", type=int, default=None, help=argparse.SUPPRESS)
    raw_argv = list(sys.argv[1:] if argv is None else argv)
    callback_cli = any(
        item == "--oauth-callback" or item.startswith("--oauth-callback=")
        for item in raw_argv
    )
    if callback_cli and any(
        item not in {"--oauth-callback", "--json", "-h", "--help"}
        for item in raw_argv
    ):
        parser.error("OAuth callback input must be provided via stdin")
    if any(
        item in {"--code", "--state"}
        or item.startswith("--code=")
        or item.startswith("--state=")
        for item in raw_argv
    ):
        parser.error("OAuth callback parameters must be provided via stdin")
    args = parser.parse_args(raw_argv)
    if args.oauth_start:
        result = get_doc_auth_url()
        print(json.dumps(result, ensure_ascii=False, separators=(",", ":") if args.json else None, indent=None if args.json else 2))
        return 0 if result.get("ok") is True else 2
    if args.oauth_callback:
        try:
            code, state = _read_oauth_callback_input()
        except ValueError as exc:
            result = {
                "success": False,
                "isError": True,
                "error_class": "OAUTH_INPUT_INVALID",
                "error": _fallback_text(str(exc)),
            }
        else:
            result = call_doc_auth_callback(code=code, state=state)
        print(json.dumps(result, ensure_ascii=False, separators=(",", ":") if args.json else None, indent=None if args.json else 2))
        structured = result.get("structuredContent") if isinstance(result, dict) else None
        return 0 if isinstance(structured, dict) and structured.get("success") is True and result.get("isError") is False else 2
    if args.callback_listener:
        return run_callback_listener(
            ttl_seconds=args.ttl_seconds,
            ready_fd=args.ready_fd,
        )
    if args.send:
        load_send_environment()
    rows = load_health_rows(Path(args.health_json))
    result = run(rows, send=args.send, target_mode=args.target_mode, topic_target=args.topic_target, cooldown_seconds=args.cooldown_seconds)
    if args.json:
        print(json.dumps(result, ensure_ascii=False, separators=(",", ":")))
    else:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result.get("ok") is True else 2


if __name__ == "__main__":
    raise SystemExit(main())
