#!/usr/bin/env python3
"""Read-only Feishu credential health audit for doc OAuth and Meegle CLI.

Execution truth is live probing, not token-file metadata.  The doc surface
imports and reuses the direct Open API keepwarm() probe.  The project surface
uses the installed official Meegle CLI to validate its user access token and
read the current user's identity.  The third row verifies that the CLI's
non-interactive Device-Code reauthorization flags are available.

No token/app_secret values are printed or written.
"""
from __future__ import annotations

import argparse
import datetime as dt
import importlib.util
import json
import math
import os
import re
import shutil
import subprocess
import time
from pathlib import Path
from typing import Any

APP_ID = "cli_a99b38e0a29b500b"
KEEPWARM_PATH = Path(__file__).resolve().with_name("feishu_doc_token_keepwarm.py")
WORKSPACE_ROOT = Path("/Users/songying/.hermes/workspace-work")
DEFAULT_OUTPUT_DIR = WORKSPACE_ROOT / "knowledge" / "outputs" / "feishu-credential-health"
MEEGLE_PATH = "/usr/local/bin/meegle"
PROJECT_OWNER_ENV = "PNC_MEEGLE_AUTH_OWNER_NAME"

HEALTH_VALUES = {"OK", "EXPIRING(<7d)", "EXPIRED", "REAUTH_REQUIRED", "PROBE_FAILED"}
EXPECTED_HEALTH_SURFACES = frozenset({"doc", "project", "meegle_cli"})
AUTH_PATTERNS = (
    "401",
    "403",
    "unauthorized",
    "forbidden",
    "invalid token",
    "expired token",
    "token expired",
    "MEEGLE_USER_ACCESS_TOKEN",
    "token rejected by server",
    "unauthenticated",
    "not authenticated",
)
SECRET_KEY_RE = re.compile(r"(token|secret|password|authorization|app_secret)", re.I)
SECRET_VALUE_RE = re.compile(r"\b(?:u-|t-|m-)[A-Za-z0-9_-]{12,}\b|Bearer\s+\S+", re.I)
SECRET_ASSIGN_RE = re.compile(
    r"(?i)(?:^|[^A-Za-z0-9])[\"']?"
    r"(?:token|access[_-]?token|refresh[_-]?token|(?:app|client)[_-]?secret|"
    r"secret|password|authorization)[\"']?\s*[:=]\s*"
    r"(?:\"[^\"]*\"|'[^']*'|\S+)"
)


def now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).astimezone().isoformat(timespec="seconds")


def redact_text(text: str) -> str:
    return SECRET_ASSIGN_RE.sub("<redacted>", SECRET_VALUE_RE.sub("<redacted>", str(text)))


def safe_error_message(text: str, limit: int = 240) -> str:
    raw = str(text)
    try:
        parsed = json.loads(raw)
    except (TypeError, ValueError, json.JSONDecodeError):
        rendered = redact_text(raw)
    else:
        rendered = json.dumps(scrub(parsed), ensure_ascii=False, separators=(",", ":"))
    return rendered.replace("\n", " ")[:limit]


def scrub(value: Any) -> Any:
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for key, item in value.items():
            if SECRET_KEY_RE.search(str(key)):
                out[key] = "<redacted>"
            else:
                out[key] = scrub(item)
        return out
    if isinstance(value, list):
        return [scrub(item) for item in value]
    if isinstance(value, str):
        return redact_text(value)
    return value


def load_keepwarm_module():
    spec = importlib.util.spec_from_file_location("feishu_doc_token_keepwarm", KEEPWARM_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load keepwarm module: {KEEPWARM_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def classify_project_error(message: str) -> str:
    low = message.lower()
    if any(pattern.lower() in low for pattern in AUTH_PATTERNS):
        return "REAUTH_REQUIRED"
    return "PROBE_FAILED"


def doc_surface(checked_at: str | None = None, keepwarm_module: Any | None = None) -> dict[str, Any]:
    checked_at = checked_at or now_iso()
    module = keepwarm_module or load_keepwarm_module()
    rc, result = module.keepwarm()
    health = result.get("health") if result.get("health") in HEALTH_VALUES else "PROBE_FAILED"
    row: dict[str, Any] = {
        "surface": "doc",
        "owner": result.get("owner"),
        "expires_at": result.get("after_expiresAt") or result.get("before_expiresAt"),
        "days_left": None,
        "health": health,
        "checked_at": checked_at,
    }
    if health == "OK" and not _future_expiry(row["expires_at"], checked_at):
        row["health"] = "PROBE_FAILED"
        row["error_class"] = "EXPIRY_UNAVAILABLE"
    if rc != 0:
        row["error_class"] = result.get("error_class") or health
    return scrub(row)


def _meegle_executable() -> str:
    return shutil.which("meegle") or MEEGLE_PATH


def _meegle_environment() -> dict[str, str]:
    """Keep headless probes pinned to the Feishu Project host."""
    environment = os.environ.copy()
    environment.setdefault("MEEGLE_HOST", "project.feishu.cn")
    return environment


def _project_owner(
    *, status: dict[str, Any] | None = None, identity: dict[str, Any] | None = None
) -> str | None:
    """Prefer the CLI identity, then an explicit local owner binding."""
    def find_owner(value: Any, preferred_keys: tuple[str, ...]) -> str | None:
        if isinstance(value, dict):
            for key in preferred_keys:
                candidate = value.get(key)
                if isinstance(candidate, str) and candidate.strip():
                    return candidate.strip()
            for nested in value.values():
                candidate = find_owner(nested, preferred_keys)
                if candidate:
                    return candidate
        elif isinstance(value, list):
            for nested in value:
                candidate = find_owner(nested, preferred_keys)
                if candidate:
                    return candidate
        return None

    owner = find_owner(status, ("owner", "owner_name", "user_name", "username"))
    if owner:
        return owner
    owner = find_owner(
        identity,
        (
            "name",
            "name_cn",
            "name_en",
            "display_name",
            "displayName",
            "user_name",
            "username",
            "owner",
            "owner_name",
        ),
    )
    if owner:
        return owner
    return os.getenv(PROJECT_OWNER_ENV, "").strip() or None


def call_project_auth_status(timeout_seconds: int = 30) -> dict[str, Any]:
    """Validate the Meegle user access token through the official CLI."""
    executable = _meegle_executable()
    try:
        proc = subprocess.run(
            [executable, "auth", "status", "--format", "json"],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout_seconds,
            check=False,
            env=_meegle_environment(),
        )
    except FileNotFoundError as exc:
        raise RuntimeError("meegle CLI not found") from exc
    stdout = proc.stdout.strip()
    stderr = proc.stderr.strip()
    if not stdout:
        raise RuntimeError(safe_error_message(stderr or f"meegle auth status exited {proc.returncode}"))
    try:
        result = json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(safe_error_message(stdout)) from exc
    if not isinstance(result, dict):
        raise RuntimeError("meegle auth status returned a non-object")
    if proc.returncode != 0:
        if proc.returncode == 1 and result.get("authenticated") is False:
            raise RuntimeError("meegle unauthenticated")
        raise RuntimeError(safe_error_message(stderr or result.get("error") or result.get("reason") or "meegle auth status failed"))
    return scrub(result)


def call_project_identity(timeout_seconds: int = 30) -> dict[str, Any]:
    """Read the authenticated Meegle user's identity without using MCP."""
    executable = _meegle_executable()
    try:
        proc = subprocess.run(
            [executable, "user", "me", "--format", "json"],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout_seconds,
            check=False,
            env=_meegle_environment(),
        )
    except FileNotFoundError as exc:
        raise RuntimeError("meegle CLI not found") from exc
    stdout = proc.stdout.strip()
    stderr = proc.stderr.strip()
    if proc.returncode != 0:
        raise RuntimeError(
            safe_error_message(stderr or stdout or f"meegle user me exited {proc.returncode}")
        )
    if not stdout:
        raise RuntimeError("meegle user me returned an empty response")
    try:
        result = json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(safe_error_message(stdout)) from exc
    if not isinstance(result, dict):
        raise RuntimeError("meegle user me returned a non-object")
    return scrub(result)


def call_project_list_todo(timeout_seconds: int = 30) -> dict[str, Any]:
    """Backward-compatible name for callers migrating from the MCP probe."""
    return call_project_auth_status(timeout_seconds=timeout_seconds)


def _checked_epoch_ms(checked_at: str) -> int:
    try:
        return int(dt.datetime.fromisoformat(checked_at.replace("Z", "+00:00")).timestamp() * 1000)
    except (TypeError, ValueError, AttributeError):
        return int(time.time() * 1000)


def _future_expiry(value: Any, checked_at: str) -> bool:
    try:
        expiry = float(value)
    except (TypeError, ValueError):
        return False
    return math.isfinite(expiry) and expiry > _checked_epoch_ms(checked_at)


def _project_expiry(status: dict[str, Any], checked_at: str) -> int | None:
    raw_minutes = status.get("expires_in_minutes")
    if isinstance(raw_minutes, bool) or not isinstance(raw_minutes, (str, int, float)):
        return None
    try:
        minutes = int(raw_minutes)
    except (TypeError, ValueError):
        return None
    if minutes < 0:
        return None
    return _checked_epoch_ms(checked_at) + minutes * 60 * 1000


def project_surface(checked_at: str | None = None) -> dict[str, Any]:
    checked_at = checked_at or now_iso()
    try:
        status = call_project_auth_status()
        owner = _project_owner(status=status)
        authenticated = status.get("authenticated")
        if authenticated is not True:
            return {
                "surface": "project",
                "owner": owner,
                "expires_at": _project_expiry(status, checked_at),
                "days_left": None,
                "health": "REAUTH_REQUIRED" if authenticated is False else "PROBE_FAILED",
                "checked_at": checked_at,
                "error_class": "REAUTH_REQUIRED" if authenticated is False else "STATUS_UNKNOWN",
            }
        expiry = _project_expiry(status, checked_at)
        if not _future_expiry(expiry, checked_at):
            return {
                "surface": "project",
                "owner": owner,
                "expires_at": expiry,
                "days_left": None,
                "health": "PROBE_FAILED",
                "checked_at": checked_at,
                "error_class": "EXPIRY_UNAVAILABLE",
            }
        if not owner:
            try:
                owner = _project_owner(identity=call_project_identity())
            except Exception:
                owner = _project_owner()
        if not owner:
            return {
                "surface": "project",
                "owner": None,
                "expires_at": expiry,
                "days_left": None,
                "health": "PROBE_FAILED",
                "checked_at": checked_at,
                "error_class": "OWNER_UNAVAILABLE",
            }
        return {
            "surface": "project",
            "owner": owner,
            "expires_at": expiry,
            "days_left": None,
            "health": "OK",
            "checked_at": checked_at,
        }
    except Exception as exc:  # noqa: BLE001 - CLI classifies all probe failures.
        message = safe_error_message(str(exc))
        health = classify_project_error(message)
        return {
            "surface": "project",
            "owner": _project_owner(),
            "expires_at": None,
            "days_left": None,
            "health": health,
            "checked_at": checked_at,
            "error_class": type(exc).__name__,
        }


def device_code_surface(checked_at: str | None = None) -> dict[str, Any]:
    """Verify Device-Code support without starting an auth flow."""
    checked_at = checked_at or now_iso()
    executable = _meegle_executable()
    try:
        proc = subprocess.run(
            [executable, "auth", "login", "--help"],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=10,
            check=False,
            env=_meegle_environment(),
        )
        help_text = f"{proc.stdout}\n{proc.stderr}"
        available = proc.returncode == 0 and "--device-code" in help_text
        row = {
            "surface": "meegle_cli",
            "owner": _project_owner(),
            "expires_at": None,
            "days_left": None,
            "health": "OK" if available else "PROBE_FAILED",
            "device_code_available": available,
            "checked_at": checked_at,
        }
        if not available:
            row["error_class"] = "DEVICE_CODE_UNAVAILABLE"
        return scrub(row)
    except Exception as exc:  # noqa: BLE001 - health CLI classifies all failures.
        return {
            "surface": "meegle_cli",
            "owner": _project_owner(),
            "expires_at": None,
            "days_left": None,
            "health": "PROBE_FAILED",
            "device_code_available": False,
            "checked_at": checked_at,
            "error_class": type(exc).__name__,
        }


def run_health() -> list[dict[str, Any]]:
    checked_at = now_iso()
    return [doc_surface(checked_at), project_surface(checked_at), device_code_surface(checked_at)]


def health_rows_valid(rows: Any) -> bool:
    if not isinstance(rows, list) or len(rows) != len(EXPECTED_HEALTH_SURFACES):
        return False
    if any(not isinstance(row, dict) for row in rows):
        return False
    surfaces = [row.get("surface") for row in rows]
    return (
        all(isinstance(surface, str) for surface in surfaces)
        and set(surfaces) == EXPECTED_HEALTH_SURFACES
        and all(
            isinstance(row.get("health"), str)
            and row.get("health") in HEALTH_VALUES
            for row in rows
        )
    )


def write_output(rows: list[dict[str, Any]], output_dir: Path = DEFAULT_OUTPUT_DIR) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    path = output_dir / f"feishu-credential-health-{stamp}.json"
    payload = {"checked_at": rows[0]["checked_at"] if rows else now_iso(), "rows": rows}
    path.write_text(json.dumps(scrub(payload), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    latest = output_dir / "latest.json"
    latest.write_text(path.read_text(encoding="utf-8"), encoding="utf-8")
    return path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Audit feishu-doc OAuth and Meegle user credential health")
    parser.add_argument("--json", action="store_true", help="Print JSON result")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="Directory for JSON audit output")
    args = parser.parse_args(argv)
    rows = run_health()
    path = write_output(rows, Path(args.output_dir))
    payload = {"output_path": str(path), "rows": rows}
    if args.json:
        print(json.dumps(scrub(payload), ensure_ascii=False, separators=(",", ":")))
    else:
        print(json.dumps(scrub(payload), ensure_ascii=False, indent=2))
    return 0 if health_rows_valid(rows) and all(row.get("health") == "OK" for row in rows) else 2


if __name__ == "__main__":
    raise SystemExit(main())
