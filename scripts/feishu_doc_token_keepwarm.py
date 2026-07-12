#!/usr/bin/env python3
"""Keep feishu-doc OAuth alive by calling the real MCP read tool.

The feishu-doc MCP server refreshes tokens lazily inside ``ensureValidToken``:
it only rotates the OAuth tokens when the current access token is close to
expiry/expired.  Therefore the launchd cadence must be longer than the access
token lifetime (about 2h).  Batch 4 should schedule this at 6h so every run is
eligible to refresh/rotate the refresh token and reset the ≈30-day idle window;
running more frequently than the access-token lifetime can create a false
keep-warm where calls succeed but the refresh token never rotates.

This script never prints token/secret values.  It only reports non-secret
metadata: expiresAt, owner, health, and an error class/message when needed.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

APP_ID = "cli_a99b38e0a29b500b"
HERMES_LIVE_ROOT = Path("/Users/songying/.hermes/runtime/hermes-live")
AUTH_PATH = Path(
    "/Users/songying/.hermes/mcp-storage/feishu-doc/feishu-service/feishu/auth"
) / APP_ID
MCP_CALLER = Path("/tmp/hermes_feishu_doc_keepwarm_call.mjs")

REAUTH_PATTERNS = (
    "99991665",
    "99991666",
    "Token 刷新失败",
    "refresh_token",
    "refresh token",
    "invalid refresh",
)
SECRET_KEY_RE = re.compile(r"(token|secret|password|authorization|app_secret)", re.I)
SECRET_VALUE_RE = re.compile(r"\b(?:u-|t-|m-)[A-Za-z0-9_-]{12,}\b|Bearer\s+\S+", re.I)


class KeepwarmError(Exception):
    def __init__(self, message: str, *, error_class: str = "KeepwarmError") -> None:
        super().__init__(message)
        self.error_class = error_class


def _redact_text(text: str) -> str:
    return SECRET_VALUE_RE.sub("<redacted>", text)


def _safe_error_message(text: str, limit: int = 240) -> str:
    return _redact_text(str(text)).replace("\n", " ")[:limit]


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


def read_auth_metadata(auth_path: Path | None = None) -> dict[str, Any]:
    auth_path = auth_path or AUTH_PATH
    if not auth_path.exists():
        return {"exists": False, "expiresAt": None, "owner": None}
    data = json.loads(auth_path.read_text(encoding="utf-8"))
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


def _ensure_node_caller(path: Path | None = None) -> Path:
    path = path or MCP_CALLER
    # The script uses the same stdio MCP server configured for Hermes/gateway,
    # and calls the read-only feishu_get_user_info(appId=...) tool.  It does not
    # call Feishu HTTP directly and does not implement token refresh itself.
    source = r'''
import { Client } from '/Users/songying/.hermes/local-mcp/feishu-doc/node_modules/@modelcontextprotocol/sdk/dist/esm/client/index.js';
import { StdioClientTransport } from '/Users/songying/.hermes/local-mcp/feishu-doc/node_modules/@modelcontextprotocol/sdk/dist/esm/client/stdio.js';
import fs from 'fs';
import yaml from '/Users/songying/.hermes/local-mcp/feishu-doc/node_modules/js-yaml/dist/js-yaml.mjs';
const config = yaml.load(fs.readFileSync('/Users/songying/.hermes/config.yaml','utf8'));
const s = config.mcp_servers['feishu-doc'];
const transport = new StdioClientTransport({command:s.command,args:s.args,env:{...process.env,...s.env}});
const client = new Client({name:'hermes-feishu-doc-token-keepwarm',version:'0.1.0'});
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
  const result = await client.callTool({name:'feishu_get_user_info', arguments:{appId:'cli_a99b38e0a29b500b'}});
  console.log(JSON.stringify(scrub(result)));
} finally {
  await client.close();
}
'''.lstrip()
    if not path.exists() or path.read_text(encoding="utf-8") != source:
        path.write_text(source, encoding="utf-8")
    return path


def call_feishu_get_user_info(timeout_seconds: int = 45) -> dict[str, Any]:
    caller = _ensure_node_caller()
    proc = subprocess.run(
        ["node", str(caller)],
        cwd=str(HERMES_LIVE_ROOT),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout_seconds,
        check=False,
    )
    stdout = proc.stdout.strip()
    stderr = proc.stderr.strip()
    if proc.returncode != 0:
        msg = stderr or stdout or f"node caller exited {proc.returncode}"
        raise KeepwarmError(_safe_error_message(msg), error_class="MCP_CALL_FAILED")
    if not stdout:
        raise KeepwarmError("empty MCP response", error_class="EMPTY_MCP_RESPONSE")
    try:
        result = json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise KeepwarmError(_safe_error_message(stdout), error_class=type(exc).__name__) from exc
    if result.get("isError"):
        text = "\n".join(
            item.get("text", "") for item in result.get("content", []) if item.get("type") == "text"
        )
        raise KeepwarmError(_safe_error_message(text or json.dumps(result, ensure_ascii=False)), error_class="MCP_TOOL_ERROR")
    return _scrub(result)


def keepwarm() -> tuple[int, dict[str, Any]]:
    before = read_auth_metadata()
    owner = before.get("owner")
    try:
        call_feishu_get_user_info()
        after = read_auth_metadata()
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
        health = classify_error(msg)
        after = read_auth_metadata()
        return 2, {
            "before_expiresAt": before.get("expiresAt"),
            "after_expiresAt": after.get("expiresAt"),
            "rotated": False,
            "owner": after.get("owner") or owner,
            "health": health,
            "error_class": getattr(exc, "error_class", type(exc).__name__),
            "error": msg,
        }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Keep feishu-doc OAuth token warm via real MCP read tool")
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
