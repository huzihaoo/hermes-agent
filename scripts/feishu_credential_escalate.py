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
import json
import os
import subprocess
import sys
import tempfile
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

DEFAULT_OWNER_OPEN_ID = os.getenv(
    "FEISHU_CREDENTIAL_DEFAULT_OWNER_OPEN_ID",
    "ou_d1d3cfeba1be0a22faa36aaf4fb3907d",
).strip()
STATE_PATH = Path("/Users/songying/.hermes/runtime/shared-state/feishu_credential_escalation_state.json")
DEFAULT_HEALTH_PATH = Path(
    "/Users/songying/.hermes/workspace-work/knowledge/outputs/feishu-credential-health/latest.json"
)
RUNBOOK_PATH = "/Users/songying/.hermes/workspace-work/knowledge/wiki/runbooks/FEISHU_CREDENTIAL_RUNBOOK.md"
ESCALATE_HEALTH = {"REAUTH_REQUIRED", "EXPIRED", "EXPIRING(<7d)", "PROBE_FAILED"}
DEFAULT_COOLDOWN_SECONDS = 24 * 3600
APP_ID = "cli_a99b38e0a29b500b"
CALLBACK_HOST = "127.0.0.1"
CALLBACK_PORT = 3010
AUTH_URL_TTL_SECONDS = int(os.getenv("FEISHU_CREDENTIAL_AUTH_URL_TTL_SECONDS", "900") or "900")
CALLBACK_LOG_PATH = Path("/Users/songying/.hermes/logs/feishu-credential-callback-listener.log")


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
    source = """
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


def get_doc_auth_url() -> dict[str, Any]:
    result = _call_feishu_doc_tool("feishu_auth_url", {"appId": APP_ID})
    sc = result.get("structuredContent") if isinstance(result, dict) else None
    if not isinstance(sc, dict) or not sc.get("authUrl") or not sc.get("state"):
        return {"ok": False, "error": str(result.get("error") or result.get("content") or "auth_url_failed")[:240]}
    return {"ok": True, "auth_url": sc.get("authUrl"), "state": sc.get("state"), "appId": sc.get("appId") or APP_ID}


def call_doc_auth_callback(*, code: str, state: str) -> dict[str, Any]:
    return _call_feishu_doc_tool("feishu_auth_callback", {"appId": APP_ID, "code": code, "state": state}, timeout_seconds=60)


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


def start_callback_listener(expected_state: str, *, ttl_seconds: int = AUTH_URL_TTL_SECONDS) -> dict[str, Any]:
    if not expected_state:
        return {"started": False, "reason": "missing_state"}
    CALLBACK_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    cmd = [sys.executable, str(Path(__file__).resolve()), "--callback-listener", "--state", expected_state, "--ttl-seconds", str(ttl_seconds)]
    log = CALLBACK_LOG_PATH.open("a", encoding="utf-8")
    try:
        proc = subprocess.Popen(cmd, stdout=log, stderr=log, start_new_session=True)
        return {"started": True, "pid": proc.pid, "host": CALLBACK_HOST, "port": CALLBACK_PORT, "ttl_seconds": ttl_seconds}
    except Exception as exc:
        return {"started": False, "reason": f"{type(exc).__name__}: {exc}"[:200]}


def run_callback_listener(expected_state: str, *, ttl_seconds: int = AUTH_URL_TTL_SECONDS) -> int:
    deadline = time.time() + max(1, ttl_seconds)
    handled = {"done": False}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path != "/oauth/feishu/callback":
                self.send_response(404); self.end_headers(); return
            qs = parse_qs(parsed.query)
            code = (qs.get("code") or [""])[0]
            state = (qs.get("state") or [""])[0]
            if not code or state != expected_state:
                self.send_response(400)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.end_headers()
                self.wfile.write("<html><body>授权失败：state 不匹配或缺少 code。请回到 runbook 走 paste-code。</body></html>".encode("utf-8"))
                handled["done"] = True
                return
            result = call_doc_auth_callback(code=code, state=state)
            success = bool((result.get("structuredContent") or {}).get("success")) and not result.get("isError")
            self.send_response(200 if success else 500)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            body = "<html><body><h2>飞书授权成功</h2><p>可以关闭此页面。</p></body></html>" if success else "<html><body><h2>飞书授权未完成</h2><p>请回到 runbook 走 paste-code。</p></body></html>"
            self.wfile.write(body.encode("utf-8"))
            handled["done"] = True

        def log_message(self, fmt, *args):
            return

    try:
        server = HTTPServer((CALLBACK_HOST, CALLBACK_PORT), Handler)
    except OSError as exc:
        print(json.dumps({"ok": False, "error_class": type(exc).__name__, "error": str(exc)[:200]}, ensure_ascii=False), flush=True)
        return 2
    server.timeout = 1
    print(json.dumps({"ok": True, "listening": f"{CALLBACK_HOST}:{CALLBACK_PORT}", "ttl_seconds": ttl_seconds}, ensure_ascii=False), flush=True)
    while time.time() < deadline and not handled["done"]:
        server.handle_request()
    server.server_close()
    print(json.dumps({"ok": True, "closed": True, "handled": handled["done"]}, ensure_ascii=False), flush=True)
    return 0


def now_ts() -> float:
    return time.time()


def today_bucket() -> str:
    return dt.datetime.now(dt.timezone.utc).astimezone().strftime("%Y-%m-%d")


def load_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp.replace(path)


def load_health_rows(path: Path = DEFAULT_HEALTH_PATH) -> list[dict[str, Any]]:
    body = load_json(path)
    rows = body.get("rows") if isinstance(body, dict) else None
    return [row for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []


def open_id_for_owner(owner: str | None, *, default_open_id: str = DEFAULT_OWNER_OPEN_ID) -> str:
    owner = str(owner or "").strip()
    mapping = _load_user_id_mapping()
    if owner:
        for open_id, name in mapping.items():
            if str(name).strip() == owner:
                return str(open_id).strip()
        if owner.startswith("ou_"):
            return owner
        # Do NOT accept doc token userId (e.g. fefb829e) as a mention target.
        return ""
    return str(default_open_id or "").strip()


def target_for_open_id(open_id: str, *, mode: str = "dm", topic_target: str = "") -> str:
    open_id = str(open_id or "").strip()
    if mode == "dm":
        return f"feishu:{open_id}" if open_id.startswith("ou_") else ""
    if mode == "topic":
        # Explicit operator-provided target like feishu:oc_xxx:om_xxx/topic:om_xxx.
        return str(topic_target or "").strip()
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
    sent = state.get("sent") if isinstance(state.get("sent"), dict) else {}
    return sorted(
        key for key in sent
        if _notify_key_surface(str(key)) == surface
        and str(key).split("|", 1)[0] in ESCALATE_HEALTH
    )


def clear_recovered_surface_ledger(row: dict[str, Any], state: dict[str, Any]) -> list[str]:
    keys = recovered_surface_ledger_keys(row, state)
    sent = state.get("sent") if isinstance(state.get("sent"), dict) else {}
    for key in keys:
        sent.pop(key, None)
    if keys:
        state["sent"] = sent
    return keys


def build_message(row: dict[str, Any], open_id: str, *, auth_url: str = "") -> str:
    surface = str(row.get("surface") or "")
    health = str(row.get("health") or "")
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
    section = "1 文档 OAuth paste-code 重新授权" if surface == "doc" else "3 飞书项目 PAT 处置"
    # Keep plain text: no markdown bullets/backticks; _plain strips accidental markdown from components.
    if surface == "doc":
        link = f"授权链接：{auth_url}。" if auth_url else "授权链接生成失败时，按 runbook 重新生成。"
        flow = (
            f"{link}"
            "链接约 10 到 15 分钟内有效，过期后回复即可重发。"
            "如果在这台 Mac 的浏览器打开并授权，会自动回调落库；"
            "如果在手机或异地设备打开，localhost 会落在那台设备上，host 收不到回调，请从连接被拒页地址栏复制 code 和 state 走 runbook paste-code。"
        )
    else:
        flow = "项目侧流程是人工再签发 PAT，修改 config 前先确认 owner，并重启对应 MCP 后复测。"
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
) -> dict[str, Any]:
    surface = str(row.get("surface") or "")
    health = str(row.get("health") or "")
    state = state if state is not None else load_json(STATE_PATH)
    if health not in ESCALATE_HEALTH:
        cleared = clear_recovered_surface_ledger(row, state) if send else recovered_surface_ledger_keys(row, state)
        if send and cleared:
            write_json(STATE_PATH, state)
        return {
            "surface": surface,
            "health": health,
            "skipped": True,
            "reason": "health_ok_or_not_escalating",
            "recovery_cleared_count": len(cleared),
            "recovery_cleared_keys": cleared,
        }
    open_id = open_id_for_owner(row.get("owner"), default_open_id=default_open_id if surface == "project" else "")
    if not open_id or not open_id.startswith("ou_"):
        return {"surface": surface, "health": health, "refused": True, "reason": "owner_open_id_unresolved", "open_id": open_id}
    target = target_for_open_id(open_id, mode=target_mode, topic_target=topic_target)
    if not target:
        return {"surface": surface, "health": health, "refused": True, "reason": "no_explicit_target", "open_id": open_id}
    notify_key = notify_key_for_row(row)
    suppressed = should_suppress(state, notify_key, now=now, cooldown_seconds=cooldown_seconds)
    quiet_suppressed = bool(surface == "doc" and in_quiet_hours())
    auth_info: dict[str, Any] = {}
    if surface == "doc" and health in {"REAUTH_REQUIRED", "EXPIRED", "EXPIRING(<7d)"}:
        auth_info = get_doc_auth_url()
    auth_url = str(auth_info.get("auth_url") or "")
    callback_listener = {"started": False, "reason": "dry_run_or_no_auth_url"}
    if send and not suppressed and not quiet_suppressed and auth_url and auth_info.get("state"):
        callback_listener = start_callback_listener(str(auth_info.get("state")))
    message = build_message(row, open_id, auth_url=auth_url)
    result: dict[str, Any] = {
        "surface": surface,
        "health": health,
        "target": target,
        "open_id": open_id,
        "has_mention": bool(build_at_mention(open_id, resolve_display_name(open_id))),
        "notify_key": notify_key,
        "suppressed": suppressed,
        "quiet_hours_suppressed": quiet_suppressed,
        "dry_run": not send,
        "auth_url": auth_url or None,
        "auth_state": auth_info.get("state"),
        "callback_listener": callback_listener,
        "preview": message,
    }
    if suppressed or quiet_suppressed:
        return result
    if not send:
        return result
    raw = (send_func or send_message_tool)({"action": "send", "target": target, "message": message})
    try:
        send_result = json.loads(raw)
    except Exception:
        send_result = {"raw": str(raw)[:300]}
    ok = isinstance(send_result, dict) and bool(send_result.get("success"))
    result.update({"sent": ok, "send_result": send_result})
    if ok:
        state.setdefault("sent", {})[notify_key] = now_ts() if now is None else now
        write_json(STATE_PATH, state)
    return result


def run(rows: list[dict[str, Any]], **kwargs: Any) -> dict[str, Any]:
    state = load_json(STATE_PATH)
    rendered = [row_escalation(row, state=state, **kwargs) for row in rows]
    return {"ok": True, "results": rendered}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Dry-run or send Feishu credential health escalations")
    parser.add_argument("--health-json", default=str(DEFAULT_HEALTH_PATH))
    parser.add_argument("--send", action="store_true", help="Actually send messages; default is dry-run")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--target-mode", choices=["dm", "topic"], default=os.getenv("FEISHU_CREDENTIAL_ESCALATION_TARGET_MODE", "dm"))
    parser.add_argument("--topic-target", default=os.getenv("FEISHU_CREDENTIAL_ESCALATION_TOPIC_TARGET", ""))
    parser.add_argument("--cooldown-seconds", type=int, default=DEFAULT_COOLDOWN_SECONDS)
    parser.add_argument("--callback-listener", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--state", default="", help=argparse.SUPPRESS)
    parser.add_argument("--ttl-seconds", type=int, default=AUTH_URL_TTL_SECONDS, help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    if args.callback_listener:
        return run_callback_listener(args.state, ttl_seconds=args.ttl_seconds)
    load_send_environment()
    rows = load_health_rows(Path(args.health_json))
    result = run(rows, send=args.send, target_mode=args.target_mode, topic_target=args.topic_target, cooldown_seconds=args.cooldown_seconds)
    if args.json:
        print(json.dumps(result, ensure_ascii=False, separators=(",", ":")))
    else:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
