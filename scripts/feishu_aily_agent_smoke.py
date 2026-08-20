#!/usr/bin/env python3
"""Redacted, opt-in smoke probe for the Aily Agent Chat API.

The default invocation only checks the managed configuration.  ``--execute``
is required before a token or Agent request is sent.  Secrets and response
payloads are never printed; the probe is an endpoint check, not a resident
gateway canary.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

AUTH_APP_ID = "FEISHU_AILY_AUTH_APP_ID"
AUTH_APP_SECRET = "FEISHU_AILY_AUTH_APP_SECRET"
AGENT_ID = "FEISHU_AILY_AGENT_ID"
AILY_DOMAIN = "FEISHU_AILY_DOMAIN"
FEISHU_DOMAIN = "FEISHU_DOMAIN"
TOKEN_PATH = "/open-apis/auth/v3/tenant_access_token/internal"
CHAT_PATH = "/open-apis/aily/v1/agents/{agent_id}/chats"
CHAT_RESULT_PATH = "/open-apis/aily/v1/agents/{agent_id}/chats/{agent_chat_id}"
MAX_RESPONSE_BYTES = 8 * 1024 * 1024
MAX_ERROR_BYTES = 32 * 1024
MAX_QUESTION_CHARS = 10_000
POLL_INTERVAL_SECONDS = 1.0
MAX_POLL_REQUESTS = 120


@dataclass(frozen=True)
class ProbeConfig:
    auth_app_id: str
    auth_app_secret: str
    agent_id: str
    domain: str

    @property
    def base_url(self) -> str:
        return "https://open.larksuite.com" if self.domain == "lark" else "https://open.feishu.cn"


class ProbeFailure(RuntimeError):
    def __init__(self, payload: dict[str, Any]):
        self.payload = payload
        super().__init__(payload.get("error", "Aily Agent smoke probe failed"))


def _redact(value: Any, *, limit: int = 300) -> str:
    text = str(value)
    try:
        from agent.redact import redact_sensitive_text

        text = redact_sensitive_text(text, force=True)
    except Exception:  # pragma: no cover - standalone fallback
        pass
    return text[:limit]


def _default_env_file() -> Path:
    home = Path(os.environ.get("HERMES_HOME", Path.home() / ".hermes"))
    try:
        from hermes_constants import get_env_path_for_home

        return get_env_path_for_home(home)
    except ImportError:  # pragma: no cover
        return home / ".env"


def _read_values(env_file: Path | None) -> dict[str, str]:
    names = {AUTH_APP_ID, AUTH_APP_SECRET, AGENT_ID, AILY_DOMAIN, FEISHU_DOMAIN}
    values = {name: os.environ.get(name, "") for name in names}
    path = env_file or _default_env_file()
    if env_file is not None and not path.is_file():
        raise ProbeFailure({
            "ok": False,
            "phase": "config",
            "error": "explicit env file does not exist or is not a regular file",
            "network_request_sent": False,
        })
    if path.exists():
        if not path.is_file():
            raise ProbeFailure({
                "ok": False,
                "phase": "config",
                "error": "env path is not a regular file",
                "network_request_sent": False,
            })
        try:
            if path.stat().st_mode & 0o077:
                raise ProbeFailure({
                    "ok": False,
                    "phase": "config",
                    "error": "env file must not be group/world-readable (use mode 0600)",
                    "network_request_sent": False,
                })
            from dotenv import dotenv_values

            parsed = dotenv_values(path, interpolate=False)
            for name in names:
                if parsed.get(name) is not None:
                    values[name] = str(parsed[name])
        except ProbeFailure:
            raise
        except (OSError, ValueError) as exc:
            raise ProbeFailure({
                "ok": False,
                "phase": "config",
                "error": f"cannot read env file: {_redact(exc)}",
                "network_request_sent": False,
            }) from exc
    return {name: value.strip() for name, value in values.items()}


def load_probe_config(env_file: Path | None = None) -> ProbeConfig:
    values = _read_values(env_file)
    missing = [name for name in (AUTH_APP_ID, AUTH_APP_SECRET, AGENT_ID) if not values.get(name)]
    if missing:
        raise ProbeFailure({
            "ok": False,
            "phase": "config",
            "error": "missing required Aily Agent configuration: " + ", ".join(missing),
            "network_request_sent": False,
        })
    domain = (values.get(AILY_DOMAIN) or values.get(FEISHU_DOMAIN) or "feishu").lower()
    if domain not in {"feishu", "lark"}:
        raise ProbeFailure({
            "ok": False,
            "phase": "config",
            "error": f"{AILY_DOMAIN} must be 'feishu' or 'lark'",
            "network_request_sent": False,
        })
    if len(values[AGENT_ID]) > 65:
        raise ProbeFailure({
            "ok": False,
            "phase": "config",
            "error": f"{AGENT_ID} must be at most 65 characters",
            "network_request_sent": False,
        })
    return ProbeConfig(
        auth_app_id=values[AUTH_APP_ID],
        auth_app_secret=values[AUTH_APP_SECRET],
        agent_id=values[AGENT_ID],
        domain=domain,
    )


def config_summary(config: ProbeConfig) -> dict[str, Any]:
    return {
        "ok": True,
        "phase": "config",
        "auth_app_configured": bool(config.auth_app_id and config.auth_app_secret),
        "agent_configured": bool(config.agent_id),
        "agent_id_shape": config.agent_id.startswith("agent_"),
        "domain": config.domain,
        "network_request_sent": False,
    }


def _header(headers: Any, name: str) -> str | None:
    if headers is None:
        return None
    try:
        value = headers.get(name)
    except AttributeError:
        value = None
    return str(value) if value is not None else None


def _read_response(response: Any, *, limit: int) -> bytes:
    try:
        body = response.read(limit + 1)
    finally:
        close = getattr(response, "close", None)
        if callable(close):
            close()
    if len(body) > limit:
        raise ProbeFailure({
            "ok": False,
            "phase": "network",
            "error": "response exceeded the configured byte limit",
            "network_request_sent": True,
        })
    return body


def _request(
    opener: Callable[..., Any],
    request: urllib.request.Request,
    *,
    timeout: float,
    limit: int,
) -> tuple[int, Any, bytes]:
    try:
        response = opener(request, timeout=timeout)
        status = int(getattr(response, "status", 200))
        headers = getattr(response, "headers", {})
        return status, headers, _read_response(response, limit=limit)
    except urllib.error.HTTPError as exc:
        body = _read_response(exc, limit=min(limit, MAX_ERROR_BYTES))
        return int(exc.code), getattr(exc, "headers", {}), body
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raise ProbeFailure({
            "ok": False,
            "phase": "network",
            "error": _redact(exc),
            "network_request_sent": True,
        }) from exc


def run_probe(
    config: ProbeConfig,
    question: str,
    *,
    timeout: float = 120.0,
    opener: Callable[..., Any] = urllib.request.urlopen,
) -> dict[str, Any]:
    if not isinstance(question, str) or not question.strip():
        raise ProbeFailure({
            "ok": False,
            "phase": "input",
            "error": "question is required",
            "network_request_sent": False,
        })
    question = question.strip()
    if len(question) > MAX_QUESTION_CHARS:
        raise ProbeFailure({
            "ok": False,
            "phase": "input",
            "error": f"question must be at most {MAX_QUESTION_CHARS} characters",
            "network_request_sent": False,
        })

    token_request = urllib.request.Request(
        config.base_url + TOKEN_PATH,
        data=json.dumps({"app_id": config.auth_app_id, "app_secret": config.auth_app_secret}).encode(),
        headers={"Content-Type": "application/json; charset=utf-8", "Accept": "application/json"},
        method="POST",
    )
    token_status, _token_headers, token_body = _request(
        opener, token_request, timeout=timeout, limit=MAX_ERROR_BYTES
    )
    try:
        token_json = json.loads(token_body.decode("utf-8", errors="replace"))
    except (TypeError, ValueError) as exc:
        raise ProbeFailure({
            "ok": False,
            "phase": "api",
            "http_status": token_status,
            "error": "token endpoint returned non-JSON",
            "network_request_sent": True,
        }) from exc
    token_code = token_json.get("code", 0)
    if token_status < 200 or token_status >= 300 or token_code not in (None, 0):
        raise ProbeFailure({
            "ok": False,
            "phase": "api",
            "http_status": token_status,
            "code": token_code,
            "error": _redact(token_json.get("msg", "tenant token request failed")),
            "network_request_sent": True,
        })
    token = token_json.get("tenant_access_token")
    if not isinstance(token, str) or not token:
        data = token_json.get("data") if isinstance(token_json.get("data"), dict) else {}
        token = data.get("tenant_access_token")
    if not isinstance(token, str) or not token:
        raise ProbeFailure({
            "ok": False,
            "phase": "api",
            "http_status": token_status,
            "error": "token endpoint returned no tenant_access_token",
            "network_request_sent": True,
        })

    chat_body = {
        "user_message": {"content": [{"type": "text", "text": question}]},
        "stream": False,
    }
    chat_request = urllib.request.Request(
        config.base_url + CHAT_PATH.format(agent_id=urllib.parse.quote(config.agent_id, safe="")),
        data=json.dumps(chat_body, ensure_ascii=False).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json; charset=utf-8",
            "Accept": "application/json",
        },
        method="POST",
    )
    chat_status, chat_headers, chat_body_bytes = _request(
        opener, chat_request, timeout=timeout, limit=MAX_RESPONSE_BYTES
    )
    try:
        created = json.loads(chat_body_bytes.decode("utf-8", errors="replace"))
    except (TypeError, ValueError) as exc:
        raise ProbeFailure({
            "ok": False,
            "phase": "api",
            "http_status": chat_status,
            "error": "Agent create endpoint returned non-JSON",
            "network_request_sent": True,
        }) from exc
    if chat_status < 200 or chat_status >= 300 or created.get("code", 0) not in (None, 0):
        payload = {
            "ok": False,
            "phase": "api",
            "http_status": chat_status,
            "network_request_sent": True,
        }
        if "code" in created:
            payload["code"] = created["code"]
        payload["error"] = _redact(created.get("msg", f"Agent chat HTTP {chat_status}"))
        log_id = _header(chat_headers, "x-tt-logid")
        if log_id:
            payload["log_id"] = _redact(log_id)
        raise ProbeFailure(payload)

    data = created.get("data") if isinstance(created.get("data"), dict) else {}
    chat_id = data.get("agent_chat_id")
    session_id = data.get("session_id")
    if not isinstance(chat_id, str) or not chat_id or len(chat_id) > 64:
        raise ProbeFailure({
            "ok": False,
            "phase": "api",
            "http_status": chat_status,
            "error": "Agent create endpoint returned no valid agent_chat_id",
            "network_request_sent": True,
        })

    deadline = time.monotonic() + timeout
    result_status = chat_status
    result_headers: Any = chat_headers
    result_data: dict[str, Any] | None = None
    poll_count = 0
    while result_data is None:
        poll_count += 1
        if poll_count > MAX_POLL_REQUESTS:
            raise ProbeFailure({
                "ok": False,
                "phase": "api",
                "http_status": result_status,
                "error": "Agent result exceeded the poll request limit",
                "network_request_sent": True,
            })
        result_request = urllib.request.Request(
            config.base_url
            + CHAT_RESULT_PATH.format(
                agent_id=urllib.parse.quote(config.agent_id, safe=""),
                agent_chat_id=urllib.parse.quote(chat_id, safe=""),
            ),
            headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
            method="GET",
        )
        result_status, result_headers, result_body = _request(
            opener, result_request, timeout=timeout, limit=MAX_RESPONSE_BYTES
        )
        try:
            result_json = json.loads(result_body.decode("utf-8", errors="replace"))
        except (TypeError, ValueError) as exc:
            raise ProbeFailure({
                "ok": False,
                "phase": "api",
                "http_status": result_status,
                "error": "Agent result endpoint returned non-JSON",
                "network_request_sent": True,
            }) from exc
        if result_status < 200 or result_status >= 300 or result_json.get("code", 0) not in (None, 0):
            raise ProbeFailure({
                "ok": False,
                "phase": "api",
                "http_status": result_status,
                "code": result_json.get("code"),
                "error": _redact(result_json.get("msg", "Agent result request failed")),
                "network_request_sent": True,
            })
        candidate = result_json.get("data")
        if not isinstance(candidate, dict):
            raise ProbeFailure({
                "ok": False,
                "phase": "api",
                "http_status": result_status,
                "error": "Agent result endpoint returned no data",
                "network_request_sent": True,
            })
        status = str(candidate.get("status") or "").lower()
        finish_reason = candidate.get("finish_reason")
        if isinstance(finish_reason, str) and finish_reason:
            if finish_reason.lower() in {"error", "failed", "failure", "cancelled", "canceled"}:
                raise ProbeFailure({
                    "ok": False,
                    "phase": "api",
                    "http_status": result_status,
                    "error": _redact(finish_reason),
                    "network_request_sent": True,
                })
            result_data = candidate
        elif status in {"completed", "complete", "done", "success", "succeeded"}:
            result_data = candidate
        elif status in {"failed", "failure", "error"}:
            raise ProbeFailure({
                "ok": False,
                "phase": "api",
                "http_status": result_status,
                "error": _redact(candidate.get("status")),
                "network_request_sent": True,
            })
        elif time.monotonic() >= deadline:
            raise ProbeFailure({
                "ok": False,
                "phase": "api",
                "http_status": result_status,
                "error": "Agent result timed out before completion",
                "network_request_sent": True,
            })
        else:
            time.sleep(POLL_INTERVAL_SECONDS)

    content_items = result_data.get("content")
    content = ""
    if isinstance(content_items, list):
        content = "".join(
            item.get("text", "")
            for item in content_items
            if isinstance(item, dict) and isinstance(item.get("text"), str)
        )
    elif isinstance(content_items, str):
        content = content_items
    return {
        "ok": True,
        "phase": "finished",
        "http_status": result_status,
        "content_type": _header(result_headers, "content-type"),
        "answer_available": bool(content),
        "answer": _redact(content),
        "finish_reason": result_data.get("finish_reason"),
        "artifact_count": sum(
            1
            for item in (content_items or [])
            if isinstance(item, dict) and item.get("agent_artifact_id")
        ),
        "session_id_present": isinstance(session_id, str) and bool(session_id),
        "network_request_sent": True,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute", action="store_true", help="send the token and Agent requests")
    parser.add_argument("--env-file", type=Path, help="explicit 0600 Hermes env file")
    parser.add_argument("--question", help="question text (prefer --question-stdin for sensitive text)")
    parser.add_argument("--question-stdin", action="store_true", help="read the question from stdin")
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument("--pretty", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.question and args.question_stdin:
        payload = {"ok": False, "phase": "input", "error": "use only one question source", "network_request_sent": False}
        print(json.dumps(payload, ensure_ascii=False, indent=2 if args.pretty else None))
        return 2
    if args.timeout <= 0 or args.timeout > 300:
        payload = {"ok": False, "phase": "input", "error": "timeout must be >0 and <=300 seconds", "network_request_sent": False}
        print(json.dumps(payload, ensure_ascii=False, indent=2 if args.pretty else None))
        return 2
    try:
        config = load_probe_config(args.env_file)
        if not args.execute:
            payload = config_summary(config)
        else:
            question = sys.stdin.read() if args.question_stdin else (args.question or "")
            payload = run_probe(config, question, timeout=args.timeout)
    except ProbeFailure as exc:
        payload = exc.payload
    print(json.dumps(payload, ensure_ascii=False, indent=2 if args.pretty else None, sort_keys=True))
    if payload.get("ok"):
        return 0
    return 2 if payload.get("phase") in {"config", "input"} else 3


if __name__ == "__main__":
    raise SystemExit(main())
