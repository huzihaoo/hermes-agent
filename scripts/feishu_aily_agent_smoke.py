#!/usr/bin/env python3
"""Redacted, opt-in smoke probe for the Aily Agent Chat API.

The default invocation only checks the managed configuration.  ``--execute``
is required before a token or Agent request is sent.  Secrets and unbounded
response payloads are never printed; bounded answer text requires the explicit
``--show-answer`` flag.  The probe is an endpoint check, not a resident gateway
canary.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable

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
POLL_INITIAL_INTERVAL_SECONDS = 1.0
POLL_MAX_INTERVAL_SECONDS = 5.0
MAX_POLL_REQUESTS = 120

AILY_AGENT_ERROR_MESSAGES = {
    10001: "Aily Agent request validation failed; check path and body parameters",
    10002: "Agent chat was not found; verify the agent_chat_id and calling identity",
    10006: "The Aily Agent has not enabled the OpenAPI channel",
    10007: (
        "Access to the Aily Agent was denied; verify the OpenAPI identity type, user "
        "or App ID access range, same-tenant requirement, and attachment ownership"
    ),
    10008: "This tenant has not enabled Aily OpenAPI access",
    10009: "The Aily Agent OpenAPI channel has not enabled application identity",
    10010: "The calling App ID is not in the Aily Agent OpenAPI application allowlist",
    10011: "The calling user is outside the Aily Agent OpenAPI visibility range",
    2700001: "Aily Agent request parameters are invalid",
    50001: "Aily Agent returned an internal error; retry once later or contact support",
}

NON_TERMINAL_STATUSES = {"queued", "pending", "running", "processing"}
SUCCESS_STATUSES = {"completed"}
FAILED_STATUSES = {"failed", "failure", "error", "cancelled", "canceled"}


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


def _redact(
    value: Any,
    *,
    limit: int = 300,
    secrets: Iterable[str] = (),
) -> str:
    text = str(value)
    for secret in secrets:
        if secret:
            text = text.replace(secret, "[REDACTED]")
    try:
        from agent.redact import redact_sensitive_text

        text = redact_sensitive_text(text, force=True)
    except Exception:  # pragma: no cover - standalone fallback
        return "[redaction unavailable]"
    return str(text)[: max(0, limit)]


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
    if not values[AGENT_ID].startswith("agent_"):
        raise ProbeFailure({
            "ok": False,
            "phase": "config",
            "error": f"{AGENT_ID} must start with 'agent_'",
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


def _log_id(
    payload: Any,
    headers: Any = None,
    *,
    secrets: Iterable[str] = (),
) -> str | None:
    """Return a bounded diagnostic log id without exposing response bodies."""
    header_id = _header(headers, "x-tt-logid")
    if header_id:
        return _redact(header_id, limit=128, secrets=secrets)
    if isinstance(payload, dict):
        for key in ("log_id", "logid"):
            value = payload.get(key)
            if isinstance(value, str) and value:
                return _redact(value, limit=128, secrets=secrets)
        error = payload.get("error")
        if isinstance(error, dict):
            return _log_id(error, secrets=secrets)
    return None


def _api_error_text(
    code: Any,
    msg: Any,
    *,
    secrets: Iterable[str] = (),
) -> str:
    mapped = AILY_AGENT_ERROR_MESSAGES.get(code) if isinstance(code, int) else None
    safe_msg = _redact(msg or "unknown error", secrets=secrets)
    if not mapped:
        return safe_msg
    if not safe_msg or safe_msg == "unknown error" or safe_msg in mapped:
        return mapped
    return f"{mapped}; API: {safe_msg}"


def _agent_text(data: dict[str, Any]) -> str:
    content_items = data.get("content")
    if isinstance(content_items, list):
        return "".join(
            item.get("text", "")
            for item in content_items
            if isinstance(item, dict) and isinstance(item.get("text"), str)
        )
    return ""


def _json_object(body: bytes, *, label: str, http_status: int) -> dict[str, Any]:
    try:
        payload = json.loads(body.decode("utf-8", errors="replace"))
    except (TypeError, ValueError) as exc:
        raise ProbeFailure({
            "ok": False,
            "phase": "api",
            "http_status": http_status,
            "error": f"{label} returned non-JSON",
            "network_request_sent": True,
        }) from exc
    if not isinstance(payload, dict):
        raise ProbeFailure({
            "ok": False,
            "phase": "api",
            "http_status": http_status,
            "error": f"{label} returned non-object JSON",
            "network_request_sent": True,
        })
    code = payload.get("code", 0)
    if code is not None and (isinstance(code, bool) or not isinstance(code, int)):
        raise ProbeFailure({
            "ok": False,
            "phase": "api",
            "http_status": http_status,
            "error": f"{label} returned an invalid business code",
            "network_request_sent": True,
        })
    return payload


def _remaining_timeout(deadline: float, *, network_request_sent: bool) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise ProbeFailure({
            "ok": False,
            "phase": "network",
            "error": "Aily Agent probe exceeded its total timeout",
            "network_request_sent": network_request_sent,
        })
    return remaining


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
    secrets: Iterable[str] = (),
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
            "error": _redact(exc, secrets=secrets),
            "network_request_sent": True,
        }) from exc


def run_probe(
    config: ProbeConfig,
    question: str,
    *,
    timeout: float = 120.0,
    include_answer: bool = False,
    opener: Callable[..., Any] = urllib.request.urlopen,
) -> dict[str, Any]:
    if (
        isinstance(timeout, bool)
        or not isinstance(timeout, (int, float))
        or not math.isfinite(timeout)
        or timeout <= 0
        or timeout > 300
    ):
        raise ProbeFailure({
            "ok": False,
            "phase": "input",
            "error": "timeout must be finite, >0 and <=300 seconds",
            "network_request_sent": False,
        })
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

    deadline = time.monotonic() + float(timeout)
    known_secrets = (config.auth_app_secret,)

    token_request = urllib.request.Request(
        config.base_url + TOKEN_PATH,
        data=json.dumps({"app_id": config.auth_app_id, "app_secret": config.auth_app_secret}).encode(),
        headers={"Content-Type": "application/json; charset=utf-8", "Accept": "application/json"},
        method="POST",
    )
    token_status, _token_headers, token_body = _request(
        opener,
        token_request,
        timeout=_remaining_timeout(deadline, network_request_sent=False),
        limit=MAX_ERROR_BYTES,
        secrets=known_secrets,
    )
    token_json = _json_object(
        token_body, label="token endpoint", http_status=token_status
    )
    token_code = token_json.get("code", 0)
    if token_status < 200 or token_status >= 300 or token_code not in (None, 0):
        payload = {
            "ok": False,
            "phase": "api",
            "http_status": token_status,
            "code": token_code,
            "error": _redact(
                token_json.get("msg", "tenant token request failed"),
                secrets=known_secrets,
            ),
            "network_request_sent": True,
        }
        log_id = _log_id(token_json, _token_headers, secrets=known_secrets)
        if log_id:
            payload["log_id"] = log_id
        raise ProbeFailure(payload)
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
    known_secrets = (config.auth_app_secret, token)

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
        opener,
        chat_request,
        timeout=_remaining_timeout(deadline, network_request_sent=True),
        limit=MAX_RESPONSE_BYTES,
        secrets=known_secrets,
    )
    created = _json_object(
        chat_body_bytes, label="Agent create endpoint", http_status=chat_status
    )
    if chat_status < 200 or chat_status >= 300 or created.get("code", 0) not in (None, 0):
        payload = {
            "ok": False,
            "phase": "api",
            "http_status": chat_status,
            "network_request_sent": True,
        }
        if "code" in created:
            payload["code"] = created["code"]
        payload["error"] = _api_error_text(
            created.get("code"),
            created.get("msg", f"Agent chat HTTP {chat_status}"),
            secrets=known_secrets,
        )
        log_id = _log_id(created, chat_headers, secrets=known_secrets)
        if log_id:
            payload["log_id"] = log_id
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

    result_status = chat_status
    result_headers: Any = chat_headers
    result_data: dict[str, Any] | None = None
    poll_count = 0
    poll_interval = POLL_INITIAL_INTERVAL_SECONDS
    time.sleep(min(poll_interval, max(0.0, deadline - time.monotonic())))
    poll_interval = min(POLL_MAX_INTERVAL_SECONDS, poll_interval * 2)
    while result_data is None:
        if time.monotonic() >= deadline:
            raise ProbeFailure({
                "ok": False,
                "phase": "api",
                "http_status": result_status,
                "error": "Agent result timed out before completion",
                "network_request_sent": True,
            })
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
            opener,
            result_request,
            timeout=_remaining_timeout(deadline, network_request_sent=True),
            limit=MAX_RESPONSE_BYTES,
            secrets=known_secrets,
        )
        result_json = _json_object(
            result_body,
            label="Agent result endpoint",
            http_status=result_status,
        )
        if result_status < 200 or result_status >= 300 or result_json.get("code", 0) not in (None, 0):
            payload = {
                "ok": False,
                "phase": "api",
                "http_status": result_status,
                "code": result_json.get("code"),
                "error": _api_error_text(
                    result_json.get("code"),
                    result_json.get("msg", "Agent result request failed"),
                    secrets=known_secrets,
                ),
                "network_request_sent": True,
            }
            log_id = _log_id(result_json, result_headers, secrets=known_secrets)
            if log_id:
                payload["log_id"] = log_id
            raise ProbeFailure(payload)
        candidate = result_json.get("data")
        if not isinstance(candidate, dict):
            raise ProbeFailure({
                "ok": False,
                "phase": "api",
                "http_status": result_status,
                "error": "Agent result endpoint returned no data",
                "network_request_sent": True,
            })
        raw_status = candidate.get("status")
        if not isinstance(raw_status, str) or not raw_status or len(raw_status) > 64:
            raise ProbeFailure({
                "ok": False,
                "phase": "api",
                "http_status": result_status,
                "error": "Agent result endpoint returned an invalid status",
                "network_request_sent": True,
            })
        status = raw_status.lower()
        finish_reason = candidate.get("finish_reason")
        if status in FAILED_STATUSES:
            raise ProbeFailure({
                "ok": False,
                "phase": "api",
                "http_status": result_status,
                "error": _redact(
                    f"status={raw_status} "
                    f"finish_reason={finish_reason or 'unknown'}",
                    secrets=known_secrets,
                ),
                "network_request_sent": True,
            })
        if status in NON_TERMINAL_STATUSES:
            result_data = None
        elif status in SUCCESS_STATUSES:
            result_data = candidate
        if result_data is None:
            if time.monotonic() >= deadline:
                raise ProbeFailure({
                    "ok": False,
                    "phase": "api",
                    "http_status": result_status,
                    "error": "Agent result timed out before completion",
                    "network_request_sent": True,
                })
            time.sleep(min(poll_interval, max(0.0, deadline - time.monotonic())))
            poll_interval = min(POLL_MAX_INTERVAL_SECONDS, poll_interval * 2)

    content_items = result_data.get("content")
    if content_items is not None and not isinstance(content_items, list):
        raise ProbeFailure({
            "ok": False,
            "phase": "api",
            "http_status": result_status,
            "error": "Agent result endpoint returned invalid content",
            "network_request_sent": True,
        })
    content = _agent_text(result_data)
    result = {
        "ok": True,
        "phase": "finished",
        "http_status": result_status,
        "content_type": _redact(
            _header(result_headers, "content-type") or "",
            limit=128,
            secrets=known_secrets,
        ),
        "answer_available": bool(content),
        "answer_length": len(content),
        "status": "Completed",
        "finish_reason": _redact(
            result_data.get("finish_reason") or "",
            limit=128,
            secrets=known_secrets,
        ),
        "poll_count": poll_count,
        "artifact_count": sum(
            1
            for item in (content_items or [])
            if isinstance(item, dict) and item.get("agent_artifact_id")
        ),
        "session_id_present": isinstance(session_id, str) and bool(session_id),
        "network_request_sent": True,
    }
    if include_answer:
        result["answer"] = _redact(content, secrets=known_secrets)
    log_id = _log_id(result_data, result_headers, secrets=known_secrets)
    if log_id:
        result["log_id"] = log_id
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute", action="store_true", help="send the token and Agent requests")
    parser.add_argument("--env-file", type=Path, help="explicit 0600 Hermes env file")
    parser.add_argument("--question", help="question text (prefer --question-stdin for sensitive text)")
    parser.add_argument("--question-stdin", action="store_true", help="read the question from stdin")
    parser.add_argument(
        "--show-answer",
        action="store_true",
        help="include at most 300 answer characters in output (may contain internal data)",
    )
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument("--pretty", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.question and args.question_stdin:
        payload = {"ok": False, "phase": "input", "error": "use only one question source", "network_request_sent": False}
        print(json.dumps(payload, ensure_ascii=False, indent=2 if args.pretty else None))
        return 2
    if not math.isfinite(args.timeout) or args.timeout <= 0 or args.timeout > 300:
        payload = {"ok": False, "phase": "input", "error": "timeout must be finite, >0 and <=300 seconds", "network_request_sent": False}
        print(json.dumps(payload, ensure_ascii=False, indent=2 if args.pretty else None))
        return 2
    try:
        config = load_probe_config(args.env_file)
        if not args.execute:
            payload = config_summary(config)
        else:
            question = sys.stdin.read() if args.question_stdin else (args.question or "")
            payload = run_probe(
                config,
                question,
                timeout=args.timeout,
                include_answer=args.show_answer,
            )
    except ProbeFailure as exc:
        payload = exc.payload
    print(json.dumps(payload, ensure_ascii=False, indent=2 if args.pretty else None, sort_keys=True))
    if payload.get("ok"):
        return 0
    return 2 if payload.get("phase") in {"config", "input"} else 3


if __name__ == "__main__":
    raise SystemExit(main())
