#!/usr/bin/env python3
"""Run a redacted, opt-in smoke probe against the Feishu Aily ask API.

The default mode only validates the managed configuration.  ``--execute`` is
required before any network request is made.  Credentials are read from the
Hermes environment file or process environment and are never accepted as
command-line arguments or included in the JSON receipt.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable
from urllib.parse import quote

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.feishu_aily_knowledge_tool import (  # noqa: E402
    SSEProtocolError,
    iter_sse_payloads,
    validate_aily_finished_payload,
)


AUTH_APP_ID = "FEISHU_AILY_AUTH_APP_ID"
AUTH_APP_SECRET = "FEISHU_AILY_AUTH_APP_SECRET"
TARGET_APP_ID = "FEISHU_AILY_TARGET_APP_ID"
AILY_DOMAIN = "FEISHU_AILY_DOMAIN"
FEISHU_DOMAIN = "FEISHU_DOMAIN"
TOKEN_PATH = "/open-apis/auth/v3/tenant_access_token/internal"
ASK_PATH = "/open-apis/aily/v1/apps/{app_id}/knowledges/ask"
MAX_ERROR_BYTES = 32 * 1024
MAX_TEXT_CHARS = 50_000


@dataclass(frozen=True)
class ProbeConfig:
    auth_app_id: str
    auth_app_secret: str
    target_app_id: str
    domain: str

    @property
    def base_url(self) -> str:
        return (
            "https://open.larksuite.com"
            if self.domain == "lark"
            else "https://open.feishu.cn"
        )


class ProbeFailure(RuntimeError):
    """A safe, already-redacted probe failure."""

    def __init__(self, payload: dict[str, Any]):
        self.payload = payload
        super().__init__(payload.get("error", "Aily smoke probe failed"))


def _redact(value: Any, secrets: Iterable[str] = (), *, limit: int = 300) -> str:
    """Bound diagnostic text and remove both known and pattern secrets."""
    text = str(value)
    for secret in secrets:
        if secret:
            text = text.replace(secret, "<redacted-secret>")
    try:
        from agent.redact import redact_sensitive_text

        text = redact_sensitive_text(text, force=True)
    except Exception:  # pragma: no cover - bootstrap fallback
        pass
    return text[:limit]


def _default_env_file() -> Path:
    home = Path(os.environ.get("HERMES_HOME", Path.home() / ".hermes"))
    try:
        from hermes_constants import get_env_path_for_home

        return get_env_path_for_home(home)
    except ImportError:  # pragma: no cover - standalone bootstrap fallback
        return home / ".env"


def _read_probe_values(env_file: Path | None) -> dict[str, str]:
    """Read only Aily-related names without rewriting the env file."""
    names = {AUTH_APP_ID, AUTH_APP_SECRET, TARGET_APP_ID, AILY_DOMAIN, FEISHU_DOMAIN}
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
        except OSError as exc:
            raise ProbeFailure({
                "ok": False,
                "phase": "config",
                "error": f"cannot stat env file: {_redact(exc)}",
                "network_request_sent": False,
            }) from exc
        try:
            from dotenv import dotenv_values

            parsed = dotenv_values(path, interpolate=False)
            # Hermes .env values take precedence over stale shell values, just
            # as load_hermes_dotenv does, but only the five allowlisted names
            # are read into this process.
            for name in names:
                value = parsed.get(name)
                if value is not None:
                    values[name] = str(value)
        except (OSError, ValueError) as exc:
            raise ProbeFailure({
                "ok": False,
                "phase": "config",
                "error": f"cannot read env file: {_redact(exc)}",
                "network_request_sent": False,
            }) from exc
    return {name: value.strip() for name, value in values.items()}


def load_probe_config(env_file: Path | None = None) -> ProbeConfig:
    values = _read_probe_values(env_file)
    missing = [
        name
        for name in (AUTH_APP_ID, AUTH_APP_SECRET, TARGET_APP_ID)
        if not values.get(name)
    ]
    if missing:
        raise ProbeFailure({
            "ok": False,
            "phase": "config",
            "error": "missing required Aily configuration: " + ", ".join(missing),
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
    if values[TARGET_APP_ID] == values[AUTH_APP_ID]:
        raise ProbeFailure({
            "ok": False,
            "phase": "config",
            "error": (
                f"{TARGET_APP_ID} must be the Aily platform app ID, not the "
                "caller Open Platform app ID"
            ),
            "network_request_sent": False,
        })
    if len(values[TARGET_APP_ID]) > 255:
        raise ProbeFailure({
            "ok": False,
            "phase": "config",
            "error": f"{TARGET_APP_ID} must be at most 255 characters",
            "network_request_sent": False,
        })
    return ProbeConfig(
        auth_app_id=values[AUTH_APP_ID],
        auth_app_secret=values[AUTH_APP_SECRET],
        target_app_id=values[TARGET_APP_ID],
        domain=domain,
    )


def config_summary(config: ProbeConfig) -> dict[str, Any]:
    """Return a receipt that contains presence/shape only, never identifiers."""
    return {
        "auth_app_configured": bool(config.auth_app_id and config.auth_app_secret),
        "target_app_configured": bool(config.target_app_id),
        "target_looks_like_aily_app": config.target_app_id.startswith("spring_"),
        "domain": config.domain,
    }


def _header(headers: Any, name: str) -> str | None:
    if headers is None:
        return None
    try:
        value = headers.get(name)
    except AttributeError:
        value = None
    if value is None:
        try:
            value = next(
                (
                    candidate
                    for key, candidate in headers.items()
                    if str(key).lower() == name.lower()
                ),
                None,
            )
        except AttributeError:
            value = None
    return str(value) if value is not None else None


def _bounded_read(response: Any) -> bytes:
    try:
        return response.read(MAX_ERROR_BYTES)
    except TypeError:
        return response.read()


def _status(response: Any) -> int | None:
    value = getattr(response, "status", None)
    if isinstance(value, int):
        return value
    try:
        value = response.getcode()
    except AttributeError:
        return None
    return value if isinstance(value, int) else None


def _json_body(raw: bytes | str) -> dict[str, Any] | None:
    try:
        value = json.loads(
            raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else raw
        )
    except (TypeError, ValueError):
        return None
    return value if isinstance(value, dict) else None


def _http_failure(
    *,
    phase: str,
    status: int | None,
    headers: Any,
    body: bytes | str,
    secrets: Iterable[str],
) -> ProbeFailure:
    parsed = _json_body(body) or {}
    payload: dict[str, Any] = {
        "ok": False,
        "phase": phase,
        "network_request_sent": True,
        "error": _redact(parsed.get("msg") or "HTTP request failed", secrets),
    }
    if status is not None:
        payload["http_status"] = status
    if parsed.get("code") is not None:
        payload["code"] = parsed["code"]
    log_id = _header(headers, "x-tt-logid")
    reset = _header(headers, "x-ogw-ratelimit-reset")
    if log_id:
        payload["log_id"] = _redact(log_id, secrets)
    if reset:
        payload["rate_limit_reset"] = _redact(reset, secrets)
    return ProbeFailure(payload)


def _open_json(
    request: urllib.request.Request,
    *,
    timeout: float,
    opener: Callable[..., Any],
    secrets: Iterable[str],
    phase: str,
) -> dict[str, Any]:
    try:
        response = opener(request, timeout=timeout)
    except urllib.error.HTTPError as exc:
        raise _http_failure(
            phase=phase,
            status=exc.code,
            headers=exc.headers,
            body=_bounded_read(exc),
            secrets=secrets,
        ) from exc
    except (OSError, urllib.error.URLError) as exc:
        raise ProbeFailure({
            "ok": False,
            "phase": phase,
            "network_request_sent": True,
            "error": _redact(exc, secrets),
        }) from exc
    with response:
        status = _status(response)
        headers = getattr(response, "headers", None)
        body = _bounded_read(response)
    if status is not None and not 200 <= status < 300:
        raise _http_failure(
            phase=phase,
            status=status,
            headers=headers,
            body=body,
            secrets=secrets,
        )
    parsed = _json_body(body)
    if parsed is None:
        raise ProbeFailure({
            "ok": False,
            "phase": phase,
            "network_request_sent": True,
            "error": "expected a JSON response",
        })
    return parsed


def _tenant_access_token(
    config: ProbeConfig,
    *,
    timeout: float,
    opener: Callable[..., Any],
) -> str:
    body = json.dumps({
        "app_id": config.auth_app_id,
        "app_secret": config.auth_app_secret,
    }).encode("utf-8")
    request = urllib.request.Request(
        config.base_url + TOKEN_PATH,
        data=body,
        method="POST",
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json; charset=utf-8",
        },
    )
    payload = _open_json(
        request,
        timeout=timeout,
        opener=opener,
        secrets=(config.auth_app_secret,),
        phase="token",
    )
    if payload.get("code", 0) not in (None, 0):
        raise ProbeFailure({
            "ok": False,
            "phase": "token",
            "network_request_sent": True,
            "code": payload.get("code"),
            "error": _redact(
                payload.get("msg") or "token request failed", (config.auth_app_secret,)
            ),
        })
    token = payload.get("tenant_access_token")
    if not token and isinstance(payload.get("data"), dict):
        token = payload["data"].get("tenant_access_token")
    if not isinstance(token, str) or not token:
        raise ProbeFailure({
            "ok": False,
            "phase": "token",
            "network_request_sent": True,
            "error": "token response did not contain tenant_access_token",
        })
    return token


def _evidence_counts(payload: dict[str, Any]) -> dict[str, int]:
    process_data = payload.get("process_data")
    if not isinstance(process_data, dict):
        return {}
    counts: dict[str, int] = {}
    for name in ("chunks", "chart_dsls", "sql_data"):
        value = process_data.get(name)
        if isinstance(value, list):
            counts[name] = len(value)
    return counts


def _clean_asset_values(values: list[str] | None, name: str) -> list[str] | None:
    if values is None:
        return None
    if any(not isinstance(value, str) for value in values):
        raise ProbeFailure({
            "ok": False,
            "phase": "input",
            "error": f"{name} must be an array of strings",
            "network_request_sent": False,
        })
    if len(values) > 65_535:
        raise ProbeFailure({
            "ok": False,
            "phase": "input",
            "error": f"{name} must contain at most 65535 items",
            "network_request_sent": False,
        })
    return [value.strip() for value in values if value.strip()]


def run_probe(
    config: ProbeConfig,
    question: str,
    *,
    data_asset_ids: list[str] | None = None,
    data_asset_tag_ids: list[str] | None = None,
    timeout: float = 120.0,
    opener: Callable[..., Any] = urllib.request.urlopen,
) -> dict[str, Any]:
    question = question.strip()
    if timeout <= 0:
        raise ProbeFailure({
            "ok": False,
            "phase": "input",
            "error": "timeout must be greater than zero",
            "network_request_sent": False,
        })
    if not question:
        raise ProbeFailure({
            "ok": False,
            "phase": "input",
            "error": "question is required",
            "network_request_sent": False,
        })
    if len(question) > 65_535:
        raise ProbeFailure({
            "ok": False,
            "phase": "input",
            "error": "question exceeds 65535 characters",
            "network_request_sent": False,
        })

    data_asset_ids = _clean_asset_values(data_asset_ids, "data_asset_ids")
    data_asset_tag_ids = _clean_asset_values(data_asset_tag_ids, "data_asset_tag_ids")
    token = _tenant_access_token(config, timeout=timeout, opener=opener)
    body: dict[str, Any] = {"message": {"content": question}}
    if data_asset_ids:
        body["data_asset_ids"] = data_asset_ids
    if data_asset_tag_ids:
        body["data_asset_tag_ids"] = data_asset_tag_ids
    request = urllib.request.Request(
        config.base_url + ASK_PATH.format(app_id=quote(config.target_app_id, safe="")),
        data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
        method="POST",
        headers={
            "Accept": "text/event-stream",
            "Content-Type": "application/json; charset=utf-8",
            "Authorization": f"Bearer {token}",
        },
    )
    secrets = (config.auth_app_secret, token)
    deadline = time.monotonic() + timeout
    try:
        response = opener(request, timeout=timeout)
    except urllib.error.HTTPError as exc:
        raise _http_failure(
            phase="ask",
            status=exc.code,
            headers=exc.headers,
            body=_bounded_read(exc),
            secrets=secrets,
        ) from exc
    except (OSError, urllib.error.URLError) as exc:
        raise ProbeFailure({
            "ok": False,
            "phase": "ask",
            "network_request_sent": True,
            "error": _redact(exc, secrets),
        }) from exc

    status = _status(response)
    headers = getattr(response, "headers", None)
    content_type = _header(headers, "content-type") or ""
    base_receipt: dict[str, Any] = {
        "network_request_sent": True,
        "http_status": status,
        "content_type": _redact(content_type, secrets),
    }
    log_id = _header(headers, "x-tt-logid")
    if log_id:
        base_receipt["log_id"] = _redact(log_id, secrets)

    with response:
        if status is not None and not 200 <= status < 300:
            raise _http_failure(
                phase="ask",
                status=status,
                headers=headers,
                body=_bounded_read(response),
                secrets=secrets,
            )
        if "event-stream" not in content_type.lower():
            body_bytes = _bounded_read(response)
            parsed = _json_body(body_bytes) or {}
            raise ProbeFailure({
                **base_receipt,
                "ok": False,
                "phase": "ask",
                "code": parsed.get("code"),
                "error": _redact(
                    parsed.get("msg") or "expected text/event-stream response",
                    secrets,
                ),
            })

        finished: dict[str, Any] | None = None
        event_count = 0

        def deadline_lines() -> Iterable[bytes | str]:
            for line in response:
                if time.monotonic() > deadline:
                    raise SSEProtocolError("SSE response exceeded the total deadline")
                yield line

        try:
            for payload in iter_sse_payloads(deadline_lines()):
                event_count += 1
                code = payload.get("code")
                if code not in (None, 0):
                    raise ProbeFailure({
                        **base_receipt,
                        "ok": False,
                        "phase": "ask",
                        "code": code,
                        "error": _redact(
                            payload.get("msg") or "Aily ask failed", secrets
                        ),
                        "event_count": event_count,
                    })
                if payload.get("status") == "finished":
                    finished = payload
                    break
        except SSEProtocolError as exc:
            raise ProbeFailure({
                **base_receipt,
                "ok": False,
                "phase": "ask",
                "error": str(exc),
                "event_count": event_count,
            }) from exc

    if finished is None:
        raise ProbeFailure({
            **base_receipt,
            "ok": False,
            "phase": "ask",
            "error": "stream ended before a finished event",
            "event_count": event_count,
        })

    try:
        finish_type, has_answer = validate_aily_finished_payload(finished)
    except SSEProtocolError as exc:
        raise ProbeFailure({
            **base_receipt,
            "ok": False,
            "phase": "ask",
            "error": str(exc),
            "event_count": event_count,
        }) from exc

    message = finished.get("message")
    content = message.get("content", "") if isinstance(message, dict) else ""
    faq_result = finished.get("faq_result")
    if not content and isinstance(faq_result, dict):
        content = faq_result.get("answer", "")
    if not isinstance(content, str):
        content = ""
    result = {
        **base_receipt,
        "ok": True,
        "phase": "ask",
        "status": "finished",
        "finish_type": finish_type,
        "has_answer": has_answer,
        "grounded": has_answer,
        "answer_available": bool(content),
        "content": _redact(content, secrets, limit=MAX_TEXT_CHARS),
        "event_count": event_count,
    }
    evidence = _evidence_counts(finished)
    if evidence:
        result["evidence_counts"] = evidence
    if isinstance(faq_result, dict) and faq_result.get("question"):
        result["matched_question"] = _redact(
            faq_result["question"], secrets, limit=2_000
        )
    if not result["answer_available"]:
        result["message"] = "Aily finished without an answer content"
    return result


def _print(payload: dict[str, Any], pretty: bool) -> None:
    print(
        json.dumps(
            payload, ensure_ascii=False, indent=2 if pretty else None, sort_keys=True
        )
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Redacted Feishu Aily knowledge-QA smoke probe"
    )
    parser.add_argument(
        "--env-file", type=Path, help="optional Hermes-compatible env file"
    )
    parser.add_argument(
        "--execute", action="store_true", help="send token and ask requests"
    )
    question_source = parser.add_mutually_exclusive_group()
    question_source.add_argument(
        "--question",
        help="plain-text question; prefer --question-stdin for private text",
    )
    question_source.add_argument(
        "--question-stdin", action="store_true", help="read the question from stdin"
    )
    parser.add_argument("--data-asset-id", action="append", default=[])
    parser.add_argument("--data-asset-tag-id", action="append", default=[])
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument("--pretty", action="store_true")
    args = parser.parse_args(argv)

    try:
        config = load_probe_config(args.env_file)
    except ProbeFailure as exc:
        _print(exc.payload, args.pretty)
        return 2

    if not args.execute:
        _print(
            {
                "ok": True,
                "mode": "config-check",
                "network_request_sent": False,
                "config": config_summary(config),
            },
            args.pretty,
        )
        return 0
    question = sys.stdin.read(65_536) if args.question_stdin else args.question
    if not question:
        _print(
            {
                "ok": False,
                "phase": "input",
                "error": "--question or --question-stdin is required with --execute",
                "network_request_sent": False,
            },
            args.pretty,
        )
        return 2

    try:
        result = run_probe(
            config,
            question,
            data_asset_ids=args.data_asset_id,
            data_asset_tag_ids=args.data_asset_tag_id,
            timeout=args.timeout,
        )
    except ProbeFailure as exc:
        _print(exc.payload, args.pretty)
        if exc.payload.get("phase") in {"config", "input"} and not exc.payload.get(
            "network_request_sent", False
        ):
            return 2
        return 3
    except Exception as exc:  # pragma: no cover - final diagnostic guard
        _print(
            {
                "ok": False,
                "phase": "probe",
                "network_request_sent": True,
                "error": f"internal probe failure ({type(exc).__name__})",
            },
            args.pretty,
        )
        return 3
    _print(result, args.pretty)
    return 0 if result.get("ok") else 4


if __name__ == "__main__":
    raise SystemExit(main())
