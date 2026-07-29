#!/usr/bin/env python3
"""Read-only B13 patrol for the CVEStudio web bundle.

The patrol checks the CVEStudio SPA entry point and the same-origin JavaScript
assets it advertises.  It deliberately does not submit an ``mcapPath`` query,
open a browser, or parse an MCAP file.  An HTTP 200 therefore means only that
the SPA endpoint is alive; it is never reported as MCAP parse/render success.

The expected bundle SHA is intentionally configuration, not a constant in
this repository.  The renderer bundle is owned by the CVEStudio host and the
canonical B13 contract says that a change from the 2026-07-01 baseline must
alert.  Until an owner supplies that baseline, the result is ``unconfigured``
and the command exits non-zero.
"""

from __future__ import annotations

import argparse
from html.parser import HTMLParser
import hashlib
import hmac
import json
import os
import re
import ssl
from typing import Any, Iterable, Sequence
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin, urlsplit, urlunsplit
from urllib.request import (
    HTTPSHandler,
    HTTPRedirectHandler,
    Request,
    build_opener,
)


SCHEMA_VERSION = "pnc_cvestudio_bundle_patrol_v1"
DEFAULT_RENDERER_URL = "https://192.168.21.217/"
DEFAULT_TIMEOUT_SECONDS = 10.0
MAX_ENTRY_BYTES = 64 * 1024 * 1024
MAX_ROOT_BYTES = 4 * 1024 * 1024
READ_CHUNK_BYTES = 64 * 1024
SHA256_RE = re.compile(r"^[0-9a-fA-F]{64}$")

SPA_LIVENESS_NOTE = (
    "HTTP 200 proves only SPA endpoint liveness; it does not prove MCAP "
    "parse or render success."
)
PRIVATE_CONTRACT_NOTE = (
    "The ds=foxglove-http&ds.mcapPath= query is a private CVEStudio "
    "contract backed by browser observation; this patrol does not claim to "
    "verify MCAP parsing."
)
FINGERPRINT_DEFINITION = (
    "sha256 of sorted '<asset-label>\\t<asset-sha256>\\n' records; the "
    "index HTML is labelled index.html"
)


class PatrolConfigError(ValueError):
    """A renderer or patrol configuration cannot be trusted."""

    def __init__(self, code: str, detail: str):
        self.code = code
        self.detail = detail
        super().__init__(detail)


class _NoRedirect(HTTPRedirectHandler):
    """Keep a redirect visible to the patrol instead of following it."""

    def redirect_request(self, *_args: Any, **_kwargs: Any) -> None:
        return None


class _ScriptSourceParser(HTMLParser):
    """Extract script ``src`` attributes without executing page content."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.sources: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "script":
            return
        for name, value in attrs:
            if name.lower() == "src" and value:
                self.sources.append(value)
                break


def _sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _renderer_url(value: str | None) -> str:
    raw = str(value or DEFAULT_RENDERER_URL).strip()
    if not raw or not raw.isascii():
        raise PatrolConfigError(
            "cvestudio_renderer_url_invalid", "renderer URL must be ASCII"
        )
    try:
        parsed = urlsplit(raw)
    except ValueError as exc:
        raise PatrolConfigError(
            "cvestudio_renderer_url_invalid", "renderer URL is malformed"
        ) from exc
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or parsed.path not in {"", "/"}
    ):
        raise PatrolConfigError(
            "cvestudio_renderer_url_invalid",
            "renderer URL must be a credential-free origin root without a query",
        )
    try:
        port = parsed.port
    except ValueError as exc:
        raise PatrolConfigError(
            "cvestudio_renderer_url_invalid", "renderer URL port is invalid"
        ) from exc
    if port is not None and not 1 <= port <= 65535:
        raise PatrolConfigError(
            "cvestudio_renderer_url_invalid", "renderer URL port is invalid"
        )
    # Preserve the caller's host spelling only after requiring a stable netloc.
    return urlunsplit((parsed.scheme, parsed.netloc, "/", "", ""))


def _same_origin_asset_url(renderer_url: str, source: str) -> str:
    source = str(source or "").strip()
    if (
        not source
        or "\x00" in source
        or "\\" in source
        or any(ord(char) < 0x20 or ord(char) == 0x7F for char in source)
    ):
        raise PatrolConfigError(
            "cvestudio_bundle_script_src_invalid", "empty script source"
        )
    candidate = urljoin(renderer_url, source)
    base = urlsplit(renderer_url)
    parsed = urlsplit(candidate)
    if (
        parsed.scheme != base.scheme
        or parsed.netloc != base.netloc
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
        or not parsed.path
        or any(part == ".." for part in parsed.path.split("/"))
    ):
        raise PatrolConfigError(
            "cvestudio_bundle_script_cross_origin",
            "CVEStudio advertised a non-same-origin script",
        )
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, parsed.query, ""))


def extract_script_urls(html: bytes, renderer_url: str) -> list[str]:
    """Return unique, same-origin script URLs in document order."""

    parser = _ScriptSourceParser()
    try:
        parser.feed(html.decode("utf-8"))
        parser.close()
    except (UnicodeDecodeError, ValueError) as exc:
        raise PatrolConfigError(
            "cvestudio_renderer_html_invalid", "renderer HTML is not valid UTF-8"
        ) from exc

    urls: list[str] = []
    seen: set[str] = set()
    for source in parser.sources:
        url = _same_origin_asset_url(renderer_url, source)
        if url not in seen:
            seen.add(url)
            urls.append(url)
    return urls


def bundle_fingerprint(entries: Iterable[tuple[str, bytes]]) -> str:
    """Compute the deterministic fingerprint used by the patrol contract."""

    records: list[str] = []
    seen: set[str] = set()
    for label, body in entries:
        label = str(label)
        if not label or label in seen:
            raise ValueError("bundle fingerprint labels must be unique and non-empty")
        seen.add(label)
        records.append(f"{label}\t{_sha256(bytes(body))}\n")
    if not records:
        raise ValueError("bundle fingerprint requires at least one entry")
    return _sha256("".join(sorted(records)).encode("utf-8"))


def _asset_label(url: str) -> str:
    parsed = urlsplit(url)
    label = parsed.path.lstrip("/") or "/"
    if parsed.query:
        label += "?" + parsed.query
    return label


def _build_default_opener(*, insecure_tls: bool) -> Any:
    handlers: list[Any] = [_NoRedirect()]
    if insecure_tls:
        handlers.append(
            HTTPSHandler(context=ssl._create_unverified_context())  # noqa: S323
        )
    return build_opener(*handlers)


def _open_with(opener: Any, request: Request, timeout: float) -> Any:
    if hasattr(opener, "open"):
        return opener.open(request, timeout=timeout)
    if callable(opener):
        return opener(request, timeout=timeout)
    raise TypeError("opener must expose open() or be callable")


def _read_bounded(response: Any, *, limit: int) -> bytes:
    headers = getattr(response, "headers", None)
    if headers is not None:
        try:
            declared = headers.get("Content-Length")
            if declared is not None and int(declared) > limit:
                raise PatrolConfigError(
                    "cvestudio_bundle_entry_too_large",
                    "renderer response exceeds the patrol size limit",
                )
        except ValueError:
            # An invalid length is not trusted; the bounded read below remains
            # the final protection.
            pass

    chunks: list[bytes] = []
    total = 0
    while True:
        chunk = response.read(READ_CHUNK_BYTES)
        if not chunk:
            break
        if not isinstance(chunk, bytes):
            if isinstance(chunk, bytearray):
                chunk = bytes(chunk)
            else:
                raise PatrolConfigError(
                    "cvestudio_renderer_body_invalid",
                    "renderer returned a non-byte response body",
                )
        total += len(chunk)
        if total > limit:
            raise PatrolConfigError(
                "cvestudio_bundle_entry_too_large",
                "renderer response exceeds the patrol size limit",
            )
        chunks.append(chunk)
    return b"".join(chunks)


def _fetch(
    url: str,
    *,
    opener: Any,
    timeout: float,
    limit: int,
) -> dict[str, Any]:
    request = Request(
        url,
        method="GET",
        headers={
            "Accept": "text/html,application/javascript,*/*;q=0.1",
            "Cache-Control": "no-cache",
            "User-Agent": "pnc-cvestudio-bundle-patrol/1",
        },
    )
    response: Any = None
    try:
        response = _open_with(opener, request, timeout)
        status = getattr(response, "status", None)
        if status is None:
            getcode = getattr(response, "getcode", None)
            status = getcode() if callable(getcode) else None
        if status is None:
            raise ValueError("renderer response did not expose an HTTP status")
        status = int(status)
        headers = getattr(response, "headers", {}) or {}
        if status != 200:
            return {
                "url": url,
                "status_code": status,
                "headers": {
                    "etag": headers.get("ETag"),
                    "last_modified": headers.get("Last-Modified"),
                },
                "body": b"",
                "error_code": "cvestudio_renderer_http_status",
                "error_detail": f"renderer returned HTTP {status}",
            }
        body = _read_bounded(response, limit=limit)
        return {
            "url": url,
            "status_code": status,
            "headers": {
                "etag": headers.get("ETag"),
                "last_modified": headers.get("Last-Modified"),
            },
            "body": body,
            "error_code": None,
            "error_detail": None,
        }
    except HTTPError as exc:
        return {
            "url": url,
            "status_code": int(exc.code),
            "headers": {},
            "body": b"",
            "error_code": "cvestudio_renderer_http_status",
            "error_detail": f"renderer returned HTTP {exc.code}",
        }
    except PatrolConfigError as exc:
        return {
            "url": url,
            "status_code": None,
            "headers": {},
            "body": b"",
            "error_code": exc.code,
            "error_detail": exc.detail,
        }
    except (OSError, URLError, TimeoutError, ValueError) as exc:
        return {
            "url": url,
            "status_code": None,
            "headers": {},
            "body": b"",
            "error_code": "cvestudio_renderer_unreachable",
            "error_detail": str(exc) or exc.__class__.__name__,
        }
    finally:
        close = getattr(response, "close", None)
        if callable(close):
            close()


def _expected_hash(value: str | None) -> tuple[str | None, str | None, str | None]:
    raw = str(value or os.getenv("PNC_CVESTUDIO_BUNDLE_SHA256") or "").strip()
    if not raw:
        return None, "cvestudio_bundle_hash_unconfigured", (
            "PNC_CVESTUDIO_BUNDLE_SHA256"
        )
    if SHA256_RE.fullmatch(raw) is None:
        return raw, "cvestudio_bundle_expected_hash_invalid", None
    return raw.lower(), None, None


def _base_result(renderer_url: str) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "alert",
        "alert": True,
        "error_code": None,
        "error_detail": None,
        "read_only": True,
        "renderer": {
            "url": renderer_url,
            "status_code": None,
            "liveness": "unavailable",
            "parse_verified": False,
            "note": SPA_LIVENESS_NOTE,
            "private_contract_note": PRIVATE_CONTRACT_NOTE,
            "scripts": [],
        },
        "bundle": {
            "hash_algorithm": "sha256",
            "fingerprint_definition": FINGERPRINT_DEFINITION,
            "expected_sha256": None,
            "observed_sha256": None,
            "hash_status": "unavailable",
            "entries": [],
        },
        "live_configuration_required": [],
        "production_actions": {
            "writes": 0,
            "restarts": 0,
            "external_effects": 0,
        },
    }


def run_patrol(
    *,
    renderer_url: str | None = None,
    expected_bundle_sha256: str | None = None,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
    opener: Any = None,
    insecure_tls: bool = False,
) -> dict[str, Any]:
    """Run the read-only patrol and return a JSON-serializable result."""

    try:
        canonical_url = _renderer_url(
            renderer_url or os.getenv("PNC_CVESTUDIO_RENDERER_URL")
        )
    except PatrolConfigError as exc:
        result = _base_result(str(renderer_url or ""))
        result["error_code"] = exc.code
        result["error_detail"] = exc.detail
        return result

    result = _base_result(canonical_url)
    expected, expected_error, required = _expected_hash(expected_bundle_sha256)
    result["bundle"]["expected_sha256"] = expected
    if required:
        result["live_configuration_required"].append(required)
    if expected_error == "cvestudio_bundle_expected_hash_invalid":
        result["error_code"] = expected_error
        result["error_detail"] = "expected bundle SHA must be 64 hexadecimal characters"
        result["bundle"]["hash_status"] = "invalid_configuration"
        return result

    if timeout <= 0:
        result["error_code"] = "cvestudio_patrol_timeout_invalid"
        result["error_detail"] = "timeout must be positive"
        return result

    opener = opener or _build_default_opener(insecure_tls=insecure_tls)
    root = _fetch(
        canonical_url,
        opener=opener,
        timeout=timeout,
        limit=MAX_ROOT_BYTES,
    )
    result["renderer"]["status_code"] = root["status_code"]
    result["renderer"]["headers"] = root["headers"]
    if root["status_code"] == 200:
        result["renderer"]["liveness"] = "spa_endpoint_only"
    else:
        result["error_code"] = root["error_code"] or "cvestudio_renderer_unreachable"
        result["error_detail"] = root["error_detail"]
        return result

    try:
        script_urls = extract_script_urls(root["body"], canonical_url)
    except PatrolConfigError as exc:
        result["error_code"] = exc.code
        result["error_detail"] = exc.detail
        return result
    if not script_urls:
        result["error_code"] = "cvestudio_bundle_scripts_missing"
        result["error_detail"] = "renderer HTML advertised no JavaScript bundle"
        return result

    entries: list[tuple[str, bytes]] = [("index.html", root["body"])]
    all_assets_ok = True
    for url in script_urls:
        asset = _fetch(
            url,
            opener=opener,
            timeout=timeout,
            limit=MAX_ENTRY_BYTES,
        )
        asset_record = {
            "url": url,
            "label": _asset_label(url),
            "status_code": asset["status_code"],
            "bytes": len(asset["body"]),
            "sha256": _sha256(asset["body"]) if asset["status_code"] == 200 else None,
            "error_code": asset["error_code"],
            "error_detail": asset["error_detail"],
        }
        result["renderer"]["scripts"].append(asset_record)
        if asset["status_code"] != 200:
            all_assets_ok = False
            continue
        entries.append((asset_record["label"], asset["body"]))

    if not all_assets_ok:
        result["error_code"] = "cvestudio_bundle_asset_unavailable"
        result["error_detail"] = "one or more renderer bundle assets were unavailable"
        result["bundle"]["entries"] = [
            {
                "label": label,
                "sha256": _sha256(body),
                "bytes": len(body),
            }
            for label, body in entries
        ]
        return result

    observed = bundle_fingerprint(entries)
    result["bundle"]["observed_sha256"] = observed
    result["bundle"]["entries"] = [
        {
            "label": label,
            "sha256": _sha256(body),
            "bytes": len(body),
        }
        for label, body in sorted(entries)
    ]

    if expected is None:
        result["status"] = "unconfigured"
        result["bundle"]["hash_status"] = "unconfigured"
        result["error_code"] = expected_error
        result["error_detail"] = (
            "expected CVEStudio bundle SHA is not configured; observed hash is "
            "reported for owner binding"
        )
        return result
    if not hmac.compare_digest(observed, expected):
        result["status"] = "alert"
        result["bundle"]["hash_status"] = "changed"
        result["error_code"] = "cvestudio_bundle_hash_changed"
        result["error_detail"] = (
            "observed CVEStudio bundle fingerprint differs from the configured baseline"
        )
        return result

    result["status"] = "ok"
    result["alert"] = False
    result["bundle"]["hash_status"] = "match"
    result["error_code"] = None
    result["error_detail"] = None
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--renderer-url",
        default=None,
        help="CVEStudio origin root (default: PNC_CVESTUDIO_RENDERER_URL or the fixed B13 host)",
    )
    parser.add_argument(
        "--expected-bundle-sha256",
        default=None,
        help="expected aggregate bundle SHA (default: PNC_CVESTUDIO_BUNDLE_SHA256)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_TIMEOUT_SECONDS,
    )
    parser.add_argument(
        "--insecure-tls",
        action="store_true",
        help="use an unverified TLS context for the internal renderer probe",
    )
    # Keep an explicit machine-readable switch for runbooks; JSON is always
    # emitted so a failed patrol remains easy to archive and inspect.
    parser.add_argument("--json", action="store_true", help=argparse.SUPPRESS)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        result = run_patrol(
            renderer_url=args.renderer_url,
            expected_bundle_sha256=args.expected_bundle_sha256,
            timeout=args.timeout,
            insecure_tls=args.insecure_tls,
        )
    except Exception as exc:  # pragma: no cover - final fail-visible guard
        result = _base_result(str(args.renderer_url or DEFAULT_RENDERER_URL))
        result["error_code"] = "cvestudio_patrol_internal_error"
        result["error_detail"] = str(exc) or exc.__class__.__name__
    print(json.dumps(result, ensure_ascii=True, sort_keys=True))
    return 0 if result.get("status") == "ok" else 2


if __name__ == "__main__":
    raise SystemExit(main())
