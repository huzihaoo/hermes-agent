#!/usr/bin/env python3
"""Generate fail-closed Chromium CDP evidence for a sealed RCA HTML report."""

from __future__ import annotations

import argparse
import base64
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import secrets
import shutil
import socket
import stat
import struct
import subprocess
import sys
import tempfile
import time
from typing import Any, Callable, Mapping, Protocol, Sequence
from urllib.parse import urldefrag, urlparse

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from gateway.pnc_rca_delivery_contract import (
    DELIVERY_CONTRACT_SCHEMA_VERSION,
    DELIVERY_MANIFEST_SCHEMA_VERSION,
    MAX_DELIVERY_INDEX_HTML_BYTES,
    DeliveryContractError,
    build_report_artifact_url,
    canonical_artifact_root,
    compute_artifact_set_id,
    validate_report_asset_url,
    validate_report_url,
)


SCHEMA_VERSION = "pnc_rca_html_browser_smoke_v2"
ARTIFACT_POLICY = "passive_static_html_v1"
MAX_INPUT_BYTES = 8 * 1024 * 1024
MAX_CDP_MESSAGE_BYTES = 64 * 1024 * 1024
DEFAULT_TIMEOUT_SECONDS = 30
DEFAULT_CHROME_PATHS = (
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    "/Applications/Chromium.app/Contents/MacOS/Chromium",
)


class BrowserSmokeError(RuntimeError):
    def __init__(self, code: str, detail: str = ""):
        self.code = str(code or "browser_smoke_failed")[:120]
        self.detail = str(detail or self.code)[:1000]
        super().__init__(self.detail)


@dataclass(frozen=True)
class Viewport:
    name: str
    width: int
    height: int
    device_scale_factor: float
    mobile: bool


VIEWPORTS = (
    Viewport("desktop", 1440, 1000, 1.0, False),
    Viewport("mobile", 390, 844, 3.0, True),
)


class BrowserRunner(Protocol):
    def capture(
        self,
        report_url: str,
        viewports: Sequence[Viewport],
        *,
        timeout_seconds: int,
    ) -> Mapping[str, Any]: ...


def _utc_iso(value: datetime | None = None) -> str:
    current = value or datetime.now(timezone.utc)
    if current.tzinfo is None or current.utcoffset() is None:
        raise ValueError("datetime must be timezone-aware")
    return current.astimezone(timezone.utc).isoformat()


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_json(value: Any) -> str:
    return _sha256_bytes(_canonical_json(value).encode("utf-8"))


def _strict_json_file(path: Path, *, artifact: str) -> tuple[dict[str, Any], bytes]:
    try:
        info = os.lstat(path)
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
            raise BrowserSmokeError(f"{artifact}_invalid_file")
        if info.st_size <= 0 or info.st_size > MAX_INPUT_BYTES:
            raise BrowserSmokeError(f"{artifact}_size_invalid")
        raw = path.read_bytes()
    except BrowserSmokeError:
        raise
    except OSError as exc:
        raise BrowserSmokeError(f"{artifact}_unreadable", type(exc).__name__) from exc

    def unique_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"duplicate key: {key}")
            result[key] = value
        return result

    try:
        body = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=unique_pairs,
            parse_constant=lambda value: (_ for _ in ()).throw(
                ValueError(f"invalid JSON constant: {value}")
            ),
        )
    except (UnicodeError, ValueError, json.JSONDecodeError) as exc:
        raise BrowserSmokeError(f"{artifact}_json_invalid", type(exc).__name__) from exc
    if not isinstance(body, dict):
        raise BrowserSmokeError(f"{artifact}_json_invalid")
    return body, raw


def _artifact_relative_path(path_value: Any, *, artifact_root: str) -> str:
    raw = str(path_value or "").strip()
    if not raw or "\x00" in raw or "\\" in raw:
        raise BrowserSmokeError("manifest_artifact_path_invalid", raw)
    if raw.startswith("/"):
        if not raw.startswith(artifact_root):
            raise BrowserSmokeError("manifest_artifact_path_invalid", raw)
        raw = raw[len(artifact_root) :]
    path = PurePosixPath(raw)
    if path.is_absolute() or not path.parts or ".." in path.parts:
        raise BrowserSmokeError("manifest_artifact_path_invalid", raw)
    return path.as_posix()


def _verify_inputs(
    manifest: Mapping[str, Any],
    contract: Mapping[str, Any],
) -> dict[str, Any]:
    if manifest.get("schema_version") != DELIVERY_MANIFEST_SCHEMA_VERSION:
        raise BrowserSmokeError("delivery_manifest_schema_unsupported")
    if manifest.get("sealed") is not True:
        raise BrowserSmokeError("delivery_manifest_not_sealed")
    if manifest.get("deliverable_kind") != "html":
        raise BrowserSmokeError("delivery_kind_unsupported")
    if manifest.get("dependencies_complete") is not True:
        raise BrowserSmokeError("delivery_dependencies_incomplete")
    if contract.get("schema_version") != DELIVERY_CONTRACT_SCHEMA_VERSION:
        raise BrowserSmokeError("delivery_contract_schema_unsupported")

    submission_key = str(manifest.get("submission_key") or "").strip()
    try:
        artifact_root = canonical_artifact_root(submission_key)
        computed_artifact_set_id = compute_artifact_set_id(manifest)
    except DeliveryContractError as exc:
        raise BrowserSmokeError(exc.code, exc.detail) from exc
    artifact_set_id = str(manifest.get("artifact_set_id") or "").strip()
    if artifact_set_id != computed_artifact_set_id:
        raise BrowserSmokeError("artifact_set_id_mismatch")
    try:
        report_url = validate_report_url(
            manifest.get("report_url"),
            submission_key=submission_key,
            artifact_set_id=artifact_set_id,
        )
    except DeliveryContractError as exc:
        raise BrowserSmokeError(exc.code, exc.detail) from exc

    if str(contract.get("task_id") or "").strip() != submission_key:
        raise BrowserSmokeError("delivery_contract_submission_mismatch")
    run_id = str(contract.get("run_id") or "").strip()
    if run_id and run_id != submission_key:
        raise BrowserSmokeError("delivery_contract_submission_mismatch")
    contract_artifacts = contract.get("artifacts")
    if not isinstance(contract_artifacts, Mapping):
        raise BrowserSmokeError("delivery_contract_artifacts_invalid")
    if contract_artifacts.get("artifact_set_id") != artifact_set_id:
        raise BrowserSmokeError("artifact_set_reference_mismatch")
    if contract_artifacts.get("delivery_manifest_vm") != (
        artifact_root + "delivery_manifest.json"
    ):
        raise BrowserSmokeError("delivery_manifest_reference_mismatch")
    report = contract.get("report")
    if (
        contract.get("business_state") != "report_completed"
        or not isinstance(report, Mapping)
        or report.get("is_deliverable") is not True
        or report.get("deliverable_kind") != "html"
    ):
        raise BrowserSmokeError("delivery_contract_not_ready")

    rows = manifest.get("artifacts")
    if not isinstance(rows, list) or not rows:
        raise BrowserSmokeError("delivery_manifest_artifacts_invalid")
    url_by_relative_path: dict[str, str] = {}
    sha_by_url: dict[str, str] = {}
    index_row: Mapping[str, Any] | None = None
    roles: set[str] = set()
    for row in rows:
        if not isinstance(row, Mapping):
            raise BrowserSmokeError("delivery_manifest_artifacts_invalid")
        role = str(row.get("role") or "").strip()
        if not role or role in roles:
            raise BrowserSmokeError("delivery_manifest_artifacts_invalid")
        roles.add(role)
        relative = _artifact_relative_path(row.get("path"), artifact_root=artifact_root)
        if relative in url_by_relative_path:
            raise BrowserSmokeError("delivery_manifest_artifacts_invalid")
        digest = str(row.get("sha256") or "").strip().lower()
        if len(digest) != 64 or any(ch not in "0123456789abcdef" for ch in digest):
            raise BrowserSmokeError("artifact_hash_invalid")
        try:
            url = build_report_artifact_url(report_url, relative)
            validate_report_asset_url(
                url,
                submission_key=submission_key,
                artifact_set_id=artifact_set_id,
            )
        except DeliveryContractError as exc:
            raise BrowserSmokeError(exc.code, exc.detail) from exc
        url_by_relative_path[relative] = url
        sha_by_url[url] = digest
        if role == "index_html":
            index_row = row
            if row.get("required") is not True:
                raise BrowserSmokeError("required_html_artifact_missing")

    if index_row is None:
        raise BrowserSmokeError("required_html_artifact_missing")
    index_relative = _artifact_relative_path(
        index_row.get("path"), artifact_root=artifact_root
    )
    if url_by_relative_path[index_relative] != report_url:
        raise BrowserSmokeError("report_url_identity_mismatch")
    if contract_artifacts.get("index_html_vm") != artifact_root + index_relative:
        raise BrowserSmokeError("delivery_artifact_reference_mismatch")
    return {
        "submission_key": submission_key,
        "artifact_set_id": artifact_set_id,
        "report_url": report_url,
        "index_html_sha256": str(index_row["sha256"]).lower(),
        "allowed_urls": tuple(sorted(sha_by_url)),
        "sha_by_url": sha_by_url,
    }


_DOM_AUDIT_EXPRESSION = r"""
(() => {
  const inertScriptTypes = new Set(['application/json', 'application/ld+json']);
  const scripts = [...document.querySelectorAll('script')];
  const executableScriptCount = scripts.filter((node) => {
    const type = (node.getAttribute('type') || '').trim().toLowerCase();
    return !inertScriptTypes.has(type);
  }).length;
  let inlineEventHandlerCount = 0;
  for (const node of document.querySelectorAll('*')) {
    for (const attr of node.attributes || []) {
      if (attr.name.toLowerCase().startsWith('on')) inlineEventHandlerCount++;
    }
  }
  const externalActiveDocumentCount = document.querySelectorAll(
    'iframe,frame,object,embed,base,meta[http-equiv="refresh"]'
  ).length;
  const visible = [...document.querySelectorAll('body *')].filter((node) => {
    const style = getComputedStyle(node);
    if (style.display === 'none' || style.visibility === 'hidden' ||
        Number(style.opacity || 1) === 0) return false;
    return node.getClientRects().length > 0;
  });
  const visibleTextLength = ((document.body && document.body.innerText) || '').trim().length;
  const visibleMediaCount = visible.filter((node) =>
    ['IMG', 'VIDEO', 'CANVAS', 'SVG'].includes(node.tagName)
  ).length;
  const root = document.documentElement;
  return {
    executable_script_count: executableScriptCount,
    inline_event_handler_count: inlineEventHandlerCount,
    external_active_document_count: externalActiveDocumentCount,
    visible_element_count: visible.length,
    visible_text_length: visibleTextLength,
    visible_media_count: visibleMediaCount,
    document_width: root ? root.scrollWidth : 0,
    document_height: root ? root.scrollHeight : 0,
    title: document.title || ''
  };
})()
""".strip()


class _WebSocketCdp:
    """Small RFC6455 client sufficient for a local Chrome DevTools socket."""

    def __init__(self, websocket_url: str, *, timeout_seconds: int):
        parsed = urlparse(websocket_url)
        if parsed.scheme != "ws" or parsed.hostname not in {"127.0.0.1", "localhost"}:
            raise BrowserSmokeError("cdp_websocket_url_invalid")
        port = parsed.port or 80
        self.sock = socket.create_connection(
            (parsed.hostname, port), timeout=timeout_seconds
        )
        self.sock.settimeout(0.5)
        self._buffer = bytearray()
        self._next_id = 1
        key = base64.b64encode(secrets.token_bytes(16)).decode("ascii")
        path = parsed.path or "/"
        if parsed.query:
            path += "?" + parsed.query
        request = (
            f"GET {path} HTTP/1.1\r\n"
            f"Host: {parsed.hostname}:{port}\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {key}\r\n"
            "Sec-WebSocket-Version: 13\r\n\r\n"
        ).encode("ascii")
        self.sock.sendall(request)
        response = bytearray()
        while b"\r\n\r\n" not in response:
            chunk = self.sock.recv(4096)
            if not chunk or len(response) > 64 * 1024:
                raise BrowserSmokeError("cdp_websocket_handshake_failed")
            response.extend(chunk)
        header, remainder = bytes(response).split(b"\r\n\r\n", 1)
        if not header.startswith(b"HTTP/1.1 101"):
            raise BrowserSmokeError("cdp_websocket_handshake_failed")
        response_headers = {}
        for line in header.split(b"\r\n")[1:]:
            if b":" not in line:
                continue
            name, value = line.split(b":", 1)
            response_headers[name.strip().lower()] = value.strip()
        expected_accept = base64.b64encode(
            hashlib.sha1(
                (key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode("ascii"),
                usedforsecurity=False,
            ).digest()
        )
        if response_headers.get(b"sec-websocket-accept") != expected_accept:
            raise BrowserSmokeError("cdp_websocket_handshake_failed")
        self._buffer.extend(remainder)

    def close(self) -> None:
        try:
            self.sock.close()
        except OSError:
            pass

    def _read_exact(self, size: int) -> bytes:
        while len(self._buffer) < size:
            chunk = self.sock.recv(max(4096, size - len(self._buffer)))
            if not chunk:
                raise BrowserSmokeError("cdp_websocket_closed")
            self._buffer.extend(chunk)
            if len(self._buffer) > MAX_CDP_MESSAGE_BYTES:
                raise BrowserSmokeError("cdp_message_too_large")
        result = bytes(self._buffer[:size])
        del self._buffer[:size]
        return result

    def _send_frame(self, payload: bytes, *, opcode: int = 1) -> None:
        mask = secrets.token_bytes(4)
        length = len(payload)
        header = bytearray([0x80 | opcode])
        if length < 126:
            header.append(0x80 | length)
        elif length <= 0xFFFF:
            header.append(0x80 | 126)
            header.extend(struct.pack("!H", length))
        else:
            header.append(0x80 | 127)
            header.extend(struct.pack("!Q", length))
        masked = bytes(value ^ mask[index % 4] for index, value in enumerate(payload))
        self.sock.sendall(bytes(header) + mask + masked)

    def _receive_frame(self) -> tuple[bool, int, bytes]:
        first, second = self._read_exact(2)
        finished = bool(first & 0x80)
        opcode = first & 0x0F
        length = second & 0x7F
        if second & 0x80:
            raise BrowserSmokeError("cdp_websocket_protocol_error")
        if length == 126:
            length = struct.unpack("!H", self._read_exact(2))[0]
        elif length == 127:
            length = struct.unpack("!Q", self._read_exact(8))[0]
        if length > MAX_CDP_MESSAGE_BYTES:
            raise BrowserSmokeError("cdp_message_too_large")
        payload = self._read_exact(length)
        if opcode == 9:
            self._send_frame(payload, opcode=10)
            return self._receive_frame()
        if opcode == 8:
            raise BrowserSmokeError("cdp_websocket_closed")
        return finished, opcode, payload

    def receive(self) -> dict[str, Any]:
        fragments = bytearray()
        message_opcode = 0
        while True:
            finished, opcode, payload = self._receive_frame()
            if opcode in {1, 2}:
                if message_opcode:
                    raise BrowserSmokeError("cdp_websocket_protocol_error")
                message_opcode = opcode
                fragments.extend(payload)
            elif opcode == 0 and message_opcode:
                fragments.extend(payload)
            else:
                raise BrowserSmokeError("cdp_websocket_protocol_error")
            if finished:
                break
        try:
            body = json.loads(bytes(fragments).decode("utf-8"))
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise BrowserSmokeError("cdp_message_invalid") from exc
        if not isinstance(body, dict):
            raise BrowserSmokeError("cdp_message_invalid")
        return body

    def call(
        self,
        method: str,
        params: Mapping[str, Any] | None = None,
        *,
        session_id: str | None = None,
        events: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        call_id = self._next_id
        self._next_id += 1
        message: dict[str, Any] = {"id": call_id, "method": method}
        if params:
            message["params"] = dict(params)
        if session_id:
            message["sessionId"] = session_id
        self._send_frame(_canonical_json(message).encode("utf-8"))
        while True:
            response = self.receive()
            if response.get("id") == call_id:
                if "error" in response:
                    raise BrowserSmokeError(
                        "cdp_command_failed",
                        f"{method}: {response['error']}",
                    )
                result = response.get("result")
                return dict(result) if isinstance(result, Mapping) else {}
            if events is not None:
                events.append(response)


def _event_method(event: Mapping[str, Any]) -> str:
    return str(event.get("method") or "")


def _capture_cdp_viewport(
    cdp: Any,
    *,
    session_id: str,
    report_url: str,
    viewport: Viewport,
    timeout_seconds: int,
) -> dict[str, Any]:
    events: list[dict[str, Any]] = []
    for method in (
        "Page.enable",
        "Network.enable",
        "Runtime.enable",
        "Log.enable",
        "Debugger.enable",
    ):
        cdp.call(method, session_id=session_id, events=events)
    cdp.call(
        "Network.setCacheDisabled",
        {"cacheDisabled": True},
        session_id=session_id,
        events=events,
    )
    cdp.call(
        "Emulation.setDeviceMetricsOverride",
        {
            "width": viewport.width,
            "height": viewport.height,
            "deviceScaleFactor": viewport.device_scale_factor,
            "mobile": viewport.mobile,
        },
        session_id=session_id,
        events=events,
    )
    cdp.call(
        "Page.navigate",
        {"url": report_url},
        session_id=session_id,
        events=events,
    )
    deadline = time.monotonic() + timeout_seconds
    loaded = any(_event_method(event) == "Page.loadEventFired" for event in events)
    while not loaded and time.monotonic() < deadline:
        try:
            event = cdp.receive()
        except (TimeoutError, socket.timeout):
            continue
        events.append(event)
        loaded = _event_method(event) == "Page.loadEventFired"
    if not loaded:
        raise BrowserSmokeError("browser_page_load_timeout", viewport.name)
    settle_deadline = min(deadline, time.monotonic() + 0.75)
    while time.monotonic() < settle_deadline:
        try:
            events.append(cdp.receive())
        except (TimeoutError, socket.timeout):
            continue
    main_request_ids = {
        str(event["params"].get("requestId") or "")
        for event in events
        if _event_method(event) == "Network.requestWillBeSent"
        and isinstance(event.get("params"), Mapping)
        and str(event["params"].get("type") or "") == "Document"
        and isinstance(event["params"].get("request"), Mapping)
        and urldefrag(str(event["params"]["request"].get("url") or ""))[0] == report_url
    }
    main_request_ids.discard("")
    if len(main_request_ids) != 1:
        raise BrowserSmokeError("browser_main_document_request_invalid", viewport.name)
    response_body = cdp.call(
        "Network.getResponseBody",
        {"requestId": next(iter(main_request_ids))},
        session_id=session_id,
        events=events,
    )
    body_value = response_body.get("body")
    base64_encoded = response_body.get("base64Encoded")
    if not isinstance(body_value, str) or not isinstance(base64_encoded, bool):
        raise BrowserSmokeError("browser_main_document_body_invalid", viewport.name)
    try:
        index_html_bytes = (
            base64.b64decode(body_value, validate=True)
            if base64_encoded
            else body_value.encode("utf-8")
        )
    except (UnicodeError, ValueError) as exc:
        raise BrowserSmokeError(
            "browser_main_document_body_invalid", viewport.name
        ) from exc
    if not index_html_bytes or len(index_html_bytes) > MAX_DELIVERY_INDEX_HTML_BYTES:
        raise BrowserSmokeError("browser_main_document_body_invalid", viewport.name)
    index_html_sha256 = _sha256_bytes(index_html_bytes)
    page_events = tuple(events)
    evaluation = cdp.call(
        "Runtime.evaluate",
        {
            "expression": _DOM_AUDIT_EXPRESSION,
            "returnByValue": True,
            "awaitPromise": True,
        },
        session_id=session_id,
        events=events,
    )
    result = evaluation.get("result")
    dom = result.get("value") if isinstance(result, Mapping) else None
    if not isinstance(dom, Mapping):
        raise BrowserSmokeError("browser_dom_audit_invalid", viewport.name)

    request_urls: list[str] = []
    console_error_count = 0
    runtime_exception_count = 0
    log_error_count = 0
    network_error_count = 0
    active_document_request_count = 0
    executed_script_count = 0
    for event in events:
        method = _event_method(event)
        params = event.get("params")
        if not isinstance(params, Mapping):
            continue
        if method == "Network.requestWillBeSent":
            request = params.get("request")
            if isinstance(request, Mapping):
                request_url = str(request.get("url") or "")
                request_urls.append(request_url)
                if (
                    str(params.get("type") or "") == "Document"
                    and urldefrag(request_url)[0] != report_url
                ):
                    active_document_request_count += 1
        elif method == "Network.loadingFailed":
            network_error_count += 1
        elif method == "Network.responseReceived":
            response = params.get("response")
            if isinstance(response, Mapping):
                try:
                    if int(response.get("status") or 0) >= 400:
                        network_error_count += 1
                except (TypeError, ValueError):
                    network_error_count += 1
        elif method == "Runtime.exceptionThrown":
            runtime_exception_count += 1
        elif method == "Runtime.consoleAPICalled" and str(params.get("type") or "") in {
            "error",
            "assert",
        }:
            console_error_count += 1
        elif method == "Log.entryAdded":
            entry = params.get("entry")
            if isinstance(entry, Mapping) and str(entry.get("level") or "") == "error":
                log_error_count += 1
    for event in page_events:
        if _event_method(event) != "Debugger.scriptParsed":
            continue
        params = event.get("params")
        if not isinstance(params, Mapping):
            continue
        url = str(params.get("url") or "")
        if url.startswith(("chrome-extension://", "devtools://")):
            continue
        executed_script_count += 1
    return {
        **asdict(viewport),
        "request_urls": request_urls,
        "console_error_count": console_error_count,
        "runtime_exception_count": runtime_exception_count,
        "log_error_count": log_error_count,
        "network_error_count": network_error_count,
        "executed_script_count": executed_script_count,
        "active_document_request_count": active_document_request_count,
        "index_html_sha256": index_html_sha256,
        "index_html_size_bytes": len(index_html_bytes),
        "dom": dict(dom),
    }


def _find_chrome(explicit: str = "") -> str:
    if explicit:
        path = Path(explicit).expanduser()
        if path.is_file() and os.access(path, os.X_OK):
            return str(path.resolve())
        raise BrowserSmokeError("chromium_executable_missing", explicit)
    for path in DEFAULT_CHROME_PATHS:
        candidate = Path(path)
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return str(candidate.resolve())
    for command in (
        "google-chrome",
        "google-chrome-stable",
        "chromium",
        "chromium-browser",
    ):
        resolved = shutil.which(command)
        if resolved:
            return str(Path(resolved).resolve())
    raise BrowserSmokeError("chromium_executable_missing")


class ChromiumCdpRunner:
    def __init__(
        self,
        *,
        chrome_path: str = "",
        process_factory: Callable[..., Any] = subprocess.Popen,
        cdp_factory: Callable[..., Any] = _WebSocketCdp,
    ):
        self.chrome_path = _find_chrome(chrome_path)
        self.process_factory = process_factory
        self.cdp_factory = cdp_factory

    def capture(
        self,
        report_url: str,
        viewports: Sequence[Viewport],
        *,
        timeout_seconds: int,
    ) -> Mapping[str, Any]:
        profile = Path(tempfile.mkdtemp(prefix="pnc-rca-browser-smoke-"))
        process = None
        cdp = None
        try:
            process = self.process_factory(
                [
                    self.chrome_path,
                    "--headless=new",
                    "--remote-debugging-port=0",
                    f"--user-data-dir={profile}",
                    "--no-first-run",
                    "--no-default-browser-check",
                    "--disable-extensions",
                    "--disable-component-update",
                    "--disable-sync",
                    "about:blank",
                ],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            active_port = profile / "DevToolsActivePort"
            deadline = time.monotonic() + timeout_seconds
            lines: list[str] = []
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    raise BrowserSmokeError("chromium_exited_early")
                try:
                    lines = active_port.read_text(encoding="utf-8").splitlines()
                except OSError:
                    time.sleep(0.05)
                    continue
                if len(lines) >= 2:
                    break
            if len(lines) < 2 or not lines[0].isdigit() or not lines[1].startswith("/"):
                raise BrowserSmokeError("chromium_cdp_endpoint_unavailable")
            websocket_url = f"ws://127.0.0.1:{int(lines[0])}{lines[1]}"
            cdp = self.cdp_factory(websocket_url, timeout_seconds=timeout_seconds)
            version = cdp.call("Browser.getVersion")
            captures: list[dict[str, Any]] = []
            for viewport in viewports:
                context = cdp.call("Target.createBrowserContext")
                context_id = str(context.get("browserContextId") or "")
                if not context_id:
                    raise BrowserSmokeError("cdp_browser_context_failed")
                target_id = ""
                try:
                    target = cdp.call(
                        "Target.createTarget",
                        {"url": "about:blank", "browserContextId": context_id},
                    )
                    target_id = str(target.get("targetId") or "")
                    attached = cdp.call(
                        "Target.attachToTarget",
                        {"targetId": target_id, "flatten": True},
                    )
                    session_id = str(attached.get("sessionId") or "")
                    if not target_id or not session_id:
                        raise BrowserSmokeError("cdp_target_attach_failed")
                    captures.append(
                        _capture_cdp_viewport(
                            cdp,
                            session_id=session_id,
                            report_url=report_url,
                            viewport=viewport,
                            timeout_seconds=timeout_seconds,
                        )
                    )
                finally:
                    if target_id:
                        cdp.call("Target.closeTarget", {"targetId": target_id})
                    cdp.call(
                        "Target.disposeBrowserContext",
                        {"browserContextId": context_id},
                    )
            return {
                "engine": "chromium",
                "browser_executable": self.chrome_path,
                "browser_product": str(version.get("product") or ""),
                "browser_protocol_version": str(version.get("protocolVersion") or ""),
                "viewports": captures,
            }
        finally:
            if cdp is not None:
                cdp.close()
            if process is not None and process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=3)
            shutil.rmtree(profile, ignore_errors=True)


def _exact_nonnegative_int(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise BrowserSmokeError("browser_trace_invalid", field)
    return value


def generate_browser_smoke(
    *,
    manifest_path: str | Path,
    contract_path: str | Path,
    runner: BrowserRunner | None = None,
    now: datetime | None = None,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    if timeout_seconds < 1 or timeout_seconds > 120:
        raise BrowserSmokeError("browser_timeout_invalid")
    manifest, manifest_raw = _strict_json_file(
        Path(manifest_path).expanduser(), artifact="delivery_manifest"
    )
    contract, contract_raw = _strict_json_file(
        Path(contract_path).expanduser(), artifact="delivery_contract"
    )
    verified = _verify_inputs(manifest, contract)
    browser = runner or ChromiumCdpRunner()
    try:
        raw_capture = browser.capture(
            verified["report_url"],
            VIEWPORTS,
            timeout_seconds=timeout_seconds,
        )
    except BrowserSmokeError:
        raise
    except Exception as exc:
        raise BrowserSmokeError("chromium_capture_failed", type(exc).__name__) from exc
    if not isinstance(raw_capture, Mapping) or raw_capture.get("engine") != "chromium":
        raise BrowserSmokeError("browser_trace_invalid", "engine")
    raw_views = raw_capture.get("viewports")
    if not isinstance(raw_views, list) or len(raw_views) != len(VIEWPORTS):
        raise BrowserSmokeError("browser_trace_invalid", "viewports")

    allowed_urls = set(verified["allowed_urls"])
    views: dict[str, dict[str, Any]] = {}
    all_request_urls: list[str] = []
    totals = {
        "request_count": 0,
        "unmanifested_request_count": 0,
        "executable_script_count": 0,
        "inline_event_handler_count": 0,
        "external_active_document_count": 0,
        "console_error_count": 0,
        "runtime_exception_count": 0,
        "log_error_count": 0,
        "network_error_count": 0,
    }
    expected_viewports = {item.name: item for item in VIEWPORTS}
    observed_index_hashes: list[str] = []
    for raw_view in raw_views:
        if not isinstance(raw_view, Mapping):
            raise BrowserSmokeError("browser_trace_invalid", "viewport")
        name = str(raw_view.get("name") or "")
        expected = expected_viewports.get(name)
        if expected is None or name in views:
            raise BrowserSmokeError("browser_trace_invalid", "viewport name")
        for field in ("width", "height", "mobile", "device_scale_factor"):
            if raw_view.get(field) != getattr(expected, field):
                raise BrowserSmokeError("browser_trace_invalid", f"{name}.{field}")
        raw_urls = raw_view.get("request_urls")
        if not isinstance(raw_urls, list) or not all(
            isinstance(value, str) for value in raw_urls
        ):
            raise BrowserSmokeError("browser_trace_invalid", f"{name}.request_urls")
        http_urls: list[str] = []
        for value in raw_urls:
            url = urldefrag(value)[0]
            if urlparse(url).scheme.lower() not in {"http", "https"}:
                continue
            http_urls.append(url)
        unmanifested = sorted(set(http_urls) - allowed_urls)
        dom = raw_view.get("dom")
        if not isinstance(dom, Mapping):
            raise BrowserSmokeError("browser_trace_invalid", f"{name}.dom")
        visible_elements = _exact_nonnegative_int(
            dom.get("visible_element_count"), f"{name}.visible_element_count"
        )
        visible_text = _exact_nonnegative_int(
            dom.get("visible_text_length"), f"{name}.visible_text_length"
        )
        visible_media = _exact_nonnegative_int(
            dom.get("visible_media_count"), f"{name}.visible_media_count"
        )
        document_width = _exact_nonnegative_int(
            dom.get("document_width"), f"{name}.document_width"
        )
        document_height = _exact_nonnegative_int(
            dom.get("document_height"), f"{name}.document_height"
        )
        nonblank = bool(
            visible_elements > 0
            and (visible_text > 0 or visible_media > 0)
            and document_width > 0
            and document_height > 0
        )
        count_fields = (
            "console_error_count",
            "runtime_exception_count",
            "log_error_count",
            "network_error_count",
            "executed_script_count",
            "active_document_request_count",
        )
        trace_counts = {
            field: _exact_nonnegative_int(raw_view.get(field), f"{name}.{field}")
            for field in count_fields
        }
        observed_index_sha256 = str(raw_view.get("index_html_sha256") or "")
        if len(observed_index_sha256) != 64 or any(
            character not in "0123456789abcdef" for character in observed_index_sha256
        ):
            raise BrowserSmokeError(
                "browser_trace_invalid", f"{name}.index_html_sha256"
            )
        observed_index_size = _exact_nonnegative_int(
            raw_view.get("index_html_size_bytes"),
            f"{name}.index_html_size_bytes",
        )
        if (
            observed_index_size < 1
            or observed_index_size > MAX_DELIVERY_INDEX_HTML_BYTES
        ):
            raise BrowserSmokeError(
                "browser_trace_invalid", f"{name}.index_html_size_bytes"
            )
        dom_counts = {
            field: _exact_nonnegative_int(dom.get(field), f"{name}.{field}")
            for field in (
                "executable_script_count",
                "inline_event_handler_count",
                "external_active_document_count",
            )
        }
        dom_counts["executable_script_count"] = max(
            dom_counts["executable_script_count"],
            trace_counts.pop("executed_script_count"),
        )
        dom_counts["external_active_document_count"] = max(
            dom_counts["external_active_document_count"],
            trace_counts.pop("active_document_request_count"),
        )
        requested = sorted(set(http_urls))
        view = {
            **asdict(expected),
            "nonblank": nonblank,
            "request_count": len(http_urls),
            "requested_urls": requested,
            "request_list_sha256": _sha256_json(requested),
            "unmanifested_urls": unmanifested,
            "unmanifested_request_count": len(unmanifested),
            **trace_counts,
            **dom_counts,
            "visible_element_count": visible_elements,
            "visible_text_length": visible_text,
            "visible_media_count": visible_media,
            "document_width": document_width,
            "document_height": document_height,
            "title_sha256": _sha256_bytes(str(dom.get("title") or "").encode("utf-8")),
            "index_html_sha256": observed_index_sha256,
            "index_html_size_bytes": observed_index_size,
        }
        views[name] = view
        all_request_urls.extend(http_urls)
        totals["request_count"] += len(http_urls)
        totals["unmanifested_request_count"] += len(unmanifested)
        for field, value in {**trace_counts, **dom_counts}.items():
            totals[field] += value
        observed_index_hashes.append(observed_index_sha256)

    if set(views) != set(expected_viewports):
        raise BrowserSmokeError("browser_trace_invalid", "viewport coverage")
    requested_urls = sorted(set(all_request_urls))
    blockers: list[str] = []
    if not views["desktop"]["nonblank"]:
        blockers.append("desktop_dom_blank")
    if not views["mobile"]["nonblank"]:
        blockers.append("mobile_dom_blank")
    for field in (
        "unmanifested_request_count",
        "executable_script_count",
        "inline_event_handler_count",
        "external_active_document_count",
        "console_error_count",
        "runtime_exception_count",
        "log_error_count",
        "network_error_count",
    ):
        if totals[field] != 0:
            blockers.append(field)
    if verified["report_url"] not in requested_urls:
        blockers.append("report_index_not_requested")
    unique_index_hashes = set(observed_index_hashes)
    if (
        len(unique_index_hashes) != 1
        or next(iter(unique_index_hashes), "") != verified["index_html_sha256"]
    ):
        blockers.append("index_html_hash_mismatch")
    observed_index_sha256 = (
        next(iter(unique_index_hashes)) if len(unique_index_hashes) == 1 else ""
    )
    evidence = {
        "schema_version": SCHEMA_VERSION,
        "ok": not blockers,
        "machine_generated": True,
        "source": "chromium_cdp_network_runtime_log",
        "engine": "chromium",
        "artifact_policy": ARTIFACT_POLICY,
        "observed_at": _utc_iso(now),
        "artifact_set_id": verified["artifact_set_id"],
        "report_url": verified["report_url"],
        "index_html_sha256": observed_index_sha256,
        "manifest_sha256": _sha256_bytes(manifest_raw),
        "delivery_contract_sha256": _sha256_bytes(contract_raw),
        "manifest_url_count": len(allowed_urls),
        "manifest_url_set_sha256": _sha256_json(sorted(allowed_urls)),
        "requested_urls": requested_urls,
        "request_list_sha256": _sha256_json(requested_urls),
        "network_closure": "manifest_allowlist",
        "desktop_nonblank": views["desktop"]["nonblank"],
        "mobile_nonblank": views["mobile"]["nonblank"],
        **totals,
        "browser": {
            "executable": str(raw_capture.get("browser_executable") or ""),
            "product": str(raw_capture.get("browser_product") or ""),
            "protocol_version": str(raw_capture.get("browser_protocol_version") or ""),
        },
        "viewports": views,
        "blockers": sorted(set(blockers)),
    }
    evidence["evidence_sha256"] = _sha256_json(evidence)
    return evidence


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--contract", required=True, type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--chrome", default="")
    parser.add_argument("--timeout-seconds", type=int, default=DEFAULT_TIMEOUT_SECONDS)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        evidence = generate_browser_smoke(
            manifest_path=args.manifest,
            contract_path=args.contract,
            runner=ChromiumCdpRunner(chrome_path=args.chrome),
            timeout_seconds=args.timeout_seconds,
        )
    except BrowserSmokeError as exc:
        evidence = {
            "schema_version": SCHEMA_VERSION,
            "ok": False,
            "machine_generated": True,
            "source": "chromium_cdp_network_runtime_log",
            "engine": "chromium",
            "observed_at": _utc_iso(),
            "error": exc.code,
            "error_detail": exc.detail,
        }
    encoded = json.dumps(evidence, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if args.output:
        output = args.output.expanduser()
        output.parent.mkdir(parents=True, exist_ok=True)
        temporary = output.with_name(f".{output.name}.{os.getpid()}.tmp")
        temporary.write_text(encoded, encoding="utf-8")
        os.chmod(temporary, 0o600)
        os.replace(temporary, output)
    print(encoded, end="")
    return 0 if evidence.get("ok") is True else 2


if __name__ == "__main__":
    raise SystemExit(main())
