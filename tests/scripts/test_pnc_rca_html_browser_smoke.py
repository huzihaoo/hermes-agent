from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path

import pytest

from gateway.pnc_rca_delivery_contract import build_report_artifact_url
from scripts import pnc_rca_html_browser_smoke as smoke_module
from scripts.pnc_rca_html_browser_smoke import (
    BrowserSmokeError,
    ChromiumCdpRunner,
    SCHEMA_VERSION,
    VIEWPORTS,
    _capture_cdp_viewport,
    generate_browser_smoke,
)
from tests.gateway.test_pnc_rca_delivery_contract import _bundle


NOW = datetime(2026, 7, 10, 10, 0, tzinfo=timezone.utc)


def _write_inputs(tmp_path: Path):
    _admission, contract, manifest, _observed, _dependencies = _bundle()
    manifest_path = tmp_path / "delivery_manifest.json"
    contract_path = tmp_path / "delivery_contract.json"
    manifest_raw = json.dumps(manifest, sort_keys=True).encode("utf-8")
    contract_raw = json.dumps(contract, sort_keys=True).encode("utf-8")
    manifest_path.write_bytes(manifest_raw)
    contract_path.write_bytes(contract_raw)
    return manifest_path, contract_path, manifest, contract, manifest_raw, contract_raw


def _dom(**updates):
    body = {
        "executable_script_count": 0,
        "inline_event_handler_count": 0,
        "external_active_document_count": 0,
        "visible_element_count": 5,
        "visible_text_length": 120,
        "visible_media_count": 1,
        "document_width": 1200,
        "document_height": 900,
        "title": "RCA report",
    }
    body.update(updates)
    return body


class FakeRunner:
    def __init__(
        self,
        request_urls,
        *,
        index_html_sha256="0" * 64,
        index_html_size_bytes=1,
        dom_updates=None,
        trace_updates=None,
    ):
        self.request_urls = list(request_urls)
        self.index_html_sha256 = index_html_sha256
        self.index_html_size_bytes = index_html_size_bytes
        self.dom_updates = dict(dom_updates or {})
        self.trace_updates = dict(trace_updates or {})
        self.calls = []

    def capture(self, report_url, viewports, *, timeout_seconds):
        self.calls.append((report_url, tuple(viewports), timeout_seconds))
        rows = []
        for viewport in viewports:
            row = {
                **asdict(viewport),
                "request_urls": list(self.request_urls),
                "console_error_count": 0,
                "runtime_exception_count": 0,
                "log_error_count": 0,
                "network_error_count": 0,
                "executed_script_count": 0,
                "active_document_request_count": 0,
                "index_html_sha256": self.index_html_sha256,
                "index_html_size_bytes": self.index_html_size_bytes,
                "dom": _dom(**self.dom_updates),
            }
            row.update(self.trace_updates)
            rows.append(row)
        return {
            "engine": "chromium",
            "browser_executable": "/fake/chrome",
            "browser_product": "Chrome/140.0.0.0",
            "browser_protocol_version": "1.3",
            "viewports": rows,
        }


def _manifest_request_urls(manifest):
    report_url = manifest["report_url"]
    return [
        build_report_artifact_url(report_url, row["path"])
        for row in manifest["artifacts"]
        if row["role"] in {"index_html", "video"}
    ]


def _fake_runner(manifest, *, request_urls=None, **kwargs):
    index = next(row for row in manifest["artifacts"] if row["role"] == "index_html")
    return FakeRunner(
        request_urls if request_urls is not None else _manifest_request_urls(manifest),
        index_html_sha256=index["sha256"],
        index_html_size_bytes=index["size"],
        **kwargs,
    )


def test_valid_sealed_report_produces_v2_cdp_evidence(tmp_path):
    (
        manifest_path,
        contract_path,
        manifest,
        _contract,
        manifest_raw,
        contract_raw,
    ) = _write_inputs(tmp_path)
    runner = _fake_runner(manifest)

    evidence = generate_browser_smoke(
        manifest_path=manifest_path,
        contract_path=contract_path,
        runner=runner,
        now=NOW,
        timeout_seconds=17,
    )

    assert evidence["schema_version"] == SCHEMA_VERSION
    assert evidence["ok"] is True
    assert evidence["artifact_policy"] == "passive_static_html_v1"
    assert evidence["observed_at"] == NOW.isoformat()
    assert evidence["artifact_set_id"] == manifest["artifact_set_id"]
    assert evidence["report_url"] == manifest["report_url"]
    assert evidence["manifest_sha256"] == hashlib.sha256(manifest_raw).hexdigest()
    assert (
        evidence["delivery_contract_sha256"] == hashlib.sha256(contract_raw).hexdigest()
    )
    assert evidence["desktop_nonblank"] is True
    assert evidence["mobile_nonblank"] is True
    assert evidence["unmanifested_request_count"] == 0
    assert evidence["executable_script_count"] == 0
    assert evidence["inline_event_handler_count"] == 0
    assert evidence["external_active_document_count"] == 0
    assert evidence["console_error_count"] == 0
    assert len(evidence["request_list_sha256"]) == 64
    assert len(evidence["manifest_url_set_sha256"]) == 64
    assert set(evidence["viewports"]) == {"desktop", "mobile"}
    assert runner.calls == [(manifest["report_url"], VIEWPORTS, 17)]


def test_observed_index_body_hash_must_match_sealed_manifest(tmp_path):
    manifest_path, contract_path, manifest, *_rest = _write_inputs(tmp_path)
    observed_hash = "f" * 64

    evidence = generate_browser_smoke(
        manifest_path=manifest_path,
        contract_path=contract_path,
        runner=_fake_runner(
            manifest,
            trace_updates={"index_html_sha256": observed_hash},
        ),
        now=NOW,
    )

    assert evidence["ok"] is False
    assert evidence["index_html_sha256"] == observed_hash
    assert "index_html_hash_mismatch" in evidence["blockers"]
    assert all(
        view["index_html_sha256"] == observed_hash
        for view in evidence["viewports"].values()
    )


def test_unmanifested_http_request_fails_network_closure(tmp_path):
    manifest_path, contract_path, manifest, *_rest = _write_inputs(tmp_path)
    runner = _fake_runner(
        manifest,
        request_urls=[
            *_manifest_request_urls(manifest),
            "https://example.invalid/tracker.js",
        ],
    )

    evidence = generate_browser_smoke(
        manifest_path=manifest_path,
        contract_path=contract_path,
        runner=runner,
        now=NOW,
    )

    assert evidence["ok"] is False
    assert evidence["unmanifested_request_count"] == 2
    assert "unmanifested_request_count" in evidence["blockers"]
    for view in evidence["viewports"].values():
        assert view["unmanifested_urls"] == ["https://example.invalid/tracker.js"]


@pytest.mark.parametrize(
    ("dom_updates", "trace_updates", "blocker"),
    [
        ({"executable_script_count": 1}, {}, "executable_script_count"),
        ({}, {"executed_script_count": 1}, "executable_script_count"),
        ({"inline_event_handler_count": 1}, {}, "inline_event_handler_count"),
        (
            {"external_active_document_count": 1},
            {},
            "external_active_document_count",
        ),
        (
            {},
            {"active_document_request_count": 1},
            "external_active_document_count",
        ),
        ({}, {"console_error_count": 1}, "console_error_count"),
    ],
)
def test_active_content_and_console_errors_fail_closed(
    tmp_path, dom_updates, trace_updates, blocker
):
    manifest_path, contract_path, manifest, *_rest = _write_inputs(tmp_path)
    evidence = generate_browser_smoke(
        manifest_path=manifest_path,
        contract_path=contract_path,
        runner=_fake_runner(
            manifest,
            dom_updates=dom_updates,
            trace_updates=trace_updates,
        ),
        now=NOW,
    )

    assert evidence["ok"] is False
    assert evidence[blocker] == 2
    assert blocker in evidence["blockers"]


@pytest.mark.parametrize(
    ("mutation", "code"),
    [
        (
            lambda contract, manifest: manifest.update(sealed=False),
            "delivery_manifest_not_sealed",
        ),
        (
            lambda contract, manifest: contract["artifacts"].update(
                artifact_set_id="g1q3-rca-artifact-v1-" + "f" * 64
            ),
            "artifact_set_reference_mismatch",
        ),
        (
            lambda contract, manifest: manifest.update(
                report_url="https://example.invalid/report/index.html"
            ),
            "report_url_invalid",
        ),
    ],
)
def test_sealed_identity_and_canonical_url_are_verified_before_browser(
    tmp_path, mutation, code
):
    manifest_path, contract_path, manifest, contract, *_rest = _write_inputs(tmp_path)
    mutation(contract, manifest)
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    contract_path.write_text(json.dumps(contract), encoding="utf-8")
    runner = FakeRunner([])

    with pytest.raises(BrowserSmokeError) as error:
        generate_browser_smoke(
            manifest_path=manifest_path,
            contract_path=contract_path,
            runner=runner,
            now=NOW,
        )

    assert error.value.code == code
    assert runner.calls == []


class FakeCdpSession:
    def __init__(
        self,
        request_url,
        *,
        response_bytes=b"<!doctype html><title>RCA</title>",
        base64_encoded=False,
    ):
        self.request_url = request_url
        self.response_bytes = response_bytes
        self.base64_encoded = base64_encoded
        self.calls = []

    def call(self, method, params=None, *, session_id=None, events=None):
        self.calls.append((method, params, session_id))
        if method == "Page.navigate":
            events.extend([
                {
                    "method": "Network.requestWillBeSent",
                    "params": {
                        "requestId": "main-document-request",
                        "request": {"url": self.request_url},
                        "type": "Document",
                    },
                    "sessionId": session_id,
                },
                {
                    "method": "Page.loadEventFired",
                    "params": {},
                    "sessionId": session_id,
                },
            ])
        if method == "Network.getResponseBody":
            body = (
                smoke_module.base64.b64encode(self.response_bytes).decode("ascii")
                if self.base64_encoded
                else self.response_bytes.decode("utf-8")
            )
            return {"body": body, "base64Encoded": self.base64_encoded}
        if method == "Runtime.evaluate":
            return {"result": {"value": _dom()}}
        return {}

    def receive(self):
        raise TimeoutError


@pytest.mark.parametrize("base64_encoded", [False, True])
def test_cdp_capture_enables_network_runtime_log_and_applies_viewport(
    base64_encoded,
):
    viewport = VIEWPORTS[0]
    report_url = (
        "http://192.168.26.174:18081/G1Q3_RCA/cases/"
        "g1q3-rca-s1-" + "a" * 64 + "/g1q3-rca-artifact-v1-" + "b" * 64 + "/index.html"
    )
    response_bytes = b"<!doctype html><title>observed</title>"
    cdp = FakeCdpSession(
        report_url,
        response_bytes=response_bytes,
        base64_encoded=base64_encoded,
    )

    trace = _capture_cdp_viewport(
        cdp,
        session_id="session-1",
        report_url=report_url,
        viewport=viewport,
        timeout_seconds=1,
    )

    methods = [call[0] for call in cdp.calls]
    assert "Network.enable" in methods
    assert "Runtime.enable" in methods
    assert "Log.enable" in methods
    assert "Debugger.enable" in methods
    assert "Network.setCacheDisabled" in methods
    assert "Emulation.setDeviceMetricsOverride" in methods
    assert "Page.navigate" in methods
    assert "Network.getResponseBody" in methods
    assert "Runtime.evaluate" in methods
    assert trace["request_urls"] == [report_url]
    assert trace["dom"]["visible_element_count"] == 5
    assert trace["index_html_sha256"] == hashlib.sha256(response_bytes).hexdigest()
    assert trace["index_html_size_bytes"] == len(response_bytes)
    metrics = next(
        call[1] for call in cdp.calls if call[0] == "Emulation.setDeviceMetricsOverride"
    )
    assert metrics == {
        "width": 1440,
        "height": 1000,
        "deviceScaleFactor": 1.0,
        "mobile": False,
    }


def test_chromium_runner_uses_ephemeral_profile_port_zero_and_fake_cdp(
    tmp_path, monkeypatch
):
    chrome = tmp_path / "chrome"
    chrome.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    chrome.chmod(0o755)
    captured = {}

    class FakeProcess:
        stopped = False

        def poll(self):
            return 0 if self.stopped else None

        def terminate(self):
            self.stopped = True

        def wait(self, timeout=None):
            self.stopped = True
            return 0

        def kill(self):
            self.stopped = True

    def process_factory(command, **kwargs):
        captured["command"] = command
        captured["process_kwargs"] = kwargs
        profile_arg = next(
            value for value in command if value.startswith("--user-data-dir=")
        )
        profile = Path(profile_arg.split("=", 1)[1])
        (profile / "DevToolsActivePort").write_text(
            "4567\n/devtools/browser/fake\n", encoding="utf-8"
        )
        return FakeProcess()

    class FakeBrowserCdp(FakeCdpSession):
        def __init__(self, websocket_url, *, timeout_seconds):
            super().__init__("unused")
            captured["websocket_url"] = websocket_url
            captured["cdp_timeout"] = timeout_seconds
            self.context = 0
            self.target = 0

        def call(self, method, params=None, *, session_id=None, events=None):
            if method == "Browser.getVersion":
                return {"product": "Chrome/140", "protocolVersion": "1.3"}
            if method == "Target.createBrowserContext":
                self.context += 1
                return {"browserContextId": f"context-{self.context}"}
            if method == "Target.createTarget":
                self.target += 1
                return {"targetId": f"target-{self.target}"}
            if method == "Target.attachToTarget":
                return {"sessionId": f"session-{self.target}"}
            if method == "Page.navigate":
                self.request_url = params["url"]
            return super().call(
                method,
                params,
                session_id=session_id,
                events=events,
            )

        def close(self):
            captured["cdp_closed"] = True

    monkeypatch.setattr(smoke_module.time, "sleep", lambda _seconds: None)
    runner = ChromiumCdpRunner(
        chrome_path=str(chrome),
        process_factory=process_factory,
        cdp_factory=FakeBrowserCdp,
    )
    report_url = (
        "http://192.168.26.174:18081/G1Q3_RCA/cases/"
        "g1q3-rca-s1-" + "a" * 64 + "/g1q3-rca-artifact-v1-" + "b" * 64 + "/index.html"
    )

    result = runner.capture(report_url, VIEWPORTS, timeout_seconds=2)

    assert "--remote-debugging-port=0" in captured["command"]
    assert any(
        value.startswith("--user-data-dir=pnc-rca-browser-smoke-")
        or "/pnc-rca-browser-smoke-" in value
        for value in captured["command"]
    )
    assert captured["websocket_url"] == ("ws://127.0.0.1:4567/devtools/browser/fake")
    assert captured["cdp_closed"] is True
    assert result["engine"] == "chromium"
    assert len(result["viewports"]) == 2
