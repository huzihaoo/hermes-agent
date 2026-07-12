from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.pnc_release_html_gate import validate


def test_pnc_release_html_gate_passes_with_vm_file_and_http(monkeypatch, tmp_path):
    version = "0.13.10"
    rel = tmp_path / "pnc-agent-release" / f"pnc-agent-release-{version}" / "release"
    rel.mkdir(parents=True)
    html = rel / f"pnc-agent-runtime-patch-release-{version}.html"
    html.write_text("<!doctype html><title>PNC-Agent Release 0.13.10</title>PNC-Agent 0.13.10", encoding="utf-8")

    class FakeResponse:
        status = 200
        headers = {"Content-Type": "text/html; charset=utf-8"}
        def __enter__(self):
            return self
        def __exit__(self, *exc):
            return False
        def getcode(self):
            return 200
        def read(self, _limit):
            return html.read_bytes()

    monkeypatch.setattr("scripts.pnc_release_html_gate._read_url", lambda *a, **k: FakeResponse())

    result = validate(version, vm_root=tmp_path, http_base="http://host:8088", cifs_base="//hfs/tmp", timeout=1)

    assert result["ok"] is True
    assert result["http_url"] == "http://host:8088/pnc-agent-release/pnc-agent-release-0.13.10/release/pnc-agent-runtime-patch-release-0.13.10.html"
    assert result["verification_url"] == "http://host:8088/pnc-agent-release/pnc-agent-release-0.13.10/release/vm-publish-verification.json"
    assert result["cifs_dir"] == "//hfs/tmp/pnc-agent-release/pnc-agent-release-0.13.10/release/"
    assert {c["name"] for c in result["checks"]} >= {"write_read_unlink_probe", "http_content_type_html", "http_body_has_version"}


def test_pnc_release_html_gate_fails_when_http_is_not_html(monkeypatch, tmp_path):
    version = "0.13.10"
    rel = tmp_path / "pnc-agent-release" / f"pnc-agent-release-{version}" / "release"
    rel.mkdir(parents=True)
    html = rel / f"pnc-agent-runtime-patch-release-{version}.html"
    html.write_text("<!doctype html><title>PNC-Agent Release 0.13.10</title>PNC-Agent 0.13.10", encoding="utf-8")

    class FakeResponse:
        status = 200
        headers = {"Content-Type": "text/plain"}
        def __enter__(self):
            return self
        def __exit__(self, *exc):
            return False
        def getcode(self):
            return 200
        def read(self, _limit):
            return html.read_bytes()

    monkeypatch.setattr("scripts.pnc_release_html_gate._read_url", lambda *a, **k: FakeResponse())

    result = validate(version, vm_root=tmp_path, http_base="http://host:8088", cifs_base="//hfs/tmp", timeout=1)

    assert result["ok"] is False
    failed = {c["name"] for c in result["checks"] if not c["ok"]}
    assert "http_content_type_html" in failed


def test_pnc_release_html_gate_rejects_unsafe_version():
    with pytest.raises(SystemExit):
        validate("../0.13.10", vm_root=Path("/tmp"), http_base="http://host", cifs_base="//hfs", timeout=1)
