from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

SCRIPT_PATH = Path(__file__).resolve().parents[2] / "scripts" / "pnc_release_publish_all.py"
spec = importlib.util.spec_from_file_location("pnc_release_publish_all", SCRIPT_PATH)
assert spec is not None
assert spec.loader is not None
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
spec.loader.exec_module(mod)


def test_main_rejects_missing_content_file(tmp_path: Path) -> None:
    missing = tmp_path / "missing.md"
    try:
        mod.main(["--version", "0.14.2", "--content-file", str(missing), "--json"])
    except SystemExit as exc:
        assert "content file not found" in str(exc)
    else:
        raise AssertionError("missing content file should fail")


def test_main_generate_template_creates_release_doc(tmp_path: Path, capsys) -> None:
    out = tmp_path / "generated.md"

    def fake_closeout(version: str):
        return {"ok": True, "version": version, "steps": []}

    def fake_write_release_pages(version: str, **kwargs):
        rel = tmp_path / "html" / "pnc-agent-release" / f"pnc-agent-release-{version}" / "release"
        rel.mkdir(parents=True)
        html_url = (rel / f"pnc-agent-runtime-patch-release-{version}.html").as_uri()
        return {"ok": True, "version": version, "http_url": html_url}

    def fake_validate(version: str, html_url: str, http_base: str = mod.DEFAULT_HTTP_BASE):
        return {"ok": True, "expected": html_url, "html_url": html_url}

    old_closeout = getattr(mod, "_run_closeout")
    old_write_html = mod.html_template.write_release_pages
    old_validate = getattr(mod, "validate_html_binding")
    setattr(mod, "_run_closeout", fake_closeout)
    mod.html_template.write_release_pages = fake_write_release_pages
    setattr(mod, "validate_html_binding", fake_validate)
    try:
        rc = mod.main([
            "--version",
            "0.14.3",
            "--generate-template",
            "--template-output",
            str(out),
            "--generate-html",
            "--highlight",
            "模板生成器接入 publish_all",
            "--validation",
            "focused tests passed",
            "--json",
        ])
    finally:
        setattr(mod, "_run_closeout", old_closeout)
        mod.html_template.write_release_pages = old_write_html
        setattr(mod, "validate_html_binding", old_validate)

    assert rc == 0
    assert out.is_file()
    text = out.read_text(encoding="utf-8")
    assert "<!-- PNC_RELEASE_HTML_URL -->" in text
    assert "模板生成器接入 publish_all" in text
    stdout = capsys.readouterr().out
    assert '"mode": "dry-run"' in stdout
    assert "主 release 页面：file://" in stdout


def test_build_expected_html_url() -> None:
    url = mod.build_expected_html_url("0.14.2")
    assert url.endswith("/pnc-agent-release/pnc-agent-release-0.14.2/release/pnc-agent-runtime-patch-release-0.14.2.html")


def test_inject_html_url_appends_section() -> None:
    out = mod.inject_html_url("# Release\n", "http://example/html")
    assert "## HTML 主入口" in out
    assert "http://example/html" in out


def test_inject_html_url_replaces_marker() -> None:
    out = mod.inject_html_url("before\n<!-- PNC_RELEASE_HTML_URL -->\nafter\n", "http://example/html")
    assert "<!-- PNC_RELEASE_HTML_URL -->" not in out
    assert "主 release 页面：http://example/html" in out


def test_publish_all_dry_run_does_not_call_external_publish(tmp_path: Path) -> None:
    content = tmp_path / "release.md"
    content.write_text("# release\n", encoding="utf-8")

    calls = []

    def fake_closeout(version: str):
        calls.append(("closeout", version))
        return {"ok": True, "version": version, "steps": []}

    def fake_publish_release_doc(**kwargs):
        calls.append(("publish", kwargs))
        return {"ok": True}

    old_closeout = getattr(mod, "_run_closeout")
    old_publish = mod.publish_runner.publish_release_doc
    setattr(mod, "_run_closeout", fake_closeout)
    mod.publish_runner.publish_release_doc = fake_publish_release_doc
    try:
        result = mod.publish_all(version="0.14.2", content_file=content, execute=False)
    finally:
        setattr(mod, "_run_closeout", old_closeout)
        mod.publish_runner.publish_release_doc = old_publish

    assert result["ok"] is True
    assert result["mode"] == "dry-run"
    assert [name for name, _ in calls] == ["closeout"]
    assert result["payload"]["parentNodeToken"] == "DWcXwxUwIiJoIAkgSbFclfcfnLd"


def test_publish_all_execute_calls_external_publish(tmp_path: Path) -> None:
    content = tmp_path / "release.md"
    content.write_text("# release\n", encoding="utf-8")

    calls = []

    def fake_closeout(version: str):
        calls.append(("closeout", version))
        return {"ok": True, "version": version, "steps": []}

    def fake_publish_release_doc(**kwargs):
        calls.append(("publish", kwargs))
        return {"ok": True, "readback": {"documentId": "doc123"}}

    old_closeout = getattr(mod, "_run_closeout")
    old_publish = mod.publish_runner.publish_release_doc
    setattr(mod, "_run_closeout", fake_closeout)
    mod.publish_runner.publish_release_doc = fake_publish_release_doc
    try:
        result = mod.publish_all(version="0.14.2", content_file=content, execute=True)
    finally:
        setattr(mod, "_run_closeout", old_closeout)
        mod.publish_runner.publish_release_doc = old_publish

    assert result["ok"] is True
    assert result["mode"] == "execute"
    assert [name for name, _ in calls] == ["closeout", "publish"]
    assert result["publish"]["readback"]["documentId"] == "doc123"


def test_publish_all_validates_html_url_and_injects_it(tmp_path: Path) -> None:
    content = tmp_path / "release.md"
    content.write_text("# release\n<!-- PNC_RELEASE_HTML_URL -->\n", encoding="utf-8")

    def fake_closeout(version: str):
        return {"ok": True, "version": version, "steps": []}

    def fake_validate(version: str, html_url: str, http_base: str = mod.DEFAULT_HTTP_BASE):
        return {"ok": True, "expected": html_url, "html_url": html_url}

    old_closeout = getattr(mod, "_run_closeout")
    old_validate = getattr(mod, "validate_html_binding")
    setattr(mod, "_run_closeout", fake_closeout)
    setattr(mod, "validate_html_binding", fake_validate)
    try:
        result = mod.publish_all(
            version="0.14.2",
            content_file=content,
            execute=False,
            html_url="http://192.168.26.174:8088/pnc-agent-release/pnc-agent-release-0.14.2/release/pnc-agent-runtime-patch-release-0.14.2.html",
        )
    finally:
        setattr(mod, "_run_closeout", old_closeout)
        setattr(mod, "validate_html_binding", old_validate)

    assert result["html_binding"]["ok"] is True
    assert "主 release 页面：http://192.168.26.174:8088/pnc-agent-release/pnc-agent-release-0.14.2/release/pnc-agent-runtime-patch-release-0.14.2.html" in result["payload"]["content"]
