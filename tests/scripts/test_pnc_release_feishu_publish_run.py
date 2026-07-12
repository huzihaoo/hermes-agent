from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

SCRIPT_PATH = Path(__file__).resolve().parents[2] / "scripts" / "pnc_release_feishu_publish_run.py"
spec = importlib.util.spec_from_file_location("pnc_release_feishu_publish_run", SCRIPT_PATH)
assert spec is not None
assert spec.loader is not None
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
spec.loader.exec_module(mod)


def test_extract_document_id_from_upload_result() -> None:
    doc_id = mod._extract_document_id({"structuredContent": {"documentId": "abc123"}})
    assert doc_id == "abc123"


def test_extract_document_id_rejects_missing_field() -> None:
    try:
        mod._extract_document_id({"structuredContent": {}})
    except RuntimeError as exc:
        assert "missing documentId" in str(exc)
    else:
        raise AssertionError("missing documentId should fail")


def test_main_rejects_missing_content_file(tmp_path: Path) -> None:
    missing = tmp_path / "missing.md"
    try:
        mod.main(["--version", "0.14.2", "--content-file", str(missing), "--json"])
    except SystemExit as exc:
        assert "content file not found" in str(exc)
    else:
        raise AssertionError("missing content file should fail")


def test_publish_result_shape_with_stubbed_tool_calls(tmp_path: Path) -> None:
    content = tmp_path / "release.md"
    content.write_text("# PNC-Agent Release 0.14.2\nbody\n", encoding="utf-8")

    calls: list[tuple[str, dict]] = []

    def fake_run(tool_name: str, payload: dict):
        calls.append((tool_name, payload))
        if tool_name == "mcp_feishu_doc_feishu_upload_markdown":
            return {"structuredContent": {"documentId": "doc123", "url": "https://feishu.cn/docx/doc123"}}
        if tool_name == "mcp_feishu_doc_feishu_get_document":
            return {"structuredContent": {"documentId": "doc123", "title": "PNC-Agent Release 0.14.2", "revisionId": 7, "content": "PNC-Agent Release 0.14.2\nbody"}}
        raise AssertionError(tool_name)

    old = getattr(mod, "_run_hermes_tool")
    setattr(mod, "_run_hermes_tool", fake_run)
    try:
        result = mod.publish_release_doc(version="0.14.2", title="", content_file=content)
    finally:
        setattr(mod, "_run_hermes_tool", old)

    assert result["ok"] is True
    assert result["payload"]["parentNodeToken"] == "DWcXwxUwIiJoIAkgSbFclfcfnLd"
    assert result["readback"]["documentId"] == "doc123"
    assert [name for name, _ in calls] == [
        "mcp_feishu_doc_feishu_upload_markdown",
        "mcp_feishu_doc_feishu_get_document",
    ]
