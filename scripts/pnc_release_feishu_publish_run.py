#!/usr/bin/env python3
"""One-shot PNC-Agent release document publisher for Feishu wiki.

This wraps the guarded payload builder, executes the Feishu MCP upload tool via
Hermes CLI, reads the uploaded doc back, and returns a compact machine-readable
receipt. The target wiki node is fail-closed.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

try:
    from scripts.pnc_release_feishu_publish import build_metadata, build_payload
except ModuleNotFoundError as exc:
    if exc.name not in {"scripts", "scripts.pnc_release_feishu_publish"}:
        raise
    from pnc_release_feishu_publish import build_metadata, build_payload  # type: ignore[no-redef]


def _run_hermes_tool(tool_name: str, payload: dict[str, Any]) -> dict[str, Any]:
    cmd = [
        str(Path.home() / "bin" / "hermes"),
        "--toolsets",
        "mcp_feishu_doc",
        "-p",
        tool_name,
        json.dumps(payload, ensure_ascii=False),
    ]
    proc = subprocess.run(cmd, text=True, capture_output=True)
    merged = ((proc.stdout or "") + ("\n" if proc.stdout and proc.stderr else "") + (proc.stderr or "")).strip()
    if proc.returncode != 0:
        raise RuntimeError(f"{tool_name} failed rc={proc.returncode}: {merged}")
    text = (proc.stdout or "").strip()
    if not text:
        raise RuntimeError(f"{tool_name} returned empty stdout")
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"{tool_name} did not return JSON: {text[:400]}") from exc


def _extract_document_id(upload_result: dict[str, Any]) -> str:
    structured = upload_result.get("structuredContent") or {}
    doc_id = structured.get("documentId")
    if isinstance(doc_id, str) and doc_id:
        return doc_id
    raise RuntimeError(f"upload result missing documentId: {json.dumps(upload_result, ensure_ascii=False)[:500]}")


def publish_release_doc(*, version: str, title: str, content_file: Path, app_id: str = "") -> dict[str, Any]:
    content = content_file.read_text(encoding="utf-8")
    payload = build_payload(version=version, title=title, content=content, app_id=app_id)
    upload_result = _run_hermes_tool("mcp_feishu_doc_feishu_upload_markdown", payload)
    document_id = _extract_document_id(upload_result)
    readback_result = _run_hermes_tool(
        "mcp_feishu_doc_feishu_get_document",
        {"appId": app_id, "documentId": document_id},
    )
    structured = readback_result.get("structuredContent") or {}
    readback_title = structured.get("title")
    readback_content = structured.get("content") or ""
    expected_title = title or f"PNC-Agent Release {version}"
    ok = bool(readback_title == expected_title and version in readback_content)
    return {
        "ok": ok,
        "metadata": build_metadata(version=version, title=title),
        "content_file": str(content_file),
        "payload": payload,
        "upload": upload_result,
        "readback": {
            "documentId": document_id,
            "title": readback_title,
            "revisionId": structured.get("revisionId"),
            "has_version": version in readback_content,
        },
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", required=True)
    parser.add_argument("--title", default="")
    parser.add_argument("--content-file", required=True)
    parser.add_argument("--app-id", default="")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)

    content_file = Path(args.content_file).expanduser().resolve()
    if not content_file.is_file():
        raise SystemExit(f"content file not found: {content_file}")

    result = publish_release_doc(
        version=args.version,
        title=args.title,
        content_file=content_file,
        app_id=args.app_id,
    )
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(f"PNC_RELEASE_FEISHU_PUBLISH {'PASS' if result['ok'] else 'FAIL'} version={args.version}")
        print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["ok"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
