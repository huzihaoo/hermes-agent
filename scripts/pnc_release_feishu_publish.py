#!/usr/bin/env python3
"""Build a guarded Feishu wiki-upload payload for PNC-Agent release docs.

This script does not call Feishu directly. It fail-closes on wrong release targets,
then prints the exact upload payload that a caller should pass to the Feishu MCP
upload tool or another approved publisher.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

try:
    from scripts.pnc_release_feishu_target_common import (
        EXPECTED_SPACE_ID,
        EXPECTED_WIKI_NODE,
        EXPECTED_WIKI_URL,
        ensure_release_target,
    )
except ModuleNotFoundError as exc:
    if exc.name not in {"scripts", "scripts.pnc_release_feishu_target_common"}:
        raise
    from pnc_release_feishu_target_common import (  # type: ignore[no-redef]
        EXPECTED_SPACE_ID,
        EXPECTED_WIKI_NODE,
        EXPECTED_WIKI_URL,
        ensure_release_target,
    )


def build_payload(*, version: str, title: str, content: str, app_id: str = "") -> dict[str, Any]:
    ensure_release_target(EXPECTED_WIKI_NODE, EXPECTED_SPACE_ID)
    return {
        "appId": app_id,
        "content": content,
        "filePath": "",
        "title": title or f"PNC-Agent Release {version}",
        "targetType": "wiki",
        "targetId": EXPECTED_SPACE_ID,
        "parentNodeToken": EXPECTED_WIKI_NODE,
        "removeFrontMatter": True,
        "uploadImages": True,
        "uploadAttachments": True,
        "downloadRemoteImages": False,
        "downloadRemoteAttachments": False,
        "workingDirectory": str(Path.cwd()),
    }


def build_metadata(*, version: str, title: str) -> dict[str, Any]:
    return {
        "version": version,
        "title": title or f"PNC-Agent Release {version}",
        "wiki_node": EXPECTED_WIKI_NODE,
        "space_id": EXPECTED_SPACE_ID,
        "wiki_url": EXPECTED_WIKI_URL,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", required=True)
    parser.add_argument("--title", default="")
    parser.add_argument("--content-file", required=True, help="Markdown file to publish")
    parser.add_argument("--app-id", default="")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)

    content_path = Path(args.content_file).expanduser().resolve()
    if not content_path.is_file():
        raise SystemExit(f"content file not found: {content_path}")
    content = content_path.read_text(encoding="utf-8")
    payload = build_payload(version=args.version, title=args.title, content=content, app_id=args.app_id)
    result = {
        "ok": True,
        "metadata": build_metadata(version=args.version, title=args.title),
        "content_file": str(content_path),
        "payload": payload,
    }
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(f"PNC_RELEASE_FEISHU_PUBLISH_PAYLOAD PASS version={args.version}")
        print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
