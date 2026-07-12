#!/usr/bin/env python3
"""Generate the standard Markdown body for a PNC-Agent release document.

The template is intentionally Feishu-friendly Markdown and includes the HTML URL
placeholder consumed by `pnc_release_publish_all.py --html-url`.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

HTML_URL_MARKER = "<!-- PNC_RELEASE_HTML_URL -->"
DEFAULT_NOTES = "本次 release 文档由标准模板生成；正式发布前请补充 release highlights、验证证据和回滚方式。"


def _split_items(raw: str) -> list[str]:
    return [item.strip() for item in raw.split(";") if item.strip()]


def _render_bullets(items: list[str], *, fallback: str) -> str:
    source = items or [fallback]
    return "\n".join(f"- {item}" for item in source)


def build_release_markdown(
    *,
    version: str,
    title: str = "",
    summary: str = "",
    highlights: list[str] | None = None,
    validations: list[str] | None = None,
    rollback: list[str] | None = None,
    notes: str = DEFAULT_NOTES,
) -> str:
    doc_title = title or f"PNC-Agent Release {version}"
    highlights = highlights or []
    validations = validations or []
    rollback = rollback or []
    summary = summary or f"PNC-Agent {version} release closeout：以 VM HTTP HTML 主入口为用户发布面，以 Feishu 文档作为可检索发布记录。"

    sections = [
        f"# {doc_title}",
        "",
        "## 发布结论",
        "",
        summary,
        "",
        "## HTML 主入口",
        "",
        HTML_URL_MARKER,
        "",
        "## 用户会直接感受到的变化",
        "",
        _render_bullets(highlights, fallback="请补充本次 release 对用户可见的变化。"),
        "",
        "## 验收与验证",
        "",
        _render_bullets(validations, fallback="请补充本次 release 已通过的测试、gate、服务 readback 或浏览器验收。"),
        "",
        "## 回滚与边界",
        "",
        _render_bullets(rollback, fallback="请补充本次 release 的回滚方式、未覆盖边界和外部依赖。"),
        "",
        "## 发布记录",
        "",
        f"- release 标题：{doc_title}",
        f"- release 版本：{version}",
        "- 发布目录：PNC-Agent release documents",
        "- 文档用途：作为 Feishu 可检索发布记录；用户打开入口以 VM HTTP HTML 页面为准。",
        "",
        "## 备注",
        "",
        notes,
        "",
    ]
    return "\n".join(sections)


def build_result(*, version: str, output: Path | None, content: str) -> dict[str, Any]:
    return {
        "ok": True,
        "version": version,
        "output": str(output) if output else "",
        "has_html_url_marker": HTML_URL_MARKER in content,
        "bytes": len(content.encode("utf-8")),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", required=True)
    parser.add_argument("--title", default="")
    parser.add_argument("--summary", default="")
    parser.add_argument("--highlight", action="append", default=[], help="Repeatable user-facing highlight. Also accepts semicolon-separated items.")
    parser.add_argument("--validation", action="append", default=[], help="Repeatable validation evidence. Also accepts semicolon-separated items.")
    parser.add_argument("--rollback", action="append", default=[], help="Repeatable rollback/boundary note. Also accepts semicolon-separated items.")
    parser.add_argument("--notes", default=DEFAULT_NOTES)
    parser.add_argument("--output", default="", help="Write Markdown to this file. If omitted, prints Markdown to stdout.")
    parser.add_argument("--json", action="store_true", help="Print machine-readable generation receipt instead of Markdown.")
    args = parser.parse_args(argv)

    highlights = [item for raw in args.highlight for item in _split_items(raw)]
    validations = [item for raw in args.validation for item in _split_items(raw)]
    rollback = [item for raw in args.rollback for item in _split_items(raw)]
    content = build_release_markdown(
        version=args.version,
        title=args.title,
        summary=args.summary,
        highlights=highlights,
        validations=validations,
        rollback=rollback,
        notes=args.notes,
    )
    output_path = Path(args.output).expanduser().resolve() if args.output else None
    if output_path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(content, encoding="utf-8")
    result = build_result(version=args.version, output=output_path, content=content)
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(content)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
