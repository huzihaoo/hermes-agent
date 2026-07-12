#!/usr/bin/env python3
"""Controlled top-level publisher for PNC-Agent release closure.

Default mode is dry-run: it validates closeout and builds the Feishu payload but
performs no external write. Only `--execute` performs the Feishu upload+readback.
It can also enforce that the bound VM HTML URL matches the release version and
inject that URL into the Feishu markdown before publishing.
"""
from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import urllib.request
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import pnc_release_feishu_publish as payload_builder
import pnc_release_feishu_publish_run as publish_runner
import pnc_release_html_template as html_template
import pnc_release_markdown_template as markdown_template

DEFAULT_HTTP_BASE = "http://192.168.26.174:8088"


def build_expected_html_url(version: str, http_base: str = DEFAULT_HTTP_BASE) -> str:
    return f"{http_base.rstrip('/')}/pnc-agent-release/pnc-agent-release-{version}/release/pnc-agent-runtime-patch-release-{version}.html"


def validate_html_binding(version: str, html_url: str, http_base: str = DEFAULT_HTTP_BASE) -> dict[str, Any]:
    expected = build_expected_html_url(version, http_base=http_base)
    if html_url != expected:
        raise ValueError(f"html_url mismatch: expected {expected}, got {html_url}")
    with urllib.request.urlopen(html_url, timeout=8) as resp:
        body = resp.read(200_000).decode("utf-8", errors="replace")
    if f"PNC-Agent {version}" not in body and f"Release {version}" not in body:
        raise ValueError(f"html_url body does not mention version {version}: {html_url}")
    return {"ok": True, "expected": expected, "html_url": html_url}


def inject_html_url(content: str, html_url: str) -> str:
    marker = "<!-- PNC_RELEASE_HTML_URL -->"
    line = f"主 release 页面：{html_url}"
    if marker in content:
        return content.replace(marker, line)
    if html_url in content:
        return content
    return content.rstrip() + "\n\n## HTML 主入口\n\n" + line + "\n"


def _split_template_items(raw_items: list[str]) -> list[str]:
    return [item for raw in raw_items for item in markdown_template._split_items(raw)]


def generate_template_file(
    *,
    version: str,
    output: Path,
    title: str = "",
    summary: str = "",
    highlights: list[str] | None = None,
    validations: list[str] | None = None,
    rollback: list[str] | None = None,
    notes: str = "",
) -> Path:
    content = markdown_template.build_release_markdown(
        version=version,
        title=title,
        summary=summary,
        highlights=highlights or [],
        validations=validations or [],
        rollback=rollback or [],
        notes=notes or markdown_template.DEFAULT_NOTES,
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(content, encoding="utf-8")
    return output


def _default_template_output(version: str) -> Path:
    return REPO_ROOT / "dist" / "release-docs" / f"pnc-agent-release-{version}.md"


def _run_closeout(version: str) -> dict[str, Any]:
    uv = ["uv", "run", "--no-sync"] if (REPO_ROOT / ".venv").exists() else ["uv", "run"]
    cmd = [*uv, "python", "scripts/pnc_release_closeout.py", "--version", version, "--json"]
    proc = subprocess.run(cmd, cwd=REPO_ROOT, text=True, capture_output=True)
    merged = ((proc.stdout or "") + ("\n" if proc.stdout and proc.stderr else "") + (proc.stderr or "")).strip()
    if proc.returncode != 0:
        raise RuntimeError(f"closeout failed rc={proc.returncode}: {merged}")
    return json.loads(proc.stdout)


def publish_all(*, version: str, content_file: Path, title: str = "", app_id: str = "", execute: bool = False, html_url: str = "", http_base: str = DEFAULT_HTTP_BASE) -> dict[str, Any]:
    closeout = _run_closeout(version)
    if not closeout.get("ok"):
        raise RuntimeError(f"release closeout not green for {version}")
    content = content_file.read_text(encoding="utf-8")
    html_binding = None
    final_content = content
    if html_url:
        html_binding = validate_html_binding(version, html_url, http_base=http_base)
        final_content = inject_html_url(content, html_url)
    payload = payload_builder.build_payload(version=version, title=title, content=final_content, app_id=app_id)
    result: dict[str, Any] = {
        "ok": True,
        "mode": "execute" if execute else "dry-run",
        "version": version,
        "content_file": str(content_file),
        "closeout": closeout,
        "html_binding": html_binding,
        "payload": payload,
    }
    if execute:
        if html_url:
            temp = content_file.parent / f".{content_file.name}.publish.tmp.md"
            temp.write_text(final_content, encoding="utf-8")
            publish_file = temp
        else:
            publish_file = content_file
        try:
            publish = publish_runner.publish_release_doc(
                version=version,
                title=title,
                content_file=publish_file,
                app_id=app_id,
            )
        finally:
            if html_url and publish_file.exists() and publish_file.name.endswith('.publish.tmp.md'):
                publish_file.unlink()
        result["publish"] = publish
        result["ok"] = bool(publish.get("ok"))
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", required=True)
    parser.add_argument("--content-file", help="Existing Markdown file to publish. If omitted, --generate-template is required.")
    parser.add_argument("--generate-template", action="store_true", help="Generate a standard release Markdown template before publishing/dry-run.")
    parser.add_argument("--template-output", default="", help="Where to write generated template Markdown. Defaults to dist/release-docs/pnc-agent-release-<version>.md.")
    parser.add_argument("--summary", default="", help="Template summary when --generate-template is used.")
    parser.add_argument("--highlight", action="append", default=[], help="Template highlight; repeatable, semicolon-separated supported.")
    parser.add_argument("--validation", action="append", default=[], help="Template validation evidence; repeatable, semicolon-separated supported.")
    parser.add_argument("--rollback", action="append", default=[], help="Template rollback/boundary note; repeatable, semicolon-separated supported.")
    parser.add_argument("--notes", default="", help="Template notes when --generate-template is used.")
    parser.add_argument("--title", default="")
    parser.add_argument("--app-id", default="")
    parser.add_argument("--html-url", default="", help="Expected VM HTTP HTML release URL for this version; validates and injects into markdown.")
    parser.add_argument("--http-base", default=DEFAULT_HTTP_BASE)
    parser.add_argument("--execute", action="store_true", help="Actually upload to Feishu; default is dry-run only.")
    parser.add_argument("--generate-html", action="store_true", help="Generate the product-style VM HTTP HTML release pages before closeout.")
    parser.add_argument("--html-output-root", default="/mnt/tmp", help="Root for generated release HTML pages; defaults to VM /mnt/tmp.")
    parser.add_argument("--html-publish-root", default="/home/mini/workspace/minieye_ci_eval/ci_report_publish", help="HTTP publish root for release-page symlink.")
    parser.add_argument("--html-spec-file", default="", help="Optional JSON override for the product-style release HTML template.")
    parser.add_argument("--no-html-publish-symlink", action="store_true", help="Generate HTML files without creating/updating the publish symlink.")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)

    if args.generate_template:
        content_file = Path(args.template_output).expanduser().resolve() if args.template_output else _default_template_output(args.version)
        generate_template_file(
            version=args.version,
            output=content_file,
            title=args.title,
            summary=args.summary,
            highlights=_split_template_items(args.highlight),
            validations=_split_template_items(args.validation),
            rollback=_split_template_items(args.rollback),
            notes=args.notes,
        )
    else:
        if not args.content_file:
            raise SystemExit("content file not found: pass --content-file or use --generate-template")
        content_file = Path(args.content_file).expanduser().resolve()
        if not content_file.is_file():
            raise SystemExit(f"content file not found: {content_file}")

    if args.generate_html:
        html_result = html_template.write_release_pages(
            args.version,
            output_root=Path(args.html_output_root),
            spec_file=args.html_spec_file,
            publish_root=None if args.no_html_publish_symlink else Path(args.html_publish_root),
            http_base=args.http_base,
        )
        if not args.html_url:
            args.html_url = str(html_result["http_url"])

    result = publish_all(
        version=args.version,
        content_file=content_file,
        title=args.title,
        app_id=args.app_id,
        execute=args.execute,
        html_url=args.html_url,
        http_base=args.http_base,
    )
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(f"PNC_RELEASE_PUBLISH_ALL {'PASS' if result['ok'] else 'FAIL'} mode={result['mode']} version={args.version}")
        print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["ok"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
