#!/usr/bin/env python3
"""Browserless interaction gate for PNC-Agent release HTML pages.

This verifies the user-facing structure and interaction contract without needing
an external browser driver. It inspects the HTML/JS contract directly and checks
linked lane pages over HTTP.
"""
from __future__ import annotations

import argparse
import json
import re
import urllib.request
from dataclasses import dataclass, asdict

DEFAULT_HTTP_BASE = "http://192.168.26.174:8088"
EXPECTED_SECTION_HEADINGS = [
    "用户会直接感受到的变化",
    "前后对比",
    "补充内容从这里进入",
    "这版不混写什么",
    "内部验收状态（用户版）",
    "分享方式",
]
EXPECTED_LANE_LINKS = [
    "skills-lane-release-note-{version}.html",
    "workspace-knowledge-lane-release-note-{version}.html",
    "tools-tests-lane-release-note-{version}.html",
    "release-lanes-index-{version}.html",
]
EXPECTED_MODAL_IDS = ["m1", "m2", "m3", "m4"]
EXPECTED_TABS = ["p1", "p2", "p3"]


@dataclass
class CheckResult:
    name: str
    ok: bool
    detail: str


def _read(url: str) -> str:
    with urllib.request.urlopen(url, timeout=8) as resp:
        return resp.read().decode("utf-8", errors="replace")


def _find_heading_texts(html: str) -> list[str]:
    return [re.sub(r"<[^>]+>", "", m).strip() for m in re.findall(r"<h[1-3][^>]*>(.*?)</h[1-3]>", html, re.I | re.S)]


def validate(version: str, *, http_base: str) -> dict:
    version = version.strip()
    base = f"{http_base.rstrip('/')}/pnc-agent-release/pnc-agent-release-{version}/release"
    main_url = f"{base}/pnc-agent-runtime-patch-release-{version}.html"
    html = _read(main_url)
    checks: list[CheckResult] = []

    title = re.search(r"<title[^>]*>(.*?)</title>", html, re.I | re.S)
    checks.append(CheckResult("title_has_version", bool(title and version in title.group(1)), title.group(1).strip() if title else "missing title"))

    h1 = re.search(r"<h1[^>]*>(.*?)</h1>", html, re.I | re.S)
    checks.append(CheckResult("h1_user_facing", bool(h1 and f"PNC-Agent {version}：一个入口" in re.sub(r"<[^>]+>", "", h1.group(1))), re.sub(r"<[^>]+>", "", h1.group(1)).strip() if h1 else "missing h1"))

    headings = _find_heading_texts(html)
    for section in EXPECTED_SECTION_HEADINGS:
        checks.append(CheckResult(f"section:{section}", section in headings, section))

    script_text = "\n".join(re.findall(r"<script[^>]*>(.*?)</script>", html, re.I | re.S))
    checks.append(CheckResult("has_modal_script", "[data-modal]" in script_text and ".modal" in script_text, "modal event handlers present" if "[data-modal]" in script_text else "missing modal handler"))
    checks.append(CheckResult("has_tab_script", ".tab" in script_text and ".panel" in script_text, "tab event handlers present" if ".tab" in script_text else "missing tab handler"))

    for modal_id in EXPECTED_MODAL_IDS:
        ok = f'id="{modal_id}"' in html and f'data-modal="{modal_id}"' in html
        checks.append(CheckResult(f"modal:{modal_id}", ok, modal_id))
    checks.append(CheckResult("modal_has_usage_blocks", html.count("使用方式：") >= 4 and html.count("<ul>") >= 4, f"usage_blocks={html.count('使用方式：')} ul_count={html.count('<ul>')}"))

    for tab_id in EXPECTED_TABS:
        ok = f'data-tab="{tab_id}"' in html and f'id="{tab_id}"' in html
        checks.append(CheckResult(f"tab:{tab_id}", ok, tab_id))

    # Linked lane pages
    for pattern in EXPECTED_LANE_LINKS:
        href = pattern.format(version=version)
        checks.append(CheckResult(f"link_declared:{href}", href in html, href))
        try:
            linked = _read(f"{base}/{href}")
            linked_title = re.search(r"<title[^>]*>(.*?)</title>", linked, re.I | re.S)
            checks.append(CheckResult(f"link_fetch:{href}", True, linked_title.group(1).strip() if linked_title else "fetched"))
        except Exception as exc:
            checks.append(CheckResult(f"link_fetch:{href}", False, repr(exc)))

    ok = all(c.ok for c in checks)
    return {
        "ok": ok,
        "version": version,
        "main_url": main_url,
        "checks": [asdict(c) for c in checks],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", required=True)
    parser.add_argument("--http-base", default=DEFAULT_HTTP_BASE)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    result = validate(args.version, http_base=args.http_base)
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(f"PNC_RELEASE_BROWSER_GATE {'PASS' if result['ok'] else 'FAIL'} version={result['version']}")
        print(f"Main URL: {result['main_url']}")
        for check in result['checks']:
            print(f"{'OK' if check['ok'] else 'FAIL'} {check['name']}: {check['detail']}")
    return 0 if result['ok'] else 2


if __name__ == "__main__":
    raise SystemExit(main())
