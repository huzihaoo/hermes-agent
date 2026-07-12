#!/usr/bin/env python3
"""Check that future PNC-Agent release docs target the correct Feishu wiki folder."""
from __future__ import annotations

import argparse
import json
import sys
import urllib.request
from dataclasses import dataclass, asdict
from pnc_release_feishu_target_common import (
    EXPECTED_SPACE_ID,
    EXPECTED_WIKI_NODE,
    EXPECTED_WIKI_URL as DEFAULT_FEISHU_WIKI_URL,
)


@dataclass
class CheckResult:
    name: str
    ok: bool
    detail: str


def _http_ok(url: str) -> tuple[bool, str]:
    try:
        with urllib.request.urlopen(url, timeout=8) as resp:
            code = getattr(resp, "status", None) or resp.getcode()
        return 200 <= int(code) < 400, f"status={code}"
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"


def run(target_node: str, space_id: str, wiki_url: str) -> dict:
    checks = [
        CheckResult("target_node_matches_expected", target_node == EXPECTED_WIKI_NODE, target_node),
        CheckResult("space_id_matches_expected", space_id == EXPECTED_SPACE_ID, space_id),
        CheckResult("wiki_url_matches_expected", wiki_url.rstrip("/") == DEFAULT_FEISHU_WIKI_URL, wiki_url),
    ]
    http_ok, detail = _http_ok(wiki_url)
    checks.append(CheckResult("wiki_url_http_ok", http_ok, detail))
    ok = all(c.ok for c in checks)
    return {
        "ok": ok,
        "expected": {
            "wiki_node": EXPECTED_WIKI_NODE,
            "space_id": EXPECTED_SPACE_ID,
            "wiki_url": DEFAULT_FEISHU_WIKI_URL,
        },
        "actual": {
            "wiki_node": target_node,
            "space_id": space_id,
            "wiki_url": wiki_url,
        },
        "checks": [asdict(c) for c in checks],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target-node", default=EXPECTED_WIKI_NODE)
    parser.add_argument("--space-id", default=EXPECTED_SPACE_ID)
    parser.add_argument("--wiki-url", default=DEFAULT_FEISHU_WIKI_URL)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    result = run(args.target_node, args.space_id, args.wiki_url)
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(f"PNC_RELEASE_FEISHU_TARGET {'PASS' if result['ok'] else 'FAIL'}")
        for check in result["checks"]:
            print(f"{'OK' if check['ok'] else 'FAIL'} {check['name']}: {check['detail']}")
    return 0 if result["ok"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
