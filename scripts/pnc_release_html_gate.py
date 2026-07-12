#!/usr/bin/env python3
"""Validate the PNC-Agent VM-hosted HTTP HTML release surface.

This gate intentionally checks the user-facing artifact shape that proved useful
in the 0.13.9 release: a VM /mnt/tmp release directory exposed through the
192.168.26.174:8088 static HTTP server. A release is not closeout-complete until
this gate passes.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass, asdict
from pathlib import Path


DEFAULT_HTTP_BASE = "http://192.168.26.174:8088"
DEFAULT_CIFS_BASE = "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp"


@dataclass
class CheckResult:
    name: str
    ok: bool
    detail: str


def _version_token(version: str) -> str:
    version = version.strip()
    if not re.fullmatch(r"\d+(?:\.\d+){1,3}(?:[-_A-Za-z0-9.]+)?", version):
        raise SystemExit(f"unsafe version: {version!r}")
    return version


def _read_url(url: str, method: str = "GET", timeout: float = 5.0):
    req = urllib.request.Request(url, method=method)
    return urllib.request.urlopen(req, timeout=timeout)


def validate(version: str, *, vm_root: Path, http_base: str, cifs_base: str, timeout: float) -> dict:
    version = _version_token(version)
    rel_dir = Path("pnc-agent-release") / f"pnc-agent-release-{version}" / "release"
    html_name = f"pnc-agent-runtime-patch-release-{version}.html"
    vm_dir = vm_root / rel_dir
    html_path = vm_dir / html_name
    http_dir_url = f"{http_base.rstrip('/')}/{rel_dir.as_posix()}"
    http_url = f"{http_dir_url}/{html_name}"
    verification_url = f"{http_dir_url}/vm-publish-verification.json"
    cifs_dir = f"{cifs_base.rstrip('/')}/{rel_dir.as_posix()}/"

    checks: list[CheckResult] = []
    local_vm_visible = vm_dir.is_dir() or vm_root.exists()
    checks.append(CheckResult("vm_dir_exists_or_remote_verification", vm_dir.is_dir(), str(vm_dir)))
    checks.append(CheckResult("html_file_exists_or_remote_verification", html_path.is_file(), str(html_path)))

    if html_path.is_file():
        text = html_path.read_text(encoding="utf-8", errors="replace")
        checks.append(CheckResult("html_has_title", "<title" in text.lower(), "title tag present"))
        checks.append(CheckResult("html_has_version", version in text, f"version {version} present"))
        checks.append(CheckResult("html_mentions_pnc_agent", "PNC-Agent" in text or "PNC Agent" in text, "PNC-Agent title/copy present"))
    elif local_vm_visible:
        checks.extend([
            CheckResult("html_has_title", False, "html file missing"),
            CheckResult("html_has_version", False, "html file missing"),
            CheckResult("html_mentions_pnc_agent", False, "html file missing"),
        ])

    # Exact target write/read/unlink probe when the VM filesystem is directly visible.
    # On macOS the VM /mnt/tmp path is often not mounted (or /mnt is read-only), so
    # a VM-side publish can instead be proven by vm-publish-verification.json over
    # the HTTP publish surface.
    probe_path = vm_dir / ".pnc_release_gate_probe"
    if local_vm_visible:
        try:
            vm_dir.mkdir(parents=True, exist_ok=True)
            probe_path.write_text("probe", encoding="utf-8")
            probe_ok = probe_path.read_text(encoding="utf-8") == "probe"
            probe_path.unlink(missing_ok=True)
            checks.append(CheckResult("write_read_unlink_probe", probe_ok, str(probe_path)))
        except Exception as exc:
            checks.append(CheckResult("write_read_unlink_probe", False, f"{probe_path}: {exc!r}"))

    try:
        with _read_url(http_url, method="HEAD", timeout=timeout) as resp:
            status = getattr(resp, "status", None) or resp.getcode()
            ctype = resp.headers.get("Content-Type", "")
        checks.append(CheckResult("http_head_ok", 200 <= int(status) < 300, f"status={status}"))
        checks.append(CheckResult("http_content_type_html", "text/html" in ctype.lower(), f"Content-Type={ctype}"))
    except Exception as exc:
        checks.append(CheckResult("http_head_ok", False, f"{type(exc).__name__}: {exc}"))
        checks.append(CheckResult("http_content_type_html", False, "HEAD failed"))

    try:
        with _read_url(http_url, method="GET", timeout=timeout) as resp:
            body = resp.read(200_000).decode("utf-8", errors="replace")
        checks.append(CheckResult("http_body_has_title", "<title" in body.lower(), "title tag present in fetched body"))
        checks.append(CheckResult("http_body_has_version", version in body, f"version {version} present in fetched body"))
    except Exception as exc:
        checks.append(CheckResult("http_body_has_title", False, f"{type(exc).__name__}: {exc}"))
        checks.append(CheckResult("http_body_has_version", False, "GET failed"))

    remote_verification = None
    if not html_path.is_file() or not any(c.name == "write_read_unlink_probe" for c in checks):
        try:
            with _read_url(verification_url, method="GET", timeout=timeout) as resp:
                remote_verification = json.loads(resp.read(200_000).decode("utf-8", errors="replace"))
            checks.append(CheckResult("remote_vm_verification_ok", bool(remote_verification.get("ok")), verification_url))
            checks.append(CheckResult("remote_vm_verification_target", remote_verification.get("target_dir") == str(vm_dir), str(remote_verification.get("target_dir"))))
            files = remote_verification.get("files") if isinstance(remote_verification, dict) else {}
            checks.append(CheckResult("remote_vm_verification_html", html_name in files and files.get(html_name, {}).get("size_bytes", 0) > 0, html_name))
        except Exception as exc:
            checks.append(CheckResult("remote_vm_verification_ok", False, f"{type(exc).__name__}: {exc}"))
            checks.append(CheckResult("remote_vm_verification_target", False, "verification fetch failed"))
            checks.append(CheckResult("remote_vm_verification_html", False, "verification fetch failed"))

    # If direct VM checks are unavailable but remote verification passed, mark the
    # local placeholders as satisfied by remote evidence so the overall gate can pass.
    remote_ok = any(c.name == "remote_vm_verification_ok" and c.ok for c in checks)
    if remote_ok:
        for c in checks:
            if c.name in {"vm_dir_exists_or_remote_verification", "html_file_exists_or_remote_verification"} and not c.ok:
                c.ok = True
                c.detail += " (proved by remote vm-publish-verification.json)"

    ok = all(c.ok for c in checks)
    return {
        "ok": ok,
        "version": version,
        "vm_dir": str(vm_dir),
        "html_path": str(html_path),
        "http_url": http_url,
        "verification_url": verification_url,
        "cifs_dir": cifs_dir,
        "remote_verification": remote_verification,
        "checks": [asdict(c) for c in checks],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", required=True)
    parser.add_argument("--vm-root", default="/mnt/tmp")
    parser.add_argument("--http-base", default=DEFAULT_HTTP_BASE)
    parser.add_argument("--cifs-base", default=DEFAULT_CIFS_BASE)
    parser.add_argument("--timeout", type=float, default=5.0)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    result = validate(args.version, vm_root=Path(args.vm_root), http_base=args.http_base, cifs_base=args.cifs_base, timeout=args.timeout)
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(f"PNC_RELEASE_HTML_GATE {'PASS' if result['ok'] else 'FAIL'} version={result['version']}")
        print(f"HTTP: {result['http_url']}")
        print(f"VM: {result['html_path']}")
        print(f"CIFS: {result['cifs_dir']}")
        for check in result["checks"]:
            print(f"{'OK' if check['ok'] else 'FAIL'} {check['name']}: {check['detail']}")
    return 0 if result["ok"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
