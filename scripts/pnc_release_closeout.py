#!/usr/bin/env python3
"""Run the full PNC-Agent release closeout gate stack.

This command is the single operator entrypoint for deciding whether a PNC-Agent
release can be declared closed out. It chains the version/runtime verification
slice, VM HTML publish gate, and browser interaction gate into one PASS/FAIL
summary so future releases do not depend on ad-hoc manual sequencing.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent


@dataclass
class StepResult:
    name: str
    ok: bool
    command: list[str]
    returncode: int
    summary: str
    stdout_tail: str


def _tail(text: str, lines: int = 40) -> str:
    parts = text.strip().splitlines()
    return "\n".join(parts[-lines:])


def _run(cmd: list[str]) -> StepResult:
    proc = subprocess.run(cmd, cwd=REPO_ROOT, text=True, capture_output=True)
    stdout = proc.stdout or ""
    stderr = proc.stderr or ""
    merged = (stdout + ("\n" if stdout and stderr else "") + stderr).strip()
    return StepResult(
        name="",
        ok=proc.returncode == 0,
        command=cmd,
        returncode=proc.returncode,
        summary=_tail(merged, lines=6) if merged else "",
        stdout_tail=_tail(merged, lines=40),
    )


def _uv_prefix() -> list[str]:
    if (REPO_ROOT / ".venv").exists():
        return ["uv", "run", "--no-sync"]
    return ["uv", "run"]


def _version_gate(version: str) -> list[StepResult]:
    uv = _uv_prefix()
    steps: list[tuple[str, list[str]]] = [
        ("version_check", [*uv, "python", "check_versions.py"]),
        (
            "release_targeted_tests",
            [
                *uv,
                "pytest",
                "tests/scripts/test_pnc_release_html_gate.py",
                "tests/scripts/test_pnc_release_browser_gate.py",
                "tests/scripts/test_pnc_release_feishu_target_common.py",
                "tests/scripts/test_pnc_release_feishu_target_gate.py",
                "tests/scripts/test_pnc_release_feishu_publish.py",
                "tests/scripts/test_pnc_release_feishu_publish_run.py",
                "tests/scripts/test_pnc_release_publish_all.py",
                "tests/scripts/test_pnc_release_markdown_template.py",
                "tests/scripts/test_pnc_release_html_template.py",
                "-q",
                "-o",
                "addopts=",
            ],
        ),
        (
            "release_py_compile",
            [
                *uv,
                "python",
                "-m",
                "py_compile",
                "scripts/pnc_release_html_gate.py",
                "scripts/pnc_release_browser_gate.py",
            ],
        ),
    ]
    out = []
    for name, cmd in steps:
        step = _run(cmd)
        step.name = name
        out.append(step)
        if not step.ok:
            break
    return out


def _json_step(name: str, cmd: list[str], *, expect_ok: bool = True) -> StepResult:
    proc = subprocess.run(cmd, cwd=REPO_ROOT, text=True, capture_output=True)
    merged = ((proc.stdout or "") + ("\n" if proc.stdout and proc.stderr else "") + (proc.stderr or "")).strip()
    ok = proc.returncode == 0 if expect_ok else proc.returncode in (0, 2)
    summary = ""
    try:
        payload = json.loads(proc.stdout)
        ok = bool(payload.get("ok")) if expect_ok else ok
        summary = json.dumps({"ok": payload.get("ok"), "version": payload.get("version")}, ensure_ascii=False)
    except Exception:
        pass
    return StepResult(name=name, ok=ok, command=cmd, returncode=proc.returncode, summary=summary or _tail(merged, 6), stdout_tail=_tail(merged, 40))


def run_closeout(version: str) -> dict[str, Any]:
    version = version.strip()
    steps: list[StepResult] = []
    for step in _version_gate(version):
        steps.append(step)
        if not step.ok:
            return {"ok": False, "version": version, "steps": [asdict(s) for s in steps]}

    uv = _uv_prefix()
    html_gate = _json_step(
        "vm_html_publish_gate",
        [*uv, "python", "scripts/pnc_release_html_gate.py", "--version", version, "--json"],
    )
    steps.append(html_gate)
    if not html_gate.ok:
        return {"ok": False, "version": version, "steps": [asdict(s) for s in steps]}

    browser_gate = _json_step(
        "browser_interaction_gate",
        [*uv, "python", "scripts/pnc_release_browser_gate.py", "--version", version, "--json"],
    )
    steps.append(browser_gate)
    if not browser_gate.ok:
        return {"ok": False, "version": version, "steps": [asdict(s) for s in steps]}

    feishu_target_gate = _json_step(
        "feishu_release_target_gate",
        [*uv, "python", "scripts/pnc_release_feishu_target_gate.py", "--json"],
    )
    steps.append(feishu_target_gate)
    ok = all(step.ok for step in steps)
    return {"ok": ok, "version": version, "steps": [asdict(s) for s in steps]}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", required=True)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    result = run_closeout(args.version)
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(f"PNC_RELEASE_CLOSEOUT {'PASS' if result['ok'] else 'FAIL'} version={result['version']}")
        for step in result["steps"]:
            print(f"{'OK' if step['ok'] else 'FAIL'} {step['name']}: {step['summary']}")
    return 0 if result["ok"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
