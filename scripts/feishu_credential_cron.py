#!/usr/bin/env python3
"""Orchestrate Feishu credential health + escalation.

Runs batch-2 health once.  The doc health path already calls keepwarm(), which
is the only intended trigger for feishu-doc ensureValidToken refresh/rotation;
this script does not run keepwarm a second time.  Then it feeds the generated
latest health JSON to batch-3 escalation.  Default is dry-run; launchd passes
--send explicitly.
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

HEALTH_PATH = Path("/Users/songying/.hermes/runtime/shared-state/bin/feishu_credential_health.py")
ESCALATE_PATH = REPO_ROOT / "scripts" / "feishu_credential_escalate.py"
DEFAULT_OUTPUT_DIR = Path("/Users/songying/.hermes/workspace-work/knowledge/outputs/feishu-credential-health")


def load_send_environment() -> list[Path]:
    """Load ~/.hermes/.env for standalone launchd/cron send path.

    Feishu bot credentials live in ~/.hermes/.env.  The gateway loads them at
    process startup; this independent launchd job must do the same before any
    --send escalation calls send_message_tool.
    """
    try:
        from hermes_cli.env_loader import load_hermes_dotenv

        return load_hermes_dotenv()
    except Exception:
        return []


def load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load module {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def orchestrate(*, send: bool = False, output_dir: Path = DEFAULT_OUTPUT_DIR) -> tuple[int, dict[str, Any]]:
    health_mod = load_module(HEALTH_PATH, "feishu_credential_health")
    escalate_mod = load_module(ESCALATE_PATH, "feishu_credential_escalate")

    rows = health_mod.run_health()
    output_path = health_mod.write_output(rows, output_dir)
    escalation = escalate_mod.run(rows, send=send)
    # Health schema intentionally reports current expires_at, not before/after.
    # Do not infer or fake rotation here; P1 rotation evidence comes from a real
    # post-expiry keepwarm run where before/after expiresAt can be compared.
    rotated = {row.get("surface"): None for row in rows}
    non_ok = [row for row in rows if row.get("health") != "OK"]
    escalation_errors = [
        item for item in escalation.get("results", [])
        if item.get("refused") or item.get("sent") is False or item.get("send_result", {}).get("error")
    ]
    payload = {
        "output_path": str(output_path),
        "health_rows": rows,
        "escalation_results": escalation.get("results", []),
        # Batch-1 live rotation evidence is in health rows' doc expires_at delta
        # and the keepwarm output; this boolean is only an observation summary.
        "rotated_observed": rotated,
        "send": send,
    }
    rc = 2 if non_ok or escalation_errors else 0
    return rc, payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run Feishu credential health audit and escalation")
    parser.add_argument("--send", action="store_true", help="Actually send escalation messages; default dry-run")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    args = parser.parse_args(argv)
    try:
        load_send_environment()
        rc, payload = orchestrate(send=args.send, output_dir=Path(args.output_dir))
    except Exception as exc:  # noqa: BLE001 - launchd logs need compact failure.
        payload = {"error_class": type(exc).__name__, "error": str(exc)[:240], "send": args.send}
        rc = 2
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
    else:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
