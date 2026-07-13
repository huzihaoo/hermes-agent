#!/usr/bin/env python3
"""Guard G1Q3 RCA request contract drift between host and VM copies."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
from typing import Any

BEGIN = "# === RCA_REQUEST_CONTRACT:BEGIN (do not edit between markers without updating host copy) ==="
END = "# === RCA_REQUEST_CONTRACT:END ==="


def extract_contract_text(path: Path) -> str:
    text = path.read_text(encoding="utf-8")
    try:
        start = text.index(BEGIN) + len(BEGIN)
        stop = text.index(END, start)
    except ValueError as exc:
        raise ValueError(f"contract_markers_missing:{path}") from exc
    segment = text[start:stop]
    if segment.startswith("\n"):
        segment = segment[1:]
    return segment


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def check_contract_drift(
    host_path: Path,
    vm_path: Path,
    *,
    allow_missing: bool = False,
) -> dict[str, Any]:
    """Compare the marked request contract, failing closed by default.

    A missing counterpart is not evidence of parity.  Development diagnostics
    may opt into the historical skip behavior explicitly, but release gates
    must use the default.
    """
    missing = [str(path) for path in (host_path, vm_path) if not path.exists()]
    if missing:
        return {
            "ok": bool(allow_missing),
            "status": "skip" if allow_missing else "contract_unverified",
            "error": "counterpart_missing",
            "missing": missing,
        }

    try:
        host_text = extract_contract_text(host_path)
        vm_text = extract_contract_text(vm_path)
    except ValueError as exc:
        return {"ok": False, "status": "contract_drift", "error": str(exc)}

    host_sha = sha256_text(host_text)
    vm_sha = sha256_text(vm_text)
    if host_sha != vm_sha:
        return {
            "ok": False,
            "status": "contract_drift",
            "host_path": str(host_path),
            "vm_path": str(vm_path),
            "host_sha256": host_sha,
            "vm_sha256": vm_sha,
        }
    return {
        "ok": True,
        "status": "pass",
        "host_path": str(host_path),
        "vm_path": str(vm_path),
        "sha256": host_sha,
    }


def default_host_path() -> Path:
    return Path(__file__).resolve().parents[1] / "gateway" / "pnc_rca_schema.py"


def default_vm_path() -> Path:
    override = os.environ.get("HERMES_G1Q3_RCA_VM_CONTRACT_PATH")
    if override:
        return Path(override)
    return Path(
        "/Users/songying/Mounts/mini_root/data3/yj-evaluation-server/"
        "api/g1q3_rca/scripts/rca_request_contract.py"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", type=Path, default=default_host_path())
    parser.add_argument("--vm", type=Path, default=default_vm_path())
    parser.add_argument(
        "--allow-missing",
        action="store_true",
        help="development-only: report a missing counterpart as a successful skip",
    )
    parser.add_argument("--json", action="store_true", help="emit JSON result")
    args = parser.parse_args()

    result = check_contract_drift(
        args.host,
        args.vm,
        allow_missing=args.allow_missing,
    )
    if args.json:
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    else:
        print(result["status"])
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
