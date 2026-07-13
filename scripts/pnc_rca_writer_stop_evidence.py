#!/usr/bin/env python3
"""Collect fail-closed proof that every RCA store writer is stopped."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Sequence

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.pnc_rca_store_migration_drill import (
    MigrationDrillError,
    collect_writer_stop_evidence,
    write_receipt_atomic,
)


WRITER_STOP_FILENAME = "writer_stop_evidence.json"


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--evidence-dir",
        type=Path,
        required=True,
        help="Release evidence directory; the output filename is fixed.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        evidence = collect_writer_stop_evidence()
        destination = args.evidence_dir.expanduser() / WRITER_STOP_FILENAME
        write_receipt_atomic(destination, evidence)
    except MigrationDrillError as exc:
        print(json.dumps({"ok": False, "code": exc.code}, sort_keys=True))
        return 2
    print(
        json.dumps(
            {
                "ok": True,
                "output": str(destination),
                "evidence": evidence,
            },
            ensure_ascii=True,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
