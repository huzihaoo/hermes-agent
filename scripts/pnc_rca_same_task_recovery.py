#!/usr/bin/env python3
"""Run the authorized generation-7 Host/VM same-task recovery."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from types import SimpleNamespace

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from dotenv import load_dotenv

from gateway.pnc_rca_same_task_resume import (
    AUTHORIZED_BUSINESS_KEY,
    AUTHORIZED_GENERATION,
    AUTHORIZED_ISSUE_ID,
    AUTHORIZED_TASK_ID,
    SUPPORTED_BLOCKER,
    SUPPORTED_OPERATION,
    resume_same_task,
)
from gateway import pnc_rca_same_task_watch_rearm as watch_rearm
from hermes_constants import get_hermes_home


SCHEMA_VERSION = "pnc_rca_same_task_recovery_operator_v1"
DEFAULT_DB_PATH = (
    Path(get_hermes_home())
    / "runtime/pnc_agent/feishu_issue_kafka_rca/control.sqlite3"
)


def _claim() -> SimpleNamespace:
    return SimpleNamespace(
        task_id=AUTHORIZED_TASK_ID,
        submission_key=AUTHORIZED_TASK_ID,
        business_key=AUTHORIZED_BUSINESS_KEY,
        generation=AUTHORIZED_GENERATION,
        work_item_id=AUTHORIZED_ISSUE_ID,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--db-path", type=Path)
    parser.add_argument("--env-file", type=Path)
    parser.add_argument("--defer-seconds", type=int, default=120)
    args = parser.parse_args(argv)
    env_file = args.env_file or Path(get_hermes_home()) / ".env"
    load_dotenv(env_file, override=False, interpolate=False)
    db_path = args.db_path or Path(
        os.environ.get(
            "HERMES_RCA_DELIVERY_COLLECTOR_CONTROL_DB_PATH",
            str(DEFAULT_DB_PATH),
        )
    ).expanduser()
    try:
        before = watch_rearm.preflight(db_path)
        if not args.apply:
            print(
                json.dumps(
                    {
                        "schema_version": SCHEMA_VERSION,
                        "ok": True,
                        "apply": False,
                        "watch": before,
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                )
            )
            return 0
        rearmed = watch_rearm.rearm(
            db_path,
            defer_seconds=args.defer_seconds,
        )
        remediation = resume_same_task(
            _claim(),
            {"kind": SUPPORTED_BLOCKER, "retryable": True},
            {"op": SUPPORTED_OPERATION, "resume_from_stage": "s2_remote_read"},
            90,
        )
        expedited = None
        if remediation.get("success") is True:
            expedited = watch_rearm.expedite(
                db_path,
                rearm_token=rearmed["rearm_token"],
                remediation_result=remediation,
            )
        output = {
            "schema_version": SCHEMA_VERSION,
            "ok": remediation.get("success") is True and expedited is not None,
            "apply": True,
            "before": before,
            "rearmed": rearmed,
            "remediation": remediation,
            "expedited": expedited,
            "external_writes": False,
            "created_task_ids": [],
        }
        print(json.dumps(output, ensure_ascii=False, sort_keys=True))
        return 0 if output["ok"] else 3
    except Exception as exc:
        code = getattr(exc, "code", type(exc).__name__)
        print(
            json.dumps(
                {
                    "schema_version": SCHEMA_VERSION,
                    "ok": False,
                    "apply": bool(args.apply),
                    "error_code": str(code)[:120],
                    "external_writes": False,
                    "created_task_ids": [],
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
