#!/usr/bin/env python3
"""Read-only RCA requester identity metrics with a post-cutover fail-closed gate."""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from gateway.pnc_rca_requester_identity import classify_rca_requester


def _timestamp(value: str) -> datetime:
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("requester_identity_created_at_invalid") from exc


def _expected_actor(platform: str) -> str | None:
    if platform == "operator":
        return "automation"
    if platform == "feishu":
        return "human"
    return None


def build_report(*, control_db: Path, enforce_after: str) -> dict[str, Any]:
    cutoff = _timestamp(enforce_after)
    uri = f"file:{control_db.absolute()}?mode=ro&immutable=1"
    connection = sqlite3.connect(uri, uri=True, timeout=1)
    connection.row_factory = sqlite3.Row
    try:
        rows = list(
            connection.execute(
                "SELECT rowid, source_id, source_kind, platform, requester_id, "
                "created_at FROM rca_trigger_sources ORDER BY rowid"
            )
        )
    finally:
        connection.close()

    all_counts: Counter[str] = Counter()
    historical_counts: Counter[str] = Counter()
    post_cutover_counts: Counter[str] = Counter()
    violations: list[dict[str, Any]] = []
    manual_count = 0
    for row in rows:
        if str(row["source_kind"] or "") != "feishu_group_manual":
            continue
        manual_count += 1
        actor_kind = classify_rca_requester(str(row["requester_id"] or ""))
        all_counts[actor_kind] += 1
        created_at = _timestamp(str(row["created_at"] or ""))
        if created_at <= cutoff:
            historical_counts[actor_kind] += 1
            continue
        post_cutover_counts[actor_kind] += 1
        platform = str(row["platform"] or "")
        expected = _expected_actor(platform)
        if expected is None or actor_kind != expected:
            violations.append({
                "rowid": int(row["rowid"]),
                "source_id": str(row["source_id"]),
                "platform": platform,
                "actor_kind": actor_kind,
                "created_at": str(row["created_at"]),
            })

    kinds = ("human", "automation", "legacy_automation", "unknown")
    actor_counts = {kind: all_counts[kind] for kind in kinds}
    historical = {kind: historical_counts[kind] for kind in kinds}
    post_cutover = {kind: post_cutover_counts[kind] for kind in kinds}
    return {
        "schema_version": "pnc_rca_requester_identity_report_v1",
        "ok": not violations,
        "control_db": str(control_db.absolute()),
        "enforce_after": enforce_after,
        "manual_trigger_count": manual_count,
        "actor_counts": actor_counts,
        "historical_actor_counts": historical,
        "post_cutover_actor_counts": post_cutover,
        "denominators": {
            "human": actor_counts["human"],
            "automation": actor_counts["automation"],
            "legacy_excluded": actor_counts["legacy_automation"],
            "unknown_excluded": actor_counts["unknown"],
        },
        "post_cutover_violation_count": len(violations),
        "post_cutover_violations": violations,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--control-db", type=Path, required=True)
    parser.add_argument("--enforce-after", required=True)
    args = parser.parse_args()
    try:
        result = build_report(
            control_db=args.control_db,
            enforce_after=args.enforce_after,
        )
    except (OSError, sqlite3.Error, ValueError) as exc:
        result = {
            "schema_version": "pnc_rca_requester_identity_report_v1",
            "ok": False,
            "error": str(exc),
        }
    print(json.dumps(result, ensure_ascii=True, indent=2, sort_keys=True))
    return 0 if result["ok"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
