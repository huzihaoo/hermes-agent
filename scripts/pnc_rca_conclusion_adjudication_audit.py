#!/usr/bin/env python3
"""Read-only, recomputable audit of PNC RCA conclusion adjudications."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
import stat
import sys
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from gateway.pnc_rca_conclusion_adjudication import (
    ADJUDICATION_EFFECT_SCHEMA_VERSION,
    ADJUDICATION_SCHEMA_VERSION,
)


AUDIT_SCHEMA_VERSION = "pnc_rca_conclusion_adjudication_audit_v1"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table,),
    ).fetchone() is not None


def _scalar(conn: sqlite3.Connection, sql: str, parameters: tuple[Any, ...] = ()) -> int:
    row = conn.execute(sql, parameters).fetchone()
    return int(row[0] or 0) if row is not None else 0


def audit_conclusion_adjudications(control_db: str | Path) -> dict[str, Any]:
    path = Path(control_db).expanduser()
    if not path.is_absolute():
        raise ValueError("control DB path must be absolute")
    observed = path.lstat()
    if (
        stat.S_ISLNK(observed.st_mode)
        or not stat.S_ISREG(observed.st_mode)
        or observed.st_nlink != 1
        or observed.st_size <= 0
    ):
        raise ValueError("control DB must be a non-empty, single-link regular file")
    wal_path = Path(f"{path}-wal")
    try:
        wal_size = wal_path.lstat().st_size
    except FileNotFoundError:
        wal_size = 0
    if wal_size:
        raise RuntimeError(
            "immutable audit requires a checkpointed database with no WAL bytes"
        )
    uri = f"{path.resolve().as_uri()}?mode=ro&immutable=1"
    conn = sqlite3.connect(uri, uri=True, isolation_level=None)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA query_only=ON")
        conn.execute("BEGIN")
        required_delivery_tables = {
            "rca_delivery_jobs",
            "rca_delivery_effects",
        }
        available = {
            str(row["name"])
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        if not required_delivery_tables.issubset(available):
            raise RuntimeError("delivery tables are unavailable")
        delivery_marker = ""
        if "rca_delivery_meta" in available:
            marker = conn.execute(
                "SELECT value FROM rca_delivery_meta WHERE key = 'schema_version'"
            ).fetchone()
            delivery_marker = str(marker["value"] or "") if marker else ""
        schema_ready = "rca_conclusion_adjudications" in available
        published = _scalar(
            conn,
            """
            SELECT COUNT(*)
              FROM rca_delivery_jobs AS j
              JOIN rca_delivery_effects AS e
                ON e.delivery_id = j.delivery_id
               AND e.effect_kind = 'feishu_issue_comment'
               AND e.target_key = j.target_key
             WHERE j.outcome = 'success'
               AND e.status = 'succeeded'
               AND e.write_phase = 'settled'
            """,
        )
        effect_pattern = (
            f'%"schema_version":"{ADJUDICATION_EFFECT_SCHEMA_VERSION}"%'
        )
        adjudication_effects = _scalar(
            conn,
            "SELECT COUNT(*) FROM rca_delivery_effects WHERE payload_json LIKE ?",
            (effect_pattern,),
        )
        status_rows = conn.execute(
            """
            SELECT status, COUNT(*) AS count
              FROM rca_delivery_effects
             WHERE payload_json LIKE ?
             GROUP BY status ORDER BY status
            """,
            (effect_pattern,),
        ).fetchall()
        adjudication_statuses = {
            str(row["status"]): int(row["count"]) for row in status_rows
        }
        if schema_ready:
            adjudications = _scalar(
                conn, "SELECT COUNT(*) FROM rca_conclusion_adjudications"
            )
            invalidated = _scalar(
                conn,
                "SELECT COUNT(*) FROM rca_conclusion_adjudications "
                "WHERE conclusion_state = 'invalidated'",
            )
            recognized = _scalar(
                conn,
                "SELECT COUNT(*) FROM rca_conclusion_adjudications "
                "WHERE conclusion_state = 'recognized'",
            )
            budget_violations = _scalar(
                conn,
                """
                SELECT COUNT(*) FROM (
                    SELECT business_key
                      FROM rca_conclusion_adjudications
                     GROUP BY business_key HAVING COUNT(*) > 1
                )
                """,
            )
            lineage_unresolved = _scalar(
                conn,
                """
                SELECT COUNT(*) FROM rca_conclusion_adjudications
                 WHERE evaluator_refs_json = '[]'
                    OR responsibility_domain = 'unresolved'
                """,
            )
            dangling = _scalar(
                conn,
                """
                SELECT COUNT(*)
                  FROM rca_conclusion_adjudications AS a
             LEFT JOIN rca_delivery_effects AS e
                    ON e.effect_key = a.correction_effect_key
                 WHERE e.effect_key IS NULL
                """,
            )
            schema_versions = [
                str(row[0])
                for row in conn.execute(
                    "SELECT DISTINCT schema_version "
                    "FROM rca_conclusion_adjudications ORDER BY schema_version"
                ).fetchall()
            ]
        else:
            adjudications = invalidated = recognized = 0
            budget_violations = lineage_unresolved = dangling = 0
            schema_versions = []
        conn.commit()
    finally:
        conn.close()
    db_sha256 = _sha256(path)
    final_observed = path.lstat()
    if (
        final_observed.st_dev != observed.st_dev
        or final_observed.st_ino != observed.st_ino
        or final_observed.st_size != observed.st_size
        or final_observed.st_mtime_ns != observed.st_mtime_ns
    ):
        raise RuntimeError("control DB changed during immutable audit")
    return {
        "schema_version": AUDIT_SCHEMA_VERSION,
        "observed_at": datetime.now(timezone.utc).isoformat(),
        "external_writes": False,
        "sqlite_mode": "ro+immutable",
        "control_db": {
            "path": str(path),
            "device": int(observed.st_dev),
            "inode": int(observed.st_ino),
            "size": int(observed.st_size),
            "mtime_ns": int(observed.st_mtime_ns),
            "sha256": db_sha256,
            "delivery_schema_version": delivery_marker,
            "wal_size": wal_size,
        },
        "adjudication_schema": {
            "ready": schema_ready,
            "expected_version": ADJUDICATION_SCHEMA_VERSION,
            "observed_versions": schema_versions,
        },
        "counts": {
            "published_conclusions": published,
            "adjudications": adjudications,
            "invalidated": invalidated,
            "recognized": recognized,
            "adjudication_effects": adjudication_effects,
            "adjudication_effect_statuses": adjudication_statuses,
            "comment_budget_violations": budget_violations,
            "lineage_unresolved": lineage_unresolved,
            "dangling_correction_effects": dangling,
        },
        "invariants": {
            "ledger_effect_count_equal": adjudications == adjudication_effects,
            "comment_budget_clean": budget_violations == 0,
            "correction_effects_linked": dangling == 0,
        },
        "ga_acceptance_claimed": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--control-db", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    try:
        result = audit_conclusion_adjudications(args.control_db)
    except (OSError, RuntimeError, ValueError, sqlite3.Error) as exc:
        result = {
            "schema_version": AUDIT_SCHEMA_VERSION,
            "observed_at": datetime.now(timezone.utc).isoformat(),
            "external_writes": False,
            "ok": False,
            "error": f"{type(exc).__name__}:{exc}",
        }
    else:
        result["ok"] = True
    encoded = json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(encoded, encoding="utf-8")
    print(encoded, end="")
    return 0 if result.get("ok") else 2


if __name__ == "__main__":
    raise SystemExit(main())
