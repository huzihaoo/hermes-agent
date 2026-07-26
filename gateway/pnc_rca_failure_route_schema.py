"""Dependency-light canonical contract for the durable W2 failure-route table.

This module intentionally imports only the standard library so read-only audit
tools can validate the route schema without importing Hermes runtime services.
"""

from __future__ import annotations

import sqlite3
from typing import Any


FAILURE_ROUTE_REQUIRED_COLUMNS = frozenset({
    "route_key",
    "dedupe_key",
    "submission_key",
    "business_key",
    "generation",
    "task_id",
    "terminal_error_code",
    "lane",
    "route_kind",
    "owner",
    "status",
    "work_started_at",
    "deadline_at",
    "first_observed_at",
    "last_observed_at",
    "remediation_attempt_count",
    "observation_count",
    "retry_count",
    "remediation_attempted_at",
    "remediation_result_json",
    "next_retry_at",
    "retry_exhausted",
    "audit_json",
    "route_payload_json",
    "completed_at",
    "created_at",
    "updated_at",
})
FAILURE_ROUTE_TABLE_INFO_CONTRACT = (
    ("route_key", "TEXT", 0, None, 1),
    ("dedupe_key", "TEXT", 1, None, 0),
    ("submission_key", "TEXT", 1, None, 0),
    ("business_key", "TEXT", 1, None, 0),
    ("generation", "INTEGER", 1, None, 0),
    ("task_id", "TEXT", 1, None, 0),
    ("terminal_error_code", "TEXT", 1, None, 0),
    ("lane", "TEXT", 1, None, 0),
    ("route_kind", "TEXT", 1, None, 0),
    ("owner", "TEXT", 1, None, 0),
    ("status", "TEXT", 1, None, 0),
    ("work_started_at", "TEXT", 1, None, 0),
    ("deadline_at", "TEXT", 1, None, 0),
    ("first_observed_at", "TEXT", 1, None, 0),
    ("last_observed_at", "TEXT", 1, None, 0),
    ("remediation_attempt_count", "INTEGER", 1, "0", 0),
    ("observation_count", "INTEGER", 1, "1", 0),
    ("retry_count", "INTEGER", 1, "0", 0),
    ("remediation_attempted_at", "TEXT", 0, None, 0),
    ("remediation_result_json", "TEXT", 1, "'{}'", 0),
    ("next_retry_at", "TEXT", 0, None, 0),
    ("retry_exhausted", "INTEGER", 1, "0", 0),
    ("audit_json", "TEXT", 1, None, 0),
    ("route_payload_json", "TEXT", 1, None, 0),
    ("completed_at", "TEXT", 0, None, 0),
    ("created_at", "TEXT", 1, None, 0),
    ("updated_at", "TEXT", 1, None, 0),
)
FAILURE_ROUTE_INDEX_CONTRACT = frozenset({
    ("idx_failure_routes_submission", 0, "c", 0, ("submission_key", "created_at")),
    ("idx_failure_routes_status", 0, "c", 0, ("status", "owner", "deadline_at")),
    (None, 1, "u", 0, ("dedupe_key",)),
    (None, 1, "pk", 0, ("route_key",)),
})
FAILURE_ROUTE_FOREIGN_KEY_CONTRACT = (
    (
        "rca_execution_watch",
        "submission_key",
        "submission_key",
        "NO ACTION",
        "NO ACTION",
        "NONE",
    ),
)
FAILURE_ROUTE_REQUIRED_CHECKS = frozenset({
    "check(generation>=1)",
    "check(lanein('infra_self_healable','needs_human_input','hard_defect'))",
    "check(route_kindin('infra_remediation_hold','internal_backlog','internal_alert'))",
    "check(statusin('remediation_pending','remediation_started','remediation_succeeded','remediation_held','backlog_pending','alert_pending','terminal_fallback','resolved'))",
    "check(remediation_attempt_countbetween0and1)",
    "check(observation_count>=1)",
    "check(retry_count>=0)",
    "check(retry_exhaustedin(0,1))",
})


def failure_route_schema_errors(conn: sqlite3.Connection) -> list[str]:
    """Return deterministic schema defects without querying route payloads."""

    table_info = list(conn.execute("PRAGMA table_info(rca_failure_routes)"))
    columns = {str(row[1]) for row in table_info}
    errors: list[str] = []
    missing_columns = sorted(FAILURE_ROUTE_REQUIRED_COLUMNS - columns)
    if missing_columns:
        errors.append(f"missing_columns:{','.join(missing_columns)}")
    observed_table_info = tuple(
        (
            str(row[1]),
            str(row[2]).upper(),
            int(row[3]),
            row[4],
            int(row[5]),
        )
        for row in table_info
    )
    if observed_table_info != FAILURE_ROUTE_TABLE_INFO_CONTRACT:
        errors.append("table_info_contract")

    observed_indexes: set[tuple[Any, ...]] = set()
    for index in conn.execute("PRAGMA index_list(rca_failure_routes)"):
        origin = str(index[3])
        indexed_columns = tuple(
            str(row[0])
            for row in conn.execute(
                "SELECT name FROM pragma_index_info(?) ORDER BY seqno",
                (str(index[1]),),
            )
        )
        observed_indexes.add((
            str(index[1]) if origin == "c" else None,
            int(index[2]),
            origin,
            int(index[4]),
            indexed_columns,
        ))
    if observed_indexes != FAILURE_ROUTE_INDEX_CONTRACT:
        errors.append("index_contract")

    observed_foreign_keys = tuple(
        (
            str(row[2]),
            str(row[3]),
            str(row[4]),
            str(row[5]).upper(),
            str(row[6]).upper(),
            str(row[7]).upper(),
        )
        for row in conn.execute("PRAGMA foreign_key_list(rca_failure_routes)")
    )
    if observed_foreign_keys != FAILURE_ROUTE_FOREIGN_KEY_CONTRACT:
        errors.append("foreign_key_contract")

    schema_row = conn.execute(
        "SELECT sql FROM sqlite_master "
        "WHERE type = 'table' AND name = 'rca_failure_routes'"
    ).fetchone()
    normalized_sql = "".join(str(schema_row[0] if schema_row else "").split()).lower()
    if normalized_sql.count("check(") != len(FAILURE_ROUTE_REQUIRED_CHECKS) or not all(
        required in normalized_sql for required in FAILURE_ROUTE_REQUIRED_CHECKS
    ):
        errors.append("check_constraints_contract")
    return errors
