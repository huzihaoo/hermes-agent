"""Append-only host runtime evidence for durable RCA state transitions."""

from __future__ import annotations

from datetime import datetime
import hashlib
import json
import re
import sqlite3
from typing import Any, Mapping

from gateway.pnc_rca_runtime_identity import (
    RUNTIME_IDENTITY_FIELDS,
    runtime_identity_is_valid,
)


HOST_RUNTIME_TRANSITION_SERVICES = frozenset({
    "local.pnc.rca-kafka-consumer",
    "local.pnc.rca-outbox-dispatcher",
    "local.pnc.rca-delivery-collector",
    "local.pnc.rca-delivery-dispatcher",
})
HOST_RUNTIME_TRANSITION_KINDS = frozenset({
    "kafka_ingested",
    "outbox_completed",
    "delivery_created",
    "effect_succeeded",
})
HOST_RUNTIME_TRANSITION_KIND_BY_SERVICE = {
    "local.pnc.rca-kafka-consumer": "kafka_ingested",
    "local.pnc.rca-outbox-dispatcher": "outbox_completed",
    "local.pnc.rca-delivery-collector": "delivery_created",
    "local.pnc.rca-delivery-dispatcher": "effect_succeeded",
}
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


HOST_RUNTIME_TRANSITION_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS rca_host_runtime_transitions (
    transition_id INTEGER PRIMARY KEY AUTOINCREMENT,
    submission_key TEXT NOT NULL,
    business_key TEXT NOT NULL,
    generation INTEGER NOT NULL CHECK(generation >= 1),
    service_label TEXT NOT NULL CHECK(service_label IN (
        'local.pnc.rca-kafka-consumer',
        'local.pnc.rca-outbox-dispatcher',
        'local.pnc.rca-delivery-collector',
        'local.pnc.rca-delivery-dispatcher'
    )),
    transition_kind TEXT NOT NULL CHECK(transition_kind IN (
        'kafka_ingested', 'outbox_completed',
        'delivery_created', 'effect_succeeded'
    )),
    entity_key TEXT NOT NULL,
    runtime_identity_json TEXT NOT NULL,
    runtime_identity_sha256 TEXT NOT NULL,
    transitioned_at TEXT NOT NULL,
    CHECK(
        (service_label = 'local.pnc.rca-kafka-consumer'
            AND transition_kind = 'kafka_ingested')
        OR (service_label = 'local.pnc.rca-outbox-dispatcher'
            AND transition_kind = 'outbox_completed')
        OR (service_label = 'local.pnc.rca-delivery-collector'
            AND transition_kind = 'delivery_created')
        OR (service_label = 'local.pnc.rca-delivery-dispatcher'
            AND transition_kind = 'effect_succeeded')
    ),
    UNIQUE(submission_key, service_label, transition_kind, entity_key)
)
"""
HOST_RUNTIME_TRANSITION_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_rca_host_runtime_transition_submission
    ON rca_host_runtime_transitions(submission_key, transition_id);
"""


def ensure_host_runtime_transition_schema(conn: sqlite3.Connection) -> None:
    # executescript() commits an existing transaction before running. Keep both
    # statements inside the caller's migration or business-state transaction.
    conn.execute(HOST_RUNTIME_TRANSITION_TABLE_SQL)
    conn.execute(HOST_RUNTIME_TRANSITION_INDEX_SQL)


def validate_host_runtime_transition_schema(
    conn: sqlite3.Connection,
    *,
    error_prefix: str,
) -> None:
    """Require the exact candidate table/index contract before serving traffic."""
    expected_columns = {
        "transition_id": ("INTEGER", 0, 1),
        "submission_key": ("TEXT", 1, 0),
        "business_key": ("TEXT", 1, 0),
        "generation": ("INTEGER", 1, 0),
        "service_label": ("TEXT", 1, 0),
        "transition_kind": ("TEXT", 1, 0),
        "entity_key": ("TEXT", 1, 0),
        "runtime_identity_json": ("TEXT", 1, 0),
        "runtime_identity_sha256": ("TEXT", 1, 0),
        "transitioned_at": ("TEXT", 1, 0),
    }
    rows = conn.execute(
        "PRAGMA table_info(rca_host_runtime_transitions)"
    ).fetchall()
    observed_columns = {
        str(row["name"]): (
            str(row["type"]).upper(),
            int(row["notnull"]),
            int(row["pk"]),
        )
        for row in rows
    }
    if observed_columns != expected_columns:
        raise RuntimeError(f"{error_prefix}:host_runtime_transition_columns")

    explicit_index = [
        str(row["name"])
        for row in conn.execute(
            "PRAGMA index_info(idx_rca_host_runtime_transition_submission)"
        ).fetchall()
    ]
    if explicit_index != ["submission_key", "transition_id"]:
        raise RuntimeError(f"{error_prefix}:host_runtime_transition_index")

    expected_unique = [
        "submission_key",
        "service_label",
        "transition_kind",
        "entity_key",
    ]
    unique_indexes = []
    for row in conn.execute(
        "PRAGMA index_list(rca_host_runtime_transitions)"
    ).fetchall():
        if int(row["unique"]) != 1:
            continue
        name = str(row["name"])
        unique_indexes.append([
            str(item["name"])
            for item in conn.execute(
                "SELECT name FROM pragma_index_info(?) ORDER BY seqno",
                (name,),
            ).fetchall()
        ])
    if expected_unique not in unique_indexes:
        raise RuntimeError(f"{error_prefix}:host_runtime_transition_unique")


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )


def canonical_host_runtime_identity(
    value: Mapping[str, Any],
    *,
    service_label: str,
) -> tuple[str, str]:
    identity = dict(value)
    if not runtime_identity_is_valid(identity, service_label=service_label):
        raise ValueError("host runtime identity is invalid")
    encoded = _canonical_json(identity)
    return encoded, hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def insert_host_runtime_transition(
    conn: sqlite3.Connection,
    *,
    submission_key: str,
    business_key: str,
    generation: int,
    service_label: str,
    transition_kind: str,
    entity_key: str,
    runtime_identity: Mapping[str, Any],
    transitioned_at: str,
) -> dict[str, Any]:
    submission = str(submission_key or "").strip()
    business = str(business_key or "").strip()
    entity = str(entity_key or "").strip()
    label = str(service_label or "").strip()
    kind = str(transition_kind or "").strip()
    if (
        not submission
        or len(submission) > 192
        or not business
        or len(business) > 192
        or not entity
        or len(entity) > 512
        or isinstance(generation, bool)
        or not isinstance(generation, int)
        or generation < 1
        or label not in HOST_RUNTIME_TRANSITION_SERVICES
        or kind not in HOST_RUNTIME_TRANSITION_KINDS
        or HOST_RUNTIME_TRANSITION_KIND_BY_SERVICE.get(label) != kind
    ):
        raise ValueError("host runtime transition identity is invalid")
    timestamp = datetime.fromisoformat(str(transitioned_at).replace("Z", "+00:00"))
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise ValueError("host runtime transition timestamp is invalid")
    identity_json, identity_sha256 = canonical_host_runtime_identity(
        runtime_identity,
        service_label=label,
    )
    if float(runtime_identity["process_create_time"]) > timestamp.timestamp():
        raise ValueError("host runtime transition predates its process")
    conn.execute(
        """
        INSERT OR IGNORE INTO rca_host_runtime_transitions(
            submission_key, business_key, generation, service_label,
            transition_kind, entity_key, runtime_identity_json,
            runtime_identity_sha256, transitioned_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            submission,
            business,
            generation,
            label,
            kind,
            entity,
            identity_json,
            identity_sha256,
            timestamp.isoformat(),
        ),
    )
    row = conn.execute(
        """
        SELECT * FROM rca_host_runtime_transitions
         WHERE submission_key = ? AND service_label = ?
           AND transition_kind = ? AND entity_key = ?
        """,
        (submission, label, kind, entity),
    ).fetchone()
    if row is None:
        raise RuntimeError("host runtime transition insert was not durable")
    expected = {
        "submission_key": submission,
        "business_key": business,
        "generation": generation,
        "service_label": label,
        "transition_kind": kind,
        "entity_key": entity,
        "runtime_identity_json": identity_json,
        "runtime_identity_sha256": identity_sha256,
        "transitioned_at": timestamp.isoformat(),
    }
    immutable_identity = {
        "submission_key": submission,
        "business_key": business,
        "generation": generation,
        "service_label": label,
        "transition_kind": kind,
        "entity_key": entity,
    }
    if any(row[key] != value for key, value in immutable_identity.items()):
        raise RuntimeError("host runtime transition conflicts with durable evidence")
    return {
        **{key: row[key] for key in expected},
        "runtime_identity": json.loads(str(row["runtime_identity_json"])),
    }
