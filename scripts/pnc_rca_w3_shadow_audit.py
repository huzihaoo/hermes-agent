#!/usr/bin/env python3
"""Read-only audit of durable W3 canonical request and snapshot shadow pairs."""

from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import sqlite3
import stat
from typing import Any, Mapping, Sequence


AUDIT_SCHEMA_VERSION = "pnc_rca_w3_shadow_audit_v1"
MIN_STRICT_REAL_PAIRS = 10
SUPPORTED_CONTROL_SCHEMAS = frozenset({
    "pnc_rca_control_store_v11",
    "pnc_rca_control_store_v12",
})
REQUEST_SCHEMA_VERSION = "pnc_rca_canonical_request_v1"
SNAPSHOT_SCHEMA_VERSION = "pnc_rca_admission_snapshot_v1"
SOURCE_ENVELOPE_SCHEMA_VERSION = "pnc_rca_snapshot_source_envelope_v1"

_REQUEST_FIELDS = frozenset({
    "schema_version",
    "ticket",
    "execution_intent",
    "creation_policy",
    "business_profile",
    "execution_policy",
    "publication_policy",
    "correction_lineage_policy",
})
_TICKET_FIELDS = frozenset({
    "project_key",
    "project_simple_name",
    "work_item_type_key",
    "work_item_id",
    "issue_url",
    "title",
    "title_sha256",
})
_INTENT_FIELDS = frozenset({
    "kind",
    "generation_reason",
    "generation_authorization_evidence_sha256",
})
_POLICY_FIELDS = frozenset({"version", "sha256", "value"})
_POLICY_NAMES = (
    "creation_policy",
    "business_profile",
    "execution_policy",
    "publication_policy",
    "correction_lineage_policy",
)
_SNAPSHOT_FIELDS = frozenset({
    "schema_version",
    "snapshot_id",
    "snapshot_sha256",
    "request_sha256",
    "canonical_request",
    "resolved_admission",
    "execution_admission",
    "write_fence",
})
_RESOLVED_ADMISSION_FIELDS = frozenset({
    "key_version",
    "creation_rule_version",
    "business_key",
    "submission_key",
    "generation",
    "create_once",
    "dedupe_scope",
})
_EXECUTION_ADMISSION_FIELDS = frozenset({
    "activation_epoch_id",
    "activation_ledger_id",
    "decision",
    "reason",
    "state",
    "legacy_unconfigured",
})
_SOURCE_ENVELOPE_FIELDS = frozenset({
    "schema_version",
    "source_envelope_id",
    "source_envelope_sha256",
    "source_authority_sha256",
    "snapshot_id",
    "snapshot_sha256",
    "submission_key",
    "source_id",
    "source_kind",
    "ingress_decision",
    "source_metadata",
    "anchor",
})
_INGRESS_DECISION_FIELDS = frozenset({
    "requested_mode",
    "binding_action",
    "decision",
    "authorization_evidence_sha256",
})
_ANCHOR_FIELDS = frozenset({"issue_target", "thread_target"})
_ALLOWED_DIFF_ROOTS = frozenset({"source_metadata", "anchor"})
_MAX_REPORT_PAIRS = 20
_MAX_REPORT_VIOLATIONS = 50

_REQUIRED_COLUMNS: Mapping[str, frozenset[str]] = {
    "control_meta": frozenset({"key", "value"}),
    "rca_canonical_requests": frozenset({
        "request_sha256",
        "schema_version",
        "ticket_title_sha256",
        "creation_policy_sha256",
        "business_profile_sha256",
        "execution_policy_sha256",
        "publication_policy_sha256",
        "correction_lineage_policy_sha256",
        "generation_reason",
        "generation_authorization_evidence_sha256",
        "canonical_request_json",
        "persisted_at",
    }),
    "rca_admission_snapshots": frozenset({
        "snapshot_sha256",
        "snapshot_id",
        "schema_version",
        "request_sha256",
        "business_key",
        "submission_key",
        "generation",
        "activation_epoch_id",
        "activation_ledger_id",
        "execution_decision",
        "execution_reason",
        "execution_state",
        "legacy_unconfigured",
        "creator_source_envelope_sha256",
        "creator_authority_sha256",
        "creator_source_id",
        "admission_snapshot_json",
        "persisted_at",
    }),
    "rca_snapshot_source_envelopes": frozenset({
        "source_envelope_sha256",
        "source_envelope_id",
        "schema_version",
        "snapshot_sha256",
        "snapshot_id",
        "submission_key",
        "source_authority_sha256",
        "source_id",
        "source_kind",
        "payload_sha256",
        "authorization_evidence_sha256",
        "binding_action",
        "decision",
        "source_metadata_json",
        "anchor_json",
        "ingress_decision_json",
        "source_envelope_json",
        "persisted_at",
    }),
}


class ShadowAuditError(ValueError):
    """One durable W3 record violates the audit contract."""


def _canonical_json_bytes(value: Any) -> bytes:
    def validate(item: Any, path: str = "$") -> None:
        if item is None or isinstance(item, (str, bool, int)):
            return
        if isinstance(item, float):
            if not math.isfinite(item):
                raise ShadowAuditError(f"non_finite_json:{path}")
            return
        if isinstance(item, list):
            for index, child in enumerate(item):
                validate(child, f"{path}/{index}")
            return
        if isinstance(item, dict):
            for key, child in item.items():
                if not isinstance(key, str):
                    raise ShadowAuditError(f"non_string_json_key:{path}")
                validate(child, f"{path}/{key}")
            return
        raise ShadowAuditError(f"unsupported_json_value:{path}")

    validate(value)
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _canonical_json_sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_json_bytes(value)).hexdigest()


def _strict_object(raw: Any, *, field: str) -> dict[str, Any]:
    if not isinstance(raw, str) or raw.startswith("\ufeff"):
        raise ShadowAuditError(f"{field}_json_invalid")

    def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ShadowAuditError(f"{field}_duplicate_json_key")
            result[key] = value
        return result

    def reject_constant(value: str) -> Any:
        raise ShadowAuditError(f"{field}_non_finite_json:{value}")

    try:
        value = json.loads(
            raw,
            object_pairs_hook=unique_object,
            parse_constant=reject_constant,
        )
    except ShadowAuditError:
        raise
    except (TypeError, ValueError, RecursionError) as exc:
        raise ShadowAuditError(f"{field}_json_invalid") from exc
    if not isinstance(value, dict):
        raise ShadowAuditError(f"{field}_json_object_required")
    if _canonical_json_bytes(value) != raw.encode("utf-8"):
        raise ShadowAuditError(f"{field}_json_not_canonical")
    return value


def _exact_fields(value: Any, fields: frozenset[str], *, name: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != fields:
        raise ShadowAuditError(f"{name}_exact_fields_invalid")
    return value


def _text(value: Any, *, field: str, allow_empty: bool = False) -> str:
    if not isinstance(value, str) or value != value.strip():
        raise ShadowAuditError(f"{field}_text_invalid")
    if not value and not allow_empty:
        raise ShadowAuditError(f"{field}_text_invalid")
    return value


def _sha256_value(value: Any, *, field: str, allow_zero: bool = False) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
        or (not allow_zero and value == "0" * 64)
    ):
        raise ShadowAuditError(f"{field}_sha256_invalid")
    return value


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _file_identity(path: Path) -> dict[str, int | str]:
    observed = path.lstat()
    return {
        "path": str(path),
        "device": int(observed.st_dev),
        "inode": int(observed.st_ino),
        "size": int(observed.st_size),
        "mtime_ns": int(observed.st_mtime_ns),
        "ctime_ns": int(observed.st_ctime_ns),
        "links": int(observed.st_nlink),
        "mode": int(observed.st_mode),
    }


def _assert_checkpoint(path: Path) -> tuple[dict[str, int | str], str]:
    if not path.is_absolute():
        raise ShadowAuditError("control_db_absolute_path_required")
    try:
        identity = _file_identity(path)
    except FileNotFoundError as exc:
        raise ShadowAuditError("control_db_missing") from exc
    mode = int(identity["mode"])
    if stat.S_ISLNK(mode):
        raise ShadowAuditError("control_db_symlink_forbidden")
    if not stat.S_ISREG(mode):
        raise ShadowAuditError("control_db_regular_file_required")
    if int(identity["links"]) != 1:
        raise ShadowAuditError("control_db_single_link_required")
    if int(identity["size"]) <= 0:
        raise ShadowAuditError("control_db_nonempty_required")
    for sidecar_name in (f"{path}-wal", f"{path}-shm"):
        sidecar = Path(sidecar_name)
        try:
            sidecar.lstat()
        except FileNotFoundError:
            continue
        raise ShadowAuditError(f"checkpoint_sidecar_forbidden:{sidecar.name}")
    return identity, _file_sha256(path)


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in conn.execute(f'PRAGMA table_info("{table}")')}


def _require_schema(conn: sqlite3.Connection) -> tuple[str, str]:
    schema_sql: dict[str, str] = {}
    for table, required in _REQUIRED_COLUMNS.items():
        row = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        ).fetchone()
        if row is None:
            raise ShadowAuditError(f"required_table_missing:{table}")
        missing = sorted(required - _table_columns(conn, table))
        if missing:
            raise ShadowAuditError(
                f"required_columns_missing:{table}:{','.join(missing)}"
            )
        schema_sql[table] = str(row[0] or "")
    meta = conn.execute(
        "SELECT value FROM control_meta WHERE key='schema_version'"
    ).fetchone()
    control_schema = str(meta[0] if meta is not None else "")
    if control_schema not in SUPPORTED_CONTROL_SCHEMAS:
        raise ShadowAuditError(
            f"control_schema_unsupported:{control_schema or 'missing'}"
        )
    return control_schema, _canonical_json_sha256(schema_sql)


def _diff_paths(
    left: Any, right: Any, path: tuple[str, ...] = ()
) -> list[tuple[str, ...]]:
    if isinstance(left, dict) and isinstance(right, dict):
        result: list[tuple[str, ...]] = []
        for key in sorted(set(left) | set(right)):
            child = path + (key,)
            if key not in left or key not in right:
                result.append(child)
            else:
                result.extend(_diff_paths(left[key], right[key], child))
        return result
    if isinstance(left, list) and isinstance(right, list):
        result = []
        for index in range(max(len(left), len(right))):
            child = path + (str(index),)
            if index >= len(left) or index >= len(right):
                result.append(child)
            else:
                result.extend(_diff_paths(left[index], right[index], child))
        return result
    if type(left) is not type(right) or left != right:
        return [path or ("$",)]
    return []


def _pointer(path: tuple[str, ...]) -> str:
    return "/" + "/".join(part.replace("~", "~0").replace("/", "~1") for part in path)


def _validate_policy(name: str, value: Any) -> dict[str, Any]:
    policy = _exact_fields(value, _POLICY_FIELDS, name=f"request_{name}")
    version = policy["version"]
    body = policy["value"]
    digest = policy["sha256"]
    if (
        not isinstance(version, str)
        or version != version.strip()
        or not version
        or not isinstance(body, dict)
        or not body
        or body.get("state") == "unbound"
    ):
        raise ShadowAuditError(f"request_{name}_invalid")
    _sha256_value(digest, field=f"request_{name}")
    if digest != _canonical_json_sha256({"version": version, "value": body}):
        raise ShadowAuditError(f"request_{name}_hash_mismatch")
    return policy


def _validate_request(row: sqlite3.Row) -> dict[str, Any]:
    request = _exact_fields(
        _strict_object(row["canonical_request_json"], field="canonical_request"),
        _REQUEST_FIELDS,
        name="canonical_request",
    )
    if request["schema_version"] != REQUEST_SCHEMA_VERSION:
        raise ShadowAuditError("canonical_request_schema_invalid")
    request_sha256 = _canonical_json_sha256(request)
    if request_sha256 != row["request_sha256"]:
        raise ShadowAuditError("canonical_request_hash_mismatch")
    ticket = _exact_fields(
        request["ticket"], _TICKET_FIELDS, name="canonical_request_ticket"
    )
    for field in (
        "project_key",
        "project_simple_name",
        "work_item_type_key",
        "work_item_id",
        "issue_url",
        "title",
    ):
        _text(ticket[field], field=f"canonical_request_ticket_{field}")
    title_sha256 = _canonical_json_sha256({"title": ticket["title"]})
    if ticket.get("title_sha256") != title_sha256:
        raise ShadowAuditError("canonical_request_title_hash_mismatch")
    if row["request_row_schema_version"] != request["schema_version"]:
        raise ShadowAuditError("canonical_request_schema_projection_mismatch")
    if row["ticket_title_sha256"] != title_sha256:
        raise ShadowAuditError("canonical_request_title_projection_mismatch")
    for name in _POLICY_NAMES:
        policy = _validate_policy(name, request[name])
        if row[f"{name}_sha256"] != policy["sha256"]:
            raise ShadowAuditError(f"canonical_request_{name}_projection_mismatch")
    intent = _exact_fields(
        request["execution_intent"],
        _INTENT_FIELDS,
        name="canonical_request_intent",
    )
    _text(intent["kind"], field="canonical_request_intent_kind")
    if intent["generation_reason"] not in {"initial", "explicit_user_rerun"}:
        raise ShadowAuditError("canonical_request_generation_reason_invalid")
    generation_authority = intent["generation_authorization_evidence_sha256"]
    if intent["generation_reason"] == "initial":
        if generation_authority is not None:
            raise ShadowAuditError("canonical_request_generation_authority_invalid")
    else:
        _sha256_value(
            generation_authority,
            field="canonical_request_generation_authority",
        )
    if row["generation_reason"] != intent.get("generation_reason"):
        raise ShadowAuditError("canonical_request_generation_reason_mismatch")
    if row["generation_authorization_evidence_sha256"] != intent.get(
        "generation_authorization_evidence_sha256"
    ):
        raise ShadowAuditError("canonical_request_generation_authority_mismatch")
    return request


def _validate_snapshot(
    row: sqlite3.Row,
    request: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    snapshot = _exact_fields(
        _strict_object(row["admission_snapshot_json"], field="admission_snapshot"),
        _SNAPSHOT_FIELDS,
        name="admission_snapshot",
    )
    resolved = _exact_fields(
        snapshot["resolved_admission"],
        _RESOLVED_ADMISSION_FIELDS,
        name="resolved_admission",
    )
    execution = _exact_fields(
        snapshot["execution_admission"],
        _EXECUTION_ADMISSION_FIELDS,
        name="execution_admission",
    )
    for field in (
        "key_version",
        "creation_rule_version",
        "business_key",
        "submission_key",
        "dedupe_scope",
    ):
        _text(resolved[field], field=f"resolved_admission_{field}")
    if (
        not isinstance(resolved["generation"], int)
        or isinstance(resolved["generation"], bool)
        or resolved["generation"] < 1
        or not isinstance(resolved["create_once"], bool)
    ):
        raise ShadowAuditError("resolved_admission_type_invalid")
    _text(
        execution["activation_epoch_id"],
        field="execution_admission_activation_epoch_id",
        allow_empty=True,
    )
    ledger_id = execution["activation_ledger_id"]
    if ledger_id is not None and (
        not isinstance(ledger_id, int) or isinstance(ledger_id, bool) or ledger_id < 1
    ):
        raise ShadowAuditError("execution_admission_ledger_invalid")
    if execution["decision"] not in {"admit", "shadow"}:
        raise ShadowAuditError("execution_admission_decision_invalid")
    _text(execution["reason"], field="execution_admission_reason")
    _text(execution["state"], field="execution_admission_state")
    if not isinstance(execution["legacy_unconfigured"], bool):
        raise ShadowAuditError("execution_admission_legacy_flag_invalid")
    if not isinstance(snapshot["write_fence"], dict):
        raise ShadowAuditError("admission_snapshot_write_fence_invalid")
    if snapshot["schema_version"] != SNAPSHOT_SCHEMA_VERSION:
        raise ShadowAuditError("admission_snapshot_schema_invalid")
    if snapshot["canonical_request"] != request:
        raise ShadowAuditError("forbidden_diff:/execution_core/canonical_request")
    expected_request_sha256 = _canonical_json_sha256(request)
    if snapshot["request_sha256"] != expected_request_sha256:
        raise ShadowAuditError("forbidden_diff:/execution_core/request_sha256")
    identity = {
        key: snapshot[key]
        for key in (
            "schema_version",
            "request_sha256",
            "canonical_request",
            "resolved_admission",
            "execution_admission",
            "write_fence",
        )
    }
    snapshot_sha256 = _canonical_json_sha256(identity)
    if snapshot["snapshot_sha256"] != snapshot_sha256:
        raise ShadowAuditError("admission_snapshot_hash_mismatch")
    if snapshot["snapshot_id"] != f"pnc-rca-snapshot-v1-{snapshot_sha256}":
        raise ShadowAuditError("admission_snapshot_id_mismatch")
    expected_columns = {
        "schema_version": snapshot["schema_version"],
        "snapshot_id": snapshot["snapshot_id"],
        "snapshot_sha256": snapshot["snapshot_sha256"],
        "request_sha256": snapshot["request_sha256"],
        "business_key": resolved["business_key"],
        "submission_key": resolved["submission_key"],
        "generation": resolved["generation"],
        "activation_epoch_id": execution["activation_epoch_id"],
        "activation_ledger_id": execution["activation_ledger_id"],
        "execution_decision": execution["decision"],
        "execution_reason": execution["reason"],
        "execution_state": execution["state"],
        "legacy_unconfigured": int(bool(execution["legacy_unconfigured"])),
    }
    for column, expected in expected_columns.items():
        if row[column] != expected:
            raise ShadowAuditError(f"forbidden_diff:/execution_core/column/{column}")
    execution_core = {
        key: snapshot[key]
        for key in (
            "schema_version",
            "request_sha256",
            "canonical_request",
            "resolved_admission",
            "execution_admission",
        )
    }
    return snapshot, execution_core


def _validate_envelope(
    row: sqlite3.Row,
    *,
    snapshot: Mapping[str, Any],
) -> dict[str, Any]:
    envelope = _exact_fields(
        _strict_object(row["source_envelope_json"], field="source_envelope"),
        _SOURCE_ENVELOPE_FIELDS,
        name="source_envelope",
    )
    metadata = _strict_object(row["source_metadata_json"], field="source_metadata")
    anchor = _exact_fields(
        _strict_object(row["anchor_json"], field="anchor"),
        _ANCHOR_FIELDS,
        name="anchor",
    )
    ingress = _exact_fields(
        _strict_object(row["ingress_decision_json"], field="ingress_decision"),
        _INGRESS_DECISION_FIELDS,
        name="ingress_decision",
    )
    if envelope["schema_version"] != SOURCE_ENVELOPE_SCHEMA_VERSION:
        raise ShadowAuditError("source_envelope_schema_invalid")
    for field in (
        "source_envelope_id",
        "snapshot_id",
        "submission_key",
        "source_id",
    ):
        _text(envelope[field], field=f"source_envelope_{field}")
    for field in (
        "source_envelope_sha256",
        "source_authority_sha256",
        "snapshot_sha256",
    ):
        _sha256_value(envelope[field], field=f"source_envelope_{field}")
    if envelope["source_kind"] not in {
        "kafka_workflow_event",
        "feishu_group_manual",
    }:
        raise ShadowAuditError("source_envelope_source_kind_invalid")
    if ingress["binding_action"] not in {"create", "join"}:
        raise ShadowAuditError("source_envelope_binding_action_invalid")
    if ingress["decision"] not in {"admit", "shadow"}:
        raise ShadowAuditError("source_envelope_decision_invalid")
    _sha256_value(
        ingress["authorization_evidence_sha256"],
        field="source_envelope_authorization_evidence",
    )
    _sha256_value(
        metadata.get("payload_sha256"),
        field="source_envelope_payload",
    )
    _text(anchor["issue_target"], field="source_envelope_anchor_issue_target")
    if anchor["thread_target"] is not None:
        _text(
            anchor["thread_target"],
            field="source_envelope_anchor_thread_target",
        )
    if envelope["source_metadata"] != metadata or envelope["anchor"] != anchor:
        raise ShadowAuditError("source_envelope_projection_mismatch")
    if envelope["ingress_decision"] != ingress:
        raise ShadowAuditError("source_envelope_ingress_projection_mismatch")
    identity = {
        key: envelope[key]
        for key in _SOURCE_ENVELOPE_FIELDS
        if key not in {"source_envelope_id", "source_envelope_sha256"}
    }
    envelope_sha256 = _canonical_json_sha256(identity)
    if envelope["source_envelope_sha256"] != envelope_sha256:
        raise ShadowAuditError("source_envelope_hash_mismatch")
    if envelope["source_envelope_id"] != (
        f"pnc-rca-source-envelope-v1-{envelope_sha256}"
    ):
        raise ShadowAuditError("source_envelope_id_mismatch")
    expected_columns = {
        "schema_version": envelope["schema_version"],
        "source_envelope_id": envelope["source_envelope_id"],
        "source_envelope_sha256": envelope["source_envelope_sha256"],
        "source_authority_sha256": envelope["source_authority_sha256"],
        "snapshot_id": envelope["snapshot_id"],
        "snapshot_sha256": envelope["snapshot_sha256"],
        "submission_key": envelope["submission_key"],
        "source_id": envelope["source_id"],
        "source_kind": envelope["source_kind"],
        "authorization_evidence_sha256": ingress["authorization_evidence_sha256"],
        "binding_action": ingress["binding_action"],
        "decision": ingress["decision"],
    }
    for column, expected in expected_columns.items():
        if row[column] != expected:
            raise ShadowAuditError(f"source_envelope_column_mismatch:{column}")
    if metadata.get("source_kind") != envelope["source_kind"]:
        raise ShadowAuditError("source_envelope_source_kind_mismatch")
    if metadata.get("payload_sha256") != row["payload_sha256"]:
        raise ShadowAuditError("source_envelope_payload_mismatch")
    if (
        envelope["snapshot_id"] != snapshot["snapshot_id"]
        or envelope["snapshot_sha256"] != snapshot["snapshot_sha256"]
        or envelope["submission_key"]
        != snapshot["resolved_admission"]["submission_key"]
    ):
        raise ShadowAuditError("forbidden_diff:/execution_core/source_binding")
    if anchor["issue_target"] != snapshot["canonical_request"]["ticket"]["issue_url"]:
        raise ShadowAuditError("source_envelope_anchor_issue_mismatch")
    return envelope


def _source_projection(envelope: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": envelope["schema_version"],
        "source_envelope_id": envelope["source_envelope_id"],
        "source_envelope_sha256": envelope["source_envelope_sha256"],
        "source_authority_sha256": envelope["source_authority_sha256"],
        "snapshot_id": envelope["snapshot_id"],
        "snapshot_sha256": envelope["snapshot_sha256"],
        "submission_key": envelope["submission_key"],
        "source_id": envelope["source_id"],
        "source_kind": envelope["source_kind"],
        "ingress_decision": envelope["ingress_decision"],
        "transport": envelope["source_metadata"],
    }


def _pair_identity_sha256(business_key: str, generation: int) -> str:
    return _canonical_json_sha256({
        "business_key": business_key,
        "generation": generation,
    })


def audit_w3_shadow(
    control_db: str | Path,
    *,
    strict_acceptance: bool = False,
    min_real_pairs: int = MIN_STRICT_REAL_PAIRS,
) -> dict[str, Any]:
    """Audit all durable W3 request/snapshot pairs without mutating the DB."""

    if not isinstance(strict_acceptance, bool):
        raise ShadowAuditError("strict_acceptance_boolean_required")
    if min_real_pairs < MIN_STRICT_REAL_PAIRS:
        raise ShadowAuditError("min_real_pairs_below_strict_floor")
    path = Path(control_db).expanduser()
    before, before_sha256 = _assert_checkpoint(path)

    uri = f"{path.as_uri()}?mode=ro&immutable=1"
    conn = sqlite3.connect(uri, uri=True, isolation_level=None)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA query_only=ON")
        query_only = int(conn.execute("PRAGMA query_only").fetchone()[0])
        if query_only != 1:
            raise ShadowAuditError("sqlite_query_only_not_enabled")
        conn.execute("BEGIN")
        control_schema, schema_sha256 = _require_schema(conn)
        snapshot_rows = conn.execute(
            """
            SELECT snapshot.*, request.schema_version AS request_row_schema_version,
                   request.ticket_title_sha256,
                   request.creation_policy_sha256,
                   request.business_profile_sha256,
                   request.execution_policy_sha256,
                   request.publication_policy_sha256,
                   request.correction_lineage_policy_sha256,
                   request.generation_reason,
                   request.generation_authorization_evidence_sha256,
                   request.canonical_request_json,
                   request.persisted_at AS request_persisted_at
              FROM rca_admission_snapshots AS snapshot
         LEFT JOIN rca_canonical_requests AS request
                ON request.request_sha256 = snapshot.request_sha256
             ORDER BY snapshot.business_key, snapshot.generation,
                      snapshot.snapshot_sha256
            """
        ).fetchall()
        envelope_rows = conn.execute(
            """
            SELECT * FROM rca_snapshot_source_envelopes
             ORDER BY snapshot_sha256,
                      CASE binding_action WHEN 'create' THEN 0 ELSE 1 END,
                      source_envelope_sha256
            """
        ).fetchall()
        orphan_request_count = int(
            conn.execute(
                """
            SELECT COUNT(*) FROM rca_canonical_requests AS request
             WHERE NOT EXISTS (
                SELECT 1 FROM rca_admission_snapshots AS snapshot
                 WHERE snapshot.request_sha256 = request.request_sha256
             )
            """
            ).fetchone()[0]
        )
        orphan_envelope_count = int(
            conn.execute(
                """
            SELECT COUNT(*) FROM rca_snapshot_source_envelopes AS envelope
             WHERE NOT EXISTS (
                SELECT 1 FROM rca_admission_snapshots AS snapshot
                 WHERE snapshot.snapshot_sha256 = envelope.snapshot_sha256
             )
            """
            ).fetchone()[0]
        )
        conn.rollback()
    finally:
        conn.close()

    after, after_sha256 = _assert_checkpoint(path)
    if before != after or before_sha256 != after_sha256:
        raise RuntimeError("control_db_changed_during_w3_shadow_audit")

    envelopes_by_snapshot: dict[str, list[sqlite3.Row]] = defaultdict(list)
    for row in envelope_rows:
        envelopes_by_snapshot[str(row["snapshot_sha256"])].append(row)
    group_counts: dict[tuple[str, int], int] = defaultdict(int)
    for row in snapshot_rows:
        group_counts[(str(row["business_key"]), int(row["generation"]))] += 1

    violations: list[dict[str, Any]] = []
    pair_summaries: list[dict[str, Any]] = []
    valid_pair_count = 0
    source_comparison_count = 0
    allowed_paths: set[str] = set()
    forbidden_paths: set[str] = set()

    def add_violation(
        pair_identity: str | None,
        code: str,
        *,
        path_value: str | None = None,
    ) -> None:
        item: dict[str, Any] = {"code": code}
        if pair_identity is not None:
            item["pair_identity_sha256"] = pair_identity
        if path_value is not None:
            item["path"] = path_value
            forbidden_paths.add(path_value)
        violations.append(item)

    if orphan_request_count:
        add_violation(None, "orphan_canonical_requests")
    if orphan_envelope_count:
        add_violation(None, "orphan_source_envelopes")

    for row in snapshot_rows:
        business_key = str(row["business_key"])
        generation = int(row["generation"])
        pair_identity = _pair_identity_sha256(business_key, generation)
        pair_errors_before = len(violations)
        if group_counts[(business_key, generation)] != 1:
            add_violation(
                pair_identity,
                "business_generation_pair_not_unique",
                path_value="/execution_core/resolved_admission",
            )
        if row["canonical_request_json"] is None:
            add_violation(pair_identity, "canonical_request_missing")
            continue
        try:
            request = _validate_request(row)
            snapshot, execution_core = _validate_snapshot(row, request)
            source_rows = envelopes_by_snapshot.get(str(row["snapshot_sha256"]), [])
            if not source_rows:
                raise ShadowAuditError("source_envelope_missing")
            envelopes = [
                _validate_envelope(source_row, snapshot=snapshot)
                for source_row in source_rows
            ]
            creators = [
                envelope
                for envelope in envelopes
                if envelope["ingress_decision"]["binding_action"] == "create"
            ]
            if len(creators) != 1:
                raise ShadowAuditError("creator_source_envelope_count_invalid")
            creator = creators[0]
            if (
                creator["source_envelope_sha256"]
                != row["creator_source_envelope_sha256"]
                or creator["source_authority_sha256"] != row["creator_authority_sha256"]
                or creator["source_id"] != row["creator_source_id"]
            ):
                raise ShadowAuditError("creator_source_envelope_binding_mismatch")
            joined_envelopes = [
                envelope for envelope in envelopes if envelope is not creator
            ]
            source_kinds = {str(envelope["source_kind"]) for envelope in envelopes}
            real_pair_qualified = bool(joined_envelopes) and len(source_kinds) >= 2
            baseline = {
                "execution_core": execution_core,
                "source_metadata": _source_projection(creator),
                "anchor": creator["anchor"],
            }
            pair_allowed_paths: set[str] = set()
            pair_forbidden_paths: set[str] = set()
            for envelope in joined_envelopes:
                candidate = {
                    "execution_core": execution_core,
                    "source_metadata": _source_projection(envelope),
                    "anchor": envelope["anchor"],
                }
                source_comparison_count += 1
                for diff in _diff_paths(baseline, candidate):
                    pointer = _pointer(diff)
                    if diff and diff[0] in _ALLOWED_DIFF_ROOTS:
                        pair_allowed_paths.add(pointer)
                    else:
                        pair_forbidden_paths.add(pointer)
            allowed_paths.update(pair_allowed_paths)
            forbidden_paths.update(pair_forbidden_paths)
            for pointer in sorted(pair_forbidden_paths):
                add_violation(
                    pair_identity,
                    "forbidden_projection_diff",
                    path_value=pointer,
                )
            if len(violations) == pair_errors_before and real_pair_qualified:
                valid_pair_count += 1
            pair_summaries.append({
                "pair_identity_sha256": pair_identity,
                "generation": generation,
                "request_sha256": str(row["request_sha256"]),
                "snapshot_sha256": str(row["snapshot_sha256"]),
                "execution_core_sha256": _canonical_json_sha256(execution_core),
                "source_envelope_count": len(envelopes),
                "source_kinds": sorted(source_kinds),
                "real_pair_qualified": real_pair_qualified,
                "allowed_diff_paths": sorted(pair_allowed_paths),
                "forbidden_diff_paths": sorted(pair_forbidden_paths),
            })
        except (KeyError, ShadowAuditError, TypeError, ValueError) as exc:
            code = str(exc) or type(exc).__name__
            path_value = None
            if code.startswith("forbidden_diff:"):
                path_value = code.split(":", 1)[1]
                code = "forbidden_execution_core_diff"
            add_violation(pair_identity, code, path_value=path_value)

    strict_ready = not violations and valid_pair_count >= min_real_pairs
    gate_errors = sorted({str(item["code"]) for item in violations})
    if strict_acceptance and valid_pair_count < min_real_pairs:
        gate_errors.append("too_few_real_pairs")
    gate_errors = sorted(set(gate_errors))
    audit_clean = not violations
    ok = audit_clean and (not strict_acceptance or strict_ready)
    pair_digest_rows = sorted(
        pair_summaries,
        key=lambda item: (
            str(item["pair_identity_sha256"]),
            str(item["snapshot_sha256"]),
        ),
    )
    return {
        "schema_version": AUDIT_SCHEMA_VERSION,
        "observed_at": datetime.now(timezone.utc).isoformat(),
        "sqlite_mode": "mode=ro&immutable=1;query_only=ON",
        "external_writes": False,
        "control_db": {
            **before,
            "sha256": before_sha256,
            "wal_present": False,
            "shm_present": False,
            "identity_unchanged": True,
            "sha256_unchanged": True,
            "control_schema_version": control_schema,
            "required_schema_sha256": schema_sha256,
        },
        "scope": {
            "pair_key": ["business_key", "generation"],
            "candidate_pair_count": len(snapshot_rows),
            "valid_real_pair_count": valid_pair_count,
            "source_envelope_count": len(envelope_rows),
            "source_comparison_count": source_comparison_count,
            "real_pairs_sha256": _canonical_json_sha256(pair_digest_rows),
            "allowed_diff_roots": sorted(_ALLOWED_DIFF_ROOTS),
            "strict_acceptance_requested": strict_acceptance,
            "strict_min_real_pairs": min_real_pairs,
        },
        "counts": {
            "candidate_pairs": len(snapshot_rows),
            "valid_real_pairs": valid_pair_count,
            "forbidden_diff_paths": len(forbidden_paths),
            "violations": len(violations),
            "orphan_canonical_requests": orphan_request_count,
            "orphan_source_envelopes": orphan_envelope_count,
        },
        "pairs": pair_digest_rows[:_MAX_REPORT_PAIRS],
        "allowed_diff_paths": sorted(allowed_paths),
        "forbidden_diff_paths": sorted(forbidden_paths),
        "violations": violations[:_MAX_REPORT_VIOLATIONS],
        "gate_errors": gate_errors,
        "audit_clean": audit_clean,
        "strict_acceptance_ready": strict_ready,
        "ok": ok,
        "ga_acceptance_claimed": False,
        "production_actions_performed": False,
    }


def _paths_refer_to_same_file(left: Path, right: Path) -> bool:
    try:
        if left.resolve(strict=False) == right.resolve(strict=False):
            return True
    except OSError:
        pass
    try:
        return left.exists() and right.exists() and left.samefile(right)
    except OSError:
        return False


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--control-db", type=Path, required=True)
    parser.add_argument("--strict-acceptance", action="store_true")
    parser.add_argument("--min-real-pairs", type=int, default=MIN_STRICT_REAL_PAIRS)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    if args.output and _paths_refer_to_same_file(args.control_db, args.output):
        result = {
            "schema_version": AUDIT_SCHEMA_VERSION,
            "observed_at": datetime.now(timezone.utc).isoformat(),
            "sqlite_mode": "not_opened",
            "external_writes": False,
            "ok": False,
            "error": "ShadowAuditError:output_must_not_replace_control_db",
            "ga_acceptance_claimed": False,
            "production_actions_performed": False,
        }
        print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
        return 2
    try:
        result = audit_w3_shadow(
            args.control_db,
            strict_acceptance=args.strict_acceptance,
            min_real_pairs=args.min_real_pairs,
        )
    except (OSError, RuntimeError, ShadowAuditError, sqlite3.Error) as exc:
        result = {
            "schema_version": AUDIT_SCHEMA_VERSION,
            "observed_at": datetime.now(timezone.utc).isoformat(),
            "sqlite_mode": "mode=ro&immutable=1;query_only=ON",
            "external_writes": False,
            "ok": False,
            "error": f"{type(exc).__name__}:{exc}",
            "ga_acceptance_claimed": False,
            "production_actions_performed": False,
        }
    encoded = json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(encoded, encoding="utf-8")
    print(encoded, end="")
    return 0 if result.get("ok") else 2


if __name__ == "__main__":
    raise SystemExit(main())
