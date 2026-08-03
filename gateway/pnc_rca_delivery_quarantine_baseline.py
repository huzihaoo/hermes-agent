"""Fail-closed acknowledgement for exact historical RCA delivery quarantine rows."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
from typing import Any, Mapping

from gateway.pnc_rca_delivery_contract import (
    DELIVERY_THREAD_EFFECT_KIND,
    DeliveryContractError,
    validate_delivery_subscription_target,
)
from gateway.pnc_rca_delivery_quarantine_migration import (
    COMBINED_SCHEMA_VERSION,
    QuarantineMigrationError,
    validate_combined_migration_receipt,
    validate_migration_receipt,
)
from gateway.pnc_rca_prod_bootstrap import (
    ACTIVE_RELEASE_BINDING_NAME,
    EPOCH_ID_RE,
    RcaBootstrapAuthorizationError,
    load_active_release_binding,
)


CORE_SCHEMA_VERSION = "pnc_rca_delivery_quarantine_core_v1"
RELEASE_MANIFEST_SCHEMA_VERSION = "pnc_rca_release_prepare_manifest_v1"
APPROVAL_SCHEMA_VERSION = "pnc_rca_release_approval_v1"
BASELINE_SCHEMA_VERSION = "pnc_rca_delivery_quarantine_baseline_v1"
BASELINE_PATH_ENV = "HERMES_RCA_DELIVERY_QUARANTINE_BASELINE_PATH"
BASELINE_SHA256_ENV = "HERMES_RCA_DELIVERY_QUARANTINE_BASELINE_SHA256"
PROD_RELEASE_ID_ENV = "HERMES_RCA_PROD_RELEASE_ID"
PROD_BOOTSTRAP_EPOCH_ID_ENV = "HERMES_RCA_PROD_BOOTSTRAP_EPOCH_ID"
BASELINE_NAME = "delivery-quarantine-baseline.json"
ROW_DIGEST_ALGORITHM = "sha256-canonical-json-lines-v1"
MAX_BASELINE_BYTES = 1024 * 1024
MAX_EVIDENCE_BYTES = 4 * 1024 * 1024
MAX_BACKUP_BYTES = 64 * 1024 * 1024
MAX_SETTLEMENT_RECEIPTS = 32

_HEX64_RE = re.compile(r"^[0-9a-f]{64}$")
_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{7,127}$")
_QUARANTINE_TABLES = (
    ("jobs", "rca_delivery_jobs", "delivery_id"),
    ("effects", "rca_delivery_effects", "effect_key"),
    ("subscriptions", "rca_delivery_subscriptions", "subscription_key"),
)
_DB_IDENTITY_FIELDS = frozenset({
    "path",
    "control_schema_version",
    "delivery_schema_version",
    "activation_db_logical_identity_sha256",
    "logical_identity_sha256",
})
_SNAPSHOT_FIELDS = frozenset({
    "digest_algorithm",
    "counts",
    "row_set_sha256",
    "snapshot_sha256",
})
_CORE_FIELDS = frozenset({
    "schema_version",
    "release_id",
    "snapshot_at",
    "control_db",
    "migration_binding",
    "quarantine_snapshot",
    "quarantine_event_projection",
    "settlement_receipts",
    "effect_settlement",
    "invalid_manual_thread_adjudication",
    "issuance_policy",
    "core_sha256",
})
_MIGRATION_BINDING_FIELDS = frozenset({
    "receipt_path",
    "receipt_sha256",
    "source_backup_sha256",
    "source_logical_sha256",
    "post_migration_logical_sha256",
    "migration_runtime_sha256",
    "target_live_db_path",
})
_BASELINE_FIELDS = frozenset({
    "schema_version",
    "baseline_id",
    "issued_at",
    "release_id",
    "quarantine_core",
    "quarantine_core_sha256",
    "release_manifest",
    "owner_attestation",
    "baseline_fingerprint",
})
_RECEIPT_FIELDS = frozenset({"path", "sha256"})
_RELEASE_BINDING_FIELDS = frozenset({"path", "sha256", "release_bom_sha256"})
_ADJUDICATION_FIELDS = frozenset({
    "effect_kind",
    "reason_code",
    "technical_finding",
    "proposed_disposition",
    "required",
    "source_kind",
    "source_mode",
    "count",
    "analyzed_by",
    "analyzed_at",
    "reason",
})
_APPROVAL_REQUIRED_FIELDS = frozenset({
    "schema_version",
    "release_id",
    "release_bom_sha256",
    "quarantine_core_sha256",
    "decision",
    "identity",
    "created_at",
})
_ATTESTATION_FIELDS = frozenset({
    "decision",
    "release_id",
    "quarantine_core_sha256",
    "release_bom_sha256",
    "release_manifest_sha256",
    "approval_evidence_path",
    "approval_evidence_sha256",
    "approved_by",
    "approved_at",
    "reason",
    "no_database_rows_modified",
    "no_rearm",
    "no_external_writes",
})
_RELEASE_MANIFEST_REQUIRED_FIELDS = frozenset({
    "schema_version",
    "release_id",
    "release_bom_sha256",
    "quarantine_core_sha256",
    "created_at",
    "complete",
    "side_effect_contract",
})
_ISSUANCE_POLICY = {
    "requires_approval_evidence": True,
    "approval_decision": "approved",
    "bom_binding": "quarantine_core_sha256",
    "active_binding": "final_baseline_file_sha256",
    "no_database_mutation": True,
    "no_rearm": True,
}


class DeliveryQuarantineBaselineError(RuntimeError):
    """Stable fail-closed error for an external quarantine acknowledgement."""

    def __init__(self, code: str):
        self.code = str(code or "delivery_quarantine_baseline_invalid")[:120]
        super().__init__(self.code)


@dataclass(frozen=True)
class QuarantineBaselineSettings:
    baseline_path: Path
    baseline_sha256: str
    release_id: str
    bootstrap_epoch_id: str
    active_release_binding_path: Path
    live_env_path: Path


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("datetime values must be timezone-aware")
    return value.astimezone(timezone.utc)


def _iso(value: datetime) -> str:
    return _utc(value).isoformat()


def _parse_iso(value: Any) -> datetime:
    parsed = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
    return _utc(parsed)


def _canonical_bytes(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError, RecursionError) as exc:
        raise DeliveryQuarantineBaselineError(
            "delivery_quarantine_artifact_not_canonical"
        ) from exc


def canonical_quarantine_core_bytes(value: Mapping[str, Any]) -> bytes:
    """Return deterministic core bytes suitable for a release request or BOM."""

    return _canonical_bytes(dict(value)) + b"\n"


def canonical_quarantine_release_manifest_bytes(
    value: Mapping[str, Any],
) -> bytes:
    """Return exact release manifest/BOM bytes that bind the core SHA."""

    return _canonical_bytes(dict(value)) + b"\n"


def canonical_quarantine_baseline_bytes(value: Mapping[str, Any]) -> bytes:
    """Return the exact final baseline bytes whose SHA is bound in active config."""

    return _canonical_bytes(dict(value)) + b"\n"


def _strict_json(raw: bytes, *, artifact: str) -> Mapping[str, Any]:
    def unique(items: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in items:
            if key in result:
                raise DeliveryQuarantineBaselineError(f"{artifact}_duplicate_key")
            result[key] = value
        return result

    def invalid_number(_value: str) -> None:
        raise DeliveryQuarantineBaselineError(f"{artifact}_number_invalid")

    try:
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=unique,
            parse_constant=invalid_number,
        )
    except DeliveryQuarantineBaselineError:
        raise
    except (UnicodeError, json.JSONDecodeError, RecursionError) as exc:
        raise DeliveryQuarantineBaselineError(f"{artifact}_json_invalid") from exc
    if not isinstance(value, Mapping):
        raise DeliveryQuarantineBaselineError(f"{artifact}_shape_invalid")
    return value


def _stat_identity(value: os.stat_result) -> tuple[int, ...]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_uid,
        value.st_gid,
        value.st_nlink,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _read_stable_file(
    path: str | Path,
    *,
    artifact: str,
    maximum_bytes: int,
    owner_only: bool,
    allowed_modes: frozenset[int] | None = None,
    require_owner: bool = False,
) -> tuple[bytes, str]:
    selected = Path(path).expanduser().absolute()
    if not hasattr(os, "O_NOFOLLOW"):
        raise DeliveryQuarantineBaselineError(f"{artifact}_no_follow_unavailable")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | os.O_NOFOLLOW
    try:
        descriptor = os.open(selected, flags)
    except OSError as exc:
        raise DeliveryQuarantineBaselineError(f"{artifact}_unavailable") from exc
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
            or before.st_size < 2
            or before.st_size > maximum_bytes
            or (
                owner_only
                and (
                    stat.S_IMODE(before.st_mode) != 0o600
                    or before.st_uid != os.geteuid()
                )
            )
            or (
                allowed_modes is not None
                and stat.S_IMODE(before.st_mode) not in allowed_modes
            )
            or (require_owner and before.st_uid != os.geteuid())
        ):
            raise DeliveryQuarantineBaselineError(f"{artifact}_permissions_invalid")
        chunks: list[bytes] = []
        total = 0
        while total <= maximum_bytes:
            chunk = os.read(descriptor, min(65536, maximum_bytes + 1 - total))
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
        after = os.fstat(descriptor)
        try:
            lexical = os.lstat(selected)
        except OSError as exc:
            raise DeliveryQuarantineBaselineError(f"{artifact}_unstable") from exc
        if (
            total > maximum_bytes
            or stat.S_ISLNK(lexical.st_mode)
            or (after.st_dev, after.st_ino) != (lexical.st_dev, lexical.st_ino)
            or _stat_identity(before) != _stat_identity(after)
        ):
            raise DeliveryQuarantineBaselineError(f"{artifact}_unstable")
        raw = b"".join(chunks)
        return raw, hashlib.sha256(raw).hexdigest()
    except DeliveryQuarantineBaselineError:
        raise
    except OSError as exc:
        raise DeliveryQuarantineBaselineError(f"{artifact}_unstable") from exc
    finally:
        os.close(descriptor)


def quarantine_baseline_settings(
    env: Mapping[str, str],
    *,
    hermes_home: str | Path,
    control_db_path: str | Path,
) -> QuarantineBaselineSettings:
    """Resolve the common collector/dispatcher baseline configuration."""

    home = Path(hermes_home).expanduser().absolute()
    control_path = Path(control_db_path).expanduser().absolute()
    raw_path = str(
        env.get(BASELINE_PATH_ENV, control_path.parent / BASELINE_NAME)
    ).strip()
    if not raw_path:
        raise ValueError(f"{BASELINE_PATH_ENV} is required")
    selected = Path(raw_path).expanduser()
    if not selected.is_absolute():
        selected = home / selected
    expected_sha256 = str(env.get(BASELINE_SHA256_ENV, "")).strip().lower()
    if expected_sha256 and _HEX64_RE.fullmatch(expected_sha256) is None:
        raise ValueError(f"{BASELINE_SHA256_ENV} must be a lowercase SHA-256")
    release_id = str(env.get(PROD_RELEASE_ID_ENV, "")).strip()
    bootstrap_epoch_id = str(env.get(PROD_BOOTSTRAP_EPOCH_ID_ENV, "")).strip()
    if expected_sha256 and (
        _IDENTIFIER_RE.fullmatch(release_id) is None
        or EPOCH_ID_RE.fullmatch(bootstrap_epoch_id) is None
    ):
        raise ValueError(
            "configured delivery quarantine baseline requires canonical "
            "production release and bootstrap epoch ids"
        )
    return QuarantineBaselineSettings(
        baseline_path=selected.absolute(),
        baseline_sha256=expected_sha256,
        release_id=release_id,
        bootstrap_epoch_id=bootstrap_epoch_id,
        active_release_binding_path=control_path.parent / ACTIVE_RELEASE_BINDING_NAME,
        live_env_path=home / ".env",
    )


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return (
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            (table,),
        ).fetchone()
        is not None
    )


def _meta_value(conn: sqlite3.Connection, table: str, key: str) -> str:
    if not _table_exists(conn, table):
        raise DeliveryQuarantineBaselineError(
            "delivery_quarantine_baseline_db_identity_invalid"
        )
    row = conn.execute(f"SELECT value FROM {table} WHERE key = ?", (key,)).fetchone()
    value = str(row[0] if row is not None else "").strip()
    if not value:
        raise DeliveryQuarantineBaselineError(
            "delivery_quarantine_baseline_db_identity_invalid"
        )
    return value


def _db_identity(conn: sqlite3.Connection, db_path: str | Path) -> dict[str, Any]:
    activation_sha256 = ""
    if _table_exists(conn, "rca_activation_epochs"):
        columns = {
            str(row[1])
            for row in conn.execute("PRAGMA table_info(rca_activation_epochs)")
        }
        if {"is_current", "db_logical_identity_sha256"}.issubset(columns):
            rows = conn.execute(
                "SELECT db_logical_identity_sha256 FROM rca_activation_epochs "
                "WHERE is_current = 1"
            ).fetchall()
            if len(rows) > 1:
                raise DeliveryQuarantineBaselineError(
                    "delivery_quarantine_baseline_db_identity_invalid"
                )
            if rows:
                activation_sha256 = str(rows[0][0] or "").strip().lower()
                if _HEX64_RE.fullmatch(activation_sha256) is None:
                    raise DeliveryQuarantineBaselineError(
                        "delivery_quarantine_baseline_db_identity_invalid"
                    )
    body = {
        "path": str(Path(db_path).expanduser().absolute()),
        "control_schema_version": _meta_value(conn, "control_meta", "schema_version"),
        "delivery_schema_version": _meta_value(
            conn, "rca_delivery_meta", "schema_version"
        ),
        "activation_db_logical_identity_sha256": activation_sha256,
    }
    return {
        **body,
        "logical_identity_sha256": hashlib.sha256(_canonical_bytes(body)).hexdigest(),
    }


def _db_identity_matches_baseline(
    baseline_identity: Mapping[str, Any],
    current_identity: Mapping[str, Any],
) -> bool:
    """Compare immutable store identity while allowing safe-off epoch binding.

    A quarantine baseline is issued before ``activation.create``.  That
    mutation adds the current epoch's logical identity hash, so the baseline
    necessarily has an empty activation binding while the live snapshot has a
    valid one.  The data/schema/path portions remain exact; the reverse
    transition or a different non-empty epoch hash is rejected.
    """
    if (
        set(baseline_identity) != _DB_IDENTITY_FIELDS
        or set(current_identity) != _DB_IDENTITY_FIELDS
    ):
        return False
    immutable_fields = _DB_IDENTITY_FIELDS - {
        "activation_db_logical_identity_sha256",
        "logical_identity_sha256",
    }
    for field in immutable_fields:
        if baseline_identity.get(field) != current_identity.get(field):
            return False

    def verified_activation(identity: Mapping[str, Any]) -> str | None:
        activation = identity.get("activation_db_logical_identity_sha256")
        if not isinstance(activation, str):
            return None
        if activation and _HEX64_RE.fullmatch(activation) is None:
            return None
        body = {
            "path": identity.get("path"),
            "control_schema_version": identity.get("control_schema_version"),
            "delivery_schema_version": identity.get("delivery_schema_version"),
            "activation_db_logical_identity_sha256": activation,
        }
        logical_identity = identity.get("logical_identity_sha256")
        expected = hashlib.sha256(_canonical_bytes(body)).hexdigest()
        if (
            not isinstance(logical_identity, str)
            or _HEX64_RE.fullmatch(logical_identity) is None
            or logical_identity != expected
        ):
            return None
        return activation

    baseline_activation = verified_activation(baseline_identity)
    current_activation = verified_activation(current_identity)
    if baseline_activation is None or current_activation is None:
        return False
    if baseline_activation == current_activation:
        return True
    return baseline_activation == "" and bool(current_activation)


def _row_set_digest(
    conn: sqlite3.Connection, *, table: str, primary_key: str
) -> tuple[int, str]:
    columns = tuple(
        str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})").fetchall()
    )
    if not columns or primary_key not in columns or "status" not in columns:
        raise DeliveryQuarantineBaselineError(
            "delivery_quarantine_baseline_snapshot_schema_invalid"
        )
    digest = hashlib.sha256()
    digest.update(
        _canonical_bytes({
            "columns": list(columns),
            "order_by": primary_key,
            "predicate": "status=quarantined",
            "table": table,
        })
        + b"\n"
    )
    count = 0
    for row in conn.execute(
        f"SELECT * FROM {table} WHERE status = 'quarantined' ORDER BY {primary_key}"
    ):
        digest.update(
            _canonical_bytes({column: row[column] for column in columns}) + b"\n"
        )
        count += 1
    return count, digest.hexdigest()


def quarantine_snapshot(conn: sqlite3.Connection) -> dict[str, Any]:
    """Digest every column of every quarantined delivery row in this transaction."""

    counts: dict[str, int] = {}
    row_set_sha256: dict[str, str] = {}
    for name, table, primary_key in _QUARANTINE_TABLES:
        count, digest = _row_set_digest(conn, table=table, primary_key=primary_key)
        counts[name] = count
        row_set_sha256[name] = digest
    body = {
        "digest_algorithm": ROW_DIGEST_ALGORITHM,
        "counts": counts,
        "row_set_sha256": row_set_sha256,
    }
    return {
        **body,
        "snapshot_sha256": hashlib.sha256(_canonical_bytes(body)).hexdigest(),
    }


def _quarantine_event_projection(conn: sqlite3.Connection) -> dict[str, Any]:
    table = "rca_delivery_quarantine_mutation_audit"
    expected_triggers = {
        "trg_rca_quarantine_audit_no_update",
        "trg_rca_quarantine_audit_no_delete",
        "trg_rca_delivery_job_quarantine_insert",
        "trg_rca_delivery_job_quarantine_update",
        "trg_rca_delivery_job_quarantine_delete",
        "trg_rca_delivery_effect_quarantine_insert",
        "trg_rca_delivery_effect_quarantine_update",
        "trg_rca_delivery_effect_quarantine_delete",
        "trg_rca_delivery_subscription_quarantine_insert",
        "trg_rca_delivery_subscription_quarantine_update",
        "trg_rca_delivery_subscription_quarantine_delete",
    }
    columns = tuple(str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})"))
    triggers = {
        str(row["name"]): str(row["sql"] or "")
        for row in conn.execute(
            "SELECT name, sql FROM sqlite_master "
            "WHERE type = 'trigger' AND tbl_name IN (?, 'rca_delivery_jobs', "
            "'rca_delivery_effects', 'rca_delivery_subscriptions')",
            (table,),
        )
        if str(row["name"]) in expected_triggers
    }
    if (
        columns
        != (
            "audit_id",
            "entity_kind",
            "entity_key",
            "operation",
            "old_status",
            "new_status",
            "observed_at",
        )
        or set(triggers) != expected_triggers
    ):
        raise DeliveryQuarantineBaselineError(
            "delivery_quarantine_event_audit_unavailable"
        )
    digest = hashlib.sha256()
    count = 0
    maximum = 0
    observed_identities: set[tuple[str, str]] = set()
    for row in conn.execute(f"SELECT * FROM {table} ORDER BY audit_id"):
        payload = {column: row[column] for column in columns}
        digest.update(_canonical_bytes(payload) + b"\n")
        count += 1
        maximum = int(row["audit_id"])
        observed_identities.add((str(row["entity_kind"]), str(row["entity_key"])))
    sequence_row = conn.execute(
        "SELECT seq FROM sqlite_sequence WHERE name = ?", (table,)
    ).fetchone()
    sequence = int(sequence_row[0]) if sequence_row is not None else 0
    if sequence != maximum:
        raise DeliveryQuarantineBaselineError(
            "delivery_quarantine_event_audit_sequence_invalid"
        )
    current_identities = {
        ("job", str(row[0]))
        for row in conn.execute(
            "SELECT delivery_id FROM rca_delivery_jobs WHERE status = 'quarantined'"
        )
    }
    current_identities.update(
        ("effect", str(row[0]))
        for row in conn.execute(
            "SELECT effect_key FROM rca_delivery_effects WHERE status = 'quarantined'"
        )
    )
    current_identities.update(
        ("subscription", str(row[0]))
        for row in conn.execute(
            "SELECT subscription_key FROM rca_delivery_subscriptions "
            "WHERE status = 'quarantined'"
        )
    )
    if not current_identities.issubset(observed_identities):
        raise DeliveryQuarantineBaselineError(
            "delivery_quarantine_event_audit_coverage_invalid"
        )
    return {
        "schema_version": "pnc_rca_delivery_quarantine_event_projection_v1",
        "event_count": count,
        "entity_counts": {
            name: sum(1 for kind, _key in observed_identities if kind == name[:-1])
            for name, _table, _key in _QUARANTINE_TABLES
        },
        "max_audit_id": maximum,
        "sqlite_sequence": sequence,
        "events_sha256": digest.hexdigest(),
        "trigger_set_sha256": hashlib.sha256(_canonical_bytes(triggers)).hexdigest(),
    }


def _quarantine_lifetime_counts(
    conn: sqlite3.Connection,
    current: Mapping[str, int],
) -> dict[str, int]:
    counts = {name: int(current[name]) for name, _table, _key in _QUARANTINE_TABLES}
    table = "rca_delivery_quarantine_mutation_audit"
    if not _table_exists(conn, table):
        return counts
    known = {name[:-1]: name for name, _table, _key in _QUARANTINE_TABLES}
    for row in conn.execute(
        f"SELECT entity_kind, COUNT(DISTINCT entity_key) AS entity_count "
        f"FROM {table} GROUP BY entity_kind"
    ):
        plural = known.get(str(row["entity_kind"]))
        if plural is None:
            raise DeliveryQuarantineBaselineError(
                "delivery_quarantine_event_audit_identity_invalid"
            )
        counts[plural] = max(counts[plural], int(row["entity_count"]))
    return counts


def _invalid_manual_thread_projection(conn: sqlite3.Connection) -> dict[str, int]:
    total = 0
    invalid = 0
    rows = conn.execute(
        """
        SELECT subscription.*, job.project_key, job.work_item_type_key,
               job.work_item_id, source.source_kind, source.mode
          FROM rca_delivery_subscriptions AS subscription
          LEFT JOIN rca_delivery_jobs AS job
            ON job.delivery_id = subscription.delivery_id
          LEFT JOIN rca_trigger_sources AS source
            ON source.source_id = subscription.source_id
         WHERE subscription.status = 'quarantined'
         ORDER BY subscription.subscription_key
        """
    ).fetchall()
    for row in rows:
        total += 1
        if (
            int(row["required"]) != 1
            or str(row["effect_kind"]) != DELIVERY_THREAD_EFFECT_KIND
            or str(row["source_kind"] or "") != "feishu_group_manual"
            or str(row["mode"] or "") != "rerun"
        ):
            continue
        target = _strict_json(
            str(row["target_json"] or "").encode("utf-8"),
            artifact="delivery_quarantine_subscription_target",
        )
        try:
            validate_delivery_subscription_target(
                effect_kind=str(row["effect_kind"]),
                target_key=str(row["target_key"]),
                target=target,
                project_key=str(row["project_key"] or ""),
                work_item_type_key=str(row["work_item_type_key"] or ""),
                work_item_id=str(row["work_item_id"] or ""),
            )
        except DeliveryContractError as exc:
            if exc.code == "delivery_subscription_target_invalid":
                invalid += 1
    return {"total": total, "invalid_manual_thread": invalid}


def _audit_text(actor: Any, reason: Any) -> tuple[str, str]:
    normalized_actor = str(actor or "").strip()
    normalized_reason = str(reason or "").strip()
    if (
        not normalized_actor
        or len(normalized_actor) > 200
        or "\n" in normalized_actor
        or "\r" in normalized_actor
    ):
        raise DeliveryQuarantineBaselineError(
            "delivery_quarantine_baseline_actor_invalid"
        )
    if not normalized_reason or len(normalized_reason.encode("utf-8")) > 1000:
        raise DeliveryQuarantineBaselineError(
            "delivery_quarantine_baseline_reason_invalid"
        )
    return normalized_actor, normalized_reason


def _backup_reference(path_value: Any, sha_value: Any) -> dict[str, Any]:
    path = Path(str(path_value or "")).expanduser()
    sha256 = str(sha_value or "").strip().lower()
    if not path.is_absolute() or _HEX64_RE.fullmatch(sha256) is None:
        raise DeliveryQuarantineBaselineError(
            "delivery_quarantine_settlement_backup_reference_invalid"
        )
    raw, observed_sha256 = _read_stable_file(
        path,
        artifact="delivery_quarantine_settlement_backup",
        maximum_bytes=MAX_BACKUP_BYTES,
        owner_only=False,
        allowed_modes=frozenset({0o600, 0o640, 0o644}),
        require_owner=True,
    )
    if (
        observed_sha256 != sha256
        or len(raw) < 512
        or len(raw) % 512 != 0
        or not raw.startswith(b"SQLite format 3\x00")
    ):
        raise DeliveryQuarantineBaselineError(
            "delivery_quarantine_settlement_backup_invalid"
        )
    return {"path": str(path), "sha256": sha256, "size_bytes": len(raw)}


def _effect_keys(items: Any, *, terminal_shape: bool) -> set[str]:
    if not isinstance(items, list):
        raise DeliveryQuarantineBaselineError(
            "delivery_quarantine_settlement_receipt_scope_invalid"
        )
    keys: set[str] = set()
    for item in items:
        if not isinstance(item, Mapping):
            raise DeliveryQuarantineBaselineError(
                "delivery_quarantine_settlement_receipt_scope_invalid"
            )
        effect_key = str(item.get("effect_key") or "")
        if (
            not effect_key.startswith((
                "g1q3-rca-terminal-effect-v1-",
                "g1q3-rca-effect-v1-",
            ))
            or effect_key in keys
            or (
                terminal_shape
                and (
                    item.get("status") != "quarantined"
                    or item.get("write_phase") != "settled"
                )
            )
        ):
            raise DeliveryQuarantineBaselineError(
                "delivery_quarantine_settlement_receipt_scope_invalid"
            )
        keys.add(effect_key)
    return keys


def _settlement_receipt_claims(value: Mapping[str, Any]) -> dict[str, Any]:
    schema = str(value.get("schema_version") or "")
    if schema == "rca_functional_backlog_settlement_v1":
        postconditions = value.get("postconditions")
        backup = value.get("backup")
        if (
            value.get("external_writes_performed") is not False
            or not isinstance(postconditions, Mapping)
            or postconditions.get("unresolved_issue_effects") != 0
            or not isinstance(backup, Mapping)
        ):
            raise DeliveryQuarantineBaselineError(
                "delivery_quarantine_settlement_receipt_claim_invalid"
            )
        backup_claim = _backup_reference(backup.get("path"), backup.get("sha256"))
        keys = _effect_keys(value.get("cumulative_settled"), terminal_shape=True)
        return {
            "final_zero": True,
            "effect_keys": keys,
            "backups": [backup_claim],
        }
    if schema == "rca_nonsuccess_effect_cleanup_v1":
        settled = value.get("settled")
        if (
            value.get("external_writes") is not False
            or value.get("remaining_non_success") != 0
            or not isinstance(settled, list)
            or isinstance(value.get("expected"), bool)
            or value.get("expected") != len(settled)
        ):
            raise DeliveryQuarantineBaselineError(
                "delivery_quarantine_settlement_receipt_claim_invalid"
            )
        keys = _effect_keys(settled, terminal_shape=False)
        backup_claim = _backup_reference(
            value.get("backup_path"), value.get("backup_sha256")
        )
        if any(
            item.get("status") != "quarantined"
            or item.get("outcome") not in {"quarantined", "terminal_failed"}
            for item in settled
        ):
            raise DeliveryQuarantineBaselineError(
                "delivery_quarantine_settlement_receipt_claim_invalid"
            )
        return {
            "final_zero": False,
            "effect_keys": keys,
            "backups": [backup_claim],
        }
    if schema == "rca_issue_only_subscription_semantics_fix_v1":
        subscriptions = value.get("subscription_keys")
        after = value.get("after_status_counts")
        jobs = value.get("job_status_counts")
        count = value.get("subscription_count")
        job_count = value.get("job_count")
        if (
            value.get("external_writes") is not False
            or isinstance(count, bool)
            or not isinstance(count, int)
            or count <= 0
            or not isinstance(subscriptions, list)
            or len(subscriptions) != count
            or any(
                not isinstance(key, str) or not key.startswith("g1q3-rca-sub-v1-")
                for key in subscriptions
            )
            or len(set(subscriptions)) != count
            or not isinstance(after, Mapping)
            or after.get("suppressed_optional") != count
            or isinstance(job_count, bool)
            or not isinstance(job_count, int)
            or job_count < 0
            or not isinstance(jobs, Mapping)
            or jobs.get("delivered") != job_count
        ):
            raise DeliveryQuarantineBaselineError(
                "delivery_quarantine_settlement_receipt_claim_invalid"
            )
        backup_claim = _backup_reference(
            value.get("backup_path"), value.get("backup_sha256")
        )
        return {
            "final_zero": False,
            "effect_keys": set(),
            "backups": [backup_claim],
        }
    raise DeliveryQuarantineBaselineError(
        "delivery_quarantine_settlement_receipt_schema_invalid"
    )


def _settlement_receipts(
    value: Any, *, required: bool
) -> tuple[list[dict[str, str]], str, dict[str, Any]]:
    if not isinstance(value, list) or len(value) > MAX_SETTLEMENT_RECEIPTS:
        raise DeliveryQuarantineBaselineError(
            "delivery_quarantine_baseline_settlement_receipts_invalid"
        )
    if required and not value:
        raise DeliveryQuarantineBaselineError(
            "delivery_quarantine_baseline_settlement_receipts_invalid"
        )
    normalized: list[dict[str, str]] = []
    seen: set[str] = set()
    final_zero = False
    effect_keys: set[str] = set()
    backups: list[dict[str, Any]] = []
    for item in value:
        if not isinstance(item, Mapping) or set(item) != _RECEIPT_FIELDS:
            raise DeliveryQuarantineBaselineError(
                "delivery_quarantine_baseline_settlement_receipts_invalid"
            )
        path_text = str(item.get("path") or "").strip()
        expected_sha256 = str(item.get("sha256") or "").strip().lower()
        path = Path(path_text).expanduser()
        if (
            not path.is_absolute()
            or path_text in seen
            or _HEX64_RE.fullmatch(expected_sha256) is None
        ):
            raise DeliveryQuarantineBaselineError(
                "delivery_quarantine_baseline_settlement_receipts_invalid"
            )
        raw, observed_sha256 = _read_stable_file(
            path,
            artifact="delivery_quarantine_settlement_receipt",
            maximum_bytes=MAX_EVIDENCE_BYTES,
            owner_only=False,
            allowed_modes=frozenset({0o600, 0o640, 0o644}),
            require_owner=True,
        )
        if observed_sha256 != expected_sha256:
            raise DeliveryQuarantineBaselineError(
                "delivery_quarantine_settlement_receipt_sha256_mismatch"
            )
        receipt = _strict_json(raw, artifact="delivery_quarantine_settlement_receipt")
        claims = _settlement_receipt_claims(receipt)
        final_zero = final_zero or claims["final_zero"]
        effect_keys.update(claims["effect_keys"])
        backups.extend(claims["backups"])
        seen.add(path_text)
        normalized.append({"path": path_text, "sha256": expected_sha256})
    if normalized != sorted(normalized, key=lambda item: item["path"]):
        raise DeliveryQuarantineBaselineError(
            "delivery_quarantine_baseline_settlement_receipts_invalid"
        )
    if required and not final_zero:
        raise DeliveryQuarantineBaselineError(
            "delivery_quarantine_settlement_receipt_final_zero_missing"
        )
    return (
        normalized,
        hashlib.sha256(_canonical_bytes(normalized)).hexdigest(),
        {
            "final_zero": final_zero,
            "effect_keys": effect_keys,
            "backups": backups,
        },
    )


def _effect_settlement_projection(
    conn: sqlite3.Connection, receipt_effect_keys: set[str]
) -> dict[str, Any]:
    rows = conn.execute(
        """
        SELECT effect.effect_key, effect.effect_kind, effect.required,
               effect.target_key, effect.write_phase, job.delivery_id,
               job.business_key, job.generation, job.work_item_id
          FROM rca_delivery_effects AS effect
          JOIN rca_delivery_jobs AS job
            ON job.delivery_id = effect.delivery_id
         WHERE effect.status = 'quarantined'
         ORDER BY effect.effect_key
        """
    ).fetchall()
    quarantine_keys = {str(row["effect_key"]) for row in rows}
    if not receipt_effect_keys.issubset(quarantine_keys):
        raise DeliveryQuarantineBaselineError(
            "delivery_quarantine_settlement_receipt_scope_invalid"
        )
    entries: list[dict[str, Any]] = []
    receipt_settled = 0
    superseded = 0
    for row in rows:
        effect_key = str(row["effect_key"])
        if (
            int(row["required"]) != 1
            or str(row["effect_kind"]) != "feishu_issue_comment"
            or str(row["write_phase"]) != "settled"
        ):
            raise DeliveryQuarantineBaselineError(
                "delivery_quarantine_settlement_receipt_scope_invalid"
            )
        entry = {
            "quarantined_effect_key": effect_key,
            "business_key": str(row["business_key"]),
            "work_item_id": str(row["work_item_id"]),
            "generation": int(row["generation"]),
            "effect_kind": str(row["effect_kind"]),
            "target_key": str(row["target_key"]),
        }
        if effect_key in receipt_effect_keys:
            entries.append({
                **entry,
                "disposition": "receipt_settled",
                "evidence_effect_key": effect_key,
                "superseding_delivery_id": "",
                "superseding_generation": 0,
                "superseding_completed_at": "",
                "superseding_remote_receipt_sha256": "",
                "superseding_confirmed_field_keys": [],
            })
            receipt_settled += 1
            continue
        later = conn.execute(
            """
            SELECT effect.effect_key, effect.remote_receipt_json,
                   effect.completed_at, job.delivery_id, job.generation
              FROM rca_delivery_jobs AS job
              JOIN rca_delivery_effects AS effect
                ON effect.delivery_id = job.delivery_id
             WHERE job.business_key = ? AND job.work_item_id = ?
               AND job.generation > ?
               AND job.status IN ('delivered', 'partial')
               AND effect.effect_kind = ? AND effect.target_key = ?
               AND effect.required = 1 AND effect.status = 'succeeded'
               AND effect.write_phase = 'settled'
             ORDER BY job.generation, effect.completed_at, effect.effect_key
             LIMIT 1
            """,
            (
                row["business_key"],
                row["work_item_id"],
                row["generation"],
                row["effect_kind"],
                row["target_key"],
            ),
        ).fetchone()
        if later is None or not later["completed_at"]:
            raise DeliveryQuarantineBaselineError(
                "delivery_quarantine_effect_settlement_incomplete"
            )
        try:
            _parse_iso(later["completed_at"])
            remote_receipt = _strict_json(
                str(later["remote_receipt_json"] or "").encode("utf-8"),
                artifact="delivery_quarantine_superseding_remote_receipt",
            )
        except (TypeError, ValueError, OverflowError) as exc:
            raise DeliveryQuarantineBaselineError(
                "delivery_quarantine_effect_settlement_invalid"
            ) from exc
        confirmed = remote_receipt.get("confirmed_field_keys")
        later_effect_key = str(later["effect_key"] or "")
        if (
            not str(remote_receipt.get("remote_id") or "").strip()
            or later_effect_key not in str(remote_receipt.get("marker") or "")
            or not str(remote_receipt.get("source") or "").strip()
            or not isinstance(confirmed, list)
            or any(not isinstance(key, str) or not key for key in confirmed)
            or len(set(confirmed)) != len(confirmed)
            or (later_effect_key.startswith("g1q3-rca-effect-v1-") and not confirmed)
        ):
            raise DeliveryQuarantineBaselineError(
                "delivery_quarantine_effect_settlement_invalid"
            )
        entries.append({
            **entry,
            "disposition": "superseded_by_later_success",
            "evidence_effect_key": later_effect_key,
            "superseding_delivery_id": str(later["delivery_id"]),
            "superseding_generation": int(later["generation"]),
            "superseding_completed_at": str(later["completed_at"]),
            "superseding_remote_receipt_sha256": hashlib.sha256(
                _canonical_bytes(dict(remote_receipt))
            ).hexdigest(),
            "superseding_confirmed_field_keys": list(confirmed),
        })
        superseded += 1
    body = {
        "schema_version": "pnc_rca_delivery_quarantine_effect_settlement_v1",
        "quarantined_effect_count": len(entries),
        "receipt_settled_count": receipt_settled,
        "superseded_by_later_success_count": superseded,
        "entries": entries,
    }
    return {
        **body,
        "entries_sha256": hashlib.sha256(_canonical_bytes(entries)).hexdigest(),
    }


def _open_readonly_db(
    db_path: str | Path,
    *,
    busy_timeout_ms: int,
    immutable: bool = False,
) -> sqlite3.Connection:
    selected = Path(db_path).expanduser().absolute()
    if not hasattr(os, "O_NOFOLLOW"):
        raise DeliveryQuarantineBaselineError(
            "delivery_quarantine_baseline_db_no_follow_unavailable"
        )
    try:
        descriptor = os.open(
            selected,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | os.O_NOFOLLOW,
        )
    except OSError as exc:
        raise DeliveryQuarantineBaselineError(
            "delivery_quarantine_baseline_db_unavailable"
        ) from exc
    conn: sqlite3.Connection | None = None
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
            or before.st_size <= 0
        ):
            raise DeliveryQuarantineBaselineError(
                "delivery_quarantine_baseline_db_invalid"
            )
        conn = sqlite3.connect(
            f"{selected.as_uri()}?mode=ro" + ("&immutable=1" if immutable else ""),
            uri=True,
            timeout=max(1, int(busy_timeout_ms)) / 1000,
            isolation_level=None,
        )
        conn.row_factory = sqlite3.Row
        conn.execute(f"PRAGMA busy_timeout={max(1, int(busy_timeout_ms))}")
        conn.execute("PRAGMA query_only=ON")
        after = os.fstat(descriptor)
        lexical = os.lstat(selected)
        if (
            _stat_identity(before) != _stat_identity(after)
            or stat.S_ISLNK(lexical.st_mode)
            or (after.st_dev, after.st_ino) != (lexical.st_dev, lexical.st_ino)
        ):
            raise DeliveryQuarantineBaselineError(
                "delivery_quarantine_baseline_db_unstable"
            )
        return conn
    except DeliveryQuarantineBaselineError:
        if conn is not None:
            conn.close()
        raise
    except (OSError, sqlite3.Error) as exc:
        if conn is not None:
            conn.close()
        raise DeliveryQuarantineBaselineError(
            "delivery_quarantine_baseline_db_unavailable"
        ) from exc
    finally:
        os.close(descriptor)


def _validate_migration_artifact(
    *,
    receipt_path: str,
    expected_sha256: str,
    target_live_db_path: str | Path,
    migrated_db_path: str | Path | None,
    migrated_db_is_live: bool,
    expected_migration_runtime_sha256: str,
) -> dict[str, Any]:
    try:
        # Combined-v9 receipts are the release target; retain the older
        # validator for pre-v9 rehearsal artifacts.  The source schema hash is
        # carried by the combined receipt's external predecessor contract and
        # is rechecked against its immutable source copy by the validator.
        try:
            raw, _probe_sha256 = _read_stable_file(
                receipt_path,
                artifact="delivery_quarantine_migration_receipt_probe",
                maximum_bytes=MAX_EVIDENCE_BYTES,
                owner_only=True,
            )
            parsed = json.loads(raw)
            receipt_schema = (
                parsed.get("schema_version")
                if isinstance(parsed, Mapping)
                else None
            )
        except (TypeError, ValueError, json.JSONDecodeError):
            receipt_schema = None
        if receipt_schema == COMBINED_SCHEMA_VERSION:
            source_contract = parsed.get("source_schema_contract")
            expected_source_schema_sha256 = (
                source_contract.get("schema_sha256")
                if isinstance(source_contract, Mapping)
                else ""
            )
            combined = validate_combined_migration_receipt(
                receipt_path=receipt_path,
                expected_sha256=expected_sha256,
                target_live_db_path=target_live_db_path,
                migrated_db_path=migrated_db_path,
                expected_migration_runtime_sha256=expected_migration_runtime_sha256,
                expected_source_schema_sha256=str(expected_source_schema_sha256 or ""),
            )
            binding = {
                "receipt_path": combined["receipt_path"],
                "receipt_sha256": combined["receipt_sha256"],
                "source_backup_sha256": combined["source_backup_sha256"],
                "source_logical_sha256": combined["source_logical_sha256"],
                "post_migration_logical_sha256": combined[
                    "target_logical_sha256"
                ],
                "migration_runtime_sha256": combined["migration_runtime_sha256"],
                "target_live_db_path": combined["target_live_db_path"],
            }
        else:
            binding = validate_migration_receipt(
                receipt_path=receipt_path,
                expected_sha256=expected_sha256,
                target_live_db_path=target_live_db_path,
                migrated_db_path=migrated_db_path,
                migrated_db_is_live=migrated_db_is_live,
                expected_migration_runtime_sha256=expected_migration_runtime_sha256,
            )
    except QuarantineMigrationError as exc:
        raise DeliveryQuarantineBaselineError(exc.code) from exc
    return binding


def _migration_binding(
    value: Any,
    *,
    target_live_db_path: str | Path,
    migrated_db_path: str | Path | None,
    migrated_db_is_live: bool,
) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != _MIGRATION_BINDING_FIELDS:
        raise DeliveryQuarantineBaselineError(
            "delivery_quarantine_migration_binding_invalid"
        )
    try:
        binding = _validate_migration_artifact(
            receipt_path=str(value.get("receipt_path") or ""),
            expected_sha256=str(value.get("receipt_sha256") or ""),
            target_live_db_path=target_live_db_path,
            migrated_db_path=migrated_db_path,
            migrated_db_is_live=migrated_db_is_live,
            expected_migration_runtime_sha256=str(
                value.get("migration_runtime_sha256") or ""
            ),
        )
    except QuarantineMigrationError as exc:
        raise DeliveryQuarantineBaselineError(exc.code) from exc
    if binding != dict(value):
        raise DeliveryQuarantineBaselineError(
            "delivery_quarantine_migration_binding_mismatch"
        )
    return binding


def _validate_core(
    conn: sqlite3.Connection,
    *,
    db_path: str | Path,
    value: Any,
    identity_db_path: str | Path | None = None,
    require_post_migration_match: bool = False,
) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != _CORE_FIELDS:
        raise DeliveryQuarantineBaselineError("delivery_quarantine_core_schema_invalid")
    release_id = str(value.get("release_id") or "").strip()
    core_sha256 = str(value.get("core_sha256") or "").strip().lower()
    body = {key: item for key, item in value.items() if key != "core_sha256"}
    if (
        value.get("schema_version") != CORE_SCHEMA_VERSION
        or _IDENTIFIER_RE.fullmatch(release_id) is None
        or _HEX64_RE.fullmatch(core_sha256) is None
        or core_sha256 != hashlib.sha256(_canonical_bytes(body)).hexdigest()
    ):
        raise DeliveryQuarantineBaselineError(
            "delivery_quarantine_core_identity_invalid"
        )
    try:
        snapshot_at = _parse_iso(value.get("snapshot_at"))
    except (TypeError, ValueError, OverflowError) as exc:
        raise DeliveryQuarantineBaselineError(
            "delivery_quarantine_core_time_invalid"
        ) from exc
    target_db_path = identity_db_path if identity_db_path is not None else db_path
    migration_binding = _migration_binding(
        value.get("migration_binding"),
        target_live_db_path=target_db_path,
        migrated_db_path=db_path if require_post_migration_match else None,
        migrated_db_is_live=(
            require_post_migration_match
            and Path(db_path).expanduser().absolute()
            == Path(target_db_path).expanduser().absolute()
        ),
    )
    current_db_identity = _db_identity(conn, target_db_path)
    db_identity = value.get("control_db")
    if not isinstance(db_identity, Mapping) or not _db_identity_matches_baseline(
        db_identity, current_db_identity
    ):
        raise DeliveryQuarantineBaselineError(
            "delivery_quarantine_baseline_db_identity_mismatch"
        )
    current_snapshot = quarantine_snapshot(conn)
    snapshot = value.get("quarantine_snapshot")
    if (
        not isinstance(snapshot, Mapping)
        or set(snapshot) != _SNAPSHOT_FIELDS
        or dict(snapshot) != current_snapshot
    ):
        raise DeliveryQuarantineBaselineError(
            "delivery_quarantine_baseline_snapshot_mismatch"
        )
    if value.get("quarantine_event_projection") != _quarantine_event_projection(conn):
        raise DeliveryQuarantineBaselineError(
            "delivery_quarantine_event_projection_mismatch"
        )
    receipts, receipt_set_sha256, receipt_claims = _settlement_receipts(
        value.get("settlement_receipts"),
        required=sum(current_snapshot["counts"].values()) > 0,
    )
    effect_settlement = _effect_settlement_projection(
        conn, receipt_claims["effect_keys"]
    )
    if value.get("effect_settlement") != effect_settlement:
        raise DeliveryQuarantineBaselineError(
            "delivery_quarantine_effect_settlement_mismatch"
        )
    projection = _invalid_manual_thread_projection(conn)
    adjudication = value.get("invalid_manual_thread_adjudication")
    if (
        not isinstance(adjudication, Mapping)
        or set(adjudication) != _ADJUDICATION_FIELDS
        or adjudication.get("effect_kind") != DELIVERY_THREAD_EFFECT_KIND
        or adjudication.get("reason_code") != "delivery_subscription_target_invalid"
        or adjudication.get("technical_finding") != "invalid_manual_thread_target"
        or adjudication.get("proposed_disposition") != "retain_terminal_no_rearm"
        or adjudication.get("required") is not True
        or adjudication.get("source_kind") != "feishu_group_manual"
        or adjudication.get("source_mode") != "rerun"
        or isinstance(adjudication.get("count"), bool)
        or adjudication.get("count") != projection["invalid_manual_thread"]
        or projection["invalid_manual_thread"] != projection["total"]
        or str(adjudication.get("analyzed_at") or "") != _iso(snapshot_at)
    ):
        raise DeliveryQuarantineBaselineError(
            "delivery_quarantine_baseline_manual_thread_adjudication_invalid"
        )
    _audit_text(adjudication.get("analyzed_by"), adjudication.get("reason"))
    if value.get("issuance_policy") != _ISSUANCE_POLICY:
        raise DeliveryQuarantineBaselineError(
            "delivery_quarantine_core_issuance_policy_invalid"
        )
    return {
        "release_id": release_id,
        "core_sha256": core_sha256,
        "snapshot_at": snapshot_at,
        "db_logical_identity_sha256": current_db_identity["logical_identity_sha256"],
        "snapshot_sha256": current_snapshot["snapshot_sha256"],
        "row_set_sha256": dict(current_snapshot["row_set_sha256"]),
        "settlement_receipt_count": len(receipts),
        "settlement_receipt_set_sha256": receipt_set_sha256,
        "migration_receipt_sha256": migration_binding["receipt_sha256"],
        "migration_runtime_sha256": migration_binding["migration_runtime_sha256"],
    }


def _validate_release_manifest(
    *,
    manifest_path: str | Path,
    expected_sha256: str,
    expected_release_bom_sha256: str,
    release_id: str,
    core_sha256: str,
) -> dict[str, Any]:
    normalized_expected = str(expected_sha256 or "").strip().lower()
    normalized_bom = str(expected_release_bom_sha256 or "").strip().lower()
    if (
        _HEX64_RE.fullmatch(normalized_expected) is None
        or _HEX64_RE.fullmatch(normalized_bom) is None
    ):
        raise DeliveryQuarantineBaselineError(
            "delivery_quarantine_release_manifest_not_configured"
        )
    path = Path(manifest_path).expanduser().absolute()
    raw, observed_sha256 = _read_stable_file(
        path,
        artifact="delivery_quarantine_release_manifest",
        maximum_bytes=MAX_EVIDENCE_BYTES,
        owner_only=True,
    )
    if observed_sha256 != normalized_expected:
        raise DeliveryQuarantineBaselineError(
            "delivery_quarantine_release_manifest_sha256_mismatch"
        )
    value = _strict_json(raw, artifact="delivery_quarantine_release_manifest")
    side_effect = value.get("side_effect_contract")
    if (
        not _RELEASE_MANIFEST_REQUIRED_FIELDS.issubset(value)
        or value.get("schema_version") != RELEASE_MANIFEST_SCHEMA_VERSION
        or value.get("release_id") != release_id
        or value.get("quarantine_core_sha256") != core_sha256
        or value.get("release_bom_sha256") != normalized_bom
        or value.get("complete") is not True
        or not isinstance(side_effect, Mapping)
        or side_effect.get("live_files_written") is not False
        or side_effect.get("launchctl_invoked") is not False
    ):
        raise DeliveryQuarantineBaselineError(
            "delivery_quarantine_release_manifest_scope_invalid"
        )
    try:
        created_at = _parse_iso(value.get("created_at"))
    except (TypeError, ValueError, OverflowError) as exc:
        raise DeliveryQuarantineBaselineError(
            "delivery_quarantine_release_manifest_time_invalid"
        ) from exc
    return {
        "path": str(path),
        "sha256": observed_sha256,
        "release_bom_sha256": normalized_bom,
        "created_at": created_at,
    }


def _validate_approval(
    *,
    evidence_path: str | Path,
    expected_sha256: str,
    release_id: str,
    core_sha256: str,
    release_bom_sha256: str,
) -> dict[str, Any]:
    normalized_expected = str(expected_sha256 or "").strip().lower()
    if _HEX64_RE.fullmatch(normalized_expected) is None:
        raise DeliveryQuarantineBaselineError(
            "delivery_quarantine_approval_not_configured"
        )
    path = Path(evidence_path).expanduser().absolute()
    raw, observed_sha256 = _read_stable_file(
        path,
        artifact="delivery_quarantine_approval_evidence",
        maximum_bytes=MAX_EVIDENCE_BYTES,
        owner_only=True,
    )
    if observed_sha256 != normalized_expected:
        raise DeliveryQuarantineBaselineError(
            "delivery_quarantine_approval_sha256_mismatch"
        )
    value = _strict_json(raw, artifact="delivery_quarantine_approval_evidence")
    identity = value.get("identity")
    if (
        not _APPROVAL_REQUIRED_FIELDS.issubset(value)
        or value.get("schema_version") != APPROVAL_SCHEMA_VERSION
        or value.get("release_id") != release_id
        or value.get("quarantine_core_sha256") != core_sha256
        or value.get("release_bom_sha256") != release_bom_sha256
        or value.get("decision") != "authorize_rca_delivery_quarantine_baseline"
        or not isinstance(identity, Mapping)
        or identity.get("uid") != os.geteuid()
        or not str(identity.get("username") or "").strip()
    ):
        raise DeliveryQuarantineBaselineError(
            "delivery_quarantine_approval_scope_invalid"
        )
    approved_by = str(identity["username"]).strip()
    reason = "canonical release approval for exact historical quarantine baseline"
    try:
        approved_at = _parse_iso(value.get("created_at"))
    except (TypeError, ValueError, OverflowError) as exc:
        raise DeliveryQuarantineBaselineError(
            "delivery_quarantine_approval_time_invalid"
        ) from exc
    return {
        "path": str(path),
        "sha256": observed_sha256,
        "approved_by": approved_by,
        "approved_at": approved_at,
        "reason": reason,
    }


def _validate_active_release_anchor(
    *,
    active_release_binding_path: str | Path,
    live_env_path: str | Path,
    release_id: str,
    bootstrap_epoch_id: str,
    release_bom_sha256: str,
) -> dict[str, Any]:
    if (
        _IDENTIFIER_RE.fullmatch(str(release_id or "")) is None
        or EPOCH_ID_RE.fullmatch(str(bootstrap_epoch_id or "")) is None
    ):
        raise DeliveryQuarantineBaselineError(
            "delivery_quarantine_active_release_identity_invalid"
        )
    try:
        binding = load_active_release_binding(
            path=Path(active_release_binding_path).expanduser().absolute(),
            live_env_path=Path(live_env_path).expanduser().absolute(),
            expected_release_id=release_id,
            expected_epoch_id=bootstrap_epoch_id,
            verify_live_env=True,
        )
    except RcaBootstrapAuthorizationError as exc:
        raise DeliveryQuarantineBaselineError(
            "delivery_quarantine_active_release_binding_invalid"
        ) from exc
    if binding.get("release_bom_sha256") != release_bom_sha256:
        raise DeliveryQuarantineBaselineError(
            "delivery_quarantine_active_release_cross_binding_invalid"
        )
    return {
        "active_release_binding_sha256": binding["binding_receipt_sha256"],
        "candidate_env_sha256": binding["candidate_env_sha256"],
    }


def _validate_baseline(
    conn: sqlite3.Connection,
    *,
    db_path: str | Path,
    baseline_path: str | Path,
    expected_sha256: str,
    expected_release_id: str,
    bootstrap_epoch_id: str,
    active_release_binding_path: str | Path,
    live_env_path: str | Path,
) -> dict[str, Any]:
    normalized_expected = str(expected_sha256 or "").strip().lower()
    if _HEX64_RE.fullmatch(normalized_expected) is None:
        raise DeliveryQuarantineBaselineError(
            "delivery_quarantine_baseline_not_configured"
        )
    raw, observed_sha256 = _read_stable_file(
        baseline_path,
        artifact="delivery_quarantine_baseline",
        maximum_bytes=MAX_BASELINE_BYTES,
        owner_only=True,
    )
    if observed_sha256 != normalized_expected:
        raise DeliveryQuarantineBaselineError(
            "delivery_quarantine_baseline_sha256_mismatch"
        )
    value = _strict_json(raw, artifact="delivery_quarantine_baseline")
    if set(value) != _BASELINE_FIELDS:
        raise DeliveryQuarantineBaselineError(
            "delivery_quarantine_baseline_schema_invalid"
        )
    baseline_id = str(value.get("baseline_id") or "").strip()
    release_id = str(value.get("release_id") or "").strip()
    if (
        value.get("schema_version") != BASELINE_SCHEMA_VERSION
        or _IDENTIFIER_RE.fullmatch(baseline_id) is None
        or _IDENTIFIER_RE.fullmatch(release_id) is None
        or release_id != expected_release_id
    ):
        raise DeliveryQuarantineBaselineError(
            "delivery_quarantine_baseline_schema_invalid"
        )
    body = {key: item for key, item in value.items() if key != "baseline_fingerprint"}
    fingerprint = hashlib.sha256(_canonical_bytes(body)).hexdigest()
    if value.get("baseline_fingerprint") != fingerprint:
        raise DeliveryQuarantineBaselineError(
            "delivery_quarantine_baseline_fingerprint_invalid"
        )
    core = _validate_core(conn, db_path=db_path, value=value.get("quarantine_core"))
    if (
        release_id != core["release_id"]
        or value.get("quarantine_core_sha256") != core["core_sha256"]
    ):
        raise DeliveryQuarantineBaselineError(
            "delivery_quarantine_baseline_release_identity_invalid"
        )
    release_binding = value.get("release_manifest")
    if (
        not isinstance(release_binding, Mapping)
        or set(release_binding) != _RELEASE_BINDING_FIELDS
    ):
        raise DeliveryQuarantineBaselineError(
            "delivery_quarantine_release_manifest_binding_invalid"
        )
    release_manifest = _validate_release_manifest(
        manifest_path=str(release_binding.get("path") or ""),
        expected_sha256=str(release_binding.get("sha256") or ""),
        expected_release_bom_sha256=str(
            release_binding.get("release_bom_sha256") or ""
        ),
        release_id=release_id,
        core_sha256=core["core_sha256"],
    )
    attestation = value.get("owner_attestation")
    if not isinstance(attestation, Mapping) or set(attestation) != _ATTESTATION_FIELDS:
        raise DeliveryQuarantineBaselineError(
            "delivery_quarantine_baseline_owner_attestation_invalid"
        )
    approval = _validate_approval(
        evidence_path=str(attestation.get("approval_evidence_path") or ""),
        expected_sha256=str(attestation.get("approval_evidence_sha256") or ""),
        release_id=release_id,
        core_sha256=core["core_sha256"],
        release_bom_sha256=release_manifest["release_bom_sha256"],
    )
    if (
        attestation.get("decision") != "approved"
        or attestation.get("release_id") != release_id
        or attestation.get("quarantine_core_sha256") != core["core_sha256"]
        or attestation.get("release_bom_sha256")
        != release_manifest["release_bom_sha256"]
        or attestation.get("release_manifest_sha256") != release_manifest["sha256"]
        or attestation.get("approved_by") != approval["approved_by"]
        or attestation.get("approved_at") != _iso(approval["approved_at"])
        or attestation.get("reason") != approval["reason"]
        or attestation.get("no_database_rows_modified") is not True
        or attestation.get("no_rearm") is not True
        or attestation.get("no_external_writes") is not True
    ):
        raise DeliveryQuarantineBaselineError(
            "delivery_quarantine_baseline_owner_attestation_invalid"
        )
    try:
        issued_at = _parse_iso(value.get("issued_at"))
    except (TypeError, ValueError, OverflowError) as exc:
        raise DeliveryQuarantineBaselineError(
            "delivery_quarantine_baseline_time_invalid"
        ) from exc
    if not (
        core["snapshot_at"]
        <= release_manifest["created_at"]
        <= approval["approved_at"]
        <= issued_at
    ):
        raise DeliveryQuarantineBaselineError(
            "delivery_quarantine_baseline_time_invalid"
        )
    active_anchor = _validate_active_release_anchor(
        active_release_binding_path=active_release_binding_path,
        live_env_path=live_env_path,
        release_id=release_id,
        bootstrap_epoch_id=bootstrap_epoch_id,
        release_bom_sha256=release_manifest["release_bom_sha256"],
    )
    return {
        "baseline_id": baseline_id,
        "baseline_sha256": observed_sha256,
        "baseline_fingerprint": fingerprint,
        "release_id": release_id,
        "quarantine_core_sha256": core["core_sha256"],
        "approval_evidence_sha256": approval["sha256"],
        "release_bom_sha256": release_manifest["release_bom_sha256"],
        "release_manifest_sha256": release_manifest["sha256"],
        **active_anchor,
        "db_logical_identity_sha256": core["db_logical_identity_sha256"],
        "snapshot_sha256": core["snapshot_sha256"],
        "row_set_sha256": core["row_set_sha256"],
        "settlement_receipt_count": core["settlement_receipt_count"],
        "settlement_receipt_set_sha256": core["settlement_receipt_set_sha256"],
        "migration_receipt_sha256": core["migration_receipt_sha256"],
        "migration_runtime_sha256": core["migration_runtime_sha256"],
    }


def quarantine_baseline_status_tx(
    conn: sqlite3.Connection,
    *,
    db_path: str | Path,
    baseline_path: str | Path,
    expected_sha256: str,
    expected_release_id: str = "",
    bootstrap_epoch_id: str = "",
    active_release_binding_path: str | Path = "",
    live_env_path: str | Path = "",
) -> dict[str, Any]:
    """Project baseline readiness from the caller's existing SQLite transaction."""

    snapshot = quarantine_snapshot(conn)
    lifetime = _quarantine_lifetime_counts(conn, snapshot["counts"])
    required = sum(lifetime.values()) > 0
    configured = bool(str(expected_sha256 or "").strip())
    base = {
        "schema_version": BASELINE_SCHEMA_VERSION,
        "required": required,
        "configured": configured,
        "path": str(Path(baseline_path).expanduser().absolute()),
        "expected_sha256": str(expected_sha256 or "").strip().lower(),
        "lifetime": lifetime,
    }
    if not required and not configured:
        return {
            **base,
            "ready": True,
            "state": "not_required",
            "error_code": "",
            "acknowledged": {key: 0 for key in lifetime},
            "unacknowledged": {key: 0 for key in lifetime},
            "baseline_identity": None,
        }
    try:
        identity = _validate_baseline(
            conn,
            db_path=db_path,
            baseline_path=baseline_path,
            expected_sha256=expected_sha256,
            expected_release_id=expected_release_id,
            bootstrap_epoch_id=bootstrap_epoch_id,
            active_release_binding_path=active_release_binding_path,
            live_env_path=live_env_path,
        )
    except DeliveryQuarantineBaselineError as exc:
        return {
            **base,
            "ready": False,
            "state": "unavailable",
            "error_code": exc.code,
            "acknowledged": {key: 0 for key in lifetime},
            "unacknowledged": lifetime,
            "baseline_identity": None,
        }
    return {
        **base,
        "ready": True,
        "state": "acknowledged",
        "error_code": "",
        "acknowledged": lifetime,
        "unacknowledged": {key: 0 for key in lifetime},
        "baseline_identity": identity,
    }


def disabled_quarantine_baseline_status(
    *, baseline_path: str | Path, expected_sha256: str
) -> dict[str, Any]:
    zeros = {name: 0 for name, _table, _key in _QUARANTINE_TABLES}
    return {
        "schema_version": BASELINE_SCHEMA_VERSION,
        "required": False,
        "configured": bool(str(expected_sha256 or "").strip()),
        "path": str(Path(baseline_path).expanduser().absolute()),
        "expected_sha256": str(expected_sha256 or "").strip().lower(),
        "ready": True,
        "state": "disabled",
        "error_code": "",
        "lifetime": zeros,
        "acknowledged": dict(zeros),
        "unacknowledged": dict(zeros),
        "baseline_identity": None,
    }


def read_quarantine_baseline_status(
    db_path: str | Path,
    *,
    baseline_path: str | Path,
    expected_sha256: str,
    expected_release_id: str = "",
    bootstrap_epoch_id: str = "",
    active_release_binding_path: str | Path = "",
    live_env_path: str | Path = "",
    busy_timeout_ms: int = 5000,
) -> dict[str, Any]:
    """Validate a baseline against one independently opened read-only snapshot."""

    conn: sqlite3.Connection | None = None
    try:
        conn = _open_readonly_db(db_path, busy_timeout_ms=busy_timeout_ms)
        conn.execute("BEGIN")
        result = quarantine_baseline_status_tx(
            conn,
            db_path=db_path,
            baseline_path=baseline_path,
            expected_sha256=expected_sha256,
            expected_release_id=expected_release_id,
            bootstrap_epoch_id=bootstrap_epoch_id,
            active_release_binding_path=active_release_binding_path,
            live_env_path=live_env_path,
        )
        conn.rollback()
        return result
    except (DeliveryQuarantineBaselineError, sqlite3.Error) as exc:
        code = (
            exc.code
            if isinstance(exc, DeliveryQuarantineBaselineError)
            else "delivery_quarantine_baseline_db_unavailable"
        )
        zeros = {name: 0 for name, _table, _key in _QUARANTINE_TABLES}
        return {
            "schema_version": BASELINE_SCHEMA_VERSION,
            "required": True,
            "configured": bool(str(expected_sha256 or "").strip()),
            "path": str(Path(baseline_path).expanduser().absolute()),
            "expected_sha256": str(expected_sha256 or "").strip().lower(),
            "ready": False,
            "state": "unavailable",
            "error_code": code,
            "lifetime": zeros,
            "acknowledged": dict(zeros),
            "unacknowledged": dict(zeros),
            "baseline_identity": None,
        }
    finally:
        if conn is not None:
            conn.close()


def _receipt_descriptors(paths: list[str | Path]) -> list[dict[str, str]]:
    descriptors: list[dict[str, str]] = []
    for path in sorted((Path(item).expanduser().absolute() for item in paths), key=str):
        raw, sha256 = _read_stable_file(
            path,
            artifact="delivery_quarantine_settlement_receipt",
            maximum_bytes=MAX_EVIDENCE_BYTES,
            owner_only=False,
        )
        _strict_json(raw, artifact="delivery_quarantine_settlement_receipt")
        descriptors.append({"path": str(path), "sha256": sha256})
    return descriptors


def _build_quarantine_core(
    db_path: str | Path,
    *,
    target_live_db_path: str | Path,
    migration_receipt_path: str | Path,
    expected_migration_receipt_sha256: str,
    migration_runtime_sha256: str,
    release_id: str,
    snapshot_at: datetime,
    settlement_receipt_paths: list[str | Path],
    analyzed_by: str,
    reason: str,
    busy_timeout_ms: int = 5000,
) -> dict[str, Any]:
    """Build, but never write, the deterministic unapproved release/BOM input."""

    normalized_release_id = str(release_id or "").strip()
    if _IDENTIFIER_RE.fullmatch(normalized_release_id) is None:
        raise DeliveryQuarantineBaselineError("delivery_quarantine_release_id_invalid")
    analyst, justification = _audit_text(analyzed_by, reason)
    observed_at = _iso(snapshot_at)
    descriptors = _receipt_descriptors(settlement_receipt_paths)
    target_path = Path(target_live_db_path).expanduser().absolute()
    source_path = Path(db_path).expanduser().absolute()
    is_offline_clone = source_path != target_path
    migration_binding = _validate_migration_artifact(
        receipt_path=str(migration_receipt_path),
        expected_sha256=expected_migration_receipt_sha256,
        target_live_db_path=target_path,
        migrated_db_path=source_path if is_offline_clone else None,
        migrated_db_is_live=False,
        expected_migration_runtime_sha256=migration_runtime_sha256,
    )
    conn = _open_readonly_db(
        db_path,
        busy_timeout_ms=busy_timeout_ms,
        immutable=is_offline_clone,
    )
    try:
        conn.execute("BEGIN")
        snapshot = quarantine_snapshot(conn)
        event_projection = _quarantine_event_projection(conn)
        descriptors, _receipt_set_sha256, receipt_claims = _settlement_receipts(
            descriptors, required=sum(snapshot["counts"].values()) > 0
        )
        effect_settlement = _effect_settlement_projection(
            conn, receipt_claims["effect_keys"]
        )
        projection = _invalid_manual_thread_projection(conn)
        if projection["invalid_manual_thread"] != projection["total"]:
            raise DeliveryQuarantineBaselineError(
                "delivery_quarantine_baseline_manual_thread_adjudication_invalid"
            )
        body = {
            "schema_version": CORE_SCHEMA_VERSION,
            "release_id": normalized_release_id,
            "snapshot_at": observed_at,
            "control_db": _db_identity(conn, target_path),
            "migration_binding": migration_binding,
            "quarantine_snapshot": snapshot,
            "quarantine_event_projection": event_projection,
            "settlement_receipts": descriptors,
            "effect_settlement": effect_settlement,
            "invalid_manual_thread_adjudication": {
                "effect_kind": DELIVERY_THREAD_EFFECT_KIND,
                "reason_code": "delivery_subscription_target_invalid",
                "technical_finding": "invalid_manual_thread_target",
                "proposed_disposition": "retain_terminal_no_rearm",
                "required": True,
                "source_kind": "feishu_group_manual",
                "source_mode": "rerun",
                "count": projection["invalid_manual_thread"],
                "analyzed_by": analyst,
                "analyzed_at": observed_at,
                "reason": justification,
            },
            "issuance_policy": dict(_ISSUANCE_POLICY),
        }
        core = {
            **body,
            "core_sha256": hashlib.sha256(_canonical_bytes(body)).hexdigest(),
        }
        _validate_core(
            conn,
            db_path=db_path,
            identity_db_path=target_path,
            value=core,
            require_post_migration_match=is_offline_clone,
        )
        conn.rollback()
        return core
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def build_quarantine_core(
    db_path: str | Path,
    *,
    migration_receipt_path: str | Path,
    expected_migration_receipt_sha256: str,
    migration_runtime_sha256: str,
    release_id: str,
    snapshot_at: datetime,
    settlement_receipt_paths: list[str | Path],
    analyzed_by: str,
    reason: str,
    busy_timeout_ms: int = 5000,
) -> dict[str, Any]:
    """Build a core against the exact already-migrated live database."""

    live_path = Path(db_path).expanduser().absolute()
    return _build_quarantine_core(
        live_path,
        target_live_db_path=live_path,
        migration_receipt_path=migration_receipt_path,
        expected_migration_receipt_sha256=expected_migration_receipt_sha256,
        migration_runtime_sha256=migration_runtime_sha256,
        release_id=release_id,
        snapshot_at=snapshot_at,
        settlement_receipt_paths=settlement_receipt_paths,
        analyzed_by=analyzed_by,
        reason=reason,
        busy_timeout_ms=busy_timeout_ms,
    )


def build_quarantine_core_from_offline_clone(
    migrated_clone_path: str | Path,
    *,
    target_live_db_path: str | Path,
    migration_receipt_path: str | Path,
    expected_migration_receipt_sha256: str,
    migration_runtime_sha256: str,
    release_id: str,
    snapshot_at: datetime,
    settlement_receipt_paths: list[str | Path],
    analyzed_by: str,
    reason: str,
    busy_timeout_ms: int = 5000,
) -> dict[str, Any]:
    """Build a preapproval core from a proven offline migrated clone."""

    clone_path = Path(migrated_clone_path).expanduser().absolute()
    live_path = Path(target_live_db_path).expanduser().absolute()
    if clone_path == live_path:
        raise DeliveryQuarantineBaselineError(
            "delivery_quarantine_offline_clone_is_live_database"
        )
    return _build_quarantine_core(
        clone_path,
        target_live_db_path=live_path,
        migration_receipt_path=migration_receipt_path,
        expected_migration_receipt_sha256=expected_migration_receipt_sha256,
        migration_runtime_sha256=migration_runtime_sha256,
        release_id=release_id,
        snapshot_at=snapshot_at,
        settlement_receipt_paths=settlement_receipt_paths,
        analyzed_by=analyzed_by,
        reason=reason,
        busy_timeout_ms=busy_timeout_ms,
    )


def issue_quarantine_baseline(
    db_path: str | Path,
    *,
    quarantine_core: Mapping[str, Any],
    release_manifest_path: str | Path,
    expected_release_manifest_sha256: str,
    expected_release_bom_sha256: str,
    approval_evidence_path: str | Path,
    expected_approval_evidence_sha256: str,
    baseline_id: str,
    issued_at: datetime,
    busy_timeout_ms: int = 5000,
) -> dict[str, Any]:
    """Issue a final baseline only from exact release-scoped approved evidence."""

    normalized_baseline_id = str(baseline_id or "").strip()
    if _IDENTIFIER_RE.fullmatch(normalized_baseline_id) is None:
        raise DeliveryQuarantineBaselineError("delivery_quarantine_baseline_id_invalid")
    issued = _utc(issued_at)
    conn = _open_readonly_db(
        db_path,
        busy_timeout_ms=busy_timeout_ms,
        immutable=False,
    )
    try:
        conn.execute("BEGIN")
        core_value = dict(quarantine_core)
        core = _validate_core(
            conn,
            db_path=db_path,
            value=core_value,
        )
        release_manifest = _validate_release_manifest(
            manifest_path=release_manifest_path,
            expected_sha256=expected_release_manifest_sha256,
            expected_release_bom_sha256=expected_release_bom_sha256,
            release_id=core["release_id"],
            core_sha256=core["core_sha256"],
        )
        approval = _validate_approval(
            evidence_path=approval_evidence_path,
            expected_sha256=expected_approval_evidence_sha256,
            release_id=core["release_id"],
            core_sha256=core["core_sha256"],
            release_bom_sha256=release_manifest["release_bom_sha256"],
        )
        if not (
            core["snapshot_at"]
            <= release_manifest["created_at"]
            <= approval["approved_at"]
            <= issued
        ):
            raise DeliveryQuarantineBaselineError(
                "delivery_quarantine_baseline_time_invalid"
            )
        body = {
            "schema_version": BASELINE_SCHEMA_VERSION,
            "baseline_id": normalized_baseline_id,
            "issued_at": _iso(issued),
            "release_id": core["release_id"],
            "quarantine_core": core_value,
            "quarantine_core_sha256": core["core_sha256"],
            "release_manifest": {
                "path": release_manifest["path"],
                "sha256": release_manifest["sha256"],
                "release_bom_sha256": release_manifest["release_bom_sha256"],
            },
            "owner_attestation": {
                "decision": "approved",
                "release_id": core["release_id"],
                "quarantine_core_sha256": core["core_sha256"],
                "release_bom_sha256": release_manifest["release_bom_sha256"],
                "release_manifest_sha256": release_manifest["sha256"],
                "approval_evidence_path": approval["path"],
                "approval_evidence_sha256": approval["sha256"],
                "approved_by": approval["approved_by"],
                "approved_at": _iso(approval["approved_at"]),
                "reason": approval["reason"],
                "no_database_rows_modified": True,
                "no_rearm": True,
                "no_external_writes": True,
            },
        }
        baseline = {
            **body,
            "baseline_fingerprint": hashlib.sha256(_canonical_bytes(body)).hexdigest(),
        }
        conn.rollback()
        return baseline
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
