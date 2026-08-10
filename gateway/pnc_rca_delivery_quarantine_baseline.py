"""Fail-closed acknowledgement for exact historical RCA delivery quarantine rows."""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import threading
from typing import Any, Mapping

from gateway.pnc_rca_delivery_contract import (
    DELIVERY_THREAD_EFFECT_KIND,
    DeliveryContractError,
    build_terminal_delivery,
    validate_delivery_subscription_target,
)
from gateway.pnc_rca_delivery_quarantine_migration import (
    COMBINED_SCHEMA_VERSION,
    COUPLED_SCHEMA_VERSION,
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
DEFERRED_CORE_SCHEMA_VERSION = "pnc_rca_delivery_quarantine_core_v2"
# A collector can wake in the small interval between an exact activation
# deferral and writer shutdown.  Version 3 records that very narrow, local-only
# materialization shape instead of treating it as an ordinary quarantine.
MATERIALIZED_DEFERRED_CORE_SCHEMA_VERSION = "pnc_rca_delivery_quarantine_core_v3"
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
MAX_MIGRATION_VALIDATION_CACHE_ENTRIES = 16

_HEX64_RE = re.compile(r"^[0-9a-f]{64}$")
_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{7,127}$")
_ACTIVATION_EPOCH_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
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
_DEFERRED_ADJUDICATION_FIELD = "activation_deferred_issue_comment_adjudication"
_DEFERRED_CORE_FIELDS = _CORE_FIELDS | frozenset({_DEFERRED_ADJUDICATION_FIELD})
_MATERIALIZED_DEFERRED_ADJUDICATION_FIELD = (
    "activation_deferred_materialized_delivery_adjudication"
)
_MATERIALIZED_DEFERRED_CORE_FIELDS = _DEFERRED_CORE_FIELDS | frozenset(
    {_MATERIALIZED_DEFERRED_ADJUDICATION_FIELD}
)
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
_DEFERRED_ADJUDICATION_FIELDS = frozenset({
    "effect_kind",
    "reason_code",
    "technical_finding",
    "proposed_disposition",
    "required",
    "source_id",
    "delivery_id",
    "effect_key",
    "delivery_job_present",
    "delivery_effect_present",
    "count",
    "analyzed_by",
    "analyzed_at",
    "reason",
})
_MATERIALIZED_DEFERRED_ADJUDICATION_FIELDS = frozenset({
    "effect_kind",
    "reason_code",
    "technical_finding",
    "proposed_disposition",
    "required",
    "source_kind",
    "source_mode",
    "count",
    "entries",
    "entries_sha256",
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

_MIGRATION_VALIDATION_CACHE_LOCK = threading.Lock()
_MIGRATION_VALIDATION_CACHE: OrderedDict[
    tuple[Any, ...], dict[str, Any]
] = OrderedDict()


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


def _migration_cache_file_identity(path: str | Path) -> tuple[Any, ...] | None:
    selected = Path(path).expanduser().absolute()
    try:
        value = os.lstat(selected)
    except OSError:
        return None
    if stat.S_ISLNK(value.st_mode) or not stat.S_ISREG(value.st_mode):
        return None
    sidecars: list[tuple[str, tuple[int, ...] | None]] = []
    for suffix in ("-wal", "-shm", "-journal"):
        sidecar = Path(str(selected) + suffix)
        try:
            os.lstat(sidecar)
        except FileNotFoundError:
            sidecars.append((suffix, None))
        except OSError:
            return None
        else:
            return None
    return (str(selected), _stat_identity(value), tuple(sidecars))


def _migration_validation_cache_key(
    *,
    receipt_path: str,
    expected_sha256: str,
    target_live_db_path: str | Path,
    migrated_db_path: str | Path | None,
    migrated_db_is_live: bool,
    expected_migration_runtime_sha256: str,
) -> tuple[Any, ...] | None:
    try:
        raw, observed_sha256 = _read_stable_file(
            receipt_path,
            artifact="delivery_quarantine_migration_receipt_probe",
            maximum_bytes=MAX_EVIDENCE_BYTES,
            owner_only=True,
        )
        parsed = json.loads(raw)
    except (DeliveryQuarantineBaselineError, OSError, TypeError, ValueError):
        return None
    if not isinstance(parsed, Mapping):
        return None
    artifact_paths: list[str | Path] = [receipt_path]
    for name in ("source_backup", "migrated_clone"):
        binding = parsed.get(name)
        if isinstance(binding, Mapping) and binding.get("path"):
            artifact_paths.append(str(binding["path"]))
    if migrated_db_path is not None:
        artifact_paths.append(migrated_db_path)
    identities: list[tuple[Any, ...]] = []
    seen: set[str] = set()
    for path in artifact_paths:
        selected = str(Path(path).expanduser().absolute())
        if selected in seen:
            continue
        seen.add(selected)
        identity = _migration_cache_file_identity(selected)
        if identity is None:
            return None
        identities.append(identity)
    return (
        str(Path(receipt_path).expanduser().absolute()),
        observed_sha256,
        str(expected_sha256 or "").strip().lower(),
        str(Path(target_live_db_path).expanduser().absolute()),
        (
            str(Path(migrated_db_path).expanduser().absolute())
            if migrated_db_path is not None
            else ""
        ),
        bool(migrated_db_is_live),
        str(expected_migration_runtime_sha256 or "").strip().lower(),
        tuple(identities),
    )


def _clear_migration_validation_cache() -> None:
    with _MIGRATION_VALIDATION_CACHE_LOCK:
        _MIGRATION_VALIDATION_CACHE.clear()


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


def _require_current_aborted_predecessor(
    conn: sqlite3.Connection,
    *,
    expected_epoch_id: str,
    expected_state: str,
) -> None:
    """Require one exact current predecessor before issuing a successor core."""

    if _ACTIVATION_EPOCH_ID_RE.fullmatch(expected_epoch_id) is None:
        raise DeliveryQuarantineBaselineError(
            "delivery_quarantine_successor_predecessor_epoch_invalid"
        )
    if expected_state != "aborted":
        raise DeliveryQuarantineBaselineError(
            "delivery_quarantine_successor_predecessor_state_invalid"
        )
    if not _table_exists(conn, "rca_activation_epochs"):
        raise DeliveryQuarantineBaselineError(
            "delivery_quarantine_successor_predecessor_epoch_unavailable"
        )
    columns = {
        str(row[1])
        for row in conn.execute("PRAGMA table_info(rca_activation_epochs)")
    }
    required = {
        "epoch_id",
        "state",
        "is_current",
        "db_logical_identity_json",
        "db_logical_identity_sha256",
    }
    if not required.issubset(columns):
        raise DeliveryQuarantineBaselineError(
            "delivery_quarantine_successor_predecessor_epoch_unavailable"
        )
    rows = conn.execute(
        "SELECT epoch_id, state, is_current, db_logical_identity_json, "
        "db_logical_identity_sha256 FROM rca_activation_epochs "
        "WHERE is_current = 1"
    ).fetchall()
    if (
        len(rows) != 1
        or str(rows[0]["epoch_id"] or "") != expected_epoch_id
        or str(rows[0]["state"] or "") != expected_state
        or int(rows[0]["is_current"] or 0) != 1
        or _HEX64_RE.fullmatch(
            str(rows[0]["db_logical_identity_sha256"] or "").lower()
        ) is None
        or not str(rows[0]["db_logical_identity_json"] or "").strip()
    ):
        raise DeliveryQuarantineBaselineError(
            "delivery_quarantine_successor_predecessor_not_current_aborted"
        )


def _project_preactivation_db_identity(
    identity: Mapping[str, Any],
) -> dict[str, Any]:
    """Project only the activation binding away for a pre-activation core."""

    if set(identity) != _DB_IDENTITY_FIELDS:
        raise DeliveryQuarantineBaselineError(
            "delivery_quarantine_successor_db_identity_invalid"
        )
    projected = dict(identity)
    projected["activation_db_logical_identity_sha256"] = ""
    body = {
        key: projected[key]
        for key in _DB_IDENTITY_FIELDS
        if key != "logical_identity_sha256"
    }
    projected["logical_identity_sha256"] = hashlib.sha256(
        _canonical_bytes(body)
    ).hexdigest()
    return projected


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


def _materialized_deferred_payload_matches(row: sqlite3.Row) -> bool:
    """Validate one stored issue-comment effect against its exact contract."""

    raw_payload = row["payload_json"]
    raw_contract = row["job_contract_json"]
    if (
        not isinstance(raw_payload, str)
        or not raw_payload
        or not isinstance(raw_contract, str)
        or not raw_contract
    ):
        return False
    try:
        payload = _strict_json(
            raw_payload.encode("utf-8"),
            artifact="delivery_quarantine_materialized_deferred_payload",
        )
        contract = _strict_json(
            raw_contract.encode("utf-8"),
            artifact="delivery_quarantine_materialized_deferred_contract",
        )
        rebuilt = build_terminal_delivery(
            business_key=str(row["business_key"] or ""),
            submission_key=str(row["submission_key"] or ""),
            generation=int(row["generation"]),
            project_key=str(row["job_project_key"] or ""),
            work_item_type_key=str(row["job_work_item_type_key"] or ""),
            work_item_id=str(row["job_work_item_id"] or ""),
            outcome=str(row["job_outcome"] or ""),
            terminal_state=str(row["terminal_state"] or ""),
            error_code=str(row["terminal_error_code"] or ""),
            diagnostic_code=str(contract.get("diagnostic_code") or ""),
            diagnostic_detail=str(contract.get("diagnostic_detail") or ""),
            terminal_fallback=contract.get("terminal_fallback"),
            schema_version=str(payload.get("schema_version") or ""),
        )
    except (
        DeliveryContractError,
        DeliveryQuarantineBaselineError,
        TypeError,
        ValueError,
        OverflowError,
    ):
        return False
    return (
        dict(payload) == rebuilt.effect_payload
        and dict(contract) == rebuilt.contract
        and str(row["delivery_id"] or "") == rebuilt.delivery_id
        and str(row["effect_key"] or "") == rebuilt.effect_key
        and str(row["effect_kind"] or "") == "feishu_issue_comment"
        and str(row["target_key"] or "") == rebuilt.target_key
        and str(row["job_target_key"] or "") == rebuilt.target_key
        and str(row["payload_sha256"] or "")
        == rebuilt.semantic_payload_sha256
        and payload.get("delivery_id") == rebuilt.delivery_id
        and payload.get("effect_key") == rebuilt.effect_key
        and payload.get("semantic_payload_sha256")
        == rebuilt.semantic_payload_sha256
        and payload.get("marker") == rebuilt.marker
    )


def _activation_deferred_materialized_projection(
    conn: sqlite3.Connection,
) -> dict[str, Any]:
    """Identify only the exact no-write collector race shape.

    A deferred manual submission normally has no delivery job.  If the
    collector wakes before it observes the deferral, it can leave one
    ``ready`` terminal job and one issue-comment effect in ``prewrite``.  The
    activation fence makes that effect ineligible, but the successor release
    still needs an immutable record of the local residue.  Every predicate
    below is intentionally exact; any different shape remains unrecognized
    and fails the release gate.
    """

    rows = conn.execute(
        """
        SELECT thread.subscription_key AS thread_subscription_key,
               issue.subscription_key AS issue_subscription_key,
               source.source_id, source.source_dedupe_key, source.message_id,
               source.mode, source.outcome,
               o.outbox_id, o.submission_key, o.business_key, o.generation,
               o.activation_epoch_id, o.activation_ledger_id,
               al.entrypoint AS ledger_entrypoint,
               al.source_kind AS ledger_source_kind,
               al.decision AS ledger_decision,
               al.bound_at AS ledger_bound_at,
               audit.audit_id, audit.outcome AS audit_outcome,
               audit.from_status AS audit_from_status,
               audit.to_status AS audit_to_status,
               audit.detail AS audit_detail,
               audit.operator AS audit_operator,
               audit.reason AS audit_reason,
               audit.created_at AS audit_created_at,
               w.state AS watch_state, w.delivery_id AS watch_delivery_id,
               j.delivery_id, j.status AS job_status, j.outcome AS job_outcome,
               j.terminal_state, j.terminal_error_code, j.target_key AS job_target_key,
               j.project_key AS job_project_key,
               j.work_item_type_key AS job_work_item_type_key,
               j.work_item_id AS job_work_item_id,
               j.contract_json AS job_contract_json,
               e.effect_key, e.effect_kind, e.status AS effect_status,
               e.write_phase, e.attempt, e.fence, e.target_key,
               e.payload_json, e.payload_sha256,
               e.remote_receipt_json, e.lease_token, e.lease_owner,
               e.lease_expires_at, e.next_attempt_at, e.write_started_at
          FROM rca_delivery_subscriptions AS thread
          JOIN rca_delivery_subscriptions AS issue
            ON issue.business_key = thread.business_key
           AND issue.generation = thread.generation
           AND issue.effect_kind = 'feishu_issue_comment'
           AND issue.status = 'quarantined'
           AND issue.reason = 'activation_epoch_deferred'
           AND issue.source_id IS NULL
           AND issue.delivery_id IS NULL
           AND issue.effect_key IS NULL
          JOIN rca_trigger_sources AS source
            ON source.source_id = thread.source_id
          JOIN rca_outbox AS o
            ON o.business_key = thread.business_key
           AND o.generation = thread.generation
           AND o.origin_source_id = source.source_id
           AND o.status = 'quarantined'
           AND o.last_error_code = 'activation_epoch_deferred'
           AND o.attempt = 0
           AND o.fence = 0
           AND o.lease_token IS NULL
           AND o.lease_owner IS NULL
           AND o.lease_expires_at IS NULL
          JOIN business_triggers AS trigger
            ON trigger.business_key = o.business_key
           AND trigger.generation = o.generation
           AND trigger.submission_key = o.submission_key
           AND trigger.state = 'quarantined'
          JOIN rca_trigger_bindings AS origin
            ON origin.source_id = source.source_id
           AND origin.role = 'origin'
           AND origin.business_key = o.business_key
           AND origin.generation = o.generation
          JOIN rca_activation_admission_ledger AS al
            ON al.epoch_id = o.activation_epoch_id
           AND al.ledger_id = o.activation_ledger_id
           AND al.business_key = o.business_key
           AND al.submission_key = o.submission_key
           AND al.generation = o.generation
           AND al.entrypoint = 'manual_admit'
           AND al.source_kind = 'manual'
           AND al.slot_kind IN ('manual_success', 'manual_terminal_failure')
           AND al.decision = 'admit'
           AND al.bound_at IS NOT NULL
          JOIN rca_shadow_promotion_audit AS audit
            ON audit.outbox_id = o.outbox_id
           AND audit.submission_key = o.submission_key
           AND audit.event_uid = source.message_id
           AND audit.outcome = 'deferred'
           AND audit.from_status = 'pending'
           AND audit.to_status = 'quarantined'
           AND audit.detail =
               'exact activation item deferred for reviewed manual recovery'
          JOIN rca_execution_watch AS w
            ON w.submission_outbox_id = o.outbox_id
           AND w.submission_key = o.submission_key
           AND w.business_key = o.business_key
           AND w.generation = o.generation
           AND w.state = 'delivery_created'
           AND w.poll_attempt = 0
           AND w.fence = 0
           AND w.lease_token IS NULL
           AND w.lease_owner IS NULL
           AND w.lease_expires_at IS NULL
          JOIN rca_delivery_jobs AS j
            ON j.delivery_id = w.delivery_id
           AND j.submission_key = o.submission_key
           AND j.business_key = o.business_key
           AND j.generation = o.generation
           AND j.project_key = trigger.project_key
           AND j.work_item_type_key = trigger.work_item_type_key
           AND j.work_item_id = trigger.work_item_id
           AND j.status = 'ready'
           AND j.outcome = 'quarantined'
           AND j.terminal_state = 'submission_quarantined'
           AND j.terminal_error_code = 'outbox_submission_quarantined'
           AND j.created_at >= audit.created_at
          JOIN rca_delivery_effects AS e
            ON e.delivery_id = j.delivery_id
           AND e.effect_kind = 'feishu_issue_comment'
           AND e.required = 1
           AND e.outcome = 'quarantined'
           AND e.target_key = j.target_key
           AND e.status = 'pending'
           AND e.write_phase = 'prewrite'
           AND e.attempt = 0
           AND e.fence = 0
           AND e.remote_receipt_json IS NULL
           AND e.lease_token IS NULL
           AND e.lease_owner IS NULL
           AND e.lease_expires_at IS NULL
           AND e.next_attempt_at IS NULL
           AND e.write_started_at IS NULL
           AND e.created_at >= audit.created_at
         WHERE thread.status = 'quarantined'
           AND thread.effect_kind = 'feishu_thread_reply'
           AND thread.reason = 'activation_epoch_deferred'
           AND thread.delivery_id IS NULL
           AND thread.effect_key IS NULL
           AND thread.required = 1
           AND source.source_kind = 'feishu_group_manual'
           AND source.source_dedupe_key = 'feishu:' || source.message_id
           AND source.mode = 'run_or_join'
           AND source.outcome IN ('created', 'joined')
           AND NOT EXISTS (
                 SELECT 1 FROM rca_delivery_effects AS thread_effect
                  WHERE thread_effect.delivery_id = j.delivery_id
                    AND thread_effect.effect_kind = 'feishu_thread_reply'
           )
           AND NOT EXISTS (
                 SELECT 1 FROM rca_delivery_attempts AS attempt
                  WHERE attempt.effect_key = e.effect_key
           )
           AND NOT EXISTS (
                 SELECT 1 FROM rca_delivery_observation_outbox AS observation
                  WHERE observation.effect_key = e.effect_key
           )
         ORDER BY thread.subscription_key
        """
    ).fetchall()
    entries: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        if not _materialized_deferred_payload_matches(row):
            continue
        thread_key = str(row["thread_subscription_key"] or "")
        if not thread_key or thread_key in seen:
            raise DeliveryQuarantineBaselineError(
                "delivery_quarantine_materialized_deferred_duplicate"
            )
        seen.add(thread_key)
        if (
            not str(row["message_id"] or "")
            or not str(row["activation_epoch_id"] or "")
            or int(row["activation_ledger_id"] or 0) <= 0
            or str(row["ledger_bound_at"] or "") == ""
            or str(row["audit_operator"] or "") == ""
            or str(row["audit_reason"] or "") == ""
        ):
            raise DeliveryQuarantineBaselineError(
                "delivery_quarantine_materialized_deferred_binding_invalid"
            )
        try:
            ledger_bound_at = _parse_iso(row["ledger_bound_at"])
            audit_created_at = _parse_iso(row["audit_created_at"])
            _audit_text(row["audit_operator"], row["audit_reason"])
            if audit_created_at < ledger_bound_at:
                raise ValueError("deferral audit predates activation binding")
        except (
            DeliveryQuarantineBaselineError,
            TypeError,
            ValueError,
            OverflowError,
        ) as exc:
            raise DeliveryQuarantineBaselineError(
                "delivery_quarantine_materialized_deferred_binding_invalid"
            ) from exc
        remote_receipt = str(row["remote_receipt_json"] or "")
        entries.append(
            {
                "thread_subscription_key": thread_key,
                "issue_subscription_key": str(row["issue_subscription_key"] or ""),
                "source_id": str(row["source_id"] or ""),
                "source_dedupe_key": str(row["source_dedupe_key"] or ""),
                "message_id": str(row["message_id"] or ""),
                "source_mode": str(row["mode"] or ""),
                "source_outcome": str(row["outcome"] or ""),
                "outbox_id": int(row["outbox_id"]),
                "submission_key": str(row["submission_key"] or ""),
                "business_key": str(row["business_key"] or ""),
                "generation": int(row["generation"]),
                "activation_epoch_id": str(row["activation_epoch_id"] or ""),
                "activation_ledger_id": int(row["activation_ledger_id"]),
                "ledger_entrypoint": str(row["ledger_entrypoint"] or ""),
                "ledger_source_kind": str(row["ledger_source_kind"] or ""),
                "ledger_decision": str(row["ledger_decision"] or ""),
                "ledger_bound_at": str(row["ledger_bound_at"] or ""),
                "audit_id": int(row["audit_id"]),
                "audit_outcome": str(row["audit_outcome"] or ""),
                "audit_from_status": str(row["audit_from_status"] or ""),
                "audit_to_status": str(row["audit_to_status"] or ""),
                "audit_detail": str(row["audit_detail"] or ""),
                "audit_operator": str(row["audit_operator"] or ""),
                "audit_reason_sha256": hashlib.sha256(
                    str(row["audit_reason"] or "").encode("utf-8")
                ).hexdigest(),
                "audit_created_at": str(row["audit_created_at"] or ""),
                "watch_state": str(row["watch_state"] or ""),
                "delivery_id": str(row["delivery_id"] or ""),
                "job_status": str(row["job_status"] or ""),
                "job_outcome": str(row["job_outcome"] or ""),
                "terminal_state": str(row["terminal_state"] or ""),
                "terminal_error_code": str(row["terminal_error_code"] or ""),
                "effect_key": str(row["effect_key"] or ""),
                "effect_kind": str(row["effect_kind"] or ""),
                "effect_status": str(row["effect_status"] or ""),
                "write_phase": str(row["write_phase"] or ""),
                "attempt": int(row["attempt"]),
                "fence": int(row["fence"]),
                "payload_sha256": str(row["payload_sha256"] or ""),
                "remote_receipt_sha256": (
                    hashlib.sha256(remote_receipt.encode("utf-8")).hexdigest()
                    if remote_receipt
                    else ""
                ),
            }
        )
    entries_sha256 = hashlib.sha256(_canonical_bytes(entries)).hexdigest()
    return {
        "count": len(entries),
        "entries": entries,
        "entries_sha256": entries_sha256,
    }


def _activation_deferred_unmaterialized_thread_keys(
    conn: sqlite3.Connection,
) -> set[str]:
    """Return exact manual thread companions stopped before delivery creation."""

    rows = conn.execute(
        """
        SELECT thread.subscription_key
          FROM rca_delivery_subscriptions AS thread
          JOIN rca_delivery_subscriptions AS issue
            ON issue.business_key = thread.business_key
           AND issue.generation = thread.generation
           AND issue.effect_kind = 'feishu_issue_comment'
           AND issue.required = 1
           AND issue.status = 'quarantined'
           AND issue.reason = 'activation_epoch_deferred'
           AND issue.source_id IS NULL
           AND issue.delivery_id IS NULL
           AND issue.effect_key IS NULL
          JOIN rca_trigger_sources AS source
            ON source.source_id = thread.source_id
          JOIN rca_outbox AS o
            ON o.business_key = thread.business_key
           AND o.generation = thread.generation
           AND o.origin_source_id = source.source_id
           AND o.status = 'quarantined'
           AND o.last_error_code = 'activation_epoch_deferred'
           AND o.next_attempt_at IS NULL
           AND o.lease_token IS NULL
           AND o.lease_owner IS NULL
           AND o.lease_expires_at IS NULL
           AND o.completed_at IS NULL
           AND o.result_json IS NULL
           AND o.quarantined_at IS NOT NULL
          JOIN business_triggers AS trigger
            ON trigger.business_key = o.business_key
           AND trigger.generation = o.generation
           AND trigger.submission_key = o.submission_key
           AND trigger.state = 'quarantined'
           AND trigger.activation_epoch_id = o.activation_epoch_id
           AND trigger.activation_ledger_id = o.activation_ledger_id
          JOIN rca_activation_epochs AS epoch
            ON epoch.epoch_id = o.activation_epoch_id
           AND epoch.state = 'aborted'
           AND (epoch.is_current = 1 OR epoch.superseded_at IS NOT NULL)
          JOIN rca_activation_admission_ledger AS ledger
            ON ledger.epoch_id = o.activation_epoch_id
           AND ledger.ledger_id = o.activation_ledger_id
           AND ledger.business_key = o.business_key
           AND ledger.submission_key = o.submission_key
           AND ledger.generation = o.generation
           AND ledger.entrypoint = 'manual_admit'
           AND ledger.source_kind = 'manual'
           AND ledger.slot_kind IN ('manual_success', 'manual_terminal_failure')
           AND ledger.decision = 'admit'
           AND ledger.bound_at IS NOT NULL
         WHERE thread.status = 'quarantined'
           AND thread.effect_kind = 'feishu_thread_reply'
           AND thread.required = 1
           AND thread.reason = 'activation_epoch_deferred'
           AND thread.delivery_id IS NULL
           AND thread.effect_key IS NULL
           AND source.source_kind = 'feishu_group_manual'
           AND source.source_dedupe_key = 'feishu:' || source.message_id
           AND source.mode = 'run_or_join'
           AND source.outcome IN ('created', 'joined')
           AND NOT EXISTS (
                 SELECT 1 FROM rca_execution_watch AS watch
                  WHERE watch.submission_outbox_id = o.outbox_id
                     OR watch.submission_key = o.submission_key
           )
           AND NOT EXISTS (
                 SELECT 1 FROM rca_delivery_jobs AS job
                  WHERE job.submission_key = o.submission_key
                     OR (
                          job.business_key = o.business_key
                          AND job.generation = o.generation
                     )
           )
         ORDER BY thread.subscription_key
        """
    ).fetchall()
    return {str(row["subscription_key"]) for row in rows}


def _quarantined_subscription_projection(
    conn: sqlite3.Connection,
) -> dict[str, Any]:
    total = 0
    invalid = 0
    deferred = 0
    snapshot_covered_technical_terminal = 0
    materialized_deferred = _activation_deferred_materialized_projection(conn)
    materialized_keys = {
        str(item["thread_subscription_key"])
        for item in materialized_deferred["entries"]
    }
    unmaterialized_thread_keys = (
        _activation_deferred_unmaterialized_thread_keys(conn)
    )
    unmaterialized_threads = 0
    rows = conn.execute(
        """
        SELECT subscription.*, job.project_key, job.work_item_type_key,
               job.work_item_id, job.delivery_id AS joined_delivery_id,
               effect.effect_key AS joined_effect_key,
               source.source_kind, source.mode
          FROM rca_delivery_subscriptions AS subscription
          LEFT JOIN rca_delivery_jobs AS job
            ON job.delivery_id = subscription.delivery_id
          LEFT JOIN rca_delivery_effects AS effect
            ON effect.effect_key = subscription.effect_key
          LEFT JOIN rca_trigger_sources AS source
            ON source.source_id = subscription.source_id
         WHERE subscription.status = 'quarantined'
         ORDER BY subscription.subscription_key
        """
    ).fetchall()
    for row in rows:
        total += 1
        if str(row["subscription_key"] or "") in materialized_keys:
            continue
        if str(row["subscription_key"] or "") in unmaterialized_thread_keys:
            unmaterialized_threads += 1
            continue
        if (
            int(row["required"]) == 1
            and str(row["effect_kind"]) == "feishu_issue_comment"
            and str(row["reason"] or "") == "activation_epoch_deferred"
            and row["source_id"] is None
            and row["delivery_id"] is None
            and row["effect_key"] is None
            and row["joined_delivery_id"] is None
            and row["joined_effect_key"] is None
        ):
            deferred += 1
            continue
        if (
            int(row["required"]) == 1
            and str(row["effect_kind"]) == "feishu_issue_comment"
            and str(row["reason"] or "") == "w3_execution_snapshot_missing"
            and row["source_id"] is None
            and row["delivery_id"] is None
            and row["effect_key"] is None
            and row["joined_delivery_id"] is None
            and row["joined_effect_key"] is None
        ):
            # This is a pre-delivery technical terminal. It has no provider
            # effect to adjudicate and remains bound by the full quarantine
            # row-set/event digests, so it must not block a successor release.
            snapshot_covered_technical_terminal += 1
            continue
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
    return {
        "total": total,
        "invalid_manual_thread": invalid,
        "activation_deferred_issue_comment": deferred,
        "activation_deferred_materialized_delivery": materialized_deferred["count"],
        "activation_deferred_unmaterialized_thread": unmaterialized_threads,
        "unrecognized": total
        - invalid
        - deferred
        - materialized_deferred["count"]
        - unmaterialized_threads
        - snapshot_covered_technical_terminal,
    }


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


def _prewrite_quarantine_without_external_write(
    conn: sqlite3.Connection, row: sqlite3.Row
) -> bool:
    """Recognize a terminal quarantine that provably had no provider I/O."""
    if (
        str(row["job_status"] or "") != "quarantined"
        or str(row["effect_status"] or "") != "quarantined"
        or str(row["write_phase"] or "") != "settled"
        or not str(row["quarantined_at"] or "")
        or row["remote_receipt_json"] is not None
        or row["write_started_at"] is not None
        or row["completed_at"] is not None
        or int(row["attempt"] or 0) < 1
        or int(row["fence"] or 0) < 1
    ):
        return False
    try:
        attempts = conn.execute(
            """
            SELECT outcome, remote_id, error_code
              FROM rca_delivery_attempts
             WHERE effect_key = ?
             ORDER BY attempt_no, event_seq
            """,
            (str(row["effect_key"]),),
        ).fetchall()
    except sqlite3.Error:
        return False
    if not attempts or not any(
        str(attempt["outcome"] or "") == "quarantined" for attempt in attempts
    ):
        return False
    return all(not str(attempt["remote_id"] or "").strip() for attempt in attempts)


def _effect_settlement_projection(
    conn: sqlite3.Connection, receipt_effect_keys: set[str]
) -> dict[str, Any]:
    rows = conn.execute(
        """
        SELECT effect.effect_key, effect.effect_kind, effect.required,
               effect.target_key, effect.write_phase, effect.status AS effect_status,
               effect.remote_receipt_json, effect.write_started_at,
               effect.completed_at, effect.quarantined_at, effect.attempt,
               effect.fence, effect.last_error_code, job.delivery_id,
               job.business_key, job.generation, job.project_key,
               job.work_item_type_key, job.work_item_id,
               job.status AS job_status, job.outcome AS job_outcome,
               subscription.effect_kind AS subscription_effect_kind,
               subscription.required AS subscription_required,
               subscription.status AS subscription_status,
               subscription.target_json AS subscription_target_json,
               source.source_kind, source.mode
          FROM rca_delivery_effects AS effect
          JOIN rca_delivery_jobs AS job
            ON job.delivery_id = effect.delivery_id
          LEFT JOIN rca_delivery_subscriptions AS subscription
            ON subscription.effect_key = effect.effect_key
          LEFT JOIN rca_trigger_sources AS source
            ON source.source_id = subscription.source_id
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
    prewrite_no_external_write = 0
    retained_terminal = 0
    for row in rows:
        effect_key = str(row["effect_key"])
        effect_kind = str(row["effect_kind"])
        if int(row["required"]) != 1 or str(row["write_phase"]) != "settled":
            raise DeliveryQuarantineBaselineError(
                "delivery_quarantine_settlement_receipt_scope_invalid"
            )
        if effect_kind == DELIVERY_THREAD_EFFECT_KIND:
            try:
                target = _strict_json(
                    str(row["subscription_target_json"] or "").encode("utf-8"),
                    artifact="delivery_quarantine_subscription_target",
                )
                validate_delivery_subscription_target(
                    effect_kind=effect_kind,
                    target_key=str(row["target_key"]),
                    target=target,
                    project_key=str(row["project_key"] or ""),
                    work_item_type_key=str(row["work_item_type_key"] or ""),
                    work_item_id=str(row["work_item_id"] or ""),
                )
            except (DeliveryContractError, DeliveryQuarantineBaselineError) as exc:
                raise DeliveryQuarantineBaselineError(
                    "delivery_quarantine_settlement_receipt_scope_invalid"
                ) from exc
            if (
                str(row["subscription_effect_kind"] or "") != effect_kind
                or int(row["subscription_required"] or 0) != 1
                or str(row["subscription_status"] or "")
                not in {"materialized", "quarantined"}
                or str(row["source_kind"] or "") != "feishu_group_manual"
                or str(row["mode"] or "") not in {"rerun", "run_or_join"}
            ):
                raise DeliveryQuarantineBaselineError(
                    "delivery_quarantine_settlement_receipt_scope_invalid"
                )
        elif effect_kind != "feishu_issue_comment":
            raise DeliveryQuarantineBaselineError(
                "delivery_quarantine_settlement_receipt_scope_invalid"
            )
        entry = {
            "quarantined_effect_key": effect_key,
            "business_key": str(row["business_key"]),
            "work_item_id": str(row["work_item_id"]),
            "generation": int(row["generation"]),
            "effect_kind": effect_kind,
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
        if _prewrite_quarantine_without_external_write(conn, row):
            entries.append(
                {
                    **entry,
                    "disposition": "terminal_no_external_write",
                    "evidence_effect_key": effect_key,
                    "superseding_delivery_id": "",
                    "superseding_generation": 0,
                    "superseding_completed_at": "",
                    "superseding_remote_receipt_sha256": "",
                    "superseding_confirmed_field_keys": [],
                }
            )
            prewrite_no_external_write += 1
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
            entries.append(
                {
                    **entry,
                    "disposition": "retained_terminal",
                    "evidence_effect_key": effect_key,
                    "superseding_delivery_id": "",
                    "superseding_generation": 0,
                    "superseding_completed_at": "",
                    "superseding_remote_receipt_sha256": "",
                    "superseding_confirmed_field_keys": [],
                }
            )
            retained_terminal += 1
            continue
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
    if prewrite_no_external_write:
        body["prewrite_no_external_write_count"] = prewrite_no_external_write
    if retained_terminal:
        body["retained_terminal_count"] = retained_terminal
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


def _validate_migration_artifact_uncached(
    *,
    receipt_path: str,
    expected_sha256: str,
    target_live_db_path: str | Path,
    migrated_db_path: str | Path | None,
    migrated_db_is_live: bool,
    expected_migration_runtime_sha256: str,
) -> dict[str, Any]:
    try:
        # v3 delivery-only and v4 coupled receipts are both handled by the
        # combined validator.  Only unknown schemas fall back to the original
        # v1 receipt validator; this prevents a v4 artifact from being silently
        # interpreted as an unrelated legacy migration.
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
        if receipt_schema in {COMBINED_SCHEMA_VERSION, COUPLED_SCHEMA_VERSION}:
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


def _validate_migration_artifact(
    *,
    receipt_path: str,
    expected_sha256: str,
    target_live_db_path: str | Path,
    migrated_db_path: str | Path | None,
    migrated_db_is_live: bool,
    expected_migration_runtime_sha256: str,
) -> dict[str, Any]:
    cache_key = _migration_validation_cache_key(
        receipt_path=receipt_path,
        expected_sha256=expected_sha256,
        target_live_db_path=target_live_db_path,
        migrated_db_path=migrated_db_path,
        migrated_db_is_live=migrated_db_is_live,
        expected_migration_runtime_sha256=expected_migration_runtime_sha256,
    )
    if cache_key is None:
        return _validate_migration_artifact_uncached(
            receipt_path=receipt_path,
            expected_sha256=expected_sha256,
            target_live_db_path=target_live_db_path,
            migrated_db_path=migrated_db_path,
            migrated_db_is_live=migrated_db_is_live,
            expected_migration_runtime_sha256=(
                expected_migration_runtime_sha256
            ),
        )
    with _MIGRATION_VALIDATION_CACHE_LOCK:
        cached = _MIGRATION_VALIDATION_CACHE.get(cache_key)
        if cached is not None:
            _MIGRATION_VALIDATION_CACHE.move_to_end(cache_key)
            return dict(cached)
        binding = _validate_migration_artifact_uncached(
            receipt_path=receipt_path,
            expected_sha256=expected_sha256,
            target_live_db_path=target_live_db_path,
            migrated_db_path=migrated_db_path,
            migrated_db_is_live=migrated_db_is_live,
            expected_migration_runtime_sha256=(
                expected_migration_runtime_sha256
            ),
        )
        post_validation_key = _migration_validation_cache_key(
            receipt_path=receipt_path,
            expected_sha256=expected_sha256,
            target_live_db_path=target_live_db_path,
            migrated_db_path=migrated_db_path,
            migrated_db_is_live=migrated_db_is_live,
            expected_migration_runtime_sha256=(
                expected_migration_runtime_sha256
            ),
        )
        if post_validation_key != cache_key:
            raise DeliveryQuarantineBaselineError(
                "delivery_quarantine_migration_artifact_unstable"
            )
        _MIGRATION_VALIDATION_CACHE[cache_key] = dict(binding)
        while (
            len(_MIGRATION_VALIDATION_CACHE)
            > MAX_MIGRATION_VALIDATION_CACHE_ENTRIES
        ):
            _MIGRATION_VALIDATION_CACHE.popitem(last=False)
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
    if not isinstance(value, Mapping):
        raise DeliveryQuarantineBaselineError("delivery_quarantine_core_schema_invalid")
    schema_version = value.get("schema_version")
    expected_fields = (
        _CORE_FIELDS
        if schema_version == CORE_SCHEMA_VERSION
        else (
            _DEFERRED_CORE_FIELDS
            if schema_version == DEFERRED_CORE_SCHEMA_VERSION
            else (
                _MATERIALIZED_DEFERRED_CORE_FIELDS
                if schema_version == MATERIALIZED_DEFERRED_CORE_SCHEMA_VERSION
                else None
            )
        )
    )
    if expected_fields is None or set(value) != expected_fields:
        raise DeliveryQuarantineBaselineError("delivery_quarantine_core_schema_invalid")
    release_id = str(value.get("release_id") or "").strip()
    core_sha256 = str(value.get("core_sha256") or "").strip().lower()
    body = {key: item for key, item in value.items() if key != "core_sha256"}
    if (
        _IDENTIFIER_RE.fullmatch(release_id) is None
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
    projection = _quarantined_subscription_projection(conn)
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
        or projection["unrecognized"] != 0
        or str(adjudication.get("analyzed_at") or "") != _iso(snapshot_at)
    ):
        raise DeliveryQuarantineBaselineError(
            "delivery_quarantine_baseline_manual_thread_adjudication_invalid"
        )
    _audit_text(adjudication.get("analyzed_by"), adjudication.get("reason"))
    deferred_count = projection["activation_deferred_issue_comment"]
    deferred_adjudication = value.get(_DEFERRED_ADJUDICATION_FIELD)
    if schema_version == CORE_SCHEMA_VERSION:
        if deferred_count != 0 or projection["activation_deferred_materialized_delivery"] != 0:
            raise DeliveryQuarantineBaselineError(
                "delivery_quarantine_baseline_deferred_issue_adjudication_invalid"
            )
    elif (
        deferred_count <= 0
        or not isinstance(deferred_adjudication, Mapping)
        or set(deferred_adjudication) != _DEFERRED_ADJUDICATION_FIELDS
        or deferred_adjudication.get("effect_kind") != "feishu_issue_comment"
        or deferred_adjudication.get("reason_code") != "activation_epoch_deferred"
        or deferred_adjudication.get("technical_finding")
        != "deferred_before_delivery_materialization"
        or deferred_adjudication.get("proposed_disposition")
        != "retain_terminal_for_fresh_rerun"
        or deferred_adjudication.get("required") is not True
        or deferred_adjudication.get("source_id") is not None
        or deferred_adjudication.get("delivery_id") is not None
        or deferred_adjudication.get("effect_key") is not None
        or deferred_adjudication.get("delivery_job_present") is not False
        or deferred_adjudication.get("delivery_effect_present") is not False
        or isinstance(deferred_adjudication.get("count"), bool)
        or deferred_adjudication.get("count") != deferred_count
        or str(deferred_adjudication.get("analyzed_at") or "")
        != _iso(snapshot_at)
    ):
        raise DeliveryQuarantineBaselineError(
            "delivery_quarantine_baseline_deferred_issue_adjudication_invalid"
        )
    if schema_version == DEFERRED_CORE_SCHEMA_VERSION:
        _audit_text(
            deferred_adjudication.get("analyzed_by"),
            deferred_adjudication.get("reason"),
        )
        if projection["activation_deferred_materialized_delivery"] != 0:
            raise DeliveryQuarantineBaselineError(
                "delivery_quarantine_baseline_materialized_deferred_adjudication_invalid"
            )
    elif schema_version == MATERIALIZED_DEFERRED_CORE_SCHEMA_VERSION:
        _audit_text(
            deferred_adjudication.get("analyzed_by"),
            deferred_adjudication.get("reason"),
        )
        materialized = value.get(_MATERIALIZED_DEFERRED_ADJUDICATION_FIELD)
        materialized_projection = _activation_deferred_materialized_projection(conn)
        if (
            projection["activation_deferred_materialized_delivery"] <= 0
            or not isinstance(materialized, Mapping)
            or set(materialized) != _MATERIALIZED_DEFERRED_ADJUDICATION_FIELDS
            or materialized.get("effect_kind") != "feishu_issue_comment"
            or materialized.get("reason_code") != "activation_epoch_deferred"
            or materialized.get("technical_finding")
            != "terminal_delivery_materialized_before_writer_stop"
            or materialized.get("proposed_disposition")
            != "retain_prewrite_no_rearm"
            or materialized.get("required") is not True
            or materialized.get("source_kind") != "feishu_group_manual"
            or materialized.get("source_mode") != "run_or_join"
            or isinstance(materialized.get("count"), bool)
            or materialized.get("count")
            != projection["activation_deferred_materialized_delivery"]
            or materialized.get("entries") != materialized_projection["entries"]
            or materialized.get("entries_sha256")
            != materialized_projection["entries_sha256"]
            or str(materialized.get("analyzed_at") or "") != _iso(snapshot_at)
        ):
            raise DeliveryQuarantineBaselineError(
                "delivery_quarantine_baseline_materialized_deferred_adjudication_invalid"
            )
        _audit_text(materialized.get("analyzed_by"), materialized.get("reason"))
    elif projection["activation_deferred_materialized_delivery"] != 0:
        raise DeliveryQuarantineBaselineError(
            "delivery_quarantine_baseline_materialized_deferred_adjudication_invalid"
        )
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
    expected_predecessor_epoch_id: str | None = None,
    expected_predecessor_state: str | None = None,
    project_empty_activation_binding: bool = False,
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
        if project_empty_activation_binding:
            if (
                expected_predecessor_epoch_id is None
                or expected_predecessor_state is None
            ):
                raise DeliveryQuarantineBaselineError(
                    "delivery_quarantine_successor_predecessor_required"
                )
            _require_current_aborted_predecessor(
                conn,
                expected_epoch_id=expected_predecessor_epoch_id,
                expected_state=expected_predecessor_state,
            )
        snapshot = quarantine_snapshot(conn)
        event_projection = _quarantine_event_projection(conn)
        descriptors, _receipt_set_sha256, receipt_claims = _settlement_receipts(
            descriptors, required=sum(snapshot["counts"].values()) > 0
        )
        effect_settlement = _effect_settlement_projection(
            conn, receipt_claims["effect_keys"]
        )
        projection = _quarantined_subscription_projection(conn)
        if projection["unrecognized"] != 0:
            raise DeliveryQuarantineBaselineError(
                "delivery_quarantine_baseline_manual_thread_adjudication_invalid"
            )
        control_db = _db_identity(conn, target_path)
        if project_empty_activation_binding:
            control_db = _project_preactivation_db_identity(control_db)
        deferred_count = projection["activation_deferred_issue_comment"]
        materialized_deferred = _activation_deferred_materialized_projection(conn)
        materialized_count = materialized_deferred["count"]
        body = {
            "schema_version": (
                MATERIALIZED_DEFERRED_CORE_SCHEMA_VERSION
                if materialized_count
                else (
                    DEFERRED_CORE_SCHEMA_VERSION
                    if deferred_count
                    else CORE_SCHEMA_VERSION
                )
            ),
            "release_id": normalized_release_id,
            "snapshot_at": observed_at,
            "control_db": control_db,
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
        if deferred_count:
            body[_DEFERRED_ADJUDICATION_FIELD] = {
                "effect_kind": "feishu_issue_comment",
                "reason_code": "activation_epoch_deferred",
                "technical_finding": "deferred_before_delivery_materialization",
                "proposed_disposition": "retain_terminal_for_fresh_rerun",
                "required": True,
                "source_id": None,
                "delivery_id": None,
                "effect_key": None,
                "delivery_job_present": False,
                "delivery_effect_present": False,
                "count": deferred_count,
                "analyzed_by": analyst,
                "analyzed_at": observed_at,
                "reason": justification,
            }
        if materialized_count:
            body[_MATERIALIZED_DEFERRED_ADJUDICATION_FIELD] = {
                "effect_kind": "feishu_issue_comment",
                "reason_code": "activation_epoch_deferred",
                "technical_finding": (
                    "terminal_delivery_materialized_before_writer_stop"
                ),
                "proposed_disposition": "retain_prewrite_no_rearm",
                "required": True,
                "source_kind": "feishu_group_manual",
                "source_mode": "run_or_join",
                "count": materialized_count,
                "entries": materialized_deferred["entries"],
                "entries_sha256": materialized_deferred["entries_sha256"],
                "analyzed_by": analyst,
                "analyzed_at": observed_at,
                "reason": justification,
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


def build_successor_preactivation_quarantine_core(
    db_path: str | Path,
    *,
    expected_predecessor_epoch_id: str,
    expected_predecessor_state: str,
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
    """Build a successor pre-activation core from one exact live snapshot.

    The predecessor must be the current, explicitly aborted activation epoch.
    All quarantine, event, migration, and settlement evidence is read from the
    same read-only transaction; only the activation DB binding is projected to
    the safe-off form expected before the successor epoch is created.
    """

    predecessor_id = str(expected_predecessor_epoch_id or "").strip()
    predecessor_state = str(expected_predecessor_state or "").strip()
    if _ACTIVATION_EPOCH_ID_RE.fullmatch(predecessor_id) is None:
        raise DeliveryQuarantineBaselineError(
            "delivery_quarantine_successor_predecessor_epoch_invalid"
        )
    if predecessor_state != "aborted":
        raise DeliveryQuarantineBaselineError(
            "delivery_quarantine_successor_predecessor_state_invalid"
        )
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
        expected_predecessor_epoch_id=predecessor_id,
        expected_predecessor_state=predecessor_state,
        project_empty_activation_binding=True,
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
