"""Offline-only migration evidence for pre-approved delivery quarantine cores."""

from __future__ import annotations

from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import stat
from typing import Any, Iterator, Mapping

from gateway.pnc_rca_conclusion_adjudication import (
    validate_conclusion_adjudication_schema,
)


SCHEMA_VERSION = "pnc_rca_delivery_quarantine_offline_migration_v1"
SOURCE_SCHEMA_VERSION = "pnc_rca_delivery_store_v6"
TARGET_SCHEMA_VERSION = "pnc_rca_delivery_store_v8"
COMBINED_SCHEMA_VERSION = "pnc_rca_delivery_store_offline_migration_v2"
COMBINED_SOURCE_SCHEMA_VERSIONS = frozenset(
    {"pnc_rca_delivery_store_v7", "pnc_rca_delivery_store_v8"}
)
COMBINED_TARGET_SCHEMA_VERSION = "pnc_rca_delivery_store_v9"
MAX_ARTIFACT_BYTES = 64 * 1024 * 1024
_HEX64 = frozenset("0123456789abcdef")
_FIELDS = frozenset({
    "schema_version",
    "source_backup",
    "source_backup_normalization",
    "source_schema_version",
    "source_logical_projection",
    "target_live_db_path",
    "target_schema_version",
    "post_migration_logical_projection",
    "post_migration_health",
    "migration_runtime_sha256",
    "deterministic_seed_policy",
    "no_live_database_writes",
})
_NORMALIZATION_FIELDS = frozenset({
    "method",
    "journal_mode",
    "integrity_check",
    "foreign_key_violation_count",
    "byte_identical_live_copy",
})
_POST_MIGRATION_HEALTH_FIELDS = frozenset({
    "integrity_check",
    "foreign_key_violation_count",
})
_PROJECTION_FIELDS = frozenset({"schema_sha256", "tables", "logical_sha256"})
_TABLE_PROJECTION_FIELDS = frozenset({
    "columns",
    "primary_key",
    "row_count",
    "rows_sha256",
})
_COMBINED_FIELDS = frozenset(
    {
        "schema_version",
        "source_backup",
        "source_schema_version",
        "source_schema_variant",
        "source_schema_contract",
        "source_logical_projection",
        "source_health",
        "migrated_clone",
        "target_live_db_path",
        "target_schema_version",
        "post_migration_logical_projection",
        "post_migration_health",
        "cross_projection_preservation",
        "migration_runtime_sha256",
        "migration_semantics",
        "no_live_database_writes",
    }
)
_COMBINED_HEALTH_FIELDS = frozenset(
    {"journal_mode", "quick_check", "integrity_check", "foreign_key_violation_count"}
)
_COMBINED_SEMANTICS_FIELDS = frozenset(
    {
        "execution_mode",
        "copy_method",
        "quick_check_gate",
        "rollback_path",
        "rollback_sha256",
        "live_replacement_performed",
    }
)
_COMBINED_PRESERVATION_POLICY = "all_source_owned_rows_exact_v1"
_COMBINED_ADDED_SCHEMA_POLICY = (
    "allowlisted_deterministic_tables_and_constant_default_columns_v1"
)
_COMBINED_ACTIVE_PROD_V7_VARIANT = "active_prod_v7_no_adjudication_v1"
_COMBINED_W2_V8_VARIANT = "w2_v8_failure_routes_adjudication_v1"
_COMBINED_ALLOWED_ADDED_TABLES = {
    _COMBINED_ACTIVE_PROD_V7_VARIANT: frozenset(
        {
            "rca_conclusion_adjudication_repairs",
            "rca_conclusion_adjudications",
            "rca_failure_routes",
        }
    ),
    _COMBINED_W2_V8_VARIANT: frozenset(
        {"rca_conclusion_adjudication_repairs"}
    ),
}
_COMBINED_ALLOWED_ADDED_COLUMN_DEFAULTS = {
    "rca_delivery_effects": {
        "adjudication_comment_attempt_count": 0,
        "adjudication_comment_attempted_at": None,
    }
}
_COMBINED_CANONICAL_V9_OBJECTS = {
    "idx_failure_routes_status": (
        "index",
        "rca_failure_routes",
        "b140853f5a022522ce9e75975dab2059c9ec87c407726475836da32ee91882b3",
    ),
    "idx_failure_routes_submission": (
        "index",
        "rca_failure_routes",
        "60dda2b01be6789b9938a293a2d49716dd514f56527aab76973db5b2e47b7fa5",
    ),
    "idx_rca_conclusion_adjudication_impact": (
        "index",
        "rca_conclusion_adjudications",
        "a85932259cc12bcd9780cf9fc2273a6512463622eac0d68b7c0321c44938173d",
    ),
    "idx_rca_conclusion_adjudication_repairs_status": (
        "index",
        "rca_conclusion_adjudication_repairs",
        "bcc35d19738802f60f03afe3205415ea5b90c3fdd293a49b43dd71a1743f1e0a",
    ),
    "rca_conclusion_adjudication_repairs": (
        "table",
        "rca_conclusion_adjudication_repairs",
        "6161be57110c7379ea184368f45648077f1f2ba626919af8f201655971b016b7",
    ),
    "rca_conclusion_adjudications": (
        "table",
        "rca_conclusion_adjudications",
        "12a382a8af11080bec6e7e65e80bf076bee5ff9ab15c35c24784d6270922da3e",
    ),
    "rca_failure_routes": (
        "table",
        "rca_failure_routes",
        "75ef385c21ec1908e9929acbb680eca00cd2f38ece4d65f2bbf23395d846fb52",
    ),
    "trg_rca_conclusion_adjudication_no_delete": (
        "trigger",
        "rca_conclusion_adjudications",
        "d5f15d30af872024b0fa92d820121b4ad7b2445a2474a1ec1ddd528b37bd0e1a",
    ),
    "trg_rca_conclusion_adjudication_no_update": (
        "trigger",
        "rca_conclusion_adjudications",
        "3cec6349b8b4fd3efc57bb6f612b6894d69fc8a983be4c83d131b01950ae043e",
    ),
}
_COMBINED_W2_SOURCE_TABLES = frozenset(
    {"rca_conclusion_adjudications", "rca_failure_routes"}
)


class QuarantineMigrationError(RuntimeError):
    def __init__(self, code: str):
        self.code = str(code or "delivery_quarantine_migration_invalid")[:120]
        super().__init__(self.code)


def _hex64(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    if len(normalized) != 64 or any(char not in _HEX64 for char in normalized):
        raise QuarantineMigrationError("delivery_quarantine_migration_sha256_invalid")
    return normalized


def _canonical(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode()
    except (TypeError, ValueError, OverflowError, RecursionError) as exc:
        raise QuarantineMigrationError(
            "delivery_quarantine_migration_not_canonical"
        ) from exc


def canonical_migration_receipt_bytes(value: Mapping[str, Any]) -> bytes:
    return _canonical(dict(value)) + b"\n"


def _strict_json(raw: bytes) -> Mapping[str, Any]:
    def unique(items: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in items:
            if key in result:
                raise QuarantineMigrationError(
                    "delivery_quarantine_migration_duplicate_key"
                )
            result[key] = value
        return result

    def invalid_number(_value: str) -> None:
        raise QuarantineMigrationError("delivery_quarantine_migration_number_invalid")

    try:
        value = json.loads(
            raw.decode(), object_pairs_hook=unique, parse_constant=invalid_number
        )
    except QuarantineMigrationError:
        raise
    except (UnicodeError, json.JSONDecodeError, RecursionError) as exc:
        raise QuarantineMigrationError(
            "delivery_quarantine_migration_json_invalid"
        ) from exc
    if not isinstance(value, Mapping):
        raise QuarantineMigrationError("delivery_quarantine_migration_shape_invalid")
    return value


def _identity(value: os.stat_result) -> tuple[int, ...]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_uid,
        value.st_nlink,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _read_owner_artifact(path: str | Path, *, artifact: str) -> tuple[bytes, str]:
    selected = Path(path).expanduser().absolute()
    if not hasattr(os, "O_NOFOLLOW"):
        raise QuarantineMigrationError(f"{artifact}_no_follow_unavailable")
    try:
        descriptor = os.open(
            selected,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | os.O_NOFOLLOW,
        )
    except OSError as exc:
        raise QuarantineMigrationError(f"{artifact}_unavailable") from exc
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != os.geteuid()
            or stat.S_IMODE(before.st_mode) != 0o600
            or before.st_nlink != 1
            or before.st_size < 2
            or before.st_size > MAX_ARTIFACT_BYTES
        ):
            raise QuarantineMigrationError(f"{artifact}_permissions_invalid")
        chunks: list[bytes] = []
        total = 0
        while total <= MAX_ARTIFACT_BYTES:
            chunk = os.read(descriptor, min(65536, MAX_ARTIFACT_BYTES + 1 - total))
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
        after = os.fstat(descriptor)
        try:
            lexical = os.lstat(selected)
        except OSError as exc:
            raise QuarantineMigrationError(f"{artifact}_unstable") from exc
        if (
            total > MAX_ARTIFACT_BYTES
            or stat.S_ISLNK(lexical.st_mode)
            or (after.st_dev, after.st_ino) != (lexical.st_dev, lexical.st_ino)
            or _identity(before) != _identity(after)
        ):
            raise QuarantineMigrationError(f"{artifact}_unstable")
        raw = b"".join(chunks)
        return raw, hashlib.sha256(raw).hexdigest()
    except QuarantineMigrationError:
        raise
    except OSError as exc:
        raise QuarantineMigrationError(f"{artifact}_unstable") from exc
    finally:
        os.close(descriptor)


def _assert_open_identity(
    *,
    descriptor: int,
    before: os.stat_result,
    selected: Path,
    artifact: str,
) -> None:
    try:
        after = os.fstat(descriptor)
        lexical = os.lstat(selected)
    except OSError as exc:
        raise QuarantineMigrationError(f"{artifact}_unstable") from exc
    if (
        _identity(before) != _identity(after)
        or stat.S_ISLNK(lexical.st_mode)
        or (after.st_dev, after.st_ino) != (lexical.st_dev, lexical.st_ino)
    ):
        raise QuarantineMigrationError(f"{artifact}_unstable")


def _assert_no_sqlite_sidecars(path: Path, *, artifact: str) -> None:
    for suffix in ("-wal", "-shm", "-journal"):
        try:
            os.lstat(str(path) + suffix)
        except FileNotFoundError:
            continue
        except OSError as exc:
            raise QuarantineMigrationError(f"{artifact}_sidecar_unstable") from exc
        raise QuarantineMigrationError(f"{artifact}_sidecar_present")


@contextmanager
def _open_readonly(
    path: str | Path,
    *,
    artifact: str = "delivery_quarantine_migration_db",
    require_standalone: bool = False,
    immutable: bool = False,
) -> Iterator[sqlite3.Connection]:
    selected = Path(path).expanduser().absolute()
    if require_standalone:
        _assert_no_sqlite_sidecars(selected, artifact=artifact)
    if not hasattr(os, "O_NOFOLLOW"):
        raise QuarantineMigrationError(f"{artifact}_no_follow_unavailable")
    try:
        descriptor = os.open(
            selected,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | os.O_NOFOLLOW,
        )
    except OSError as exc:
        raise QuarantineMigrationError(f"{artifact}_unavailable") from exc
    conn: sqlite3.Connection | None = None
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != os.geteuid()
            or stat.S_IMODE(before.st_mode) != 0o600
            or before.st_nlink != 1
            or before.st_size < 2
            or before.st_size > MAX_ARTIFACT_BYTES
        ):
            raise QuarantineMigrationError(f"{artifact}_permissions_invalid")
        conn = sqlite3.connect(
            f"{selected.as_uri()}?mode=ro" + ("&immutable=1" if immutable else ""),
            uri=True,
            isolation_level=None,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only=ON")
        _assert_open_identity(
            descriptor=descriptor,
            before=before,
            selected=selected,
            artifact=artifact,
        )
        if require_standalone:
            _assert_no_sqlite_sidecars(selected, artifact=artifact)
        yield conn
        _assert_open_identity(
            descriptor=descriptor,
            before=before,
            selected=selected,
            artifact=artifact,
        )
        if require_standalone:
            _assert_no_sqlite_sidecars(selected, artifact=artifact)
    except QuarantineMigrationError:
        raise
    except sqlite3.Error as exc:
        raise QuarantineMigrationError(f"{artifact}_unavailable") from exc
    finally:
        if conn is not None:
            conn.close()
        os.close(descriptor)


def _quote_identifier(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def _value(value: Any) -> Any:
    if isinstance(value, bytes):
        return {
            "bytes_sha256": hashlib.sha256(value).hexdigest(),
            "size_bytes": len(value),
        }
    return value


def logical_database_projection(conn: sqlite3.Connection) -> dict[str, Any]:
    """Canonical full logical state, including tables, indexes, and triggers."""

    schema_rows = conn.execute(
        "SELECT type, name, tbl_name, sql FROM sqlite_master "
        "WHERE name NOT LIKE 'sqlite_%' OR name = 'sqlite_sequence' "
        "ORDER BY type, name"
    ).fetchall()
    schema = [
        {
            "type": str(row["type"]),
            "name": str(row["name"]),
            "table": str(row["tbl_name"]),
            "sql": str(row["sql"] or ""),
        }
        for row in schema_rows
    ]
    tables: dict[str, Any] = {}
    for item in schema:
        if item["type"] != "table":
            continue
        table = item["name"]
        quoted_table = _quote_identifier(table)
        info = list(conn.execute(f"PRAGMA table_info({quoted_table})"))
        columns = [str(row["name"]) for row in info]
        primary = [
            str(row["name"])
            for row in sorted(info, key=lambda row: int(row["pk"]))
            if int(row["pk"]) > 0
        ]
        order = primary or columns
        order_sql = ", ".join(_quote_identifier(name) for name in order)
        rows = [
            {column: _value(row[column]) for column in columns}
            for row in conn.execute(
                f"SELECT * FROM {quoted_table} ORDER BY {order_sql}"
            )
        ]
        tables[table] = {
            "columns": columns,
            "primary_key": primary,
            "row_count": len(rows),
            "rows_sha256": hashlib.sha256(_canonical(rows)).hexdigest(),
        }
    body = {
        "schema_sha256": hashlib.sha256(_canonical(schema)).hexdigest(),
        "tables": tables,
    }
    return {
        **body,
        "logical_sha256": hashlib.sha256(_canonical(body)).hexdigest(),
    }


def _project_table_rows(
    conn: sqlite3.Connection,
    *,
    table: str,
    columns: list[str],
    order: list[str],
) -> list[dict[str, Any]]:
    selected_sql = ", ".join(_quote_identifier(name) for name in columns)
    order_sql = ", ".join(_quote_identifier(name) for name in order)
    return [
        {column: _value(row[column]) for column in columns}
        for row in conn.execute(
            f"SELECT {selected_sql} FROM {_quote_identifier(table)} "
            f"ORDER BY {order_sql}"
        )
    ]


def _combined_source_schema_variant(
    conn: sqlite3.Connection,
    *,
    source_version: str,
) -> str:
    tables = {
        str(row[0])
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        )
    }
    effect_columns = {
        str(row[1])
        for row in conn.execute("PRAGMA table_info(rca_delivery_effects)")
    }
    attempt_columns = {
        "adjudication_comment_attempt_count",
        "adjudication_comment_attempted_at",
    }
    adjudication_table = "rca_conclusion_adjudications"
    repair_table = "rca_conclusion_adjudication_repairs"
    failure_table = "rca_failure_routes"
    if source_version == "pnc_rca_delivery_store_v7":
        if (
            {adjudication_table, repair_table, failure_table} & tables
            or attempt_columns & effect_columns
        ):
            raise QuarantineMigrationError(
                "delivery_store_combined_migration_"
                "source_variant_operator_remediation"
            )
        return _COMBINED_ACTIVE_PROD_V7_VARIANT
    if source_version != "pnc_rca_delivery_store_v8":
        raise QuarantineMigrationError(
            "delivery_store_combined_migration_source_schema_invalid"
        )
    if repair_table in tables:
        repair_columns = {
            str(row[1])
            for row in conn.execute(
                "PRAGMA table_info(rca_conclusion_adjudication_repairs)"
            )
        }
        if "status" in repair_columns and conn.execute(
            "SELECT 1 FROM rca_conclusion_adjudication_repairs "
            "WHERE status = 'succeeded' LIMIT 1"
        ).fetchone() is not None:
            raise QuarantineMigrationError(
                "delivery_store_combined_migration_"
                "legacy_adjudication_receipt_operator_remediation"
            )
        raise QuarantineMigrationError(
            "delivery_store_combined_migration_source_variant_operator_remediation"
        )
    if (
        adjudication_table not in tables
        or failure_table not in tables
        or attempt_columns & effect_columns
    ):
        raise QuarantineMigrationError(
            "delivery_store_combined_migration_source_variant_operator_remediation"
        )
    adjudication_columns = {
        str(row[1])
        for row in conn.execute("PRAGMA table_info(rca_conclusion_adjudications)")
    }
    if "activation_epoch_id" not in adjudication_columns:
        raise QuarantineMigrationError(
            "delivery_store_combined_migration_source_variant_operator_remediation"
        )
    _assert_combined_schema_object_contract(
        conn,
        owned_tables=_COMBINED_W2_SOURCE_TABLES,
        error_code=(
            "delivery_store_combined_migration_source_schema_contract_invalid"
        ),
    )
    if conn.execute(
        "SELECT 1 FROM rca_conclusion_adjudications "
        "WHERE TRIM(activation_epoch_id) = '' LIMIT 1"
    ).fetchone() is not None:
        raise QuarantineMigrationError(
            "delivery_store_combined_migration_"
            "legacy_adjudication_activation_operator_remediation"
        )
    return _COMBINED_W2_V8_VARIANT


def _combined_source_schema_contract(
    *,
    source_variant: str,
    source_projection: Mapping[str, Any],
    expected_source_schema_sha256: str,
) -> dict[str, Any]:
    expected_sha256 = _hex64(expected_source_schema_sha256)
    if source_projection.get("schema_sha256") != expected_sha256:
        raise QuarantineMigrationError(
            "delivery_store_combined_migration_source_schema_contract_invalid"
        )
    return {
        "contract_id": f"external_full_schema_{source_variant}",
        "schema_sha256": expected_sha256,
        "table_count": len(source_projection.get("tables", {})),
        "authority": "caller_supplied_external_predecessor_schema",
    }


def _combined_deterministic_transforms(
    source_version: str,
    *,
    source_variant: str,
) -> list[dict[str, Any]]:
    transforms = [
        {
            "rule": "replace_exact_schema_version_marker_v1",
            "table": "rca_delivery_meta",
            "selector": {"key": "schema_version"},
            "column": "value",
            "source_value": source_version,
            "target_value": COMBINED_TARGET_SCHEMA_VERSION,
        }
    ]
    if source_variant == _COMBINED_W2_V8_VARIANT:
        transforms.append(
            {
                "rule": "backfill_pending_repairs_from_adjudication_created_at_v1",
                "source_table": "rca_conclusion_adjudications",
                "target_table": "rca_conclusion_adjudication_repairs",
                "key_column": "adjudication_id",
                "timestamp_rule": "created_at_copied_to_created_at_and_updated_at",
                "status": "pending",
                "receipt_binding": "empty",
            }
        )
    return transforms


def _expected_combined_target_rows(
    *,
    table: str,
    source_rows: list[dict[str, Any]],
    source_version: str,
) -> list[dict[str, Any]]:
    expected = [dict(row) for row in source_rows]
    if table != "rca_delivery_meta":
        return expected
    markers = [
        row
        for row in expected
        if row.get("key") == "schema_version"
        and row.get("value") == source_version
    ]
    if len(markers) != 1:
        raise QuarantineMigrationError(
            "delivery_store_combined_migration_cross_projection_mismatch"
        )
    markers[0]["value"] = COMBINED_TARGET_SCHEMA_VERSION
    return expected


def _expected_combined_added_table_rows(
    *,
    source: sqlite3.Connection,
    table: str,
    target_columns: list[str],
    source_variant: str,
) -> list[dict[str, Any]]:
    if (
        source_variant != _COMBINED_W2_V8_VARIANT
        or table != "rca_conclusion_adjudication_repairs"
    ):
        return []
    source_rows = _project_table_rows(
        source,
        table="rca_conclusion_adjudications",
        columns=["adjudication_id", "created_at"],
        order=["adjudication_id"],
    )
    expected: list[dict[str, Any]] = []
    for source_row in source_rows:
        values = {
            "adjudication_id": source_row["adjudication_id"],
            "status": "pending",
            "attempt_count": 0,
            "last_error_code": "",
            "last_error_detail": "",
            "receipt_schema_version": "",
            "receipt_path": "",
            "receipt_offset": -1,
            "receipt_length": 0,
            "receipt_sha256": "",
            "receipt_device": 0,
            "receipt_inode": 0,
            "receipt_event_id": "",
            "created_at": source_row["created_at"],
            "updated_at": source_row["created_at"],
            "completed_at": None,
        }
        if set(values) != set(target_columns):
            raise QuarantineMigrationError(
                "delivery_store_combined_migration_cross_projection_mismatch"
            )
        expected.append({column: values[column] for column in target_columns})
    return expected


def _normalized_schema_objects(
    conn: sqlite3.Connection,
) -> dict[str, dict[str, str]]:
    rows = conn.execute(
        "SELECT type, name, tbl_name, sql FROM sqlite_master "
        "WHERE name NOT LIKE 'sqlite_%' OR name = 'sqlite_sequence' "
        "ORDER BY type, name"
    ).fetchall()
    return {
        str(row[1]): {
            "type": str(row[0]),
            "table": str(row[2]),
            "sql": str(row[3] or ""),
            "normalized_sql": "".join(str(row[3] or "").split()).lower(),
        }
        for row in rows
    }


def _assert_combined_schema_object_contract(
    conn: sqlite3.Connection,
    *,
    owned_tables: frozenset[str],
    error_code: str,
) -> None:
    objects = _normalized_schema_objects(conn)
    expected = {
        name: contract
        for name, contract in _COMBINED_CANONICAL_V9_OBJECTS.items()
        if contract[1] in owned_tables
    }
    observed = {
        name: item
        for name, item in objects.items()
        if item["table"] in owned_tables
    }
    if set(observed) != set(expected):
        raise QuarantineMigrationError(error_code)
    for name, (expected_type, expected_table, expected_sha256) in expected.items():
        item = observed[name]
        observed_sha256 = hashlib.sha256(
            item["normalized_sql"].encode()
        ).hexdigest()
        if (
            item["type"] != expected_type
            or item["table"] != expected_table
            or observed_sha256 != expected_sha256
        ):
            raise QuarantineMigrationError(error_code)


def _validate_combined_target_schema_contract(conn: sqlite3.Connection) -> None:
    try:
        validate_conclusion_adjudication_schema(conn)
    except RuntimeError as exc:
        raise QuarantineMigrationError(
            "delivery_store_combined_migration_target_schema_contract_invalid"
        ) from exc
    _assert_combined_schema_object_contract(
        conn,
        owned_tables=frozenset(
            {
                "rca_conclusion_adjudications",
                "rca_conclusion_adjudication_repairs",
                "rca_failure_routes",
            }
        ),
        error_code=(
            "delivery_store_combined_migration_target_schema_contract_invalid"
        ),
    )


def _expected_effects_v9_schema_sql(source_sql: str) -> str:
    scratch = sqlite3.connect(":memory:")
    try:
        scratch.execute(source_sql)
        scratch.execute(
            "ALTER TABLE rca_delivery_effects "
            "ADD COLUMN adjudication_comment_attempt_count INTEGER NOT NULL "
            "DEFAULT 0 CHECK (adjudication_comment_attempt_count IN (0, 1))"
        )
        scratch.execute(
            "ALTER TABLE rca_delivery_effects "
            "ADD COLUMN adjudication_comment_attempted_at TEXT"
        )
        row = scratch.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'table' "
            "AND name = 'rca_delivery_effects'"
        ).fetchone()
    except sqlite3.Error as exc:
        raise QuarantineMigrationError(
            "delivery_store_combined_migration_cross_schema_mismatch"
        ) from exc
    finally:
        scratch.close()
    return "".join(str(row[0] if row else "").split()).lower()


def _combined_source_schema_preservation(
    *,
    source: sqlite3.Connection,
    clone: sqlite3.Connection,
    allowed_added_tables: frozenset[str],
) -> dict[str, Any]:
    source_objects = _normalized_schema_objects(source)
    target_objects = _normalized_schema_objects(clone)
    if not set(source_objects).issubset(target_objects):
        raise QuarantineMigrationError(
            "delivery_store_combined_migration_cross_schema_mismatch"
        )
    evidence: dict[str, Any] = {}
    for name, source_object in sorted(source_objects.items()):
        target_object = target_objects[name]
        expected_sql = source_object["normalized_sql"]
        if name == "rca_delivery_effects" and source_object["type"] == "table":
            expected_sql = _expected_effects_v9_schema_sql(source_object["sql"])
        if (
            target_object["type"] != source_object["type"]
            or target_object["table"] != source_object["table"]
            or target_object["normalized_sql"] != expected_sql
        ):
            raise QuarantineMigrationError(
                "delivery_store_combined_migration_cross_schema_mismatch"
            )
        evidence[name] = {
            "type": source_object["type"],
            "table": source_object["table"],
            "source_sql_sha256": hashlib.sha256(
                source_object["normalized_sql"].encode()
            ).hexdigest(),
            "expected_target_sql_sha256": hashlib.sha256(
                expected_sql.encode()
            ).hexdigest(),
            "observed_target_sql_sha256": hashlib.sha256(
                target_object["normalized_sql"].encode()
            ).hexdigest(),
        }
    added_objects = {
        name: item
        for name, item in target_objects.items()
        if name not in source_objects
    }
    expected_added_objects = {
        name: contract
        for name, contract in _COMBINED_CANONICAL_V9_OBJECTS.items()
        if contract[1] in allowed_added_tables
    }
    if set(added_objects) != set(expected_added_objects):
        raise QuarantineMigrationError(
            "delivery_store_combined_migration_cross_schema_mismatch"
        )
    for name, (expected_type, expected_table, expected_sha256) in (
        expected_added_objects.items()
    ):
        item = added_objects[name]
        if (
            item["type"] != expected_type
            or item["table"] != expected_table
            or hashlib.sha256(item["normalized_sql"].encode()).hexdigest()
            != expected_sha256
        ):
            raise QuarantineMigrationError(
                "delivery_store_combined_migration_cross_schema_mismatch"
            )
    return {
        "policy": "all_source_owned_sqlite_master_objects_exact_v1",
        "source_owned_objects": evidence,
        "added_target_objects": {
            name: {
                "type": item["type"],
                "table": item["table"],
                "sql_sha256": hashlib.sha256(
                    item["normalized_sql"].encode()
                ).hexdigest(),
            }
            for name, item in sorted(added_objects.items())
        },
    }


def _combined_cross_projection_preservation(
    *,
    source: sqlite3.Connection,
    clone: sqlite3.Connection,
    source_version: str,
    source_variant: str,
    source_projection: Mapping[str, Any],
    target_projection: Mapping[str, Any],
) -> dict[str, Any]:
    """Prove the v9 clone preserves every source-owned row semantically."""

    source_tables = source_projection["tables"]
    target_tables = target_projection["tables"]
    source_names = set(source_tables)
    target_names = set(target_tables)
    allowed_added_tables = _COMBINED_ALLOWED_ADDED_TABLES.get(source_variant)
    if (
        allowed_added_tables is None
        or not source_names.issubset(target_names)
        or target_names - source_names != allowed_added_tables
    ):
        raise QuarantineMigrationError(
            "delivery_store_combined_migration_cross_projection_mismatch"
        )

    schema_preservation = _combined_source_schema_preservation(
        source=source,
        clone=clone,
        allowed_added_tables=allowed_added_tables,
    )
    table_evidence: dict[str, Any] = {}
    for table in sorted(source_names):
        source_table = source_tables[table]
        target_table = target_tables[table]
        source_columns = list(source_table["columns"])
        target_columns = list(target_table["columns"])
        added_columns = target_columns[len(source_columns) :]
        allowed_defaults = _COMBINED_ALLOWED_ADDED_COLUMN_DEFAULTS.get(table, {})
        if (
            target_columns[: len(source_columns)] != source_columns
            or added_columns != list(allowed_defaults)
        ):
            raise QuarantineMigrationError(
                "delivery_store_combined_migration_cross_projection_mismatch"
            )
        order = list(source_table["primary_key"]) or source_columns
        source_rows = _project_table_rows(
            source,
            table=table,
            columns=source_columns,
            order=order,
        )
        observed_target_rows = _project_table_rows(
            clone,
            table=table,
            columns=source_columns,
            order=order,
        )
        expected_target_rows = _expected_combined_target_rows(
            table=table,
            source_rows=source_rows,
            source_version=source_version,
        )
        if observed_target_rows != expected_target_rows:
            raise QuarantineMigrationError(
                "delivery_store_combined_migration_cross_projection_mismatch"
            )
        if added_columns:
            added_values = _project_table_rows(
                clone,
                table=table,
                columns=added_columns,
                order=order,
            )
            if any(
                row != allowed_defaults
                for row in added_values
            ):
                raise QuarantineMigrationError(
                    "delivery_store_combined_migration_cross_projection_mismatch"
                )
        source_rows_sha256 = hashlib.sha256(_canonical(source_rows)).hexdigest()
        expected_target_rows_sha256 = hashlib.sha256(
            _canonical(expected_target_rows)
        ).hexdigest()
        observed_target_rows_sha256 = hashlib.sha256(
            _canonical(observed_target_rows)
        ).hexdigest()
        if (
            len(source_rows) != source_table["row_count"]
            or source_rows_sha256 != source_table["rows_sha256"]
            or len(observed_target_rows) != target_table["row_count"]
            or expected_target_rows_sha256 != observed_target_rows_sha256
        ):
            raise QuarantineMigrationError(
                "delivery_store_combined_migration_cross_projection_mismatch"
            )
        table_evidence[table] = {
            "source_columns": source_columns,
            "added_target_columns": [
                {
                    "name": name,
                    "existing_row_value": allowed_defaults[name],
                }
                for name in added_columns
            ],
            "source_row_count": len(source_rows),
            "target_preserved_row_count": len(observed_target_rows),
            "source_rows_sha256": source_rows_sha256,
            "expected_target_rows_sha256": expected_target_rows_sha256,
            "observed_target_rows_sha256": observed_target_rows_sha256,
        }

    added_table_evidence: dict[str, Any] = {}
    for table in sorted(allowed_added_tables):
        target_table = target_tables[table]
        target_columns = list(target_table["columns"])
        target_order = list(target_table["primary_key"]) or target_columns
        expected_rows = _expected_combined_added_table_rows(
            source=source,
            table=table,
            target_columns=target_columns,
            source_variant=source_variant,
        )
        observed_rows = _project_table_rows(
            clone,
            table=table,
            columns=target_columns,
            order=target_order,
        )
        expected_sha256 = hashlib.sha256(_canonical(expected_rows)).hexdigest()
        observed_sha256 = hashlib.sha256(_canonical(observed_rows)).hexdigest()
        if (
            observed_rows != expected_rows
            or target_table["row_count"] != len(expected_rows)
            or target_table["rows_sha256"] != observed_sha256
        ):
            raise QuarantineMigrationError(
                "delivery_store_combined_migration_cross_projection_mismatch"
            )
        added_table_evidence[table] = {
            "expected_row_count": len(expected_rows),
            "observed_row_count": len(observed_rows),
            "expected_rows_sha256": expected_sha256,
            "observed_rows_sha256": observed_sha256,
        }
    return {
        "policy": _COMBINED_PRESERVATION_POLICY,
        "added_schema_policy": _COMBINED_ADDED_SCHEMA_POLICY,
        "deterministic_transforms": _combined_deterministic_transforms(
            source_version,
            source_variant=source_variant,
        ),
        "source_owned_schema": schema_preservation,
        "source_owned_tables": table_evidence,
        "added_target_tables": added_table_evidence,
    }


def _database_health(conn: sqlite3.Connection) -> dict[str, Any]:
    integrity = [str(row[0]) for row in conn.execute("PRAGMA integrity_check")]
    foreign_key_violations = list(conn.execute("PRAGMA foreign_key_check"))
    if integrity != ["ok"] or foreign_key_violations:
        raise QuarantineMigrationError(
            "delivery_quarantine_migration_database_integrity_invalid"
        )
    return {
        "journal_mode": str(conn.execute("PRAGMA journal_mode").fetchone()[0]).lower(),
        "integrity_check": "ok",
        "foreign_key_violation_count": 0,
    }


def _combined_database_health(conn: sqlite3.Connection) -> dict[str, Any]:
    quick = [str(row[0]) for row in conn.execute("PRAGMA quick_check")]
    base = _database_health(conn)
    if quick != ["ok"]:
        raise QuarantineMigrationError(
            "delivery_store_combined_migration_quick_check_invalid"
        )
    return {**base, "quick_check": "ok"}


def logical_database_projection_path(
    path: str | Path,
    *,
    require_standalone: bool = False,
    require_integrity: bool = False,
) -> dict[str, Any]:
    with _open_readonly(path, require_standalone=require_standalone) as conn:
        conn.execute("BEGIN")
        if require_integrity:
            _database_health(conn)
        result = logical_database_projection(conn)
        conn.rollback()
        return result


def _guarded_live_projection(
    path: str | Path,
    *,
    validate_seed: bool = False,
    combined_health: bool = False,
) -> tuple[dict[str, Any], dict[str, Any]]:
    selected = Path(path).expanduser().absolute()
    try:
        before = os.lstat(selected)
    except OSError as exc:
        raise QuarantineMigrationError(
            "delivery_quarantine_live_database_unavailable"
        ) from exc
    _assert_no_sqlite_sidecars(
        selected,
        artifact="delivery_quarantine_live_database",
    )
    with _open_readonly(
        selected,
        artifact="delivery_quarantine_live_database",
        require_standalone=True,
        immutable=True,
    ) as conn:
        conn.execute("BEGIN")
        journal_mode_before = str(
            conn.execute("PRAGMA journal_mode").fetchone()[0]
        ).lower()
        health = (
            _combined_database_health(conn)
            if combined_health
            else _database_health(conn)
        )
        if validate_seed:
            _validate_deterministic_seed(conn)
        schema_version = _schema_version(conn)
        projection = logical_database_projection(conn)
        journal_mode_after = str(
            conn.execute("PRAGMA journal_mode").fetchone()[0]
        ).lower()
        conn.rollback()
    try:
        after = os.lstat(selected)
    except OSError as exc:
        raise QuarantineMigrationError(
            "delivery_quarantine_live_database_unstable"
        ) from exc
    _assert_no_sqlite_sidecars(
        selected,
        artifact="delivery_quarantine_live_database",
    )
    if (
        _identity(before) != _identity(after)
        or journal_mode_before != journal_mode_after
    ):
        raise QuarantineMigrationError(
            "delivery_quarantine_live_database_changed_during_validation"
        )
    return projection, {
        "journal_mode": journal_mode_before,
        "schema_version": schema_version,
        "device": int(before.st_dev),
        "inode": int(before.st_ino),
        "size_bytes": int(before.st_size),
        "main_mtime_ns": before.st_mtime_ns,
        "main_ctime_ns": before.st_ctime_ns,
        "sidecars": [],
        **health,
    }


def _standalone_projection_and_health(
    path: str | Path,
) -> tuple[dict[str, Any], dict[str, Any]]:
    with _open_readonly(
        path,
        artifact="delivery_quarantine_source_backup",
        require_standalone=True,
    ) as conn:
        conn.execute("BEGIN")
        health = _database_health(conn)
        projection = logical_database_projection(conn)
        conn.rollback()
    return projection, health


def _validate_projection(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != _PROJECTION_FIELDS:
        raise QuarantineMigrationError(
            "delivery_quarantine_migration_projection_invalid"
        )
    tables = value.get("tables")
    if not isinstance(tables, Mapping):
        raise QuarantineMigrationError(
            "delivery_quarantine_migration_projection_invalid"
        )
    normalized_tables: dict[str, Any] = {}
    for raw_name, raw_table in tables.items():
        name = str(raw_name or "")
        if (
            not name
            or name != raw_name
            or not isinstance(raw_table, Mapping)
            or set(raw_table) != _TABLE_PROJECTION_FIELDS
        ):
            raise QuarantineMigrationError(
                "delivery_quarantine_migration_projection_invalid"
            )
        columns = raw_table.get("columns")
        primary_key = raw_table.get("primary_key")
        row_count = raw_table.get("row_count")
        if (
            not isinstance(columns, list)
            or not columns
            or any(not isinstance(item, str) or not item for item in columns)
            or len(set(columns)) != len(columns)
            or not isinstance(primary_key, list)
            or any(item not in columns for item in primary_key)
            or len(set(primary_key)) != len(primary_key)
            or isinstance(row_count, bool)
            or not isinstance(row_count, int)
            or row_count < 0
        ):
            raise QuarantineMigrationError(
                "delivery_quarantine_migration_projection_invalid"
            )
        normalized_tables[name] = {
            "columns": list(columns),
            "primary_key": list(primary_key),
            "row_count": row_count,
            "rows_sha256": _hex64(raw_table.get("rows_sha256")),
        }
    normalized = {
        "schema_sha256": _hex64(value.get("schema_sha256")),
        "tables": normalized_tables,
    }
    logical_sha256 = _hex64(value.get("logical_sha256"))
    if hashlib.sha256(_canonical(normalized)).hexdigest() != logical_sha256:
        raise QuarantineMigrationError(
            "delivery_quarantine_migration_projection_invalid"
        )
    return {**normalized, "logical_sha256": logical_sha256}


def _schema_version(conn: sqlite3.Connection) -> str:
    row = conn.execute(
        "SELECT value FROM rca_delivery_meta WHERE key = 'schema_version'"
    ).fetchone()
    return str(row[0] if row is not None else "")


def _validate_deterministic_seed(conn: sqlite3.Connection) -> None:
    rows = conn.execute(
        "SELECT audit_id, entity_kind, entity_key, operation, observed_at "
        "FROM rca_delivery_quarantine_mutation_audit ORDER BY audit_id"
    ).fetchall()
    expected = (
        [
            ("job", str(row[0]), str(row[1]))
            for row in conn.execute(
                "SELECT delivery_id, updated_at FROM rca_delivery_jobs "
                "WHERE status = 'quarantined' ORDER BY delivery_id"
            )
        ]
        + [
            ("effect", str(row[0]), str(row[1]))
            for row in conn.execute(
                "SELECT effect_key, updated_at FROM rca_delivery_effects "
                "WHERE status = 'quarantined' ORDER BY effect_key"
            )
        ]
        + [
            ("subscription", str(row[0]), str(row[1]))
            for row in conn.execute(
                "SELECT subscription_key, updated_at "
                "FROM rca_delivery_subscriptions WHERE status = 'quarantined' "
                "ORDER BY subscription_key"
            )
        ]
    )
    observed = [
        (str(row["entity_kind"]), str(row["entity_key"]), str(row["observed_at"]))
        for row in rows
    ]
    if (
        observed != expected
        or any(str(row["operation"]) != "migration_observed" for row in rows)
        or [int(row["audit_id"]) for row in rows] != list(range(1, len(rows) + 1))
    ):
        raise QuarantineMigrationError(
            "delivery_quarantine_migration_seed_nondeterministic"
        )


def build_offline_migration_receipt(
    *,
    source_backup_path: str | Path,
    migrated_clone_path: str | Path,
    target_live_db_path: str | Path,
    migration_runtime_sha256: str,
) -> dict[str, Any]:
    source_raw, source_sha256 = _read_owner_artifact(
        source_backup_path, artifact="delivery_quarantine_source_backup"
    )
    if not source_raw.startswith(b"SQLite format 3\x00"):
        raise QuarantineMigrationError("delivery_quarantine_source_backup_invalid")
    with (
        _open_readonly(
            source_backup_path,
            artifact="delivery_quarantine_source_backup",
            require_standalone=True,
        ) as source,
        _open_readonly(
            migrated_clone_path,
            artifact="delivery_quarantine_migrated_clone",
        ) as clone,
    ):
        source.execute("BEGIN")
        clone.execute("BEGIN")
        source_health = _database_health(source)
        clone_health = _database_health(clone)
        if source_health["journal_mode"] != "delete":
            raise QuarantineMigrationError(
                "delivery_quarantine_source_backup_not_standalone"
            )
        if _schema_version(source) != SOURCE_SCHEMA_VERSION:
            raise QuarantineMigrationError("delivery_quarantine_source_schema_invalid")
        if _schema_version(clone) != TARGET_SCHEMA_VERSION:
            raise QuarantineMigrationError("delivery_quarantine_target_schema_invalid")
        _validate_deterministic_seed(clone)
        receipt = {
            "schema_version": SCHEMA_VERSION,
            "source_backup": {
                "path": str(Path(source_backup_path).expanduser().absolute()),
                "sha256": source_sha256,
                "size_bytes": len(source_raw),
            },
            "source_backup_normalization": {
                "method": "sqlite_backup_api_then_delete_journal_v1",
                **source_health,
                "byte_identical_live_copy": False,
            },
            "source_schema_version": SOURCE_SCHEMA_VERSION,
            "source_logical_projection": logical_database_projection(source),
            "target_live_db_path": str(
                Path(target_live_db_path).expanduser().absolute()
            ),
            "target_schema_version": TARGET_SCHEMA_VERSION,
            "post_migration_logical_projection": logical_database_projection(clone),
            "post_migration_health": {
                "integrity_check": clone_health["integrity_check"],
                "foreign_key_violation_count": clone_health[
                    "foreign_key_violation_count"
                ],
            },
            "migration_runtime_sha256": _hex64(migration_runtime_sha256),
            "deterministic_seed_policy": "existing_row_updated_at_sorted_v1",
            "no_live_database_writes": True,
        }
        source.rollback()
        clone.rollback()
        return receipt


def build_combined_offline_migration_receipt(
    *,
    source_backup_path: str | Path,
    migrated_clone_path: str | Path,
    target_live_db_path: str | Path,
    migration_runtime_sha256: str,
    expected_source_schema_sha256: str,
) -> dict[str, Any]:
    """Build a v7/W2-v8 to combined-v9 receipt from offline copies only."""

    source_path = Path(source_backup_path).expanduser().absolute()
    clone_path = Path(migrated_clone_path).expanduser().absolute()
    target_path = Path(target_live_db_path).expanduser().absolute()
    if len({str(source_path), str(clone_path), str(target_path)}) != 3:
        raise QuarantineMigrationError(
            "delivery_store_combined_migration_paths_not_isolated"
        )
    source_raw, source_sha256 = _read_owner_artifact(
        source_path, artifact="delivery_store_combined_source_backup"
    )
    clone_raw, clone_sha256 = _read_owner_artifact(
        clone_path, artifact="delivery_store_combined_migrated_clone"
    )
    if not source_raw.startswith(b"SQLite format 3\x00") or not clone_raw.startswith(
        b"SQLite format 3\x00"
    ):
        raise QuarantineMigrationError(
            "delivery_store_combined_migration_database_invalid"
        )
    with (
        _open_readonly(
            source_path,
            artifact="delivery_store_combined_source_backup",
            require_standalone=True,
        ) as source,
        _open_readonly(
            clone_path,
            artifact="delivery_store_combined_migrated_clone",
            require_standalone=True,
        ) as clone,
    ):
        source.execute("BEGIN")
        clone.execute("BEGIN")
        source_version = _schema_version(source)
        target_version = _schema_version(clone)
        source_health = _combined_database_health(source)
        clone_health = _combined_database_health(clone)
        if source_version not in COMBINED_SOURCE_SCHEMA_VERSIONS:
            raise QuarantineMigrationError(
                "delivery_store_combined_migration_source_schema_invalid"
            )
        if target_version != COMBINED_TARGET_SCHEMA_VERSION:
            raise QuarantineMigrationError(
                "delivery_store_combined_migration_target_schema_invalid"
            )
        _validate_combined_target_schema_contract(clone)
        if source_health["journal_mode"] != "delete":
            raise QuarantineMigrationError(
                "delivery_store_combined_source_backup_not_standalone"
            )
        source_variant = _combined_source_schema_variant(
            source,
            source_version=source_version,
        )
        source_projection = logical_database_projection(source)
        source_schema_contract = _combined_source_schema_contract(
            source_variant=source_variant,
            source_projection=source_projection,
            expected_source_schema_sha256=expected_source_schema_sha256,
        )
        target_projection = logical_database_projection(clone)
        cross_projection_preservation = _combined_cross_projection_preservation(
            source=source,
            clone=clone,
            source_version=source_version,
            source_variant=source_variant,
            source_projection=source_projection,
            target_projection=target_projection,
        )
        receipt = {
            "schema_version": COMBINED_SCHEMA_VERSION,
            "source_backup": {
                "path": str(source_path),
                "sha256": source_sha256,
                "size_bytes": len(source_raw),
            },
            "source_schema_version": source_version,
            "source_schema_variant": source_variant,
            "source_schema_contract": source_schema_contract,
            "source_logical_projection": source_projection,
            "source_health": source_health,
            "migrated_clone": {
                "path": str(clone_path),
                "sha256": clone_sha256,
                "size_bytes": len(clone_raw),
            },
            "target_live_db_path": str(target_path),
            "target_schema_version": COMBINED_TARGET_SCHEMA_VERSION,
            "post_migration_logical_projection": target_projection,
            "post_migration_health": clone_health,
            "cross_projection_preservation": cross_projection_preservation,
            "migration_runtime_sha256": _hex64(migration_runtime_sha256),
            "migration_semantics": {
                "execution_mode": "offline_clone_only",
                "copy_method": "sqlite_backup_then_filesystem_clone_v1",
                "quick_check_gate": "source_and_clone_ok_before_receipt",
                "rollback_path": str(source_path),
                "rollback_sha256": source_sha256,
                "live_replacement_performed": False,
            },
            "no_live_database_writes": True,
        }
        source.rollback()
        clone.rollback()
        return receipt


def _validate_combined_health(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != _COMBINED_HEALTH_FIELDS:
        raise QuarantineMigrationError(
            "delivery_store_combined_migration_health_invalid"
        )
    journal_mode = str(value.get("journal_mode") or "").lower()
    foreign_key_count = value.get("foreign_key_violation_count")
    if (
        journal_mode not in {"delete", "wal"}
        or value.get("quick_check") != "ok"
        or value.get("integrity_check") != "ok"
        or isinstance(foreign_key_count, bool)
        or not isinstance(foreign_key_count, int)
        or foreign_key_count != 0
    ):
        raise QuarantineMigrationError(
            "delivery_store_combined_migration_health_invalid"
        )
    return {
        "journal_mode": journal_mode,
        "quick_check": "ok",
        "integrity_check": "ok",
        "foreign_key_violation_count": 0,
    }


def _validate_combined_artifact_binding(
    value: Any, *, label: str
) -> tuple[Path, str, int]:
    if not isinstance(value, Mapping) or set(value) != {
        "path",
        "sha256",
        "size_bytes",
    }:
        raise QuarantineMigrationError(f"{label}_binding_invalid")
    path = Path(str(value.get("path") or "")).expanduser()
    size = value.get("size_bytes")
    sha256 = _hex64(value.get("sha256"))
    if (
        not path.is_absolute()
        or str(path) != str(path.absolute())
        or isinstance(size, bool)
        or not isinstance(size, int)
        or size < 2
        or size > MAX_ARTIFACT_BYTES
        or sha256 != value.get("sha256")
    ):
        raise QuarantineMigrationError(f"{label}_binding_invalid")
    return path, sha256, size


def validate_combined_migration_receipt(
    *,
    receipt_path: str | Path,
    expected_sha256: str,
    target_live_db_path: str | Path,
    expected_migration_runtime_sha256: str,
    expected_source_schema_sha256: str,
    migrated_db_path: str | Path | None = None,
) -> dict[str, Any]:
    """Recompute a v2 combined-schema migration receipt and its rollback copy."""

    raw, observed_sha256 = _read_owner_artifact(
        receipt_path, artifact="delivery_store_combined_migration_receipt"
    )
    if observed_sha256 != _hex64(expected_sha256):
        raise QuarantineMigrationError(
            "delivery_store_combined_migration_receipt_sha256_mismatch"
        )
    value = _strict_json(raw)
    if raw != canonical_migration_receipt_bytes(value):
        raise QuarantineMigrationError(
            "delivery_store_combined_migration_receipt_not_canonical"
        )
    source_path, source_sha256, source_size = _validate_combined_artifact_binding(
        value.get("source_backup"),
        label="delivery_store_combined_source_backup",
    )
    clone_path, clone_sha256, clone_size = _validate_combined_artifact_binding(
        value.get("migrated_clone"),
        label="delivery_store_combined_migrated_clone",
    )
    source_projection = _validate_projection(value.get("source_logical_projection"))
    target_projection = _validate_projection(
        value.get("post_migration_logical_projection")
    )
    source_health = _validate_combined_health(value.get("source_health"))
    target_health = _validate_combined_health(value.get("post_migration_health"))
    cross_projection_preservation = value.get("cross_projection_preservation")
    semantics = value.get("migration_semantics")
    target = str(Path(target_live_db_path).expanduser().absolute())
    runtime_sha256 = _hex64(value.get("migration_runtime_sha256"))
    expected_runtime_sha256 = _hex64(expected_migration_runtime_sha256)
    source_version = str(value.get("source_schema_version") or "")
    source_variant = str(value.get("source_schema_variant") or "")
    source_schema_contract = _combined_source_schema_contract(
        source_variant=source_variant,
        source_projection=source_projection,
        expected_source_schema_sha256=expected_source_schema_sha256,
    )
    expected_source_variant = {
        "pnc_rca_delivery_store_v7": _COMBINED_ACTIVE_PROD_V7_VARIANT,
        "pnc_rca_delivery_store_v8": _COMBINED_W2_V8_VARIANT,
    }.get(source_version)
    if (
        set(value) != _COMBINED_FIELDS
        or value.get("schema_version") != COMBINED_SCHEMA_VERSION
        or source_version not in COMBINED_SOURCE_SCHEMA_VERSIONS
        or source_variant != expected_source_variant
        or value.get("source_schema_contract") != source_schema_contract
        or value.get("target_schema_version") != COMBINED_TARGET_SCHEMA_VERSION
        or value.get("target_live_db_path") != target
        or len({str(source_path), str(clone_path), target}) != 3
        or value.get("no_live_database_writes") is not True
        or not isinstance(semantics, Mapping)
        or set(semantics) != _COMBINED_SEMANTICS_FIELDS
        or semantics.get("execution_mode") != "offline_clone_only"
        or semantics.get("copy_method")
        != "sqlite_backup_then_filesystem_clone_v1"
        or semantics.get("quick_check_gate")
        != "source_and_clone_ok_before_receipt"
        or semantics.get("rollback_path") != str(source_path)
        or semantics.get("rollback_sha256") != source_sha256
        or semantics.get("live_replacement_performed") is not False
        or source_health["journal_mode"] != "delete"
        or runtime_sha256 != value.get("migration_runtime_sha256")
        or runtime_sha256 != expected_runtime_sha256
    ):
        raise QuarantineMigrationError(
            "delivery_store_combined_migration_receipt_scope_invalid"
        )
    source_raw, observed_source_sha256 = _read_owner_artifact(
        source_path, artifact="delivery_store_combined_source_backup"
    )
    clone_raw, observed_clone_sha256 = _read_owner_artifact(
        clone_path, artifact="delivery_store_combined_migrated_clone"
    )
    with (
        _open_readonly(
            source_path,
            artifact="delivery_store_combined_source_backup",
            require_standalone=True,
        ) as source,
        _open_readonly(
            clone_path,
            artifact="delivery_store_combined_migrated_clone",
            require_standalone=True,
        ) as clone,
    ):
        source.execute("BEGIN")
        clone.execute("BEGIN")
        observed_source_health = _combined_database_health(source)
        observed_target_health = _combined_database_health(clone)
        _validate_combined_target_schema_contract(clone)
        observed_source_projection = logical_database_projection(source)
        observed_target_projection = logical_database_projection(clone)
        observed_source_version = _schema_version(source)
        observed_target_version = _schema_version(clone)
        observed_source_variant = _combined_source_schema_variant(
            source,
            source_version=observed_source_version,
        )
        observed_source_schema_contract = _combined_source_schema_contract(
            source_variant=observed_source_variant,
            source_projection=observed_source_projection,
            expected_source_schema_sha256=expected_source_schema_sha256,
        )
        observed_cross_projection_preservation = (
            _combined_cross_projection_preservation(
                source=source,
                clone=clone,
                source_version=observed_source_version,
                source_variant=observed_source_variant,
                source_projection=observed_source_projection,
                target_projection=observed_target_projection,
            )
        )
        source.rollback()
        clone.rollback()
    if (
        observed_source_sha256 != source_sha256
        or len(source_raw) != source_size
        or observed_clone_sha256 != clone_sha256
        or len(clone_raw) != clone_size
        or observed_source_version != value.get("source_schema_version")
        or observed_source_variant != source_variant
        or observed_source_schema_contract != source_schema_contract
        or observed_target_version != COMBINED_TARGET_SCHEMA_VERSION
        or observed_source_health != source_health
        or observed_target_health != target_health
        or observed_source_projection != source_projection
        or observed_target_projection != target_projection
        or observed_cross_projection_preservation != cross_projection_preservation
    ):
        raise QuarantineMigrationError(
            "delivery_store_combined_migration_artifact_drift"
        )
    if migrated_db_path is not None:
        selected = Path(migrated_db_path).expanduser().absolute()
        with _open_readonly(
            selected,
            artifact="delivery_store_combined_migrated_database",
            require_standalone=True,
        ) as migrated:
            migrated.execute("BEGIN")
            migrated_health = _combined_database_health(migrated)
            migrated_projection = logical_database_projection(migrated)
            migrated_version = _schema_version(migrated)
            migrated.rollback()
        if (
            migrated_version != COMBINED_TARGET_SCHEMA_VERSION
            or migrated_health != target_health
            or migrated_projection != target_projection
        ):
            raise QuarantineMigrationError(
                "delivery_store_combined_post_migration_drift"
            )
    return {
        "receipt_path": str(Path(receipt_path).expanduser().absolute()),
        "receipt_sha256": observed_sha256,
        "source_schema_version": observed_source_version,
        "source_schema_variant": observed_source_variant,
        "source_backup_path": str(source_path),
        "source_backup_sha256": source_sha256,
        "source_schema_sha256": source_projection["schema_sha256"],
        "source_logical_sha256": source_projection["logical_sha256"],
        "rollback_path": str(source_path),
        "rollback_sha256": source_sha256,
        "source_quick_check": source_health["quick_check"],
        "target_schema_version": observed_target_version,
        "migrated_clone_path": str(clone_path),
        "migrated_clone_sha256": clone_sha256,
        "target_schema_sha256": target_projection["schema_sha256"],
        "target_logical_sha256": target_projection["logical_sha256"],
        "target_quick_check": target_health["quick_check"],
        "migration_runtime_sha256": runtime_sha256,
        "target_live_db_path": target,
        "no_live_database_writes": True,
    }


def _combined_receipt_projections(
    *,
    receipt_path: str | Path,
    expected_sha256: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    raw, observed_sha256 = _read_owner_artifact(
        receipt_path,
        artifact="delivery_store_combined_migration_receipt",
    )
    if observed_sha256 != _hex64(expected_sha256):
        raise QuarantineMigrationError(
            "delivery_store_combined_migration_receipt_sha256_mismatch"
        )
    value = _strict_json(raw)
    if (
        raw != canonical_migration_receipt_bytes(value)
        or value.get("schema_version") != COMBINED_SCHEMA_VERSION
    ):
        raise QuarantineMigrationError(
            "delivery_store_combined_migration_receipt_not_canonical"
        )
    return (
        _validate_projection(value.get("source_logical_projection")),
        _validate_projection(value.get("post_migration_logical_projection")),
    )


def assert_combined_live_pre_migration_matches(
    *,
    receipt_path: str | Path,
    expected_sha256: str,
    live_db_path: str | Path,
    expected_migration_runtime_sha256: str,
    expected_source_schema_sha256: str,
) -> dict[str, Any]:
    """Require an immutable live snapshot to exactly match the v2 source."""

    binding = validate_combined_migration_receipt(
        receipt_path=receipt_path,
        expected_sha256=expected_sha256,
        target_live_db_path=live_db_path,
        expected_migration_runtime_sha256=expected_migration_runtime_sha256,
        expected_source_schema_sha256=expected_source_schema_sha256,
    )
    source_projection, _target_projection = _combined_receipt_projections(
        receipt_path=receipt_path,
        expected_sha256=expected_sha256,
    )
    projection, live_observation = _guarded_live_projection(
        live_db_path,
        combined_health=True,
    )
    if (
        live_observation["schema_version"] != binding["source_schema_version"]
        or projection != source_projection
        or projection["schema_sha256"] != binding["source_schema_sha256"]
        or projection["logical_sha256"] != binding["source_logical_sha256"]
    ):
        raise QuarantineMigrationError(
            "delivery_store_combined_live_pre_migration_drift"
        )
    return {
        **binding,
        "live_pre_migration_schema_sha256": projection["schema_sha256"],
        "live_pre_migration_logical_sha256": projection["logical_sha256"],
        "live_validation": live_observation,
    }


def assert_combined_live_post_migration_matches(
    *,
    receipt_path: str | Path,
    expected_sha256: str,
    live_db_path: str | Path,
    expected_migration_runtime_sha256: str,
    expected_source_schema_sha256: str,
) -> dict[str, Any]:
    """Require the stopped live database to exactly match the v2 v9 clone."""

    binding = validate_combined_migration_receipt(
        receipt_path=receipt_path,
        expected_sha256=expected_sha256,
        target_live_db_path=live_db_path,
        expected_migration_runtime_sha256=expected_migration_runtime_sha256,
        expected_source_schema_sha256=expected_source_schema_sha256,
    )
    _source_projection, target_projection = _combined_receipt_projections(
        receipt_path=receipt_path,
        expected_sha256=expected_sha256,
    )
    projection, live_observation = _guarded_live_projection(
        live_db_path,
        combined_health=True,
    )
    if (
        live_observation["schema_version"] != COMBINED_TARGET_SCHEMA_VERSION
        or projection != target_projection
        or projection["schema_sha256"] != binding["target_schema_sha256"]
        or projection["logical_sha256"] != binding["target_logical_sha256"]
    ):
        raise QuarantineMigrationError(
            "delivery_store_combined_live_post_migration_drift"
        )
    return {
        **binding,
        "live_post_migration_schema_sha256": projection["schema_sha256"],
        "live_post_migration_logical_sha256": projection["logical_sha256"],
        "live_validation": live_observation,
    }


def validate_migration_receipt(
    *,
    receipt_path: str | Path,
    expected_sha256: str,
    target_live_db_path: str | Path,
    migrated_db_path: str | Path | None = None,
    migrated_db_is_live: bool = False,
    expected_migration_runtime_sha256: str | None = None,
) -> dict[str, Any]:
    raw, observed_sha256 = _read_owner_artifact(
        receipt_path, artifact="delivery_quarantine_migration_receipt"
    )
    if observed_sha256 != _hex64(expected_sha256):
        raise QuarantineMigrationError(
            "delivery_quarantine_migration_receipt_sha256_mismatch"
        )
    value = _strict_json(raw)
    if raw != canonical_migration_receipt_bytes(value):
        raise QuarantineMigrationError(
            "delivery_quarantine_migration_receipt_not_canonical"
        )
    source = value.get("source_backup")
    normalization = value.get("source_backup_normalization")
    post_health = value.get("post_migration_health")
    target = str(Path(target_live_db_path).expanduser().absolute())
    source_projection = _validate_projection(value.get("source_logical_projection"))
    post_projection = _validate_projection(
        value.get("post_migration_logical_projection")
    )
    runtime_sha256 = _hex64(value.get("migration_runtime_sha256"))
    source_path = str(source.get("path") or "") if isinstance(source, Mapping) else ""
    source_size = source.get("size_bytes") if isinstance(source, Mapping) else None
    if (
        set(value) != _FIELDS
        or value.get("schema_version") != SCHEMA_VERSION
        or value.get("source_schema_version") != SOURCE_SCHEMA_VERSION
        or value.get("target_schema_version") != TARGET_SCHEMA_VERSION
        or value.get("target_live_db_path") != target
        or value.get("deterministic_seed_policy") != "existing_row_updated_at_sorted_v1"
        or value.get("no_live_database_writes") is not True
        or not isinstance(source, Mapping)
        or set(source) != {"path", "sha256", "size_bytes"}
        or not isinstance(normalization, Mapping)
        or set(normalization) != _NORMALIZATION_FIELDS
        or normalization.get("method") != "sqlite_backup_api_then_delete_journal_v1"
        or normalization.get("journal_mode") != "delete"
        or normalization.get("integrity_check") != "ok"
        or isinstance(normalization.get("foreign_key_violation_count"), bool)
        or not isinstance(normalization.get("foreign_key_violation_count"), int)
        or normalization.get("foreign_key_violation_count") != 0
        or normalization.get("byte_identical_live_copy") is not False
        or not isinstance(post_health, Mapping)
        or set(post_health) != _POST_MIGRATION_HEALTH_FIELDS
        or post_health.get("integrity_check") != "ok"
        or isinstance(post_health.get("foreign_key_violation_count"), bool)
        or not isinstance(post_health.get("foreign_key_violation_count"), int)
        or post_health.get("foreign_key_violation_count") != 0
        or not Path(source_path).is_absolute()
        or source_path != str(Path(source_path).expanduser().absolute())
        or _hex64(source.get("sha256")) != source.get("sha256")
        or isinstance(source_size, bool)
        or not isinstance(source_size, int)
        or source_size < 2
        or source_size > MAX_ARTIFACT_BYTES
        or runtime_sha256 != value.get("migration_runtime_sha256")
        or (
            expected_migration_runtime_sha256 is not None
            and runtime_sha256 != _hex64(expected_migration_runtime_sha256)
        )
    ):
        raise QuarantineMigrationError(
            "delivery_quarantine_migration_receipt_scope_invalid"
        )
    source_raw, source_sha256 = _read_owner_artifact(
        source_path, artifact="delivery_quarantine_source_backup"
    )
    observed_source_projection, source_health = _standalone_projection_and_health(
        source_path
    )
    if (
        source_sha256 != source.get("sha256")
        or len(source_raw) != source.get("size_bytes")
        or observed_source_projection != source_projection
        or source_health
        != {
            "journal_mode": normalization["journal_mode"],
            "integrity_check": normalization["integrity_check"],
            "foreign_key_violation_count": normalization["foreign_key_violation_count"],
        }
    ):
        raise QuarantineMigrationError("delivery_quarantine_source_backup_drift")
    if migrated_db_path is not None:
        if migrated_db_is_live:
            projection, _live_observation = _guarded_live_projection(
                migrated_db_path,
                validate_seed=True,
            )
        else:
            projection = logical_database_projection_path(
                migrated_db_path,
                require_integrity=True,
            )
        if projection != post_projection:
            raise QuarantineMigrationError("delivery_quarantine_post_migration_drift")
        if not migrated_db_is_live:
            with _open_readonly(
                migrated_db_path,
                artifact="delivery_quarantine_migrated_database",
            ) as conn:
                _validate_deterministic_seed(conn)
    return {
        "receipt_path": str(Path(receipt_path).expanduser().absolute()),
        "receipt_sha256": observed_sha256,
        "source_backup_sha256": source_sha256,
        "source_logical_sha256": value["source_logical_projection"]["logical_sha256"],
        "post_migration_logical_sha256": post_projection["logical_sha256"],
        "migration_runtime_sha256": runtime_sha256,
        "target_live_db_path": target,
    }


def assert_live_pre_migration_matches(
    *,
    receipt_path: str | Path,
    expected_sha256: str,
    live_db_path: str | Path,
    expected_migration_runtime_sha256: str,
) -> dict[str, Any]:
    binding = validate_migration_receipt(
        receipt_path=receipt_path,
        expected_sha256=expected_sha256,
        target_live_db_path=live_db_path,
        expected_migration_runtime_sha256=expected_migration_runtime_sha256,
    )
    raw, observed_sha256 = _read_owner_artifact(
        receipt_path, artifact="delivery_quarantine_migration_receipt"
    )
    if observed_sha256 != _hex64(expected_sha256):
        raise QuarantineMigrationError(
            "delivery_quarantine_migration_receipt_sha256_mismatch"
        )
    value = _strict_json(raw)
    if raw != canonical_migration_receipt_bytes(value):
        raise QuarantineMigrationError(
            "delivery_quarantine_migration_receipt_not_canonical"
        )
    source_projection = _validate_projection(value.get("source_logical_projection"))
    projection, live_observation = _guarded_live_projection(live_db_path)
    if projection != source_projection:
        raise QuarantineMigrationError("delivery_quarantine_live_pre_migration_drift")
    return {
        **binding,
        "live_pre_migration_logical_sha256": projection["logical_sha256"],
        "live_validation": live_observation,
    }


def assert_live_post_migration_matches(
    *,
    receipt_path: str | Path,
    expected_sha256: str,
    live_db_path: str | Path,
    expected_migration_runtime_sha256: str,
) -> dict[str, Any]:
    binding = validate_migration_receipt(
        receipt_path=receipt_path,
        expected_sha256=expected_sha256,
        target_live_db_path=live_db_path,
        migrated_db_path=live_db_path,
        migrated_db_is_live=True,
        expected_migration_runtime_sha256=expected_migration_runtime_sha256,
    )
    projection, live_observation = _guarded_live_projection(
        live_db_path,
        validate_seed=True,
    )
    if projection["logical_sha256"] != binding["post_migration_logical_sha256"]:
        raise QuarantineMigrationError("delivery_quarantine_post_migration_drift")
    return {
        **binding,
        "live_post_migration_logical_sha256": projection["logical_sha256"],
        "live_validation": live_observation,
    }
