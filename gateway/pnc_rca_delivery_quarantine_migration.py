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


SCHEMA_VERSION = "pnc_rca_delivery_quarantine_offline_migration_v1"
SOURCE_SCHEMA_VERSION = "pnc_rca_delivery_store_v6"
TARGET_SCHEMA_VERSION = "pnc_rca_delivery_store_v7"
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
        health = _database_health(conn)
        if validate_seed:
            _validate_deterministic_seed(conn)
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
