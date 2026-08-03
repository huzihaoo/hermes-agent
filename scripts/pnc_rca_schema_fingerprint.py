#!/usr/bin/env python3
"""Create or verify a read-only, release-bound RCA SQLite schema receipt."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import sys
from typing import Any, Mapping, Sequence


REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from gateway.pnc_rca_control_store import CONTROL_STORE_SCHEMA_VERSION  # noqa: E402
from gateway.pnc_rca_delivery_store import DELIVERY_STORE_SCHEMA_VERSION  # noqa: E402


RECEIPT_SCHEMA_VERSION = "pnc_rca_control_db_schema_receipt_v1"
MATERIAL_SCHEMA_VERSION = "pnc_rca_sqlite_schema_material_v1"
CLI_SCHEMA_VERSION = "pnc_rca_schema_fingerprint_cli_v1"
MAX_RECEIPT_BYTES = 8 * 1024 * 1024
_SHA256_EMPTY = hashlib.sha256(b"").hexdigest()
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class SchemaFingerprintError(RuntimeError):
    """Stable failure emitted by the schema fingerprint producer."""

    def __init__(self, code: str, detail: str = ""):
        self.code = str(code or "rca_schema_fingerprint_invalid")[:160]
        self.detail = str(detail or self.code)[:1000]
        super().__init__(self.detail)


def canonical_json_bytes(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise SchemaFingerprintError("rca_schema_fingerprint_json_invalid") from exc


def canonical_json_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as exc:
        raise SchemaFingerprintError(
            "rca_schema_fingerprint_file_unavailable", str(path)
        ) from exc
    return digest.hexdigest()


def _absolute_path(path: Path, code: str) -> Path:
    selected = path.expanduser()
    if not selected.is_absolute() or selected.absolute() != selected:
        raise SchemaFingerprintError(code, "path must be absolute and normalized")
    return selected


def _regular_owner_file(path: Path, *, allow_empty: bool = False) -> os.stat_result:
    try:
        observed = path.lstat()
    except OSError as exc:
        raise SchemaFingerprintError(
            "rca_schema_fingerprint_file_unavailable", str(path)
        ) from exc
    if (
        stat.S_ISLNK(observed.st_mode)
        or not stat.S_ISREG(observed.st_mode)
        or observed.st_uid != os.geteuid()
        or observed.st_nlink != 1
        or (not allow_empty and observed.st_size <= 0)
    ):
        raise SchemaFingerprintError(
            "rca_schema_fingerprint_file_invalid", str(path)
        )
    return observed


def _secure_output_parent(path: Path) -> None:
    try:
        observed = path.parent.lstat()
        resolved = path.parent.resolve(strict=True)
    except OSError as exc:
        raise SchemaFingerprintError(
            "rca_schema_fingerprint_output_invalid", str(path.parent)
        ) from exc
    if (
        resolved != path.parent
        or stat.S_ISLNK(observed.st_mode)
        or not stat.S_ISDIR(observed.st_mode)
        or observed.st_uid != os.geteuid()
        or stat.S_IMODE(observed.st_mode) & 0o022
    ):
        raise SchemaFingerprintError(
            "rca_schema_fingerprint_output_invalid", str(path.parent)
        )


def _new_file(path: Path) -> int:
    _secure_output_parent(path)
    try:
        return os.open(
            path,
            os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_CLOEXEC", 0),
            0o600,
        )
    except FileExistsError as exc:
        raise SchemaFingerprintError(
            "rca_schema_fingerprint_output_exists", str(path)
        ) from exc
    except OSError as exc:
        raise SchemaFingerprintError(
            "rca_schema_fingerprint_output_invalid", str(path)
        ) from exc


def _write_new_file(path: Path, raw: bytes) -> None:
    descriptor = _new_file(path)
    try:
        written = 0
        while written < len(raw):
            count = os.write(descriptor, raw[written:])
            if count <= 0:
                raise OSError("short write")
            written += count
        os.fsync(descriptor)
    except OSError as exc:
        raise SchemaFingerprintError(
            "rca_schema_fingerprint_output_invalid", str(path)
        ) from exc
    finally:
        os.close(descriptor)


def _remove_incomplete(path: Path) -> None:
    try:
        observed = path.lstat()
        if (
            stat.S_ISREG(observed.st_mode)
            and not stat.S_ISLNK(observed.st_mode)
            and observed.st_uid == os.geteuid()
            and observed.st_nlink == 1
        ):
            path.unlink()
    except FileNotFoundError:
        return
    except OSError:
        return


def _read_meta(connection: sqlite3.Connection, table: str) -> dict[str, str]:
    try:
        rows = connection.execute(f"SELECT key, value FROM {table}").fetchall()
    except sqlite3.Error as exc:
        raise SchemaFingerprintError(
            "rca_schema_fingerprint_meta_unavailable", table
        ) from exc
    return {str(row[0]): str(row[1]) for row in rows}


def _schema_material(connection: sqlite3.Connection) -> dict[str, Any]:
    try:
        rows = connection.execute(
            "SELECT type, name, tbl_name, sql FROM sqlite_master "
            "ORDER BY type COLLATE BINARY, name COLLATE BINARY, "
            "tbl_name COLLATE BINARY"
        ).fetchall()
    except sqlite3.Error as exc:
        raise SchemaFingerprintError(
            "rca_schema_fingerprint_inventory_unavailable"
        ) from exc
    objects = [
        {
            "type": str(row[0]),
            "name": str(row[1]),
            "table": str(row[2]),
            # Preserve SQLite's stored DDL verbatim. Whitespace inside SQL string
            # literals is semantic, so ad-hoc normalization would be unsafe.
            "sql": None if row[3] is None else str(row[3]),
        }
        for row in rows
    ]
    return {
        "schema_version": MATERIAL_SCHEMA_VERSION,
        "serialization": "canonical-json-v1-sqlite-master-verbatim",
        "object_count": len(objects),
        "objects": objects,
    }


def _database_observation(path: Path) -> dict[str, Any]:
    connection: sqlite3.Connection | None = None
    try:
        connection = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=10)
        connection.execute("PRAGMA query_only=ON")
        connection.execute("PRAGMA trusted_schema=OFF")
        control_meta = _read_meta(connection, "control_meta")
        delivery_meta = _read_meta(connection, "rca_delivery_meta")
        quick = connection.execute("PRAGMA quick_check(1)").fetchone()
        violations = connection.execute("PRAGMA foreign_key_check").fetchmany(101)
        material = _schema_material(connection)
        values = {
            "schema_version_counter": int(
                connection.execute("PRAGMA schema_version").fetchone()[0]
            ),
            "page_size": int(connection.execute("PRAGMA page_size").fetchone()[0]),
            "page_count": int(connection.execute("PRAGMA page_count").fetchone()[0]),
            "journal_mode": str(
                connection.execute("PRAGMA journal_mode").fetchone()[0]
            ).lower(),
            "quick_check": str(quick[0]) if quick else "missing",
            "foreign_key_violation_count": len(violations),
            "foreign_key_violations_truncated": len(violations) > 100,
            "control_schema_version": control_meta.get("schema_version"),
            "delivery_schema_version": delivery_meta.get("schema_version"),
            "database_instance_id": control_meta.get("fresh_install_db_instance_id"),
            "genesis_intent_sha256": control_meta.get(
                "fresh_install_genesis_intent_sha256"
            ),
            "delivery_database_instance_id": delivery_meta.get(
                "fresh_install_db_instance_id"
            ),
            "delivery_genesis_intent_sha256": delivery_meta.get(
                "fresh_install_genesis_intent_sha256"
            ),
            "schema_material": material,
            "schema_fingerprint_sha256": canonical_json_sha256(material),
        }
    except SchemaFingerprintError:
        raise
    except sqlite3.Error as exc:
        raise SchemaFingerprintError(
            "rca_schema_fingerprint_database_unreadable", str(path)
        ) from exc
    finally:
        if connection is not None:
            connection.close()
    if values["control_schema_version"] != CONTROL_STORE_SCHEMA_VERSION:
        raise SchemaFingerprintError("rca_schema_fingerprint_control_schema_mismatch")
    if values["delivery_schema_version"] != DELIVERY_STORE_SCHEMA_VERSION:
        raise SchemaFingerprintError("rca_schema_fingerprint_delivery_schema_mismatch")
    if (
        not values["database_instance_id"]
        or values["database_instance_id"]
        != values["delivery_database_instance_id"]
        or not values["genesis_intent_sha256"]
        or values["genesis_intent_sha256"]
        != values["delivery_genesis_intent_sha256"]
    ):
        raise SchemaFingerprintError("rca_schema_fingerprint_database_identity_mismatch")
    if values["quick_check"] != "ok":
        raise SchemaFingerprintError("rca_schema_fingerprint_quick_check_failed")
    if values["foreign_key_violation_count"]:
        raise SchemaFingerprintError("rca_schema_fingerprint_foreign_key_failed")
    return values


def _source_projection(
    path: Path,
    before: os.stat_result,
    after: os.stat_result,
    before_observation: Mapping[str, Any],
    after_observation: Mapping[str, Any],
) -> dict[str, Any]:
    stable_identity = (before.st_dev, before.st_ino) == (after.st_dev, after.st_ino)
    stable_schema = (
        before_observation["schema_version_counter"]
        == after_observation["schema_version_counter"]
        and before_observation["schema_fingerprint_sha256"]
        == after_observation["schema_fingerprint_sha256"]
    )
    if not stable_identity:
        raise SchemaFingerprintError("rca_schema_fingerprint_source_replaced")
    if not stable_schema:
        raise SchemaFingerprintError("rca_schema_fingerprint_source_schema_changed")
    return {
        "path": str(path),
        "device": before.st_dev,
        "inode": before.st_ino,
        "uid": before.st_uid,
        "mode": format(stat.S_IMODE(before.st_mode), "04o"),
        "size_bytes_before": before.st_size,
        "size_bytes_after": after.st_size,
        "mtime_ns_before": before.st_mtime_ns,
        "mtime_ns_after": after.st_mtime_ns,
        "schema_version_counter": before_observation["schema_version_counter"],
        "control_schema_version": before_observation["control_schema_version"],
        "delivery_schema_version": before_observation["delivery_schema_version"],
        "database_instance_id": before_observation["database_instance_id"],
        "genesis_intent_sha256": before_observation["genesis_intent_sha256"],
    }


def create_snapshot_receipt(
    source_path: Path,
    *,
    snapshot_path: Path,
    receipt_path: Path,
) -> dict[str, Any]:
    """Back up a live DB consistently and emit a strict schema receipt."""
    source = _absolute_path(source_path, "rca_schema_fingerprint_source_path_invalid")
    snapshot = _absolute_path(
        snapshot_path, "rca_schema_fingerprint_snapshot_path_invalid"
    )
    receipt = _absolute_path(
        receipt_path, "rca_schema_fingerprint_receipt_path_invalid"
    )
    if len({source, snapshot, receipt}) != 3:
        raise SchemaFingerprintError("rca_schema_fingerprint_path_collision")
    before = _regular_owner_file(source)
    started_at = datetime.now(timezone.utc).isoformat()
    source_before = _database_observation(source)
    descriptor = _new_file(snapshot)
    os.close(descriptor)
    completed = False
    try:
        source_connection = sqlite3.connect(
            source.as_uri() + "?mode=ro", uri=True, timeout=30
        )
        destination_connection = sqlite3.connect(snapshot, timeout=30)
        try:
            source_connection.execute("PRAGMA query_only=ON")
            source_connection.backup(destination_connection, pages=1024, sleep=0.01)
            destination_connection.commit()
            # The source is WAL-backed. Seal the task-owned snapshot as a
            # standalone file so later read-only verification cannot create
            # mutable -wal/-shm sidecars beside immutable evidence.
            journal_mode = destination_connection.execute(
                "PRAGMA journal_mode=DELETE"
            ).fetchone()
            if journal_mode is None or str(journal_mode[0]).lower() != "delete":
                raise SchemaFingerprintError(
                    "rca_schema_fingerprint_snapshot_journal_mode_invalid"
                )
        finally:
            destination_connection.close()
            source_connection.close()
        os.chmod(snapshot, 0o600)
        with snapshot.open("rb") as stream:
            os.fsync(stream.fileno())
        snapshot_stat = _regular_owner_file(snapshot)
        snapshot_observation = _database_observation(snapshot)
        source_after = _database_observation(source)
        after = _regular_owner_file(source)
        source_projection = _source_projection(
            source, before, after, source_before, source_after
        )
        if (
            snapshot_observation["database_instance_id"]
            != source_before["database_instance_id"]
            or snapshot_observation["genesis_intent_sha256"]
            != source_before["genesis_intent_sha256"]
            or snapshot_observation["schema_fingerprint_sha256"]
            != source_before["schema_fingerprint_sha256"]
        ):
            raise SchemaFingerprintError("rca_schema_fingerprint_snapshot_mismatch")
        completed_at = datetime.now(timezone.utc).isoformat()
        value = {
            "schema_version": RECEIPT_SCHEMA_VERSION,
            "observation_started_at": started_at,
            "observation_completed_at": completed_at,
            "source_database": source_projection,
            "snapshot_database": {
                "path": str(snapshot),
                "device": snapshot_stat.st_dev,
                "inode": snapshot_stat.st_ino,
                "size_bytes": snapshot_stat.st_size,
                "raw_sha256": file_sha256(snapshot),
                "page_size": snapshot_observation["page_size"],
                "page_count": snapshot_observation["page_count"],
                "journal_mode": snapshot_observation["journal_mode"],
                "quick_check": snapshot_observation["quick_check"],
                "foreign_key_violation_count": snapshot_observation[
                    "foreign_key_violation_count"
                ],
                "schema_version_counter": snapshot_observation[
                    "schema_version_counter"
                ],
            },
            "database_identity": {
                "database_instance_id": snapshot_observation[
                    "database_instance_id"
                ],
                "genesis_intent_sha256": snapshot_observation[
                    "genesis_intent_sha256"
                ],
                "control_schema_version": snapshot_observation[
                    "control_schema_version"
                ],
                "delivery_schema_version": snapshot_observation[
                    "delivery_schema_version"
                ],
            },
            "schema_material": snapshot_observation["schema_material"],
            "schema_fingerprint_sha256": snapshot_observation[
                "schema_fingerprint_sha256"
            ],
            "assertions": {
                "source_identity_stable": True,
                "source_schema_stable": True,
                "snapshot_identity_matches_source": True,
                "snapshot_schema_matches_source": True,
                "quick_check_ok": True,
                "foreign_keys_ok": True,
            },
            "read_only_attestation": {
                "source_open_mode": "mode=ro + PRAGMA query_only=ON",
                "source_mutation_performed": False,
                "external_effects_triggered": False,
                "empty_materialization_sha256": _SHA256_EMPTY,
            },
        }
        raw = json.dumps(
            value,
            ensure_ascii=True,
            indent=2,
            sort_keys=True,
            allow_nan=False,
        ).encode("utf-8") + b"\n"
        _write_new_file(receipt, raw)
        completed = True
        return {
            "receipt": value,
            "receipt_raw_sha256": hashlib.sha256(raw).hexdigest(),
        }
    except SchemaFingerprintError:
        raise
    except (OSError, sqlite3.Error) as exc:
        raise SchemaFingerprintError(
            "rca_schema_fingerprint_snapshot_failed", str(exc)
        ) from exc
    finally:
        if not completed:
            _remove_incomplete(snapshot)
            _remove_incomplete(receipt)


def _strict_json(raw: bytes) -> dict[str, Any]:
    def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in items:
            if key in result:
                raise SchemaFingerprintError(
                    "rca_schema_fingerprint_receipt_duplicate_key"
                )
            result[key] = value
        return result

    def reject_constant(_value: str) -> None:
        raise SchemaFingerprintError("rca_schema_fingerprint_receipt_json_invalid")

    try:
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=pairs,
            parse_constant=reject_constant,
        )
    except SchemaFingerprintError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SchemaFingerprintError(
            "rca_schema_fingerprint_receipt_json_invalid"
        ) from exc
    if not isinstance(value, dict):
        raise SchemaFingerprintError("rca_schema_fingerprint_receipt_shape_invalid")
    return value


def _exact_keys(value: Any, expected: set[str], code: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or set(value) != expected:
        raise SchemaFingerprintError(code)
    return value


def _digest(value: Any, code: str) -> str:
    text = str(value or "")
    if _SHA256_RE.fullmatch(text) is None:
        raise SchemaFingerprintError(code)
    return text


def _timestamp(value: Any) -> datetime:
    if not isinstance(value, str) or not value or value != value.strip():
        raise SchemaFingerprintError("rca_schema_fingerprint_receipt_time_invalid")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise SchemaFingerprintError(
            "rca_schema_fingerprint_receipt_time_invalid"
        ) from exc
    if parsed.tzinfo is None:
        raise SchemaFingerprintError("rca_schema_fingerprint_receipt_time_invalid")
    return parsed.astimezone(timezone.utc)


def _validate_material(value: Any) -> Mapping[str, Any]:
    material = _exact_keys(
        value,
        {"schema_version", "serialization", "object_count", "objects"},
        "rca_schema_fingerprint_receipt_material_invalid",
    )
    objects = material.get("objects")
    if (
        material.get("schema_version") != MATERIAL_SCHEMA_VERSION
        or material.get("serialization")
        != "canonical-json-v1-sqlite-master-verbatim"
        or isinstance(material.get("object_count"), bool)
        or not isinstance(material.get("object_count"), int)
        or not isinstance(objects, list)
        or material["object_count"] != len(objects)
        or not objects
    ):
        raise SchemaFingerprintError(
            "rca_schema_fingerprint_receipt_material_invalid"
        )
    ordering: list[tuple[str, str, str]] = []
    for item in objects:
        selected = _exact_keys(
            item,
            {"type", "name", "table", "sql"},
            "rca_schema_fingerprint_receipt_material_invalid",
        )
        if (
            selected.get("type") not in {"index", "table", "trigger", "view"}
            or not isinstance(selected.get("name"), str)
            or not selected["name"]
            or not isinstance(selected.get("table"), str)
            or not selected["table"]
            or (
                selected.get("sql") is not None
                and not isinstance(selected.get("sql"), str)
            )
        ):
            raise SchemaFingerprintError(
                "rca_schema_fingerprint_receipt_material_invalid"
            )
        ordering.append((selected["type"], selected["name"], selected["table"]))
    if ordering != sorted(ordering):
        raise SchemaFingerprintError(
            "rca_schema_fingerprint_receipt_material_invalid"
        )
    return material


def _validate_receipt_shape(value: Mapping[str, Any]) -> None:
    expected_fields = {
        "schema_version",
        "observation_started_at",
        "observation_completed_at",
        "source_database",
        "snapshot_database",
        "database_identity",
        "schema_material",
        "schema_fingerprint_sha256",
        "assertions",
        "read_only_attestation",
    }
    if set(value) != expected_fields or value.get("schema_version") != RECEIPT_SCHEMA_VERSION:
        raise SchemaFingerprintError("rca_schema_fingerprint_receipt_shape_invalid")
    started = _timestamp(value.get("observation_started_at"))
    completed = _timestamp(value.get("observation_completed_at"))
    if completed < started:
        raise SchemaFingerprintError("rca_schema_fingerprint_receipt_time_invalid")
    source = _exact_keys(
        value.get("source_database"),
        {
            "path",
            "device",
            "inode",
            "uid",
            "mode",
            "size_bytes_before",
            "size_bytes_after",
            "mtime_ns_before",
            "mtime_ns_after",
            "schema_version_counter",
            "control_schema_version",
            "delivery_schema_version",
            "database_instance_id",
            "genesis_intent_sha256",
        },
        "rca_schema_fingerprint_receipt_source_invalid",
    )
    _absolute_path(
        Path(str(source.get("path") or "")),
        "rca_schema_fingerprint_receipt_source_invalid",
    )
    integer_fields = {
        "device",
        "inode",
        "uid",
        "size_bytes_before",
        "size_bytes_after",
        "mtime_ns_before",
        "mtime_ns_after",
        "schema_version_counter",
    }
    if any(
        isinstance(source.get(name), bool)
        or not isinstance(source.get(name), int)
        or source[name] < 0
        for name in integer_fields
    ) or not re.fullmatch(r"0[0-7]{3}", str(source.get("mode") or "")):
        raise SchemaFingerprintError("rca_schema_fingerprint_receipt_source_invalid")
    identity = _exact_keys(
        value.get("database_identity"),
        {
            "database_instance_id",
            "genesis_intent_sha256",
            "control_schema_version",
            "delivery_schema_version",
        },
        "rca_schema_fingerprint_receipt_identity_invalid",
    )
    if (
        not isinstance(identity.get("database_instance_id"), str)
        or not identity["database_instance_id"]
        or _digest(
            identity.get("genesis_intent_sha256"),
            "rca_schema_fingerprint_receipt_identity_invalid",
        )
        != source.get("genesis_intent_sha256")
        or identity.get("database_instance_id") != source.get("database_instance_id")
        or identity.get("control_schema_version") != CONTROL_STORE_SCHEMA_VERSION
        or identity.get("delivery_schema_version") != DELIVERY_STORE_SCHEMA_VERSION
        or source.get("control_schema_version") != CONTROL_STORE_SCHEMA_VERSION
        or source.get("delivery_schema_version") != DELIVERY_STORE_SCHEMA_VERSION
    ):
        raise SchemaFingerprintError(
            "rca_schema_fingerprint_receipt_identity_invalid"
        )
    material = _validate_material(value.get("schema_material"))
    if _digest(
        value.get("schema_fingerprint_sha256"),
        "rca_schema_fingerprint_receipt_digest_mismatch",
    ) != canonical_json_sha256(material):
        raise SchemaFingerprintError("rca_schema_fingerprint_receipt_digest_mismatch")
    assertions = _exact_keys(
        value.get("assertions"),
        {
            "source_identity_stable",
            "source_schema_stable",
            "snapshot_identity_matches_source",
            "snapshot_schema_matches_source",
            "quick_check_ok",
            "foreign_keys_ok",
        },
        "rca_schema_fingerprint_receipt_assertions_invalid",
    )
    if any(item is not True for item in assertions.values()):
        raise SchemaFingerprintError(
            "rca_schema_fingerprint_receipt_assertions_invalid"
        )
    attestation = _exact_keys(
        value.get("read_only_attestation"),
        {
            "source_open_mode",
            "source_mutation_performed",
            "external_effects_triggered",
            "empty_materialization_sha256",
        },
        "rca_schema_fingerprint_receipt_attestation_invalid",
    )
    if attestation != {
        "source_open_mode": "mode=ro + PRAGMA query_only=ON",
        "source_mutation_performed": False,
        "external_effects_triggered": False,
        "empty_materialization_sha256": _SHA256_EMPTY,
    }:
        raise SchemaFingerprintError(
            "rca_schema_fingerprint_receipt_attestation_invalid"
        )


def verify_snapshot_receipt(receipt_path: Path) -> dict[str, Any]:
    receipt = _absolute_path(
        receipt_path, "rca_schema_fingerprint_receipt_path_invalid"
    )
    observed = _regular_owner_file(receipt)
    if stat.S_IMODE(observed.st_mode) != 0o600 or observed.st_size > MAX_RECEIPT_BYTES:
        raise SchemaFingerprintError("rca_schema_fingerprint_receipt_file_invalid")
    raw = receipt.read_bytes()
    value = _strict_json(raw)
    _validate_receipt_shape(value)
    material = value.get("schema_material")
    snapshot = _exact_keys(
        value.get("snapshot_database"),
        {
            "path",
            "device",
            "inode",
            "size_bytes",
            "raw_sha256",
            "page_size",
            "page_count",
            "journal_mode",
            "quick_check",
            "foreign_key_violation_count",
            "schema_version_counter",
        },
        "rca_schema_fingerprint_receipt_snapshot_invalid",
    )
    snapshot_path = _absolute_path(
        Path(str(snapshot.get("path") or "")),
        "rca_schema_fingerprint_snapshot_path_invalid",
    )
    snapshot_stat = _regular_owner_file(snapshot_path)
    if (
        snapshot_stat.st_dev != snapshot.get("device")
        or snapshot_stat.st_ino != snapshot.get("inode")
        or snapshot_stat.st_size != snapshot.get("size_bytes")
        or file_sha256(snapshot_path)
        != _digest(
            snapshot.get("raw_sha256"),
            "rca_schema_fingerprint_receipt_snapshot_invalid",
        )
    ):
        raise SchemaFingerprintError("rca_schema_fingerprint_snapshot_identity_mismatch")
    live = _database_observation(snapshot_path)
    if (
        live["schema_fingerprint_sha256"] != value["schema_fingerprint_sha256"]
        or live["schema_material"] != material
        or live["page_size"] != snapshot.get("page_size")
        or live["page_count"] != snapshot.get("page_count")
        or live["journal_mode"] != snapshot.get("journal_mode")
        or live["journal_mode"] != "delete"
        or live["quick_check"] != snapshot.get("quick_check")
        or live["foreign_key_violation_count"]
        != snapshot.get("foreign_key_violation_count")
        or live["schema_version_counter"]
        != snapshot.get("schema_version_counter")
        or live["database_instance_id"]
        != value["database_identity"]["database_instance_id"]
        or live["genesis_intent_sha256"]
        != value["database_identity"]["genesis_intent_sha256"]
    ):
        raise SchemaFingerprintError("rca_schema_fingerprint_snapshot_schema_mismatch")
    return {
        "schema_version": CLI_SCHEMA_VERSION,
        "ok": True,
        "receipt_path": str(receipt),
        "receipt_raw_sha256": hashlib.sha256(raw).hexdigest(),
        "snapshot_raw_sha256": snapshot["raw_sha256"],
        "schema_fingerprint_sha256": value["schema_fingerprint_sha256"],
        "object_count": material.get("object_count"),
        "database_instance_id": value["database_identity"]["database_instance_id"],
        "source_mutation_performed": False,
    }


class _SafeParser(argparse.ArgumentParser):
    def error(self, _message: str) -> None:
        raise SchemaFingerprintError("rca_schema_fingerprint_cli_arguments_invalid")


def _arguments(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = _SafeParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    snapshot = commands.add_parser("snapshot")
    snapshot.add_argument("--source-db", type=Path, required=True)
    snapshot.add_argument("--snapshot-db", type=Path, required=True)
    snapshot.add_argument("--receipt", type=Path, required=True)
    verify = commands.add_parser("verify")
    verify.add_argument("--receipt", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    command = "unknown"
    try:
        args = _arguments(argv)
        command = str(args.command)
        if command == "snapshot":
            result = create_snapshot_receipt(
                args.source_db,
                snapshot_path=args.snapshot_db,
                receipt_path=args.receipt,
            )
            receipt = result["receipt"]
            payload = {
                "schema_version": CLI_SCHEMA_VERSION,
                "command": command,
                "ok": True,
                "receipt_path": str(args.receipt),
                "receipt_raw_sha256": result["receipt_raw_sha256"],
                "snapshot_raw_sha256": receipt["snapshot_database"]["raw_sha256"],
                "schema_fingerprint_sha256": receipt[
                    "schema_fingerprint_sha256"
                ],
                "object_count": receipt["schema_material"]["object_count"],
                "database_instance_id": receipt["database_identity"][
                    "database_instance_id"
                ],
                "source_mutation_performed": False,
            }
        elif command == "verify":
            payload = {"command": command, **verify_snapshot_receipt(args.receipt)}
        else:
            raise SchemaFingerprintError(
                "rca_schema_fingerprint_cli_arguments_invalid"
            )
        print(json.dumps(payload, ensure_ascii=True, sort_keys=True))
        return 0
    except SchemaFingerprintError as exc:
        print(
            json.dumps(
                {
                    "schema_version": CLI_SCHEMA_VERSION,
                    "command": command,
                    "ok": False,
                    "code": exc.code,
                    "detail": exc.detail,
                },
                ensure_ascii=True,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
