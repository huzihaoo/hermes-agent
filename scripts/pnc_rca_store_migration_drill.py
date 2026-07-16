#!/usr/bin/env python3
"""Run an offline RCA SQLite migration and rollback drill on isolated copies."""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
import platform
from pathlib import Path
import psutil
import re
import sqlite3
import stat
import subprocess
import sys
import tempfile
import time
from typing import Any, Mapping, Sequence
import uuid


REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from gateway.pnc_rca_control_store import (
    CONTROL_STORE_SCHEMA_VERSION,
    RcaControlStore,
)
from gateway.pnc_rca_delivery_store import (
    DELIVERY_STORE_SCHEMA_VERSION,
    RcaDeliveryStore,
)


STORE_MIGRATION_RECEIPT_SCHEMA_VERSION = "pnc_rca_store_migration_receipt_v3"
FRESH_INSTALL_MATERIALIZATION_RECEIPT_SCHEMA_VERSION = (
    "pnc_rca_fresh_install_materialization_receipt_v3"
)
CAPACITY_INITIALIZATION_RECEIPT_SCHEMA_VERSION = (
    "pnc_rca_capacity_transition_initialization_receipt_v1"
)
CAPACITY_LATCH_SNAPSHOT_SCHEMA_VERSION = "pnc_rca_capacity_transition_latch_v1"
FRESH_INSTALL_MAINTENANCE_SCHEMA_VERSION = "pnc_rca_fresh_install_maintenance_v1"
FRESH_INSTALL_JOURNAL_SCHEMA_VERSION = "pnc_rca_fresh_install_journal_v1"
FRESH_INSTALL_RUNTIME_IDENTITY_SCHEMA_VERSION = (
    "pnc_rca_fresh_install_runtime_identity_v1"
)
FRESH_INSTALL_QUARANTINE_RECEIPT_SCHEMA_VERSION = (
    "pnc_rca_fresh_install_quarantine_receipt_v1"
)
FRESH_INSTALL_RESTORE_RECEIPT_SCHEMA_VERSION = (
    "pnc_rca_fresh_install_restore_receipt_v1"
)
FRESH_INSTALL_ROLLBACK_MAINTENANCE_SCHEMA_VERSION = (
    "pnc_rca_fresh_install_rollback_maintenance_v1"
)
FRESH_INSTALL_ROLLBACK_JOURNAL_SCHEMA_VERSION = (
    "pnc_rca_fresh_install_rollback_journal_v1"
)
FRESH_INSTALL_TOMBSTONE_SCHEMA_VERSION = (
    "pnc_rca_fresh_install_quarantine_tombstone_v1"
)
WRITER_STOP_EVIDENCE_SCHEMA_VERSION = "pnc_rca_writer_stop_evidence_v1"
PREDECESSOR_COMPATIBILITY_PROBE = "bom_pinned_predecessor_validator_v1"
PREDECESSOR_VALIDATOR_RESULT_SCHEMA_VERSION = (
    "pnc_rca_predecessor_validator_result_v1"
)
PREDECESSOR_VALIDATOR_MAX_OUTPUT_BYTES = 64 * 1024
PREDECESSOR_VALIDATOR_TIMEOUT_SECONDS = 30
CONTROL_PREDECESSOR_SCHEMA_VERSION = "pnc_rca_control_store_v8"
DELIVERY_PREDECESSOR_SCHEMA_VERSION = "pnc_rca_delivery_store_v5"
MIGRATION_SOURCE_RELATIVE_PATHS = (
    "gateway/pnc_rca_control_store.py",
    "gateway/pnc_rca_delivery_store.py",
    "gateway/pnc_rca_runtime_transition.py",
)
RCA_SERVICE_LABELS = frozenset({
    "local.pnc.rca-kafka-consumer",
    "local.pnc.rca-outbox-dispatcher",
    "local.pnc.rca-delivery-collector",
    "local.pnc.rca-delivery-dispatcher",
})
GATEWAY_WRITER_LABEL = "ai.hermes.gateway"
STORE_WRITER_LABELS = RCA_SERVICE_LABELS | {GATEWAY_WRITER_LABEL}
WRITER_PROCESS_PROBE = "launchctl_job_absence_psutil_process_absence_v2"
RCA_SERVICE_SCRIPT_NAMES = {
    "local.pnc.rca-kafka-consumer": "pnc_rca_kafka_consumer.py",
    "local.pnc.rca-outbox-dispatcher": "pnc_rca_outbox_dispatcher.py",
    "local.pnc.rca-delivery-collector": "pnc_rca_delivery_collector.py",
    "local.pnc.rca-delivery-dispatcher": "pnc_rca_delivery_dispatcher.py",
}
CONTROL_V8_BASELINE_INDEXES = frozenset({
    "idx_business_triggers_issue_scope",
    "idx_rca_manual_operator_rate",
})
CONTROL_V9_ACTIVATION_TABLES = frozenset({
    "rca_activation_epochs",
    "rca_activation_budget_slots",
    "rca_activation_admission_ledger",
    "rca_activation_transition_audit",
})
CONTROL_V9_ACTIVATION_INDEXES = frozenset({
    "idx_rca_single_current_activation_epoch",
    "idx_rca_activation_ledger_submission",
    "idx_rca_activation_transition_epoch",
    "idx_outbox_activation_claim",
})
CONTROL_V9_ACTIVATION_COLUMNS = {
    "kafka_inbox": frozenset({
        "activation_epoch_id",
        "activation_ingress_state",
        "activation_required",
        "activation_slot_kind",
        "activation_source_identity_sha256",
    }),
    "business_triggers": frozenset({
        "activation_epoch_id",
        "activation_ledger_id",
    }),
    "rca_outbox": frozenset({
        "activation_epoch_id",
        "activation_ledger_id",
    }),
}
HOST_RUNTIME_TRANSITION_COLUMNS = frozenset({
    "transition_id",
    "submission_key",
    "business_key",
    "generation",
    "service_label",
    "transition_kind",
    "entity_key",
    "runtime_identity_json",
    "runtime_identity_sha256",
    "transitioned_at",
})
MAX_WRITER_STOP_EVIDENCE_BYTES = 64 * 1024
SQLITE_VALIDATION_TIMEOUT_SECONDS = 120
SQLITE_BACKUP_TIMEOUT_SECONDS = 120
CAPACITY_IDENTITY_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
CAPACITY_EVIDENCE_FIELDS = (
    "final_ledger_sha256",
    "transition_authorization_sha256",
    "transition_authorization_fingerprint",
    "transition_receipt_sha256",
    "transition_receipt_fingerprint",
    "commit_marker_sha256",
    "commit_marker_fingerprint",
    "evidence_bundle_sha256",
    "evidence_bundle_fingerprint",
)
CAPACITY_EVIDENCE_TIME_FIELDS = (
    "authorization_issued_at",
    "authorization_expires_at",
    "receipt_created_at",
    "marker_committed_at",
)
CAPACITY_STATE_ROW_FIELDS = frozenset({
    "singleton_id",
    "release_id",
    "bootstrap_epoch_id",
    "state",
    "generation",
    *CAPACITY_EVIDENCE_FIELDS,
    *CAPACITY_EVIDENCE_TIME_FIELDS,
    "bootstrap_initialized_at",
    "steady_activated_at",
    "updated_at",
})
CAPACITY_AUDIT_ROW_FIELDS = frozenset({
    "audit_id",
    "release_id",
    "bootstrap_epoch_id",
    "from_state",
    "to_state",
    "from_generation",
    "to_generation",
    *CAPACITY_EVIDENCE_FIELDS,
    *CAPACITY_EVIDENCE_TIME_FIELDS,
    "transitioned_at",
})


class MigrationDrillError(RuntimeError):
    """The drill could not prove a safe, reproducible migration."""

    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


def _utc(value: datetime | None = None) -> datetime:
    current = value or datetime.now(timezone.utc)
    if current.tzinfo is None:
        raise MigrationDrillError("migration_drill_timestamp_naive")
    return current.astimezone(timezone.utc)


def _parse_timestamp(value: Any, *, code: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise MigrationDrillError(code) from exc
    if parsed.tzinfo is None:
        raise MigrationDrillError(code)
    return parsed.astimezone(timezone.utc)


def _absolute(path: str | Path) -> Path:
    expanded = Path(path).expanduser()
    if expanded.is_absolute():
        return expanded
    return Path.cwd() / expanded


def _canonical_configured_path(path: str | Path) -> Path:
    candidate = _absolute(path)
    if candidate.is_symlink():
        raise MigrationDrillError("migration_source_symlink")
    if candidate.exists():
        try:
            mode = candidate.lstat().st_mode
            resolved = candidate.resolve(strict=True)
        except OSError as exc:
            raise MigrationDrillError("migration_source_unreadable") from exc
        if not stat.S_ISREG(mode):
            raise MigrationDrillError("migration_source_not_regular")
        return resolved
    try:
        parent = candidate.parent.resolve(strict=True)
    except OSError as exc:
        raise MigrationDrillError("migration_source_parent_missing") from exc
    return parent / candidate.name


def _ensure_directory(path: str | Path) -> Path:
    candidate = _absolute(path)
    if candidate.is_symlink():
        raise MigrationDrillError("migration_output_directory_symlink")
    try:
        candidate.mkdir(parents=True, exist_ok=True, mode=0o700)
        resolved = candidate.resolve(strict=True)
        mode = resolved.lstat().st_mode
    except OSError as exc:
        raise MigrationDrillError("migration_output_directory_invalid") from exc
    info = resolved.lstat()
    if (
        not stat.S_ISDIR(mode)
        or info.st_uid != os.getuid()
        or stat.S_IMODE(info.st_mode) & 0o022
        or resolved != candidate.absolute()
    ):
        raise MigrationDrillError("migration_output_directory_invalid")
    return resolved


def _secure_directory_observation(
    path: str | Path,
    *,
    code: str,
) -> tuple[Path, dict[str, Any]]:
    candidate = _absolute(path).absolute()
    try:
        before = candidate.lstat()
        resolved = candidate.resolve(strict=True)
    except OSError as exc:
        raise MigrationDrillError(code) from exc
    if (
        resolved != candidate
        or stat.S_ISLNK(before.st_mode)
        or not stat.S_ISDIR(before.st_mode)
        or before.st_uid != os.getuid()
        or before.st_nlink < 1
        or stat.S_IMODE(before.st_mode) & 0o022
    ):
        raise MigrationDrillError(code)
    return resolved, {
        "path": str(resolved),
        "device": int(before.st_dev),
        "inode": int(before.st_ino),
        "owner_uid": int(before.st_uid),
        "mode": f"{stat.S_IMODE(before.st_mode):04o}",
    }


def _sha256_fd(descriptor: int) -> str:
    digest = hashlib.sha256()
    os.lseek(descriptor, 0, os.SEEK_SET)
    while True:
        chunk = os.read(descriptor, 1024 * 1024)
        if not chunk:
            break
        digest.update(chunk)
    return digest.hexdigest()


def observe_regular_file(path: str | Path) -> dict[str, Any]:
    """Hash one regular file without following its final path component."""
    candidate = _absolute(path)
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = -1
    try:
        before = candidate.lstat()
        if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
            raise MigrationDrillError("migration_artifact_not_regular")
        descriptor = os.open(candidate, flags)
        observed = os.fstat(descriptor)
        if not stat.S_ISREG(observed.st_mode):
            raise MigrationDrillError("migration_artifact_not_regular")
        digest = _sha256_fd(descriptor)
        after = candidate.lstat()
    except MigrationDrillError:
        raise
    except OSError as exc:
        raise MigrationDrillError("migration_artifact_unreadable") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    identity = (
        observed.st_dev,
        observed.st_ino,
        observed.st_size,
        observed.st_mtime_ns,
    )
    if identity != (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns):
        raise MigrationDrillError("migration_artifact_changed_during_read")
    if identity != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns):
        raise MigrationDrillError("migration_artifact_changed_during_read")
    return {
        "path": str(candidate.resolve(strict=True)),
        "device": int(observed.st_dev),
        "inode": int(observed.st_ino),
        "size_bytes": int(observed.st_size),
        "mtime_ns": int(observed.st_mtime_ns),
        "sha256": digest,
    }


def _git_committed_executable(
    path: str | Path,
    *,
    candidate: Mapping[str, Any],
) -> dict[str, Any]:
    """Bind an executable to the exact host candidate commit used by the drill."""
    try:
        repo_root = Path(str(candidate["repo_root"])).resolve(strict=True)
        commit = str(candidate["commit"])
        executable = _absolute(path).resolve(strict=True)
        relative = executable.relative_to(repo_root)
    except (KeyError, OSError, ValueError) as exc:
        raise MigrationDrillError("migration_predecessor_validator_outside_bom") from exc
    if not relative.parts or ".git" in relative.parts:
        raise MigrationDrillError("migration_predecessor_validator_outside_bom")
    observation = observe_regular_file(executable)
    try:
        executable_stat = executable.lstat()
    except OSError as exc:
        raise MigrationDrillError("migration_predecessor_validator_invalid") from exc
    if (
        executable_stat.st_uid != os.getuid()
        or executable_stat.st_nlink != 1
        or not executable_stat.st_mode & 0o111
    ):
        raise MigrationDrillError("migration_predecessor_validator_invalid")
    relative_text = relative.as_posix()
    try:
        committed = subprocess.run(
            ["git", "-C", str(repo_root), "show", f"{commit}:{relative_text}"],
            check=True,
            capture_output=True,
        ).stdout
        tree_line = subprocess.run(
            ["git", "-C", str(repo_root), "ls-tree", commit, "--", relative_text],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    except (OSError, subprocess.SubprocessError) as exc:
        raise MigrationDrillError("migration_predecessor_validator_not_in_bom") from exc
    tree_match = re.fullmatch(
        rf"(100755) blob [0-9a-f]{{40,64}}\t{re.escape(relative_text)}",
        tree_line,
    )
    if (
        tree_match is None
        or hashlib.sha256(committed).hexdigest() != observation["sha256"]
    ):
        raise MigrationDrillError("migration_predecessor_validator_bom_mismatch")
    return {
        "status": "verified",
        "protocol": PREDECESSOR_COMPATIBILITY_PROBE,
        "artifact": observation,
        "executable_mode": "100755",
        "owner_uid": int(executable_stat.st_uid),
        "owner_gid": int(executable_stat.st_gid),
        "link_count": int(executable_stat.st_nlink),
        "bom_binding": {
            "source": "verified_host_candidate_commit",
            "repo_root": str(repo_root),
            "commit": commit,
            "relative_path": relative_text,
            "artifact_sha256": observation["sha256"],
            "git_mode": tree_match.group(1),
        },
    }


def observe_sqlite_bundle(path: str | Path) -> dict[str, Any]:
    """Observe the DB plus every SQLite sidecar that can carry live state."""
    database = _absolute(path)
    observations: dict[str, Any] = {}
    for name, candidate in (
        ("database", database),
        ("wal", Path(f"{database}-wal")),
        ("shm", Path(f"{database}-shm")),
        ("journal", Path(f"{database}-journal")),
    ):
        present = candidate.exists() or candidate.is_symlink()
        observations[name] = {
            "present": present,
            "identity": observe_regular_file(candidate) if present else None,
        }
    return observations


def _require_sidecar_free_sqlite_bundle(
    path: Path,
    *,
    expected_database_present: bool,
) -> dict[str, Any]:
    bundle = observe_sqlite_bundle(path)
    if bundle["database"]["present"] is not expected_database_present:
        raise MigrationDrillError("migration_source_presence_changed")
    if any(bundle[name]["present"] for name in ("wal", "shm", "journal")):
        raise MigrationDrillError("migration_source_sidecar_present")
    return bundle


def _require_no_sqlite_rollback_journal(path: Path) -> None:
    journal = Path(f"{path}-journal")
    if journal.exists() or journal.is_symlink():
        raise MigrationDrillError("migration_source_sidecar_present")


def _schema_marker(
    connection: sqlite3.Connection,
    *,
    table: str,
) -> str:
    exists = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    if exists is None:
        raise MigrationDrillError("migration_schema_marker_missing")
    row = connection.execute(
        f"SELECT value FROM {table} WHERE key='schema_version'"
    ).fetchone()
    if row is None or not str(row[0]).strip():
        raise MigrationDrillError("migration_schema_marker_missing")
    return str(row[0])


def inspect_sqlite_read_only(
    path: str | Path,
    roles: Sequence[str],
) -> dict[str, Any]:
    """Reopen a SQLite artifact in mode=ro/query_only and verify its schemas."""
    role_set = frozenset(str(role) for role in roles)
    if not role_set or not role_set.issubset({"control", "delivery"}):
        raise MigrationDrillError("migration_roles_invalid")
    configured_path = _absolute(path)
    _require_sidecar_free_sqlite_bundle(
        configured_path,
        expected_database_present=True,
    )
    artifact = observe_regular_file(configured_path)
    uri = Path(artifact["path"]).as_uri() + "?mode=ro&immutable=1"
    try:
        connection = sqlite3.connect(uri, uri=True)
        connection.row_factory = sqlite3.Row
        validation_deadline = time.monotonic() + SQLITE_VALIDATION_TIMEOUT_SECONDS
        connection.set_progress_handler(
            lambda: int(time.monotonic() > validation_deadline),
            10_000,
        )
        connection.execute("PRAGMA query_only=ON")
        connection.execute("PRAGMA trusted_schema=OFF")
        connection.execute("BEGIN")
        query_only = int(connection.execute("PRAGMA query_only").fetchone()[0])
        quick_rows = [
            str(row[0]).lower()
            for row in connection.execute("PRAGMA quick_check").fetchall()
        ]
        foreign_key_rows = connection.execute("PRAGMA foreign_key_check").fetchall()
        schemas: dict[str, str] = {}
        structure: dict[str, Any] = {}
        if "control" in role_set:
            schemas["control"] = _schema_marker(
                connection,
                table="control_meta",
            )
            indexes = {
                str(row[0])
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='index'"
                ).fetchall()
            }
            tables = {
                str(row[0])
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
            activation_columns = {
                table: sorted(
                    expected
                    & {
                        str(row["name"])
                        for row in connection.execute(
                            f"PRAGMA table_info({_quoted_identifier(table)})"
                        ).fetchall()
                    }
                )
                for table, expected in CONTROL_V9_ACTIVATION_COLUMNS.items()
            }
            structure["control_v8_indexes"] = sorted(
                CONTROL_V8_BASELINE_INDEXES & indexes
            )
            structure["control_v9_activation"] = {
                "tables": sorted(CONTROL_V9_ACTIVATION_TABLES & tables),
                "indexes": sorted(CONTROL_V9_ACTIVATION_INDEXES & indexes),
                "columns": activation_columns,
            }
        if "delivery" in role_set:
            schemas["delivery"] = _schema_marker(
                connection,
                table="rca_delivery_meta",
            )
            watch_columns = {
                str(row["name"]): row
                for row in connection.execute(
                    "PRAGMA table_info(rca_execution_watch)"
                ).fetchall()
            }
            task_id = watch_columns.get("task_id")
            if task_id is None:
                raise MigrationDrillError("migration_delivery_watch_schema_missing")
            structure["delivery_task_id_notnull"] = int(task_id["notnull"])
        transition_columns = {
            str(row["name"])
            for row in connection.execute(
                "PRAGMA table_info(rca_host_runtime_transitions)"
            ).fetchall()
        }
        transition_index = [
            str(row["name"])
            for row in connection.execute(
                "PRAGMA index_info(idx_rca_host_runtime_transition_submission)"
            ).fetchall()
        ]
        structure["host_runtime_transitions"] = {
            "present": bool(transition_columns),
            "columns": sorted(transition_columns),
            "submission_index": transition_index,
        }
        try:
            connection.execute(
                "CREATE TABLE __pnc_rca_migration_read_only_probe(value INTEGER)"
            )
        except sqlite3.OperationalError:
            write_probe = "blocked_readonly"
        else:
            raise MigrationDrillError("migration_read_only_write_probe_failed")
    except MigrationDrillError:
        raise
    except sqlite3.Error as exc:
        raise MigrationDrillError("migration_sqlite_validation_failed") from exc
    finally:
        if "connection" in locals():
            connection.close()
    if query_only != 1 or quick_rows != ["ok"] or foreign_key_rows:
        raise MigrationDrillError("migration_sqlite_integrity_failed")
    if observe_regular_file(path) != artifact:
        raise MigrationDrillError("migration_artifact_changed_during_validation")
    return {
        "connection_mode": "mode=ro",
        "query_only": True,
        "write_probe": write_probe,
        "quick_check": "ok",
        "foreign_key_check_rows": 0,
        "schemas": schemas,
        "structure": structure,
    }


def _quoted_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _sqlite_value_material(value: Any) -> list[Any]:
    if value is None:
        return ["null", None]
    if isinstance(value, bytes):
        return ["blob", value.hex()]
    if isinstance(value, bool):
        return ["integer", int(value)]
    if isinstance(value, int):
        return ["integer", value]
    if isinstance(value, float):
        return ["real", value.hex()]
    return ["text", str(value)]


def _common_table_fingerprint(
    connection: sqlite3.Connection,
    *,
    table: str,
    columns: Sequence[str],
    where_clause: str = "",
) -> dict[str, Any]:
    projection = ",".join(_quoted_identifier(column) for column in columns)
    query = f"SELECT {projection} FROM {_quoted_identifier(table)}{where_clause}"
    xor_value = 0
    sum_value = 0
    row_count = 0
    modulus = 1 << 256
    for row in connection.execute(query):
        material = [_sqlite_value_material(value) for value in row]
        encoded = json.dumps(
            material,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
        integer = int.from_bytes(hashlib.sha256(encoded).digest(), "big")
        xor_value ^= integer
        sum_value = (sum_value + integer) % modulus
        row_count += 1
    return {
        "row_count": row_count,
        "columns_sha256": hashlib.sha256(
            json.dumps(
                list(columns),
                ensure_ascii=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest(),
        "row_hash_xor": f"{xor_value:064x}",
        "row_hash_sum": f"{sum_value:064x}",
    }


def _sqlite_row_digest(*values: Any) -> str:
    material = [_sqlite_value_material(value) for value in values]
    encoded = json.dumps(
        material,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _row_digest_counts(
    connection: sqlite3.Connection,
    *,
    table: str,
    columns: Sequence[str],
    where_clause: str,
):
    projection = ",".join(_quoted_identifier(column) for column in columns)
    connection.create_function(
        "pnc_rca_row_digest",
        -1,
        _sqlite_row_digest,
        deterministic=True,
    )
    return connection.execute(
        "SELECT pnc_rca_row_digest("
        f"{projection}) AS digest, COUNT(*) AS occurrences "
        f"FROM {_quoted_identifier(table)}{where_clause} "
        "GROUP BY digest ORDER BY digest"
    )


def _require_row_digest_subset(
    required: sqlite3.Connection,
    candidate: sqlite3.Connection,
    *,
    table: str,
    columns: Sequence[str],
    where_clause: str,
) -> None:
    candidate_rows = iter(
        _row_digest_counts(
            candidate,
            table=table,
            columns=columns,
            where_clause=where_clause,
        )
    )
    current = next(candidate_rows, None)
    for required_row in _row_digest_counts(
        required,
        table=table,
        columns=columns,
        where_clause=where_clause,
    ):
        required_digest = str(required_row[0])
        while current is not None and str(current[0]) < required_digest:
            current = next(candidate_rows, None)
        if (
            current is None
            or str(current[0]) != required_digest
            or int(current[1]) < int(required_row[1])
        ):
            raise MigrationDrillError("migration_restore_data_mismatch")


def compare_sqlite_common_content(
    backup_path: Path,
    restore_path: Path,
) -> dict[str, Any]:
    """Prove every pre-migration column and row survives in the candidate DB."""
    connections: list[sqlite3.Connection] = []
    _require_sidecar_free_sqlite_bundle(
        backup_path,
        expected_database_present=True,
    )
    _require_sidecar_free_sqlite_bundle(
        restore_path,
        expected_database_present=True,
    )
    observations = {
        "backup": observe_regular_file(backup_path),
        "restore": observe_regular_file(restore_path),
    }
    try:
        for path in (backup_path, restore_path):
            connection = sqlite3.connect(
                path.as_uri() + "?mode=ro&immutable=1",
                uri=True,
            )
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA query_only=ON")
            connection.execute("PRAGMA trusted_schema=OFF")
            connection.execute("BEGIN")
            connections.append(connection)
        backup, restore = connections
        excluded = {"sqlite_sequence"}
        backup_tables = {
            str(row[0])
            for row in backup.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name NOT LIKE 'sqlite_%'"
            ).fetchall()
        } - excluded
        restore_tables = {
            str(row[0])
            for row in restore.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name NOT LIKE 'sqlite_%'"
            ).fetchall()
        } - excluded
        if not backup_tables.issubset(restore_tables):
            raise MigrationDrillError("migration_restore_table_missing")
        table_fingerprints: dict[str, Any] = {}
        total_rows = 0
        for table in sorted(backup_tables):
            backup_columns = [
                str(row["name"])
                for row in backup.execute(
                    f"PRAGMA table_info({_quoted_identifier(table)})"
                ).fetchall()
            ]
            restore_columns = {
                str(row["name"])
                for row in restore.execute(
                    f"PRAGMA table_info({_quoted_identifier(table)})"
                ).fetchall()
            }
            missing_columns = [
                column for column in backup_columns if column not in restore_columns
            ]
            if missing_columns:
                raise MigrationDrillError("migration_restore_column_missing")
            where_clause = (
                " WHERE key != 'schema_version'"
                if table in {"control_meta", "rca_delivery_meta"}
                else ""
            )
            _require_row_digest_subset(
                backup,
                restore,
                table=table,
                columns=backup_columns,
                where_clause=where_clause,
            )
            before = _common_table_fingerprint(
                backup,
                table=table,
                columns=backup_columns,
                where_clause=where_clause,
            )
            table_fingerprints[table] = before
            total_rows += int(before["row_count"])
    except MigrationDrillError:
        raise
    except (sqlite3.Error, ValueError) as exc:
        raise MigrationDrillError("migration_restore_data_check_failed") from exc
    finally:
        for connection in connections:
            connection.close()
    if (
        observe_regular_file(backup_path) != observations["backup"]
        or observe_regular_file(restore_path) != observations["restore"]
    ):
        raise MigrationDrillError("migration_artifact_changed_during_validation")
    material = json.dumps(
        table_fingerprints,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return {
        "schema_version": "pnc_rca_store_data_inheritance_v2",
        "table_count": len(table_fingerprints),
        "row_count": total_rows,
        "tables_sha256": hashlib.sha256(material).hexdigest(),
    }


def _sqlite_backup(
    source: Path,
    destination: Path,
    *,
    immutable_source: bool = True,
) -> None:
    if destination.exists() or destination.is_symlink():
        raise MigrationDrillError("migration_artifact_already_exists")
    if immutable_source:
        _require_sidecar_free_sqlite_bundle(
            source,
            expected_database_present=True,
        )
    else:
        _require_no_sqlite_rollback_journal(source)
    source_uri = source.as_uri() + "?mode=ro"
    if immutable_source:
        source_uri += "&immutable=1"
    try:
        source_connection = sqlite3.connect(source_uri, uri=True, timeout=5)
        source_connection.execute("PRAGMA query_only=ON")
        destination_connection = sqlite3.connect(destination, timeout=5)
        backup_deadline = time.monotonic() + SQLITE_BACKUP_TIMEOUT_SECONDS

        def progress(_status: int, _remaining: int, _total: int) -> None:
            if time.monotonic() > backup_deadline:
                raise MigrationDrillError("migration_sqlite_backup_timeout")

        source_connection.backup(
            destination_connection,
            pages=1024,
            progress=progress,
            sleep=0.05,
        )
        destination_connection.commit()
    except MigrationDrillError:
        destination.unlink(missing_ok=True)
        raise
    except sqlite3.Error as exc:
        destination.unlink(missing_ok=True)
        raise MigrationDrillError("migration_sqlite_backup_failed") from exc
    finally:
        if "destination_connection" in locals():
            destination_connection.close()
        if "source_connection" in locals():
            source_connection.close()
    os.chmod(destination, 0o600)


def sqlite_read_only_snapshot(source: Path, destination: Path) -> None:
    """Take one online read-only snapshot, including committed WAL contents."""
    _sqlite_backup(source, destination, immutable_source=False)


def sqlite_backup_snapshot_sha256(source: Path, scratch_dir: Path) -> str:
    """Recompute one SQLite backup digest without retaining another artifact."""
    directory = _ensure_directory(scratch_dir)
    temporary = directory / f".snapshot-verify-{uuid.uuid4().hex}.sqlite3"
    try:
        _sqlite_backup(source, temporary)
        return str(observe_regular_file(temporary)["sha256"])
    finally:
        for candidate in (
            temporary,
            Path(str(temporary) + "-wal"),
            Path(str(temporary) + "-shm"),
            Path(str(temporary) + "-journal"),
        ):
            candidate.unlink(missing_ok=True)


def _checkpoint_restore(path: Path) -> None:
    try:
        connection = sqlite3.connect(path)
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        connection.execute("PRAGMA journal_mode=DELETE")
        connection.commit()
    except sqlite3.Error as exc:
        raise MigrationDrillError("migration_restore_checkpoint_failed") from exc
    finally:
        if "connection" in locals():
            connection.close()
    os.chmod(path, 0o600)


_DELIVERY_V4_WATCH_SQL = """
CREATE TABLE rca_execution_watch (
    submission_key TEXT PRIMARY KEY,
    submission_outbox_id INTEGER NOT NULL UNIQUE,
    business_key TEXT NOT NULL,
    generation INTEGER NOT NULL CHECK (generation >= 1),
    project_key TEXT NOT NULL,
    work_item_type_key TEXT NOT NULL,
    work_item_id TEXT NOT NULL,
    task_id TEXT NOT NULL UNIQUE,
    state TEXT NOT NULL,
    poll_attempt INTEGER NOT NULL DEFAULT 0,
    next_poll_at TEXT NOT NULL,
    last_observed_at TEXT,
    terminal_at TEXT,
    terminal_first_seen_at TEXT,
    fence INTEGER NOT NULL DEFAULT 0,
    lease_token TEXT,
    lease_owner TEXT,
    lease_expires_at TEXT,
    last_status_json TEXT,
    last_error_code TEXT NOT NULL DEFAULT '',
    last_error_detail TEXT NOT NULL DEFAULT '',
    delivery_id TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(business_key, generation)
)
"""


def _create_predecessor_fixture(path: Path, roles: Sequence[str]) -> None:
    role_set = frozenset(roles)
    if "control" in role_set:
        RcaControlStore(path)
    if "delivery" in role_set:
        RcaDeliveryStore(path)
    _checkpoint_restore(path)
    try:
        connection = sqlite3.connect(path)
        connection.execute("PRAGMA foreign_keys=OFF")
        if "control" in role_set:
            for index in sorted(CONTROL_V9_ACTIVATION_INDEXES):
                connection.execute(
                    f"DROP INDEX IF EXISTS {_quoted_identifier(index)}"
                )
            for table in sorted(CONTROL_V9_ACTIVATION_TABLES):
                connection.execute(
                    f"DROP TABLE IF EXISTS {_quoted_identifier(table)}"
                )
            for table, columns in CONTROL_V9_ACTIVATION_COLUMNS.items():
                existing = {
                    str(row[1])
                    for row in connection.execute(
                        f"PRAGMA table_info({_quoted_identifier(table)})"
                    ).fetchall()
                }
                for column in sorted(columns & existing):
                    connection.execute(
                        f"ALTER TABLE {_quoted_identifier(table)} "
                        f"DROP COLUMN {_quoted_identifier(column)}"
                    )
            connection.execute(
                "UPDATE control_meta SET value=? WHERE key='schema_version'",
                (CONTROL_PREDECESSOR_SCHEMA_VERSION,),
            )
        else:
            connection.execute("DROP TABLE IF EXISTS rca_host_runtime_transitions")
        if "delivery" in role_set:
            connection.execute(
                "UPDATE rca_delivery_meta SET value=? WHERE key='schema_version'",
                (DELIVERY_PREDECESSOR_SCHEMA_VERSION,),
            )
        connection.commit()
        connection.execute("PRAGMA foreign_keys=ON")
        if connection.execute("PRAGMA foreign_key_check").fetchone() is not None:
            raise MigrationDrillError("migration_predecessor_fixture_foreign_keys")
    except sqlite3.Error as exc:
        raise MigrationDrillError("migration_predecessor_fixture_failed") from exc
    finally:
        if "connection" in locals():
            connection.close()
    os.chmod(path, 0o600)


def _expected_predecessor_schemas(roles: Sequence[str]) -> dict[str, str]:
    return {
        role: (
            CONTROL_PREDECESSOR_SCHEMA_VERSION
            if role == "control"
            else DELIVERY_PREDECESSOR_SCHEMA_VERSION
        )
        for role in sorted(roles)
    }


def _expected_target_schemas(roles: Sequence[str]) -> dict[str, str]:
    return {
        role: (
            CONTROL_STORE_SCHEMA_VERSION
            if role == "control"
            else DELIVERY_STORE_SCHEMA_VERSION
        )
        for role in sorted(roles)
    }


def _schema_transitions(roles: Sequence[str]) -> dict[str, dict[str, str]]:
    return {
        role: {
            "from": (
                CONTROL_PREDECESSOR_SCHEMA_VERSION
                if role == "control"
                else DELIVERY_PREDECESSOR_SCHEMA_VERSION
            ),
            "to": (
                CONTROL_STORE_SCHEMA_VERSION
                if role == "control"
                else DELIVERY_STORE_SCHEMA_VERSION
            ),
        }
        for role in sorted(roles)
    }


def _migration_state(
    validation: Mapping[str, Any],
    roles: Sequence[str],
) -> str:
    schemas = validation.get("schemas")
    if schemas == _expected_predecessor_schemas(roles):
        return "migration_required"
    if schemas == _expected_target_schemas(roles):
        return "already_current"
    raise MigrationDrillError("migration_pre_schema_mixed")


def _run_predecessor_validator(
    *,
    validator: Mapping[str, Any],
    rollback_path: Path,
    roles: Sequence[str],
    run_root: Path,
    observed_at: datetime,
) -> dict[str, Any]:
    artifact = validator.get("artifact")
    if not isinstance(artifact, Mapping):
        raise MigrationDrillError("migration_predecessor_validator_invalid")
    executable = Path(str(artifact.get("path") or ""))
    expected_schemas = _expected_predecessor_schemas(roles)
    argv = [
        str(executable),
        "--database",
        str(rollback_path),
        "--roles-json",
        json.dumps(list(roles), ensure_ascii=True, separators=(",", ":")),
        "--expected-schemas-json",
        json.dumps(expected_schemas, ensure_ascii=True, sort_keys=True, separators=(",", ":")),
    ]
    os.chmod(rollback_path, 0o400)
    bundle_before = _require_sidecar_free_sqlite_bundle(
        rollback_path,
        expected_database_present=True,
    )
    artifact_before = observe_regular_file(rollback_path)
    executable_before = observe_regular_file(executable)
    if executable_before != artifact:
        raise MigrationDrillError("migration_predecessor_validator_changed")
    environment = {
        "HOME": str(run_root),
        "LANG": "C",
        "LC_ALL": "C",
        "PATH": "/usr/bin:/bin:/usr/sbin:/sbin",
    }
    started = time.monotonic()
    with tempfile.TemporaryFile() as stdout_file, tempfile.TemporaryFile() as stderr_file:
        try:
            process = subprocess.Popen(
                argv,
                cwd=run_root,
                env=environment,
                stdin=subprocess.DEVNULL,
                stdout=stdout_file,
                stderr=stderr_file,
            )
            child_pid = int(process.pid)
            exit_code = process.wait(timeout=PREDECESSOR_VALIDATOR_TIMEOUT_SECONDS)
        except subprocess.TimeoutExpired as exc:
            process.kill()
            process.wait()
            raise MigrationDrillError("migration_predecessor_validator_timeout") from exc
        except OSError as exc:
            raise MigrationDrillError("migration_predecessor_validator_execution_failed") from exc
        duration_ms = round((time.monotonic() - started) * 1000, 3)
        stdout_size = stdout_file.seek(0, os.SEEK_END)
        stderr_size = stderr_file.seek(0, os.SEEK_END)
        if (
            stdout_size > PREDECESSOR_VALIDATOR_MAX_OUTPUT_BYTES
            or stderr_size > PREDECESSOR_VALIDATOR_MAX_OUTPUT_BYTES
        ):
            raise MigrationDrillError("migration_predecessor_validator_output_too_large")
        stdout_file.seek(0)
        stderr_file.seek(0)
        stdout = stdout_file.read()
        stderr = stderr_file.read()
    if exit_code != 0:
        raise MigrationDrillError("migration_predecessor_validator_rejected")
    if stderr:
        raise MigrationDrillError("migration_predecessor_validator_stderr")
    try:
        stdout_text = stdout.decode("utf-8")
        if len(stdout_text.splitlines()) != 1:
            raise ValueError("validator output must be one JSON line")
        result = json.loads(stdout_text)
    except (UnicodeDecodeError, ValueError, json.JSONDecodeError) as exc:
        raise MigrationDrillError("migration_predecessor_validator_result_invalid") from exc
    expected_result = {
        "schema_version": PREDECESSOR_VALIDATOR_RESULT_SCHEMA_VERSION,
        "ok": True,
        "read_only": True,
        "side_effects": "none",
        "database_sha256": artifact_before["sha256"],
        "roles": list(roles),
        "schemas": expected_schemas,
        "quick_check": "ok",
        "foreign_key_check_rows": 0,
        "write_probe": "blocked_readonly",
    }
    if result != expected_result:
        raise MigrationDrillError("migration_predecessor_validator_result_invalid")
    expected_stdout = (
        json.dumps(
            expected_result,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")
    if stdout != expected_stdout:
        raise MigrationDrillError("migration_predecessor_validator_result_invalid")
    bundle_after = _require_sidecar_free_sqlite_bundle(
        rollback_path,
        expected_database_present=True,
    )
    if bundle_after != bundle_before or observe_regular_file(rollback_path) != artifact_before:
        raise MigrationDrillError("migration_predecessor_validator_side_effect")
    if observe_regular_file(executable) != executable_before:
        raise MigrationDrillError("migration_predecessor_validator_changed")
    return {
        "schema_version": "pnc_rca_predecessor_validator_execution_v1",
        "observed_at": observed_at.isoformat(),
        "protocol": PREDECESSOR_COMPATIBILITY_PROBE,
        "artifact_sha256": executable_before["sha256"],
        "database_artifact_before": artifact_before,
        "database_bundle_before": bundle_before,
        "database_bundle_after": bundle_after,
        "runtime": {
            "child_pid": child_pid,
            "parent_pid": os.getpid(),
            "uid": os.getuid(),
            "gid": os.getgid(),
            "cwd": str(run_root),
            "platform": sys.platform,
            "machine": platform.machine(),
            "argv": argv,
            "argv_sha256": hashlib.sha256(
                json.dumps(argv, ensure_ascii=True, separators=(",", ":")).encode(
                    "utf-8"
                )
            ).hexdigest(),
            "environment_sha256": hashlib.sha256(
                json.dumps(
                    environment,
                    ensure_ascii=True,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8")
            ).hexdigest(),
        },
        "exit_code": exit_code,
        "duration_ms": duration_ms,
        "stdout_size_bytes": len(stdout),
        "stdout_sha256": hashlib.sha256(stdout).hexdigest(),
        "stderr_size_bytes": len(stderr),
        "stderr_sha256": hashlib.sha256(stderr).hexdigest(),
        "read_only_result": expected_result,
    }


def _expected_control_activation_structure(*, present: bool) -> dict[str, Any]:
    return {
        "tables": sorted(CONTROL_V9_ACTIVATION_TABLES) if present else [],
        "indexes": sorted(CONTROL_V9_ACTIVATION_INDEXES) if present else [],
        "columns": {
            table: sorted(columns) if present else []
            for table, columns in CONTROL_V9_ACTIVATION_COLUMNS.items()
        },
    }


def _expected_host_runtime_transition_structure(*, present: bool) -> dict[str, Any]:
    return {
        "present": present,
        "columns": sorted(HOST_RUNTIME_TRANSITION_COLUMNS) if present else [],
        "submission_index": (
            ["submission_key", "transition_id"] if present else []
        ),
    }


def _validate_predecessor_fixture_structure(
    validation: Mapping[str, Any],
    roles: Sequence[str],
) -> None:
    structure = validation.get("structure")
    if not isinstance(structure, Mapping):
        raise MigrationDrillError("migration_predecessor_structure_invalid")
    if "control" in roles:
        if set(structure.get("control_v8_indexes", ())) != CONTROL_V8_BASELINE_INDEXES:
            raise MigrationDrillError("migration_control_v8_indexes_missing")
        if structure.get("control_v9_activation") != (
            _expected_control_activation_structure(present=False)
        ):
            raise MigrationDrillError("migration_control_v8_activation_present")
    if (
        "delivery" in roles
        and structure.get("delivery_task_id_notnull") != 0
    ):
        raise MigrationDrillError("migration_delivery_v5_watch_invalid")
    expected_transition = _expected_host_runtime_transition_structure(
        present="control" in roles
    )
    if structure.get("host_runtime_transitions") != expected_transition:
        raise MigrationDrillError("migration_predecessor_runtime_transition_invalid")


def _validate_supported_pre_schema(
    validation: Mapping[str, Any],
    roles: Sequence[str],
) -> None:
    schemas = validation.get("schemas")
    if not isinstance(schemas, dict) or set(schemas) != set(roles):
        raise MigrationDrillError("migration_pre_schema_invalid")
    supported = {
        "control": {
            CONTROL_PREDECESSOR_SCHEMA_VERSION,
            CONTROL_STORE_SCHEMA_VERSION,
        },
        "delivery": {
            DELIVERY_PREDECESSOR_SCHEMA_VERSION,
            DELIVERY_STORE_SCHEMA_VERSION,
        },
    }
    if any(str(schemas[role]) not in supported[role] for role in roles):
        raise MigrationDrillError("migration_pre_schema_unsupported")


def _candidate_provenance(repo_root: Path) -> dict[str, Any]:
    try:
        root = repo_root.expanduser().resolve(strict=True)
        runtime_root = REPO_ROOT.resolve(strict=True)
    except OSError as exc:
        raise MigrationDrillError("migration_candidate_repo_invalid") from exc
    try:
        top_level = subprocess.run(
            ["git", "-C", str(root), "rev-parse", "--show-toplevel"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        commit = subprocess.run(
            ["git", "-C", str(root), "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        status = subprocess.run(
            ["git", "-C", str(root), "status", "--porcelain=v1", "--untracked-files=all"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout
    except (OSError, subprocess.SubprocessError) as exc:
        raise MigrationDrillError("migration_candidate_commit_unavailable") from exc
    if Path(top_level).resolve() != root:
        raise MigrationDrillError("migration_candidate_repo_mismatch")
    if re.fullmatch(r"[0-9a-f]{40}", commit) is None:
        raise MigrationDrillError("migration_candidate_commit_invalid")
    if status:
        raise MigrationDrillError("migration_candidate_source_dirty")
    sources: dict[str, str] = {}
    for relative in MIGRATION_SOURCE_RELATIVE_PATHS:
        path = root / relative
        observed = observe_regular_file(path)
        runtime_observed = observe_regular_file(runtime_root / relative)
        try:
            committed = subprocess.run(
                ["git", "-C", str(root), "show", f"{commit}:{relative}"],
                check=True,
                capture_output=True,
            ).stdout
        except (OSError, subprocess.SubprocessError) as exc:
            raise MigrationDrillError("migration_candidate_source_untracked") from exc
        if hashlib.sha256(committed).hexdigest() != observed["sha256"]:
            raise MigrationDrillError("migration_candidate_source_dirty")
        if observed["sha256"] != runtime_observed["sha256"]:
            raise MigrationDrillError("migration_candidate_runtime_source_mismatch")
        sources[relative] = str(observed["sha256"])
    return {
        "repo_root": str(root),
        "commit": commit,
        "migration_sources": sources,
    }


def validate_writer_stop_evidence(
    value: Mapping[str, Any],
    *,
    now: datetime,
    max_age_seconds: int,
) -> dict[str, Any]:
    basic_keys = {"schema_version", "observed_at", "services"}
    probed_keys = basic_keys | {
        "process_probe",
        "probe_started_at",
        "probe_completed_at",
    }
    value_keys = frozenset(value)
    if value_keys not in {frozenset(basic_keys), frozenset(probed_keys)}:
        raise MigrationDrillError("writer_stop_evidence_shape_invalid")
    has_process_probe = value_keys == frozenset(probed_keys)
    if value.get("schema_version") != WRITER_STOP_EVIDENCE_SCHEMA_VERSION:
        raise MigrationDrillError("writer_stop_evidence_schema_invalid")
    observed = _parse_timestamp(
        value.get("observed_at"), code="writer_stop_evidence_timestamp_invalid"
    )
    age = (now - observed).total_seconds()
    if age < 0 or age > max_age_seconds:
        raise MigrationDrillError("writer_stop_evidence_stale")
    services = value.get("services")
    if not isinstance(services, dict) or set(services) != STORE_WRITER_LABELS:
        raise MigrationDrillError("writer_stop_evidence_services_invalid")
    normalized_services: dict[str, Any] = {}
    for label in sorted(STORE_WRITER_LABELS):
        service = services.get(label)
        service_keys = {
            "observed_at",
            "pid_state",
            "health_state",
        }
        if has_process_probe:
            service_keys |= {
                "process_probe",
                "launchd_job_state",
                "matching_pids",
            }
        if not isinstance(service, dict) or set(service) != service_keys:
            raise MigrationDrillError("writer_stop_evidence_service_shape_invalid")
        service_observed = _parse_timestamp(
            service.get("observed_at"),
            code="writer_stop_evidence_service_timestamp_invalid",
        )
        service_age = (now - service_observed).total_seconds()
        if (
            service_age < 0
            or service_age > max_age_seconds
            or service.get("pid_state") != "pid_absent"
            or service.get("health_state") != "stopped"
        ):
            raise MigrationDrillError("writer_stop_evidence_service_not_stopped")
        normalized_services[label] = {
            "observed_at": service_observed.isoformat(),
            "pid_state": "pid_absent",
            "health_state": "stopped",
        }
        if has_process_probe:
            if (
                service.get("process_probe") != WRITER_PROCESS_PROBE
                or service.get("launchd_job_state") != "absent"
                or service.get("matching_pids") != []
            ):
                raise MigrationDrillError("writer_stop_evidence_service_not_stopped")
            normalized_services[label].update({
                "process_probe": WRITER_PROCESS_PROBE,
                "launchd_job_state": "absent",
                "matching_pids": [],
            })
    normalized = {
        "schema_version": WRITER_STOP_EVIDENCE_SCHEMA_VERSION,
        "observed_at": observed.isoformat(),
        "services": normalized_services,
    }
    if has_process_probe:
        probe_started_at = _parse_timestamp(
            value.get("probe_started_at"),
            code="writer_stop_evidence_probe_timestamp_invalid",
        )
        probe_completed_at = _parse_timestamp(
            value.get("probe_completed_at"),
            code="writer_stop_evidence_probe_timestamp_invalid",
        )
        if (
            value.get("process_probe") != WRITER_PROCESS_PROBE
            or probe_started_at > probe_completed_at
            or probe_completed_at > now
        ):
            raise MigrationDrillError("writer_stop_evidence_probe_invalid")
        normalized.update({
            "process_probe": WRITER_PROCESS_PROBE,
            "probe_started_at": probe_started_at.isoformat(),
            "probe_completed_at": probe_completed_at.isoformat(),
        })
    return normalized


def _default_writer_process_probe() -> dict[str, dict[str, Any]]:
    matches: dict[str, set[int]] = {label: set() for label in STORE_WRITER_LABELS}
    launchd_states: dict[str, str] = {}
    for label in sorted(STORE_WRITER_LABELS):
        try:
            result = subprocess.run(
                ["launchctl", "print", f"gui/{os.getuid()}/{label}"],
                check=False,
                capture_output=True,
                text=True,
                timeout=5,
            )
        except (OSError, subprocess.SubprocessError) as exc:
            raise MigrationDrillError("writer_stop_process_probe_failed") from exc
        if result.returncode == 0:
            launchd_states[label] = "present"
            for raw_pid in re.findall(r"(?m)^\s*pid\s*=\s*([0-9]+)\s*$", result.stdout):
                matches[label].add(int(raw_pid))
        else:
            launchctl_detail = f"{result.stdout}\n{result.stderr}".lower()
            if (
                "not find service" not in launchctl_detail
                and "not found" not in launchctl_detail
            ):
                raise MigrationDrillError("writer_stop_process_probe_failed")
            launchd_states[label] = "absent"
    try:
        processes = psutil.process_iter(["pid", "cmdline"])
        for process in processes:
            cmdline = [str(item) for item in (process.info.get("cmdline") or [])]
            names = {Path(item).name for item in cmdline}
            for label, script_name in RCA_SERVICE_SCRIPT_NAMES.items():
                if script_name in names:
                    matches[label].add(int(process.info["pid"]))
            if _is_gateway_writer_cmdline(cmdline):
                matches[GATEWAY_WRITER_LABEL].add(int(process.info["pid"]))
    except (psutil.Error, OSError) as exc:
        raise MigrationDrillError("writer_stop_process_probe_failed") from exc
    return {
        label: {
            "launchd_job_state": launchd_states[label],
            "matching_pids": sorted(matches[label]),
        }
        for label in sorted(STORE_WRITER_LABELS)
    }


def _is_gateway_writer_cmdline(cmdline: Sequence[str]) -> bool:
    """Match supported Gateway argv forms without scanning shell script text."""
    normalized = [str(item).replace("\\", "/") for item in cmdline]
    for index, item in enumerate(normalized):
        following = normalized[index + 1 :]
        if (
            item == "-m"
            and len(following) >= 2
            and following[0] == "hermes_cli.main"
            and following[1] == "gateway"
        ):
            return True
        if (
            (item == "hermes_cli/main.py" or item.endswith("/hermes_cli/main.py"))
            and following
            and following[0] == "gateway"
        ):
            return True
        if Path(item).name == "hermes" and following and following[0] == "gateway":
            return True
        if item.endswith("/gateway/run.py") or item == "gateway/run.py":
            return True
    return False


def validate_writer_process_probe_result(
    result: Mapping[str, Any],
) -> dict[str, dict[str, Any]]:
    if not isinstance(result, Mapping) or set(result) != STORE_WRITER_LABELS:
        raise MigrationDrillError("writer_stop_process_probe_invalid")
    normalized: dict[str, dict[str, Any]] = {}
    for label in sorted(STORE_WRITER_LABELS):
        service = result.get(label)
        if not isinstance(service, Mapping) or set(service) != {
            "launchd_job_state",
            "matching_pids",
        }:
            raise MigrationDrillError("writer_stop_process_probe_invalid")
        pids = service.get("matching_pids")
        if (
            service.get("launchd_job_state") not in {"absent", "present"}
            or not isinstance(pids, Sequence)
            or isinstance(pids, (str, bytes))
            or any(
                isinstance(pid, bool) or not isinstance(pid, int) or pid < 1
                for pid in pids
            )
        ):
            raise MigrationDrillError("writer_stop_process_probe_invalid")
        normalized[label] = {
            "launchd_job_state": str(service["launchd_job_state"]),
            "matching_pids": sorted(set(pids)),
        }
    if any(
        service["launchd_job_state"] != "absent" or service["matching_pids"]
        for service in normalized.values()
    ):
        raise MigrationDrillError("writer_stop_process_still_running")
    return normalized


def _checked_writer_process_probe(
    probe: Any,
) -> dict[str, dict[str, Any]]:
    try:
        result = probe()
    except MigrationDrillError:
        raise
    except Exception as exc:
        raise MigrationDrillError("writer_stop_process_probe_failed") from exc
    return validate_writer_process_probe_result(result)


def collect_writer_stop_evidence(
    *,
    now: datetime | None = None,
    writer_process_probe: Any | None = None,
) -> dict[str, Any]:
    """Observe all resident writers as absent without changing launchd state."""
    probe_started_at = _utc(now)
    services = _checked_writer_process_probe(
        writer_process_probe or _default_writer_process_probe
    )
    probe_completed_at = _utc(now)
    evidence = {
        "schema_version": WRITER_STOP_EVIDENCE_SCHEMA_VERSION,
        "observed_at": probe_completed_at.isoformat(),
        "process_probe": WRITER_PROCESS_PROBE,
        "probe_started_at": probe_started_at.isoformat(),
        "probe_completed_at": probe_completed_at.isoformat(),
        "services": {
            label: {
                "observed_at": probe_completed_at.isoformat(),
                "pid_state": "pid_absent",
                "health_state": "stopped",
                "process_probe": WRITER_PROCESS_PROBE,
                **services[label],
            }
            for label in sorted(STORE_WRITER_LABELS)
        },
    }
    return validate_writer_stop_evidence(
        evidence,
        now=probe_completed_at,
        max_age_seconds=1,
    )


def _load_writer_stop_evidence(path: Path) -> dict[str, Any]:
    try:
        value, raw, _observation = _read_json_regular_file(
            path,
            max_bytes=MAX_WRITER_STOP_EVIDENCE_BYTES,
        )
    except MigrationDrillError as exc:
        raise MigrationDrillError("writer_stop_evidence_unreadable") from exc
    if not raw:
        raise MigrationDrillError("writer_stop_evidence_invalid")
    return value


def write_receipt_atomic(path: Path, receipt: Mapping[str, Any]) -> None:
    try:
        _write_json_no_clobber(path, receipt)
    except MigrationDrillError as exc:
        if exc.code == "fresh_install_evidence_conflict":
            raise MigrationDrillError("migration_receipt_conflict") from exc
        raise


def _read_json_regular_file(
    path: str | Path,
    *,
    max_bytes: int = 2 * 1024 * 1024,
) -> tuple[dict[str, Any], bytes, dict[str, Any]]:
    candidate = _absolute(path).absolute()
    _secure_directory_observation(
        candidate.parent,
        code="migration_evidence_directory_invalid",
    )
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = -1
    try:
        before = candidate.lstat()
        descriptor = os.open(candidate, flags)
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISREG(opened.st_mode)
            or stat.S_ISLNK(before.st_mode)
            or opened.st_uid != os.getuid()
            or stat.S_IMODE(opened.st_mode) != 0o600
            or opened.st_nlink != 1
            or opened.st_size < 2
            or opened.st_size > max_bytes
        ):
            raise MigrationDrillError("migration_evidence_file_invalid")
        remaining = opened.st_size
        chunks: list[bytes] = []
        while remaining:
            chunk = os.read(descriptor, min(64 * 1024, remaining))
            if not chunk:
                raise MigrationDrillError("migration_evidence_file_invalid")
            chunks.append(chunk)
            remaining -= len(chunk)
        if os.read(descriptor, 1):
            raise MigrationDrillError("migration_evidence_file_invalid")
        raw = b"".join(chunks)
        after = candidate.lstat()
    except MigrationDrillError:
        raise
    except OSError as exc:
        raise MigrationDrillError("migration_evidence_file_invalid") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    identity = (
        opened.st_dev,
        opened.st_ino,
        opened.st_mode,
        opened.st_uid,
        opened.st_nlink,
        opened.st_size,
        opened.st_mtime_ns,
        opened.st_ctime_ns,
    )
    if (
        len(raw) != opened.st_size
        or identity
        != (
            before.st_dev,
            before.st_ino,
            before.st_mode,
            before.st_uid,
            before.st_nlink,
            before.st_size,
            before.st_mtime_ns,
            before.st_ctime_ns,
        )
        or identity
        != (
            after.st_dev,
            after.st_ino,
            after.st_mode,
            after.st_uid,
            after.st_nlink,
            after.st_size,
            after.st_mtime_ns,
            after.st_ctime_ns,
        )
    ):
        raise MigrationDrillError("migration_evidence_file_changed")
    try:
        body = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MigrationDrillError("migration_evidence_file_invalid") from exc
    if not isinstance(body, dict):
        raise MigrationDrillError("migration_evidence_file_invalid")
    observation = {
        "path": str(candidate.resolve(strict=True)),
        "device": int(opened.st_dev),
        "inode": int(opened.st_ino),
        "size_bytes": int(opened.st_size),
        "mtime_ns": int(opened.st_mtime_ns),
        "sha256": hashlib.sha256(raw).hexdigest(),
    }
    return body, raw, observation


def _json_payload(value: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


@contextmanager
def _json_no_clobber_lock(destination: Path):
    lock_path = destination.parent / f".{destination.name}.publication.lock"
    descriptor = -1
    created = False
    flags = (
        os.O_RDWR
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        try:
            descriptor = os.open(
                lock_path,
                flags | os.O_CREAT | os.O_EXCL,
                0o600,
            )
            created = True
        except FileExistsError:
            descriptor = os.open(lock_path, flags)
        opened = os.fstat(descriptor)
        current = os.lstat(lock_path)
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_uid != os.getuid()
            or opened.st_nlink != 1
            or stat.S_IMODE(opened.st_mode) != 0o600
            or (opened.st_dev, opened.st_ino) != (current.st_dev, current.st_ino)
        ):
            raise MigrationDrillError("fresh_install_evidence_lock_invalid")
        if created:
            _fsync_directory(destination.parent)
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    except MigrationDrillError:
        raise
    except OSError as exc:
        raise MigrationDrillError("fresh_install_evidence_lock_failed") from exc
    finally:
        if descriptor >= 0:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
            except OSError:
                pass
            os.close(descriptor)


def _read_exact_no_clobber_temporary(
    temporary: Path,
    *,
    payload: bytes,
    expected_nlink: int,
) -> os.stat_result:
    descriptor = -1
    try:
        before = os.lstat(temporary)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != os.getuid()
            or stat.S_IMODE(before.st_mode) != 0o600
            or before.st_nlink != expected_nlink
            or before.st_size != len(payload)
        ):
            raise MigrationDrillError("fresh_install_evidence_conflict")
        descriptor = os.open(
            temporary,
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0),
        )
        opened = os.fstat(descriptor)
        if (
            opened.st_dev,
            opened.st_ino,
            opened.st_mode,
            opened.st_uid,
            opened.st_nlink,
            opened.st_size,
            opened.st_mtime_ns,
            opened.st_ctime_ns,
        ) != (
            before.st_dev,
            before.st_ino,
            before.st_mode,
            before.st_uid,
            before.st_nlink,
            before.st_size,
            before.st_mtime_ns,
            before.st_ctime_ns,
        ):
            raise MigrationDrillError("fresh_install_evidence_conflict")
        recovered = b""
        while len(recovered) < len(payload):
            chunk = os.read(descriptor, len(payload) - len(recovered))
            if not chunk:
                break
            recovered += chunk
        if recovered != payload or os.read(descriptor, 1):
            raise MigrationDrillError("fresh_install_evidence_conflict")
        after = os.lstat(temporary)
        if (
            after.st_dev,
            after.st_ino,
            after.st_mode,
            after.st_uid,
            after.st_nlink,
            after.st_size,
            after.st_mtime_ns,
            after.st_ctime_ns,
        ) != (
            opened.st_dev,
            opened.st_ino,
            opened.st_mode,
            opened.st_uid,
            opened.st_nlink,
            opened.st_size,
            opened.st_mtime_ns,
            opened.st_ctime_ns,
        ):
            raise MigrationDrillError("fresh_install_evidence_conflict")
        return after
    except MigrationDrillError:
        raise
    except OSError as exc:
        raise MigrationDrillError("fresh_install_evidence_conflict") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _require_exact_no_clobber_destination(
    destination: Path,
    *,
    payload: bytes,
) -> os.stat_result:
    try:
        info = os.lstat(destination)
    except OSError as exc:
        raise MigrationDrillError("fresh_install_evidence_conflict") from exc
    if (
        not stat.S_ISREG(info.st_mode)
        or info.st_uid != os.getuid()
        or stat.S_IMODE(info.st_mode) != 0o600
        or info.st_nlink != 1
        or info.st_size != len(payload)
    ):
        raise MigrationDrillError("fresh_install_evidence_conflict")
    try:
        _body, raw, _observation = _read_json_regular_file(destination)
    except MigrationDrillError as exc:
        raise MigrationDrillError("fresh_install_evidence_conflict") from exc
    if raw != payload:
        raise MigrationDrillError("fresh_install_evidence_conflict")
    return info


def _recover_interrupted_json_no_clobber_link(
    destination: Path,
    *,
    payload: bytes,
) -> None:
    pattern = f".{destination.name}.*.no-clobber.tmp"
    try:
        candidates = list(destination.parent.glob(pattern))
        destination_info = os.lstat(destination)
    except FileNotFoundError:
        destination_info = None
    except OSError as exc:
        raise MigrationDrillError("fresh_install_evidence_conflict") from exc
    if len(candidates) > 1:
        raise MigrationDrillError("fresh_install_evidence_conflict")

    if destination_info is None:
        if not candidates:
            return
        temporary = candidates[0]
        _read_exact_no_clobber_temporary(
            temporary,
            payload=payload,
            expected_nlink=1,
        )
        try:
            os.link(temporary, destination, follow_symlinks=False)
            _fsync_directory(destination.parent)
        except OSError as exc:
            raise MigrationDrillError("fresh_install_evidence_conflict") from exc
        destination_info = os.lstat(destination)

    if destination_info.st_nlink == 1:
        _require_exact_no_clobber_destination(destination, payload=payload)
        if candidates:
            temporary = candidates[0]
            _read_exact_no_clobber_temporary(
                temporary,
                payload=payload,
                expected_nlink=1,
            )
            try:
                temporary.unlink()
                _fsync_directory(destination.parent)
            except OSError as exc:
                raise MigrationDrillError(
                    "fresh_install_evidence_conflict"
                ) from exc
        return
    if (
        not stat.S_ISREG(destination_info.st_mode)
        or destination_info.st_uid != os.getuid()
        or stat.S_IMODE(destination_info.st_mode) != 0o600
        or destination_info.st_nlink != 2
        or destination_info.st_size != len(payload)
        or len(candidates) != 1
    ):
        raise MigrationDrillError("fresh_install_evidence_conflict")

    temporary = candidates[0]
    temporary_info = _read_exact_no_clobber_temporary(
        temporary,
        payload=payload,
        expected_nlink=2,
    )
    if (temporary_info.st_dev, temporary_info.st_ino) != (
        destination_info.st_dev,
        destination_info.st_ino,
    ):
        raise MigrationDrillError("fresh_install_evidence_conflict")
    try:
        temporary.unlink()
        _fsync_directory(destination.parent)
    except OSError as exc:
        raise MigrationDrillError("fresh_install_evidence_conflict") from exc
    _require_exact_no_clobber_destination(destination, payload=payload)


def _write_json_no_clobber(path: Path, value: Mapping[str, Any]) -> dict[str, Any]:
    destination = _absolute(path).absolute()
    parent, _parent_observation = _secure_directory_observation(
        destination.parent,
        code="fresh_install_evidence_directory_invalid",
    )
    payload = _json_payload(value)
    with _json_no_clobber_lock(destination):
        _recover_interrupted_json_no_clobber_link(
            destination,
            payload=payload,
        )
        temporary = ""
        try:
            with tempfile.NamedTemporaryFile(
                mode="wb",
                dir=parent,
                prefix=f".{destination.name}.",
                suffix=".no-clobber.tmp",
                delete=False,
            ) as handle:
                temporary = handle.name
                os.fchmod(handle.fileno(), 0o600)
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            try:
                os.link(temporary, destination, follow_symlinks=False)
            except FileExistsError:
                try:
                    existing, raw, observation = _read_json_regular_file(destination)
                except MigrationDrillError as exc:
                    raise MigrationDrillError(
                        "fresh_install_evidence_conflict"
                    ) from exc
                if raw != payload or existing != value:
                    raise MigrationDrillError("fresh_install_evidence_conflict")
                return observation
            Path(temporary).unlink()
            temporary = ""
            _fsync_directory(parent)
            written = destination.lstat()
            if (
                not stat.S_ISREG(written.st_mode)
                or written.st_uid != os.getuid()
                or stat.S_IMODE(written.st_mode) != 0o600
                or written.st_nlink != 1
                or written.st_size != len(payload)
            ):
                raise MigrationDrillError("fresh_install_evidence_write_failed")
        except MigrationDrillError:
            raise
        except OSError as exc:
            raise MigrationDrillError("fresh_install_evidence_write_failed") from exc
        finally:
            if temporary:
                try:
                    Path(temporary).unlink(missing_ok=True)
                    _fsync_directory(parent)
                except (OSError, MigrationDrillError):
                    pass
        return observe_regular_file(destination)


def _fresh_destination_state(path: Path) -> dict[str, Any]:
    return {
        name: {
            "path": str(candidate),
            "present": candidate.exists() or candidate.is_symlink(),
            "identity": (
                observe_regular_file(candidate)
                if candidate.exists() or candidate.is_symlink()
                else None
            ),
        }
        for name, candidate in (
            ("database", path),
            ("wal", Path(f"{path}-wal")),
            ("shm", Path(f"{path}-shm")),
            ("journal", Path(f"{path}-journal")),
            ("tombstone", Path(f"{path}.pnc-rca-tombstone")),
        )
    }


def _require_fresh_destination_absent(path: Path) -> dict[str, Any]:
    state = _fresh_destination_state(path)
    if any(item["present"] for item in state.values()):
        raise MigrationDrillError("fresh_install_destination_not_absent")
    return state


def _read_genesis_meta(path: Path) -> dict[str, str]:
    uri = path.as_uri() + "?mode=ro"
    try:
        connection = sqlite3.connect(uri, uri=True, timeout=5)
        connection.execute("PRAGMA query_only=ON")
        values: dict[str, str] = {}
        for table, prefix in (
            ("control_meta", "control"),
            ("rca_delivery_meta", "delivery"),
        ):
            rows = connection.execute(
                f"SELECT key, value FROM {table} WHERE key IN "
                "('fresh_install_db_instance_id', "
                "'fresh_install_genesis_intent_sha256', "
                "'fresh_install_origin_commit')"
            ).fetchall()
            if len(rows) != 3:
                raise MigrationDrillError("fresh_install_genesis_meta_missing")
            values.update({f"{prefix}.{key}": str(value) for key, value in rows})
    except MigrationDrillError:
        raise
    except sqlite3.Error as exc:
        raise MigrationDrillError("fresh_install_genesis_meta_invalid") from exc
    finally:
        if "connection" in locals():
            connection.close()
    return values


def _fresh_install_audit_hashes(
    *,
    release_id: str | None,
    operator: str | None,
    reason: str | None,
    required: bool,
) -> dict[str, str] | None:
    values = {
        "release_id": release_id,
        "operator": operator,
        "reason": reason,
    }
    if not required and all(value is None for value in values.values()):
        return None
    normalized: dict[str, str] = {}
    for name, value in values.items():
        text = str(value or "").strip()
        limit = 512 if name == "reason" else 128
        if (
            not text
            or len(text) > limit
            or any(ord(character) < 32 or ord(character) == 127 for character in text)
        ):
            raise MigrationDrillError(f"fresh_install_{name}_invalid")
        normalized[f"{name}_sha256"] = hashlib.sha256(
            text.encode("utf-8")
        ).hexdigest()
    return normalized


def _fresh_install_runtime_identity() -> dict[str, Any]:
    try:
        process = psutil.Process(os.getpid())
        executable = Path(sys.executable).expanduser().resolve(strict=True)
        executable_sha256 = str(observe_regular_file(executable)["sha256"])
        process_create_time = float(process.create_time())
    except (OSError, psutil.Error, MigrationDrillError) as exc:
        raise MigrationDrillError("fresh_install_runtime_identity_invalid") from exc
    argv = [str(value) for value in sys.argv]
    return {
        "schema_version": FRESH_INSTALL_RUNTIME_IDENTITY_SCHEMA_VERSION,
        "uid": os.getuid(),
        "gid": os.getgid(),
        "pid": os.getpid(),
        "process_create_time": process_create_time,
        "executable_path": str(executable),
        "executable_sha256": executable_sha256,
        "argv_sha256": hashlib.sha256(
            json.dumps(
                argv,
                ensure_ascii=True,
                separators=(",", ":"),
                allow_nan=False,
            ).encode("utf-8")
        ).hexdigest(),
    }


def _validate_runtime_identity_record(
    value: Any,
    *,
    code: str,
) -> dict[str, Any]:
    expected_keys = {
        "schema_version",
        "uid",
        "gid",
        "pid",
        "process_create_time",
        "executable_path",
        "executable_sha256",
        "argv_sha256",
    }
    if (
        not isinstance(value, Mapping)
        or set(value) != expected_keys
        or value.get("schema_version")
        != FRESH_INSTALL_RUNTIME_IDENTITY_SCHEMA_VERSION
        or value.get("uid") != os.getuid()
        or value.get("gid") != os.getgid()
        or not isinstance(value.get("pid"), int)
        or value["pid"] <= 0
        or not isinstance(value.get("process_create_time"), (int, float))
        or value["process_create_time"] <= 0
        or not Path(str(value.get("executable_path") or "")).is_absolute()
    ):
        raise MigrationDrillError(code)
    executable_sha256 = _require_sha256(value.get("executable_sha256"), code=code)
    _require_sha256(value.get("argv_sha256"), code=code)
    try:
        if observe_regular_file(Path(str(value["executable_path"])))["sha256"] != (
            executable_sha256
        ):
            raise MigrationDrillError(code)
    except (MigrationDrillError, OSError) as exc:
        raise MigrationDrillError(code) from exc
    return dict(value)


def _fresh_install_journal_path(
    evidence_root: Path,
    journal_id: str,
    phase: str,
) -> Path:
    if re.fullmatch(r"[0-9a-f]{64}", journal_id) is None or phase not in {
        "prepared",
        "installed",
        "receipted",
    }:
        raise MigrationDrillError("fresh_install_journal_identity_invalid")
    return evidence_root / f"fresh_install_materialization_journal.{journal_id}.{phase}.json"


def _remove_exact_json_file(
    path: Path,
    *,
    expected: Mapping[str, Any],
) -> None:
    _body, _raw, observation = _read_json_regular_file(path)
    if _body != expected:
        raise MigrationDrillError("fresh_install_maintenance_changed")
    try:
        before = path.lstat()
        if (before.st_dev, before.st_ino) != (
            observation["device"],
            observation["inode"],
        ):
            raise MigrationDrillError("fresh_install_maintenance_changed")
        path.unlink()
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except MigrationDrillError:
        raise
    except OSError as exc:
        raise MigrationDrillError("fresh_install_maintenance_release_failed") from exc


_FILE_OBSERVATION_KEYS = {
    "path",
    "device",
    "inode",
    "size_bytes",
    "mtime_ns",
    "sha256",
}


def _require_sha256(value: Any, *, code: str) -> str:
    normalized = str(value or "")
    if re.fullmatch(r"[0-9a-f]{64}", normalized) is None:
        raise MigrationDrillError(code)
    return normalized


def _artifact_identity_without_path(value: Mapping[str, Any]) -> dict[str, Any]:
    if set(value) != _FILE_OBSERVATION_KEYS:
        raise MigrationDrillError("fresh_install_artifact_identity_invalid")
    return {key: value[key] for key in sorted(_FILE_OBSERVATION_KEYS - {"path"})}


def _require_exact_artifact(
    path: Path,
    expected: Mapping[str, Any],
    *,
    allow_path_mismatch: bool = False,
    allowed_link_counts: frozenset[int] | None = None,
    code: str,
) -> dict[str, Any]:
    try:
        observed = observe_regular_file(path)
        metadata = path.lstat()
    except (MigrationDrillError, OSError) as exc:
        raise MigrationDrillError(code) from exc
    matches = (
        _artifact_identity_without_path(observed)
        == _artifact_identity_without_path(expected)
        if allow_path_mismatch
        else observed == expected
    )
    if (
        not matches
        or metadata.st_uid != os.getuid()
        or metadata.st_nlink < 1
        or (
            allowed_link_counts is not None
            and metadata.st_nlink not in allowed_link_counts
        )
        or stat.S_IMODE(metadata.st_mode) & 0o022
    ):
        raise MigrationDrillError(code)
    return observed


def _require_content_equivalent_artifact(
    path: Path,
    expected: Mapping[str, Any],
    *,
    require_single_link: bool,
    code: str,
) -> dict[str, Any]:
    try:
        observed = observe_regular_file(path)
        metadata = path.lstat()
    except (MigrationDrillError, OSError) as exc:
        raise MigrationDrillError(code) from exc
    if (
        observed.get("sha256") != expected.get("sha256")
        or observed.get("size_bytes") != expected.get("size_bytes")
        or metadata.st_uid != os.getuid()
        or (require_single_link and metadata.st_nlink != 1)
        or stat.S_IMODE(metadata.st_mode) & 0o022
    ):
        raise MigrationDrillError(code)
    return observed


def _require_no_sqlite_sidecars(
    path: Path,
    *,
    tombstone_present: bool,
    maintenance_present: bool,
    code: str,
) -> dict[str, Any]:
    state = _fresh_destination_state(path)
    for name in ("wal", "shm", "journal"):
        if state[name]["present"]:
            raise MigrationDrillError(code)
    if state["tombstone"]["present"] is not tombstone_present:
        raise MigrationDrillError(code)
    maintenance = Path(f"{path}.pnc-rca-maintenance")
    observed_maintenance = maintenance.exists() or maintenance.is_symlink()
    if observed_maintenance is not maintenance_present:
        raise MigrationDrillError(code)
    return state


def _validate_recorded_process_probes(
    value: Any,
    *,
    expected_names: set[str],
    code: str,
) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != expected_names:
        raise MigrationDrillError(code)
    normalized: dict[str, Any] = {}
    for name in sorted(expected_names):
        try:
            normalized[name] = validate_writer_process_probe_result(value[name])
        except MigrationDrillError as exc:
            raise MigrationDrillError(code) from exc
    return normalized


def _capacity_identity(
    *,
    release_id: str | None,
    bootstrap_epoch_id: str | None,
    required: bool,
) -> dict[str, str] | None:
    release = str(release_id or "").strip()
    epoch = str(bootstrap_epoch_id or "").strip()
    if not release and not epoch and not required:
        return None
    if CAPACITY_IDENTITY_RE.fullmatch(release) is None:
        raise MigrationDrillError("capacity_transition_release_id_invalid")
    if CAPACITY_IDENTITY_RE.fullmatch(epoch) is None:
        raise MigrationDrillError("capacity_transition_bootstrap_epoch_id_invalid")
    return {"release_id": release, "bootstrap_epoch_id": epoch}


def _capacity_transition_snapshot(
    database_path: str | Path,
    *,
    expected_release_id: str,
    expected_bootstrap_epoch_id: str,
) -> dict[str, Any]:
    """Read and prove the immutable capacity ratchet without mutating SQLite."""
    path = _canonical_configured_path(database_path)
    observe_regular_file(path)
    try:
        connection = sqlite3.connect(
            f"{path.resolve(strict=True).as_uri()}?mode=ro",
            uri=True,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only=ON")
        connection.execute("BEGIN")
        states = [
            dict(row)
            for row in connection.execute(
                "SELECT * FROM rca_capacity_transition_state"
            ).fetchall()
        ]
        audits = [
            dict(row)
            for row in connection.execute(
                "SELECT * FROM rca_capacity_transition_audit "
                "ORDER BY to_generation, audit_id"
            ).fetchall()
        ]
        connection.commit()
    except (OSError, sqlite3.Error) as exc:
        raise MigrationDrillError("capacity_transition_latch_unreadable") from exc
    finally:
        if "connection" in locals():
            connection.close()
    if len(states) != 1 or not audits:
        raise MigrationDrillError("capacity_transition_latch_missing")
    state = states[0]
    if set(state) != CAPACITY_STATE_ROW_FIELDS or any(
        set(row) != CAPACITY_AUDIT_ROW_FIELDS for row in audits
    ):
        raise MigrationDrillError("capacity_transition_latch_shape_invalid")
    if (
        state.get("singleton_id") != 1
        or state.get("release_id") != expected_release_id
        or state.get("bootstrap_epoch_id") != expected_bootstrap_epoch_id
        or CAPACITY_IDENTITY_RE.fullmatch(str(state.get("release_id") or "")) is None
        or CAPACITY_IDENTITY_RE.fullmatch(
            str(state.get("bootstrap_epoch_id") or "")
        )
        is None
    ):
        raise MigrationDrillError("capacity_transition_latch_identity_mismatch")
    durable_state = str(state.get("state") or "")
    expected_generation = {
        "BOOTSTRAP_PRODUCTION": 1,
        "STEADY_ACTIVE": 2,
    }.get(durable_state)
    if expected_generation is None or state.get("generation") != expected_generation:
        raise MigrationDrillError("capacity_transition_latch_state_invalid")
    if len(audits) != expected_generation:
        raise MigrationDrillError("capacity_transition_audit_chain_invalid")

    def strict_capacity_time(value: Any) -> datetime:
        parsed = _parse_timestamp(
            value,
            code="capacity_transition_timestamp_invalid",
        )
        if value != parsed.isoformat():
            raise MigrationDrillError("capacity_transition_timestamp_invalid")
        return parsed

    initialized_at = strict_capacity_time(state.get("bootstrap_initialized_at"))
    updated_at = strict_capacity_time(state.get("updated_at"))
    first = audits[0]
    if (
        first.get("release_id") != expected_release_id
        or first.get("bootstrap_epoch_id") != expected_bootstrap_epoch_id
        or first.get("from_state") != "UNCONFIGURED"
        or first.get("to_state") != "BOOTSTRAP_PRODUCTION"
        or first.get("from_generation") != 0
        or first.get("to_generation") != 1
        or first.get("transitioned_at") != state.get("bootstrap_initialized_at")
        or any(
            first.get(field) is not None
            for field in CAPACITY_EVIDENCE_FIELDS + CAPACITY_EVIDENCE_TIME_FIELDS
        )
    ):
        raise MigrationDrillError("capacity_transition_audit_chain_invalid")
    if durable_state == "BOOTSTRAP_PRODUCTION":
        if (
            initialized_at != updated_at
            or strict_capacity_time(first.get("transitioned_at")) != initialized_at
            or state.get("updated_at") != state.get("bootstrap_initialized_at")
            or state.get("steady_activated_at") is not None
            or any(
                state.get(field) is not None
                for field in CAPACITY_EVIDENCE_FIELDS
                + CAPACITY_EVIDENCE_TIME_FIELDS
            )
        ):
            raise MigrationDrillError("capacity_transition_latch_state_invalid")
    else:
        second = audits[1]
        for field in CAPACITY_EVIDENCE_FIELDS:
            _require_sha256(
                state.get(field),
                code="capacity_transition_evidence_invalid",
            )
        issued_at = strict_capacity_time(state.get("authorization_issued_at"))
        expires_at = strict_capacity_time(state.get("authorization_expires_at"))
        receipt_created_at = strict_capacity_time(state.get("receipt_created_at"))
        marker_committed_at = strict_capacity_time(state.get("marker_committed_at"))
        steady_activated_at = strict_capacity_time(state.get("steady_activated_at"))
        if not (
            initialized_at
            <= issued_at
            <= receipt_created_at
            <= marker_committed_at
            <= expires_at
            and marker_committed_at <= steady_activated_at == updated_at
        ):
            raise MigrationDrillError("capacity_transition_timestamp_invalid")
        if (
            second.get("release_id") != expected_release_id
            or second.get("bootstrap_epoch_id") != expected_bootstrap_epoch_id
            or second.get("from_state") != "BOOTSTRAP_PRODUCTION"
            or second.get("to_state") != "STEADY_ACTIVE"
            or second.get("from_generation") != 1
            or second.get("to_generation") != 2
            or second.get("transitioned_at") != state.get("steady_activated_at")
            or strict_capacity_time(second.get("transitioned_at"))
            != steady_activated_at
            or state.get("updated_at") != state.get("steady_activated_at")
            or any(
                not isinstance(state.get(field), str) or not state.get(field)
                for field in CAPACITY_EVIDENCE_FIELDS
                + CAPACITY_EVIDENCE_TIME_FIELDS
                + ("steady_activated_at",)
            )
            or any(
                second.get(field) != state.get(field)
                for field in CAPACITY_EVIDENCE_FIELDS + CAPACITY_EVIDENCE_TIME_FIELDS
            )
        ):
            raise MigrationDrillError("capacity_transition_audit_chain_invalid")
    material = {
        "schema_version": CAPACITY_LATCH_SNAPSHOT_SCHEMA_VERSION,
        "release_id": expected_release_id,
        "bootstrap_epoch_id": expected_bootstrap_epoch_id,
        "state": durable_state,
        "generation": expected_generation,
        "state_row": state,
        "audit_chain": audits,
    }
    return {
        **material,
        "audit_chain_sha256": hashlib.sha256(
            _json_payload({"state_row": state, "audit_chain": audits})
        ).hexdigest(),
    }


def _existing_capacity_identity(database_path: Path) -> dict[str, str] | None:
    try:
        connection = sqlite3.connect(
            f"{database_path.resolve(strict=True).as_uri()}?mode=ro",
            uri=True,
        )
        row = connection.execute(
            "SELECT release_id, bootstrap_epoch_id, state "
            "FROM rca_capacity_transition_state WHERE singleton_id = 1"
        ).fetchone()
    except sqlite3.OperationalError as exc:
        if "no such table" in str(exc).lower():
            return None
        raise MigrationDrillError("capacity_transition_latch_unreadable") from exc
    except (OSError, sqlite3.Error) as exc:
        raise MigrationDrillError("capacity_transition_latch_unreadable") from exc
    finally:
        if "connection" in locals():
            connection.close()
    if row is None:
        return None
    return {
        "release_id": str(row[0]),
        "bootstrap_epoch_id": str(row[1]),
        "state": str(row[2]),
    }


def _capacity_initialization_binding(
    *,
    operation: str,
    configured_databases: Mapping[str, Any],
    migration_receipt: Mapping[str, Any],
    migration_receipt_raw_sha256: str,
    capacity_identity: Mapping[str, str],
    capacity_transition: Mapping[str, Any],
    audit: Mapping[str, Any],
    writer_stop_evidence: Mapping[str, Any],
    process_probes: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "operation": operation,
        "configured_databases": dict(configured_databases),
        "migration_receipt": dict(migration_receipt),
        "migration_receipt_raw_sha256": migration_receipt_raw_sha256,
        "capacity_identity": dict(capacity_identity),
        "capacity_transition": dict(capacity_transition),
        "audit": dict(audit),
        "writer_stop_evidence": dict(writer_stop_evidence),
        "writer_stop_evidence_sha256": hashlib.sha256(
            _json_payload(writer_stop_evidence)
        ).hexdigest(),
        "process_probes": dict(process_probes),
        "process_probes_sha256": hashlib.sha256(
            _json_payload(process_probes)
        ).hexdigest(),
    }


def _write_capacity_initialization_receipt(
    *,
    receipt_path: Path,
    operation: str,
    configured_databases: Mapping[str, Any],
    migration_receipt: Mapping[str, Any],
    migration_receipt_raw_sha256: str,
    capacity_identity: Mapping[str, str],
    capacity_transition: Mapping[str, Any],
    audit: Mapping[str, Any],
    writer_stop_evidence: Mapping[str, Any],
    process_probes: Mapping[str, Any],
    observed_at: datetime,
) -> tuple[dict[str, Any], dict[str, Any]]:
    binding = _capacity_initialization_binding(
        operation=operation,
        configured_databases=configured_databases,
        migration_receipt=migration_receipt,
        migration_receipt_raw_sha256=migration_receipt_raw_sha256,
        capacity_identity=capacity_identity,
        capacity_transition=capacity_transition,
        audit=audit,
        writer_stop_evidence=writer_stop_evidence,
        process_probes=process_probes,
    )
    receipt = {
        "schema_version": CAPACITY_INITIALIZATION_RECEIPT_SCHEMA_VERSION,
        "observed_at": observed_at.isoformat(),
        "ok": True,
        **binding,
        "initialization_binding_sha256": hashlib.sha256(
            _json_payload(binding)
        ).hexdigest(),
    }
    observation = _write_json_no_clobber(receipt_path, receipt)
    return receipt, observation


def _validate_capacity_initialization_receipt(
    *,
    receipt_path: Path,
    database_path: Path,
    configured_databases: Mapping[str, Any],
    migration_receipt_observation: Mapping[str, Any],
    expected_identity: Mapping[str, str],
    allowed_operations: frozenset[str],
) -> dict[str, Any]:
    try:
        receipt, raw, observation = _read_json_regular_file(receipt_path)
    except MigrationDrillError as exc:
        raise MigrationDrillError("capacity_initialization_receipt_invalid") from exc
    expected_keys = {
        "schema_version",
        "observed_at",
        "ok",
        "operation",
        "configured_databases",
        "migration_receipt",
        "migration_receipt_raw_sha256",
        "capacity_identity",
        "capacity_transition",
        "audit",
        "writer_stop_evidence",
        "writer_stop_evidence_sha256",
        "process_probes",
        "process_probes_sha256",
        "initialization_binding_sha256",
    }
    if (
        set(receipt) != expected_keys
        or receipt.get("schema_version")
        != CAPACITY_INITIALIZATION_RECEIPT_SCHEMA_VERSION
        or receipt.get("ok") is not True
        or receipt.get("operation") not in allowed_operations
        or receipt.get("configured_databases") != dict(configured_databases)
        or receipt.get("migration_receipt")
        != dict(migration_receipt_observation)
        or receipt.get("migration_receipt_raw_sha256")
        != migration_receipt_observation.get("sha256")
        or receipt.get("capacity_identity") != dict(expected_identity)
    ):
        raise MigrationDrillError("capacity_initialization_receipt_invalid")
    for name in (
        "migration_receipt_raw_sha256",
        "writer_stop_evidence_sha256",
        "process_probes_sha256",
        "initialization_binding_sha256",
    ):
        _require_sha256(
            receipt.get(name), code="capacity_initialization_receipt_invalid"
        )
    audit = receipt.get("audit")
    if not isinstance(audit, Mapping) or set(audit) != {
        "release_id_sha256",
        "operator_sha256",
        "reason_sha256",
    }:
        raise MigrationDrillError("capacity_initialization_receipt_invalid")
    for value in audit.values():
        _require_sha256(value, code="capacity_initialization_receipt_invalid")
    observed_at = _parse_timestamp(
        receipt.get("observed_at"),
        code="capacity_initialization_receipt_invalid",
    )
    try:
        normalized_writer_stop = validate_writer_stop_evidence(
            receipt.get("writer_stop_evidence"),
            now=observed_at,
            max_age_seconds=900,
        )
    except MigrationDrillError as exc:
        raise MigrationDrillError("capacity_initialization_receipt_invalid") from exc
    expected_probe_names = (
        {"initial", "locked", "immediate_pre_install", "post_install"}
        if receipt.get("operation") == "fresh_materialization"
        else {
            "initial",
            "locked",
            "post_maintenance",
            "immediate_pre_migration",
            "post_migration",
        }
    )
    process_probes = _validate_recorded_process_probes(
        receipt.get("process_probes"),
        expected_names=expected_probe_names,
        code="capacity_initialization_receipt_invalid",
    )
    if (
        normalized_writer_stop != receipt.get("writer_stop_evidence")
        or hashlib.sha256(_json_payload(normalized_writer_stop)).hexdigest()
        != receipt.get("writer_stop_evidence_sha256")
        or hashlib.sha256(_json_payload(process_probes)).hexdigest()
        != receipt.get("process_probes_sha256")
    ):
        raise MigrationDrillError("capacity_initialization_receipt_invalid")
    initialized = receipt.get("capacity_transition")
    initialized_state_row = (
        initialized.get("state_row") if isinstance(initialized, Mapping) else None
    )
    initialized_audit = (
        initialized.get("audit_chain") if isinstance(initialized, Mapping) else None
    )
    if (
        not isinstance(initialized, Mapping)
        or set(initialized)
        != {
            "schema_version",
            "release_id",
            "bootstrap_epoch_id",
            "state",
            "generation",
            "state_row",
            "audit_chain",
            "audit_chain_sha256",
        }
        or initialized.get("schema_version")
        != CAPACITY_LATCH_SNAPSHOT_SCHEMA_VERSION
        or initialized.get("release_id") != expected_identity["release_id"]
        or initialized.get("bootstrap_epoch_id")
        != expected_identity["bootstrap_epoch_id"]
        or initialized.get("state") != "BOOTSTRAP_PRODUCTION"
        or initialized.get("generation") != 1
        or not isinstance(initialized_state_row, Mapping)
        or set(initialized_state_row) != CAPACITY_STATE_ROW_FIELDS
        or initialized_state_row.get("release_id") != expected_identity["release_id"]
        or initialized_state_row.get("bootstrap_epoch_id")
        != expected_identity["bootstrap_epoch_id"]
        or initialized_state_row.get("state") != "BOOTSTRAP_PRODUCTION"
        or initialized_state_row.get("generation") != 1
        or initialized_state_row.get("updated_at")
        != initialized_state_row.get("bootstrap_initialized_at")
        or initialized_state_row.get("steady_activated_at") is not None
        or any(
            initialized_state_row.get(field) is not None
            for field in CAPACITY_EVIDENCE_FIELDS + CAPACITY_EVIDENCE_TIME_FIELDS
        )
        or not isinstance(initialized_audit, list)
        or len(initialized_audit) != 1
        or set(initialized_audit[0]) != CAPACITY_AUDIT_ROW_FIELDS
        or initialized_audit[0].get("release_id")
        != expected_identity["release_id"]
        or initialized_audit[0].get("bootstrap_epoch_id")
        != expected_identity["bootstrap_epoch_id"]
        or initialized_audit[0].get("from_state") != "UNCONFIGURED"
        or initialized_audit[0].get("to_state") != "BOOTSTRAP_PRODUCTION"
        or initialized_audit[0].get("from_generation") != 0
        or initialized_audit[0].get("to_generation") != 1
        or initialized_audit[0].get("transitioned_at")
        != initialized_state_row.get("bootstrap_initialized_at")
        or any(
            initialized_audit[0].get(field) is not None
            for field in CAPACITY_EVIDENCE_FIELDS + CAPACITY_EVIDENCE_TIME_FIELDS
        )
        or initialized.get("audit_chain_sha256")
        != hashlib.sha256(
            _json_payload({
                "state_row": initialized.get("state_row"),
                "audit_chain": initialized.get("audit_chain"),
            })
        ).hexdigest()
    ):
        raise MigrationDrillError("capacity_initialization_receipt_invalid")
    live = _capacity_transition_snapshot(
        database_path,
        expected_release_id=expected_identity["release_id"],
        expected_bootstrap_epoch_id=expected_identity["bootstrap_epoch_id"],
    )
    if (
        live.get("state") == "BOOTSTRAP_PRODUCTION"
        and initialized != live
    ):
        raise MigrationDrillError("capacity_initialization_latch_drift")
    if live.get("state") == "STEADY_ACTIVE" and (
        live.get("audit_chain", [None])[0] != initialized_audit[0]
        or live.get("state_row", {}).get("bootstrap_initialized_at")
        != initialized_state_row.get("bootstrap_initialized_at")
    ):
        raise MigrationDrillError("capacity_initialization_latch_drift")
    binding = _capacity_initialization_binding(
        operation=str(receipt["operation"]),
        configured_databases=configured_databases,
        migration_receipt=migration_receipt_observation,
        migration_receipt_raw_sha256=str(receipt["migration_receipt_raw_sha256"]),
        capacity_identity=expected_identity,
        capacity_transition=initialized,
        audit=audit,
        writer_stop_evidence=normalized_writer_stop,
        process_probes=process_probes,
    )
    if hashlib.sha256(_json_payload(binding)).hexdigest() != receipt.get(
        "initialization_binding_sha256"
    ):
        raise MigrationDrillError("capacity_initialization_receipt_invalid")
    return {
        "receipt": receipt,
        "receipt_raw": raw,
        "receipt_observation": observation,
        "initialization_transition": initialized,
        "capacity_transition": live,
    }


def _validate_materialization_receipt(
    *,
    receipt_path: Path,
    control_path: Path,
    delivery_path: Path,
    config_sha256: str,
    audit: Mapping[str, Any],
    artifact_path: Path,
    artifact_path_may_differ: bool,
    live_present: bool,
    maintenance_present: bool,
    tombstone_present: bool,
    enforce_live_state: bool = True,
    allowed_artifact_link_counts: frozenset[int] = frozenset({1}),
) -> dict[str, Any]:
    """Revalidate every durable materialization binding and the current DB bytes."""
    try:
        receipt, receipt_raw, receipt_observation = _read_json_regular_file(
            receipt_path
        )
    except MigrationDrillError as exc:
        raise MigrationDrillError("fresh_install_materialization_receipt_invalid") from exc
    expected_receipt_keys = {
        "schema_version",
        "observed_at",
        "ok",
        "migration_receipt",
        "genesis_intent",
        "parent_directory",
        "installation_lock",
        "destination_absence_before",
        "writer_stop_evidence",
        "process_probes",
        "strategy",
        "migration_receipt_raw_sha256",
        "candidate_commit",
        "config_sha256",
        "configured_databases",
        "genesis_intent_sha256",
        "db_instance_id",
        "audit",
        "capacity_identity",
        "capacity_transition",
        "capacity_initialization_receipt",
        "started_at",
        "completed_at",
        "runtime_identities",
        "materialization_journal",
        "destination",
        "rollback_contract",
        "materialization_binding_sha256",
    }
    configured = {
        "control": str(control_path),
        "delivery": str(delivery_path),
        "same_database": True,
    }
    capacity_identity = receipt.get("capacity_identity")
    if (
        set(receipt) != expected_receipt_keys
        or receipt.get("schema_version")
        != FRESH_INSTALL_MATERIALIZATION_RECEIPT_SCHEMA_VERSION
        or receipt.get("ok") is not True
        or receipt.get("strategy") != "fresh_install_preserve"
        or receipt.get("config_sha256") != config_sha256
        or receipt.get("configured_databases") != configured
        or receipt.get("audit") != dict(audit)
        or not isinstance(capacity_identity, Mapping)
        or set(capacity_identity) != {"release_id", "bootstrap_epoch_id"}
        or _capacity_identity(
            release_id=(
                str(capacity_identity.get("release_id") or "")
                if isinstance(capacity_identity, Mapping)
                else None
            ),
            bootstrap_epoch_id=(
                str(capacity_identity.get("bootstrap_epoch_id") or "")
                if isinstance(capacity_identity, Mapping)
                else None
            ),
            required=True,
        )
        != capacity_identity
        or receipt.get("rollback_contract")
        != {
            "action": "disable_writers_and_preserve_store",
            "destructive_cleanup_allowed": False,
            "quarantine_before_replacement_required": True,
        }
    ):
        raise MigrationDrillError("fresh_install_materialization_receipt_invalid")
    try:
        current_parent = _secure_directory_observation(
            control_path.parent,
            code="fresh_install_materialization_receipt_invalid",
        )[1]
    except MigrationDrillError as exc:
        raise MigrationDrillError("fresh_install_materialization_receipt_invalid") from exc
    installation_lock = receipt.get("installation_lock")
    expected_lock_path = control_path.parent / ".pnc-rca-fresh-install.lock"
    if (
        receipt.get("parent_directory") != current_parent
        or not isinstance(installation_lock, Mapping)
        or set(installation_lock)
        != {
            "path",
            "device",
            "inode",
            "owner_pid",
            "owner_uid",
            "mode",
        }
        or installation_lock.get("path") != str(expected_lock_path)
        or installation_lock.get("owner_uid") != os.getuid()
        or installation_lock.get("mode") != "0600"
        or not isinstance(installation_lock.get("owner_pid"), int)
        or installation_lock["owner_pid"] <= 0
    ):
        raise MigrationDrillError("fresh_install_materialization_receipt_invalid")
    try:
        lock_stat = expected_lock_path.lstat()
    except OSError as exc:
        raise MigrationDrillError("fresh_install_materialization_receipt_invalid") from exc
    if (
        stat.S_ISLNK(lock_stat.st_mode)
        or not stat.S_ISREG(lock_stat.st_mode)
        or lock_stat.st_uid != os.getuid()
        or lock_stat.st_nlink != 1
        or stat.S_IMODE(lock_stat.st_mode) != 0o600
        or (int(lock_stat.st_dev), int(lock_stat.st_ino))
        != (installation_lock.get("device"), installation_lock.get("inode"))
    ):
        raise MigrationDrillError("fresh_install_materialization_receipt_invalid")
    expected_absence = {
        name: {
            "path": str(candidate),
            "present": False,
            "identity": None,
        }
        for name, candidate in (
            ("database", control_path),
            ("wal", Path(f"{control_path}-wal")),
            ("shm", Path(f"{control_path}-shm")),
            ("journal", Path(f"{control_path}-journal")),
            ("tombstone", Path(f"{control_path}.pnc-rca-tombstone")),
        )
    }
    if receipt.get("destination_absence_before") != expected_absence:
        raise MigrationDrillError("fresh_install_materialization_receipt_invalid")
    _require_sha256(
        receipt.get("migration_receipt_raw_sha256"),
        code="fresh_install_materialization_receipt_invalid",
    )
    _require_sha256(
        receipt.get("genesis_intent_sha256"),
        code="fresh_install_materialization_receipt_invalid",
    )
    _require_sha256(
        receipt.get("materialization_binding_sha256"),
        code="fresh_install_materialization_receipt_invalid",
    )
    try:
        migration_observation = receipt["migration_receipt"]
        migration_path = Path(str(migration_observation["path"]))
        migration, migration_raw, current_migration_observation = (
            _read_json_regular_file(migration_path)
        )
    except (KeyError, TypeError, MigrationDrillError) as exc:
        raise MigrationDrillError("fresh_install_materialization_receipt_invalid") from exc
    if (
        current_migration_observation != migration_observation
        or hashlib.sha256(migration_raw).hexdigest()
        != receipt["migration_receipt_raw_sha256"]
        or migration.get("schema_version") != STORE_MIGRATION_RECEIPT_SCHEMA_VERSION
        or migration.get("migration_state") != "fresh_install"
        or migration.get("configured_databases") != configured
        or migration.get("materialization_required") is not True
        or migration.get("rollback_ready") is not False
    ):
        raise MigrationDrillError("fresh_install_materialization_receipt_invalid")
    genesis_observation = receipt.get("genesis_intent")
    if not isinstance(genesis_observation, Mapping):
        raise MigrationDrillError("fresh_install_materialization_receipt_invalid")
    try:
        intent, intent_raw, current_intent_observation = _read_json_regular_file(
            Path(str(genesis_observation["path"]))
        )
    except (KeyError, MigrationDrillError) as exc:
        raise MigrationDrillError("fresh_install_materialization_receipt_invalid") from exc
    if (
        current_intent_observation != genesis_observation
        or hashlib.sha256(intent_raw).hexdigest()
        != receipt["genesis_intent_sha256"]
        or intent.get("schema_version")
        != "pnc_rca_fresh_install_genesis_intent_v1"
        or intent.get("configured_database") != str(control_path)
        or intent.get("migration_receipt_raw_sha256")
        != receipt["migration_receipt_raw_sha256"]
        or intent.get("config_sha256") != config_sha256
        or intent.get("audit") != dict(audit)
        or intent.get("capacity_identity") != capacity_identity
        or intent.get("db_instance_id") != receipt.get("db_instance_id")
        or set(intent)
        != {
            "schema_version",
            "created_at",
            "db_instance_id",
            "configured_database",
            "migration_receipt_raw_sha256",
            "seed_sha256",
            "candidate_commit",
            "config_sha256",
            "journal_id",
            "maintenance_token_sha256",
            "audit",
            "capacity_identity",
        }
    ):
        raise MigrationDrillError("fresh_install_materialization_receipt_invalid")
    journal = receipt.get("materialization_journal")
    if not isinstance(journal, Mapping) or set(journal) != {
        "schema_version",
        "journal_id",
        "maintenance_path",
        "maintenance_token_sha256",
        "prepared",
        "installed",
    }:
        raise MigrationDrillError("fresh_install_materialization_receipt_invalid")
    journal_id = _require_sha256(
        journal.get("journal_id"),
        code="fresh_install_materialization_receipt_invalid",
    )
    maintenance_token = _require_sha256(
        journal.get("maintenance_token_sha256"),
        code="fresh_install_materialization_receipt_invalid",
    )
    if (
        journal.get("schema_version") != FRESH_INSTALL_JOURNAL_SCHEMA_VERSION
        or journal.get("maintenance_path")
        != f"{control_path}.pnc-rca-maintenance"
        or intent.get("journal_id") != journal_id
        or intent.get("maintenance_token_sha256") != maintenance_token
    ):
        raise MigrationDrillError("fresh_install_materialization_receipt_invalid")
    try:
        prepared, _prepared_raw, prepared_observation = _read_json_regular_file(
            Path(str(journal["prepared"]["path"]))
        )
        installed, _installed_raw, installed_observation = _read_json_regular_file(
            Path(str(journal["installed"]["path"]))
        )
    except (KeyError, TypeError, MigrationDrillError) as exc:
        raise MigrationDrillError("fresh_install_materialization_receipt_invalid") from exc
    if (
        prepared_observation != journal["prepared"]
        or installed_observation != journal["installed"]
        or set(prepared)
        != {
            "schema_version",
            "phase",
            "journal_id",
            "maintenance_token_sha256",
            "started_at",
            "configured_database",
            "migration_receipt_raw_sha256",
            "candidate_commit",
            "config_sha256",
            "seed_sha256",
            "genesis_intent_sha256",
            "db_instance_id",
            "audit",
            "capacity_identity",
            "runtime_identity",
            "destination_absence_before",
        }
        or prepared.get("schema_version") != FRESH_INSTALL_JOURNAL_SCHEMA_VERSION
        or prepared.get("phase") != "prepared"
        or prepared.get("journal_id") != journal_id
        or prepared.get("maintenance_token_sha256") != maintenance_token
        or prepared.get("configured_database") != str(control_path)
        or prepared.get("migration_receipt_raw_sha256")
        != receipt["migration_receipt_raw_sha256"]
        or prepared.get("config_sha256") != config_sha256
        or prepared.get("genesis_intent_sha256")
        != receipt["genesis_intent_sha256"]
        or prepared.get("db_instance_id") != receipt.get("db_instance_id")
        or prepared.get("audit") != dict(audit)
        or prepared.get("capacity_identity") != capacity_identity
        or set(installed)
        != {
            "schema_version",
            "phase",
            "journal_id",
            "maintenance_token_sha256",
            "prepared_sha256",
            "installed_at",
            "destination_artifact",
            "genesis_intent_sha256",
            "db_instance_id",
            "capacity_identity",
            "capacity_transition",
            "capacity_initialization_receipt",
            "writer_stop_evidence",
            "process_probes",
            "runtime_identity",
        }
        or installed.get("schema_version") != FRESH_INSTALL_JOURNAL_SCHEMA_VERSION
        or installed.get("phase") != "installed"
        or installed.get("journal_id") != journal_id
        or installed.get("maintenance_token_sha256") != maintenance_token
        or installed.get("prepared_sha256") != prepared_observation["sha256"]
        or installed.get("genesis_intent_sha256")
        != receipt["genesis_intent_sha256"]
        or installed.get("db_instance_id") != receipt.get("db_instance_id")
        or installed.get("capacity_identity") != capacity_identity
        or installed.get("capacity_transition")
        != receipt.get("capacity_transition")
        or installed.get("capacity_initialization_receipt")
        != receipt.get("capacity_initialization_receipt")
    ):
        raise MigrationDrillError("fresh_install_materialization_receipt_invalid")
    started_identity = _validate_runtime_identity_record(
        prepared.get("runtime_identity"),
        code="fresh_install_materialization_receipt_invalid",
    )
    completed_identity = _validate_runtime_identity_record(
        installed.get("runtime_identity"),
        code="fresh_install_materialization_receipt_invalid",
    )
    if receipt.get("runtime_identities") != {
        "started": started_identity,
        "completed": completed_identity,
    }:
        raise MigrationDrillError("fresh_install_materialization_receipt_invalid")
    destination = receipt.get("destination")
    if not isinstance(destination, Mapping) or set(destination) != {
        "roles",
        "configured_path",
        "artifact",
        "state",
        "validation",
        "seed_sha256",
        "seed_inheritance",
        "genesis_meta",
    }:
        raise MigrationDrillError("fresh_install_materialization_receipt_invalid")
    expected_artifact = destination.get("artifact")
    if (
        destination.get("roles") != ["control", "delivery"]
        or destination.get("configured_path") != str(control_path)
        or not isinstance(expected_artifact, Mapping)
        or installed.get("destination_artifact") != expected_artifact
    ):
        raise MigrationDrillError("fresh_install_materialization_receipt_invalid")
    destination_state = destination.get("state")
    if (
        not isinstance(destination_state, Mapping)
        or set(destination_state)
        != {"database", "wal", "shm", "journal", "tombstone"}
        or destination_state.get("database")
        != {
            "path": str(control_path),
            "present": True,
            "identity": expected_artifact,
        }
        or any(
            destination_state.get(name)
            != {
                "path": str(candidate),
                "present": False,
                "identity": None,
            }
            for name, candidate in (
                ("wal", Path(f"{control_path}-wal")),
                ("shm", Path(f"{control_path}-shm")),
                ("journal", Path(f"{control_path}-journal")),
                ("tombstone", Path(f"{control_path}.pnc-rca-tombstone")),
            )
        )
    ):
        raise MigrationDrillError("fresh_install_materialization_receipt_invalid")
    current_artifact = _require_exact_artifact(
        artifact_path,
        expected_artifact,
        allow_path_mismatch=artifact_path_may_differ,
        allowed_link_counts=allowed_artifact_link_counts,
        code="fresh_install_materialized_database_changed",
    )
    validation = inspect_sqlite_read_only(artifact_path, ("control", "delivery"))
    genesis = _read_genesis_meta(artifact_path)
    if (
        validation != destination.get("validation")
        or validation.get("schemas")
        != {
            "control": CONTROL_STORE_SCHEMA_VERSION,
            "delivery": DELIVERY_STORE_SCHEMA_VERSION,
        }
        or genesis != destination.get("genesis_meta")
        or genesis.get("control.fresh_install_db_instance_id")
        != receipt.get("db_instance_id")
        or genesis.get("delivery.fresh_install_genesis_intent_sha256")
        != receipt.get("genesis_intent_sha256")
    ):
        raise MigrationDrillError("fresh_install_materialized_database_invalid")
    capacity_receipt_observation = receipt.get("capacity_initialization_receipt")
    if not isinstance(capacity_receipt_observation, Mapping):
        raise MigrationDrillError("fresh_install_materialization_receipt_invalid")
    capacity_receipt_path = Path(
        str(capacity_receipt_observation.get("path") or "")
    )
    if (
        capacity_receipt_path.parent != receipt_path.parent
        or capacity_receipt_path.name
        != "capacity_transition_initialization_receipt.json"
    ):
        raise MigrationDrillError("fresh_install_materialization_receipt_invalid")
    capacity_initialization = _validate_capacity_initialization_receipt(
        receipt_path=capacity_receipt_path,
        database_path=artifact_path,
        configured_databases=configured,
        migration_receipt_observation=migration_observation,
        expected_identity=capacity_identity,
        allowed_operations=frozenset({"fresh_materialization"}),
    )
    if (
        capacity_initialization["receipt_observation"]
        != capacity_receipt_observation
        or capacity_initialization["initialization_transition"]
        != receipt.get("capacity_transition")
    ):
        raise MigrationDrillError("fresh_install_materialization_receipt_invalid")
    _validate_recorded_process_probes(
        receipt.get("process_probes"),
        expected_names={
            "initial",
            "locked",
            "immediate_pre_install",
            "post_install",
        },
        code="fresh_install_materialization_receipt_invalid",
    )
    if installed.get("process_probes") != receipt.get("process_probes"):
        raise MigrationDrillError("fresh_install_materialization_receipt_invalid")
    completed_at = _parse_timestamp(
        receipt.get("completed_at"),
        code="fresh_install_materialization_receipt_invalid",
    )
    if receipt.get("observed_at") != completed_at.isoformat():
        raise MigrationDrillError("fresh_install_materialization_receipt_invalid")
    try:
        stopped = validate_writer_stop_evidence(
            receipt.get("writer_stop_evidence"),
            now=completed_at,
            max_age_seconds=900,
        )
    except MigrationDrillError as exc:
        raise MigrationDrillError("fresh_install_materialization_receipt_invalid") from exc
    if installed.get("writer_stop_evidence") != stopped:
        raise MigrationDrillError("fresh_install_materialization_receipt_invalid")
    material_keys = {
        "strategy",
        "migration_receipt_raw_sha256",
        "candidate_commit",
        "config_sha256",
        "configured_databases",
        "genesis_intent_sha256",
        "db_instance_id",
        "audit",
        "capacity_identity",
        "capacity_transition",
        "capacity_initialization_receipt",
        "started_at",
        "completed_at",
        "runtime_identities",
        "materialization_journal",
        "destination",
        "rollback_contract",
    }
    material = {key: receipt[key] for key in material_keys}
    if hashlib.sha256(_json_payload(material)).hexdigest() != receipt.get(
        "materialization_binding_sha256"
    ):
        raise MigrationDrillError("fresh_install_materialization_receipt_invalid")
    receipted_path = _fresh_install_journal_path(
        receipt_path.parent, journal_id, "receipted"
    )
    try:
        receipted, _receipted_raw, _receipted_observation = (
            _read_json_regular_file(receipted_path)
        )
    except MigrationDrillError as exc:
        raise MigrationDrillError("fresh_install_materialization_receipt_invalid") from exc
    if (
        set(receipted)
        != {
            "schema_version",
            "phase",
            "journal_id",
            "maintenance_token_sha256",
            "installed_sha256",
            "receipt",
            "receipted_at",
            "maintenance_release_required",
        }
        or receipted.get("schema_version") != FRESH_INSTALL_JOURNAL_SCHEMA_VERSION
        or receipted.get("phase") != "receipted"
        or receipted.get("journal_id") != journal_id
        or receipted.get("maintenance_token_sha256") != maintenance_token
        or receipted.get("installed_sha256") != installed_observation["sha256"]
        or receipted.get("receipt") != receipt_observation
        or receipted.get("receipted_at") != receipt.get("completed_at")
        or receipted.get("maintenance_release_required") is not True
    ):
        raise MigrationDrillError("fresh_install_materialization_receipt_invalid")
    if enforce_live_state and live_present:
        if artifact_path != control_path:
            raise MigrationDrillError("fresh_install_materialization_receipt_invalid")
        state = _require_no_sqlite_sidecars(
            control_path,
            tombstone_present=tombstone_present,
            maintenance_present=maintenance_present,
            code="fresh_install_materialized_database_fenced",
        )
        if not state["database"]["present"]:
            raise MigrationDrillError("fresh_install_materialized_database_missing")
    elif enforce_live_state and (control_path.exists() or control_path.is_symlink()):
        raise MigrationDrillError("fresh_install_materialized_database_unexpected")
    return {
        "receipt": receipt,
        "receipt_raw": receipt_raw,
        "receipt_observation": receipt_observation,
        "migration": migration,
        "migration_observation": current_migration_observation,
        "intent": intent,
        "intent_observation": current_intent_observation,
        "journal": {
            "prepared": prepared,
            "prepared_observation": prepared_observation,
            "installed": installed,
            "installed_observation": installed_observation,
            "receipted": receipted,
        },
        "artifact": current_artifact,
        "validation": validation,
        "genesis_meta": genesis,
        "capacity_transition": capacity_initialization["capacity_transition"],
    }


def _owner_only_directory_observation(
    path: str | Path,
    *,
    code: str,
) -> tuple[Path, dict[str, Any]]:
    directory, observation = _secure_directory_observation(path, code=code)
    if stat.S_IMODE(directory.lstat().st_mode) & 0o077:
        raise MigrationDrillError(code)
    return directory, observation


@contextmanager
def _fresh_install_lock(
    parent: Path,
    expected_parent: Mapping[str, Any],
):
    parent_fd = -1
    lock_fd = -1
    try:
        parent_fd = os.open(
            parent,
            os.O_RDONLY
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_NOFOLLOW", 0),
        )
        current_parent = os.fstat(parent_fd)
        if (current_parent.st_dev, current_parent.st_ino) != (
            expected_parent["device"],
            expected_parent["inode"],
        ):
            raise MigrationDrillError("fresh_install_parent_changed")
        flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0)
        lock_fd = os.open(
            ".pnc-rca-fresh-install.lock",
            flags,
            0o600,
            dir_fd=parent_fd,
        )
        lock_stat = os.fstat(lock_fd)
        if (
            not stat.S_ISREG(lock_stat.st_mode)
            or lock_stat.st_uid != os.getuid()
            or lock_stat.st_nlink != 1
            or stat.S_IMODE(lock_stat.st_mode) != 0o600
        ):
            raise MigrationDrillError("fresh_install_lock_invalid")
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            raise MigrationDrillError("fresh_install_lock_busy") from exc
        yield parent_fd, lock_stat
    finally:
        if lock_fd >= 0:
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
            finally:
                os.close(lock_fd)
        if parent_fd >= 0:
            os.close(parent_fd)


def _rollback_journal_path(
    evidence_root: Path,
    operation: str,
    transaction_id: str,
    phase: str,
) -> Path:
    allowed = {
        "quarantine": {"prepared", "linked", "unlinked", "tombstoned", "receipted"},
        "restore": {"prepared", "staged", "copied", "installed", "verified", "unfenced", "receipted"},
    }
    if (
        operation not in allowed
        or phase not in allowed[operation]
        or re.fullmatch(r"[0-9a-f]{64}", transaction_id) is None
    ):
        raise MigrationDrillError("fresh_install_rollback_journal_identity_invalid")
    return evidence_root / (
        f"fresh_install_{operation}_journal.{transaction_id}.{phase}.json"
    )


def _fsync_directory(path: Path) -> None:
    descriptor = -1
    try:
        descriptor = os.open(
            path,
            os.O_RDONLY
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_NOFOLLOW", 0),
        )
        os.fsync(descriptor)
    except OSError as exc:
        raise MigrationDrillError("fresh_install_directory_sync_failed") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _unlink_exact_artifact(
    path: Path,
    *,
    expected: Mapping[str, Any],
    code: str,
) -> None:
    _require_exact_artifact(
        path,
        expected,
        allow_path_mismatch=False,
        code=code,
    )
    try:
        path.unlink()
        _fsync_directory(path.parent)
    except OSError as exc:
        raise MigrationDrillError(code) from exc


def _recover_linked_materialization_stage(
    *,
    live_path: Path,
    stage_path: Path,
    seed_path: Path,
    expected_genesis_meta: Mapping[str, str],
    parent_fd: int,
) -> None:
    conflict_code = "fresh_install_stage_recovery_conflict"
    try:
        candidates = list(
            live_path.parent.glob(f".{live_path.name}.genesis-*.sqlite3")
        )
    except OSError as exc:
        raise MigrationDrillError(conflict_code) from exc
    if len(candidates) != 1 or candidates[0].absolute() != stage_path.absolute():
        raise MigrationDrillError(conflict_code)
    if any(
        candidate.exists() or candidate.is_symlink()
        for candidate in (
            Path(f"{stage_path}-wal"),
            Path(f"{stage_path}-shm"),
            Path(f"{stage_path}-journal"),
        )
    ):
        raise MigrationDrillError(conflict_code)

    try:
        live_before = os.lstat(live_path)
        stage_before = os.lstat(stage_path)
    except OSError as exc:
        raise MigrationDrillError(conflict_code) from exc
    if any(
        not stat.S_ISREG(info.st_mode)
        or info.st_uid != os.getuid()
        or stat.S_IMODE(info.st_mode) != 0o600
        or info.st_nlink != 2
        for info in (live_before, stage_before)
    ) or (live_before.st_dev, live_before.st_ino) != (
        stage_before.st_dev,
        stage_before.st_ino,
    ):
        raise MigrationDrillError(conflict_code)

    try:
        live_artifact = observe_regular_file(live_path)
        stage_artifact = observe_regular_file(stage_path)
        if _artifact_identity_without_path(
            live_artifact
        ) != _artifact_identity_without_path(stage_artifact):
            raise MigrationDrillError(conflict_code)
        validation = inspect_sqlite_read_only(stage_path, ("control", "delivery"))
        if validation["schemas"] != _expected_target_schemas(
            ("control", "delivery")
        ):
            raise MigrationDrillError(conflict_code)
        if _read_genesis_meta(stage_path) != dict(expected_genesis_meta):
            raise MigrationDrillError(conflict_code)
        compare_sqlite_common_content(seed_path, stage_path)
        live_after = os.lstat(live_path)
        stage_after = os.lstat(stage_path)
    except MigrationDrillError as exc:
        raise MigrationDrillError(conflict_code) from exc
    except OSError as exc:
        raise MigrationDrillError(conflict_code) from exc
    before_identity = (
        live_before.st_dev,
        live_before.st_ino,
        live_before.st_mode,
        live_before.st_uid,
        live_before.st_nlink,
        live_before.st_size,
        live_before.st_mtime_ns,
        live_before.st_ctime_ns,
    )
    if before_identity != (
        live_after.st_dev,
        live_after.st_ino,
        live_after.st_mode,
        live_after.st_uid,
        live_after.st_nlink,
        live_after.st_size,
        live_after.st_mtime_ns,
        live_after.st_ctime_ns,
    ) or before_identity != (
        stage_after.st_dev,
        stage_after.st_ino,
        stage_after.st_mode,
        stage_after.st_uid,
        stage_after.st_nlink,
        stage_after.st_size,
        stage_after.st_mtime_ns,
        stage_after.st_ctime_ns,
    ):
        raise MigrationDrillError(conflict_code)
    try:
        os.unlink(stage_path.name, dir_fd=parent_fd)
        os.fsync(parent_fd)
        repaired = os.lstat(live_path)
    except OSError as exc:
        raise MigrationDrillError(conflict_code) from exc
    if (
        not stat.S_ISREG(repaired.st_mode)
        or repaired.st_uid != os.getuid()
        or stat.S_IMODE(repaired.st_mode) != 0o600
        or repaired.st_nlink != 1
        or (repaired.st_dev, repaired.st_ino)
        != (live_before.st_dev, live_before.st_ino)
        or _artifact_identity_without_path(observe_regular_file(live_path))
        != _artifact_identity_without_path(live_artifact)
    ):
        raise MigrationDrillError(conflict_code)


def _copy_stat_identity(value: os.stat_result) -> tuple[int, ...]:
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


def _require_recoverable_partial_copy(
    source: Path,
    temporary: Path,
    *,
    expected: Mapping[str, Any],
) -> int:
    """Accept only a private, stable byte-prefix left by this restore copy."""
    conflict = "fresh_install_restore_copy_conflict"
    source_before = _require_exact_artifact(
        source,
        expected,
        allow_path_mismatch=True,
        allowed_link_counts=frozenset({2}),
        code=conflict,
    )
    source_fd = -1
    temporary_fd = -1
    try:
        temporary_before = os.lstat(temporary)
        if (
            not stat.S_ISREG(temporary_before.st_mode)
            or temporary_before.st_uid != os.getuid()
            or stat.S_IMODE(temporary_before.st_mode) != 0o600
            or temporary_before.st_nlink != 1
            or temporary_before.st_size < 0
            or temporary_before.st_size > int(expected.get("size_bytes", -1))
        ):
            raise MigrationDrillError(conflict)
        source_fd = os.open(
            source,
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0),
        )
        temporary_fd = os.open(
            temporary,
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0),
        )
        opened_source = os.fstat(source_fd)
        opened_temporary = os.fstat(temporary_fd)
        if (
            (opened_source.st_dev, opened_source.st_ino)
            != (source_before["device"], source_before["inode"])
            or opened_source.st_size != source_before["size_bytes"]
            or opened_source.st_mtime_ns != source_before["mtime_ns"]
            or opened_source.st_uid != os.getuid()
            or opened_source.st_nlink != 2
            or stat.S_IMODE(opened_source.st_mode) & 0o022
            or _copy_stat_identity(opened_temporary)
            != _copy_stat_identity(temporary_before)
        ):
            raise MigrationDrillError(conflict)
        remaining = temporary_before.st_size
        while remaining:
            temporary_chunk = os.read(temporary_fd, min(1024 * 1024, remaining))
            if not temporary_chunk:
                raise MigrationDrillError(conflict)
            if os.read(source_fd, len(temporary_chunk)) != temporary_chunk:
                raise MigrationDrillError(conflict)
            remaining -= len(temporary_chunk)
        if os.read(temporary_fd, 1):
            raise MigrationDrillError(conflict)
        temporary_after = os.lstat(temporary)
    except MigrationDrillError:
        raise
    except OSError as exc:
        raise MigrationDrillError(conflict) from exc
    finally:
        if temporary_fd >= 0:
            os.close(temporary_fd)
        if source_fd >= 0:
            os.close(source_fd)
    source_after = _require_exact_artifact(
        source,
        expected,
        allow_path_mismatch=True,
        allowed_link_counts=frozenset({2}),
        code=conflict,
    )
    if (
        source_after != source_before
        or _copy_stat_identity(temporary_after)
        != _copy_stat_identity(temporary_before)
    ):
        raise MigrationDrillError(conflict)
    return int(temporary_before.st_size)


def _copy_artifact_no_clobber(
    source: Path,
    destination: Path,
    *,
    temporary: Path,
    expected: Mapping[str, Any],
) -> dict[str, Any]:
    conflict = "fresh_install_restore_copy_conflict"
    if temporary.parent != destination.parent or temporary == destination:
        raise MigrationDrillError(conflict)
    source_before = _require_exact_artifact(
        source,
        expected,
        allow_path_mismatch=True,
        allowed_link_counts=frozenset({2}),
        code=conflict,
    )
    destination_present = destination.exists() or destination.is_symlink()
    temporary_present = temporary.exists() or temporary.is_symlink()
    if destination_present:
        destination_artifact = _require_content_equivalent_artifact(
            destination,
            expected,
            require_single_link=not temporary_present,
            code=conflict,
        )
        destination_stat = destination.lstat()
        if (
            stat.S_IMODE(destination_stat.st_mode) != 0o600
            or destination_artifact["mtime_ns"] != expected.get("mtime_ns")
        ):
            raise MigrationDrillError(conflict)
        if temporary_present:
            temporary_artifact = _require_content_equivalent_artifact(
                temporary,
                expected,
                require_single_link=False,
                code=conflict,
            )
            temporary_stat = temporary.lstat()
            if (
                destination_stat.st_nlink != 2
                or temporary_stat.st_nlink != 2
                or stat.S_IMODE(temporary_stat.st_mode) != 0o600
                or temporary_artifact["mtime_ns"] != expected.get("mtime_ns")
                or (destination_stat.st_dev, destination_stat.st_ino)
                != (temporary_stat.st_dev, temporary_stat.st_ino)
                or _artifact_identity_without_path(destination_artifact)
                != _artifact_identity_without_path(temporary_artifact)
            ):
                raise MigrationDrillError(conflict)
            try:
                temporary.unlink()
                _fsync_directory(destination.parent)
            except OSError as exc:
                raise MigrationDrillError(conflict) from exc
            destination_artifact = _require_content_equivalent_artifact(
                destination,
                expected,
                require_single_link=True,
                code=conflict,
            )
        source_after = _require_exact_artifact(
            source,
            expected,
            allow_path_mismatch=True,
            allowed_link_counts=frozenset({2}),
            code=conflict,
        )
        if source_after != source_before:
            raise MigrationDrillError(conflict)
        return destination_artifact

    if temporary_present:
        partial_size = _require_recoverable_partial_copy(
            source,
            temporary,
            expected=expected,
        )
        if partial_size < int(expected["size_bytes"]):
            partial_artifact = observe_regular_file(temporary)
            _unlink_exact_artifact(
                temporary,
                expected=partial_artifact,
                code=conflict,
            )
            temporary_present = False
        else:
            try:
                temporary_stat = temporary.lstat()
                os.utime(
                    temporary,
                    ns=(temporary_stat.st_atime_ns, int(expected["mtime_ns"])),
                    follow_symlinks=False,
                )
                temporary_fd = os.open(
                    temporary,
                    os.O_RDONLY
                    | getattr(os, "O_CLOEXEC", 0)
                    | getattr(os, "O_NOFOLLOW", 0),
                )
                try:
                    os.fsync(temporary_fd)
                finally:
                    os.close(temporary_fd)
                _fsync_directory(temporary.parent)
            except (KeyError, OSError, TypeError, ValueError) as exc:
                raise MigrationDrillError(
                    "fresh_install_restore_copy_failed"
                ) from exc

    if not temporary_present:
        source_fd = -1
        temporary_fd = -1
        try:
            source_fd = os.open(
                source,
                os.O_RDONLY
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0),
            )
            opened_source = os.fstat(source_fd)
            if (
                (opened_source.st_dev, opened_source.st_ino)
                != (source_before["device"], source_before["inode"])
                or opened_source.st_size != source_before["size_bytes"]
                or opened_source.st_mtime_ns != source_before["mtime_ns"]
                or opened_source.st_uid != os.getuid()
                or opened_source.st_nlink != 2
                or stat.S_IMODE(opened_source.st_mode) & 0o022
            ):
                raise MigrationDrillError(conflict)
            temporary_fd = os.open(
                temporary,
                os.O_WRONLY
                | os.O_CREAT
                | os.O_EXCL
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0),
                0o600,
            )
            while True:
                chunk = os.read(source_fd, 1024 * 1024)
                if not chunk:
                    break
                view = memoryview(chunk)
                while view:
                    written = os.write(temporary_fd, view)
                    if written <= 0:
                        raise OSError("short write")
                    view = view[written:]
            os.fsync(temporary_fd)
            os.utime(
                temporary,
                ns=(opened_source.st_atime_ns, opened_source.st_mtime_ns),
                follow_symlinks=False,
            )
            os.fsync(temporary_fd)
        except MigrationDrillError:
            raise
        except FileExistsError as exc:
            raise MigrationDrillError(conflict) from exc
        except OSError as exc:
            raise MigrationDrillError("fresh_install_restore_copy_failed") from exc
        finally:
            if temporary_fd >= 0:
                os.close(temporary_fd)
            if source_fd >= 0:
                os.close(source_fd)
        source_after = _require_exact_artifact(
            source,
            expected,
            allow_path_mismatch=True,
            allowed_link_counts=frozenset({2}),
            code=conflict,
        )
        if source_after != source_before:
            raise MigrationDrillError(conflict)
        _fsync_directory(temporary.parent)

    temporary_artifact = _require_content_equivalent_artifact(
        temporary,
        expected,
        require_single_link=True,
        code="fresh_install_restore_copy_invalid",
    )
    if (
        stat.S_IMODE(temporary.lstat().st_mode) != 0o600
        or temporary_artifact["mtime_ns"] != expected.get("mtime_ns")
    ):
        raise MigrationDrillError("fresh_install_restore_copy_invalid")
    try:
        os.link(temporary, destination, follow_symlinks=False)
        _fsync_directory(destination.parent)
    except FileExistsError as exc:
        raise MigrationDrillError(conflict) from exc
    except OSError as exc:
        raise MigrationDrillError("fresh_install_restore_copy_failed") from exc
    destination_artifact = _require_content_equivalent_artifact(
        destination,
        expected,
        require_single_link=False,
        code="fresh_install_restore_copy_invalid",
    )
    temporary_stat = temporary.lstat()
    destination_stat = destination.lstat()
    if (
        temporary_stat.st_nlink != 2
        or destination_stat.st_nlink != 2
        or (temporary_stat.st_dev, temporary_stat.st_ino)
        != (destination_stat.st_dev, destination_stat.st_ino)
        or _artifact_identity_without_path(temporary_artifact)
        != _artifact_identity_without_path(destination_artifact)
    ):
        raise MigrationDrillError("fresh_install_restore_copy_invalid")
    try:
        temporary.unlink()
        _fsync_directory(destination.parent)
    except OSError as exc:
        raise MigrationDrillError("fresh_install_restore_copy_failed") from exc
    final_artifact = _require_content_equivalent_artifact(
        destination,
        expected,
        require_single_link=True,
        code="fresh_install_restore_copy_invalid",
    )
    source_final = _require_exact_artifact(
        source,
        expected,
        allow_path_mismatch=True,
        allowed_link_counts=frozenset({2}),
        code=conflict,
    )
    if (
        final_artifact["mtime_ns"] != expected.get("mtime_ns")
        or stat.S_IMODE(destination.lstat().st_mode) != 0o600
        or source_final != source_before
    ):
        raise MigrationDrillError(conflict)
    return final_artifact


def materialize_fresh_install(
    *,
    migration_receipt_path: str | Path,
    control_db_path: str | Path,
    delivery_db_path: str | Path,
    config_sha256: str,
    evidence_dir: str | Path,
    writer_stop_evidence: Mapping[str, Any],
    apply: bool = False,
    now: datetime | None = None,
    max_writer_stop_age_seconds: int = 900,
    writer_process_probe: Any | None = None,
    release_id: str | None = None,
    bootstrap_epoch_id: str | None = None,
    operator: str | None = None,
    reason: str | None = None,
) -> dict[str, Any]:
    """Plan or recoverably install one shared fresh RCA database."""
    started_at = _utc(now)
    audit = _fresh_install_audit_hashes(
        release_id=release_id,
        operator=operator,
        reason=reason,
        required=apply,
    )
    capacity_identity = _capacity_identity(
        release_id=release_id,
        bootstrap_epoch_id=bootstrap_epoch_id,
        required=apply,
    )
    if (
        not isinstance(max_writer_stop_age_seconds, int)
        or max_writer_stop_age_seconds < 1
    ):
        raise MigrationDrillError("writer_stop_evidence_max_age_invalid")
    if re.fullmatch(r"[0-9a-f]{64}", config_sha256) is None:
        raise MigrationDrillError("fresh_install_config_sha256_invalid")
    migration, _raw, migration_observation = _read_json_regular_file(
        migration_receipt_path
    )
    configured = migration.get("configured_databases")
    drills = migration.get("database_drills")
    if (
        migration.get("schema_version") != STORE_MIGRATION_RECEIPT_SCHEMA_VERSION
        or migration.get("ok") is not True
        or migration.get("migration_state") != "fresh_install"
        or migration.get("rollback_strategy")
        != "disable_writers_preserve_current_store_v1"
        or migration.get("materialization_required") is not True
        or migration.get("rollback_ready") is not False
        or migration.get("blockers") != ["fresh_install_materialization_required"]
        or not isinstance(configured, Mapping)
        or configured.get("same_database") is not True
        or not isinstance(drills, list)
        or len(drills) != 1
    ):
        raise MigrationDrillError("fresh_install_migration_receipt_invalid")
    control_path = _canonical_configured_path(control_db_path)
    delivery_path = _canonical_configured_path(delivery_db_path)
    if control_path != delivery_path or configured != {
        "control": str(control_path),
        "delivery": str(delivery_path),
        "same_database": True,
    }:
        raise MigrationDrillError("fresh_install_configured_database_mismatch")
    drill = drills[0]
    if not isinstance(drill, Mapping):
        raise MigrationDrillError("fresh_install_migration_receipt_invalid")
    source = drill.get("source")
    seed = drill.get("installation_seed")
    if (
        drill.get("roles") != ["control", "delivery"]
        or drill.get("migration_state") != "fresh_install"
        or drill.get("materialization_required") is not True
        or drill.get("rollback_ready") is not False
        or not isinstance(source, Mapping)
        or source.get("mode") != "fresh_create"
        or source.get("exists") is not False
        or source.get("identity") is not None
        or source.get("bundle_before") != source.get("bundle_after")
        or not isinstance(seed, Mapping)
    ):
        raise MigrationDrillError("fresh_install_migration_receipt_invalid")
    expected_absent_bundle = {
        "database": {"present": False, "identity": None},
        "wal": {"present": False, "identity": None},
        "shm": {"present": False, "identity": None},
        "journal": {"present": False, "identity": None},
    }
    if source.get("bundle_before") != expected_absent_bundle:
        raise MigrationDrillError("fresh_install_migration_receipt_invalid")
    seed_artifact = seed.get("artifact")
    if not isinstance(seed_artifact, Mapping):
        raise MigrationDrillError("fresh_install_seed_invalid")
    seed_path = Path(str(seed_artifact.get("path") or ""))
    if (
        observe_regular_file(seed_path) != seed_artifact
        or inspect_sqlite_read_only(seed_path, ("control", "delivery"))
        != seed.get("validation")
    ):
        raise MigrationDrillError("fresh_install_seed_changed")
    candidate = migration.get("candidate")
    if not isinstance(candidate, Mapping):
        raise MigrationDrillError("fresh_install_candidate_invalid")
    candidate_commit = str(candidate.get("commit") or "")
    if re.fullmatch(r"[0-9a-f]{40}", candidate_commit) is None:
        raise MigrationDrillError("fresh_install_candidate_invalid")
    stopped = validate_writer_stop_evidence(
        writer_stop_evidence,
        now=started_at,
        max_age_seconds=max_writer_stop_age_seconds,
    )
    process_probe = writer_process_probe or _default_writer_process_probe
    initial_probe = _checked_writer_process_probe(process_probe)
    evidence_root, _evidence_root_observation = _secure_directory_observation(
        evidence_dir,
        code="fresh_install_directory_invalid",
    )
    parent, parent_observation = _secure_directory_observation(
        control_path.parent,
        code="fresh_install_directory_invalid",
    )
    receipt_path = evidence_root / "fresh_install_materialization_receipt.json"
    capacity_receipt_path = evidence_root / (
        "capacity_transition_initialization_receipt.json"
    )
    intent_path = evidence_root / "fresh_install_genesis_intent.json"
    maintenance_path = Path(f"{control_path}.pnc-rca-maintenance")
    lock_path = parent / ".pnc-rca-fresh-install.lock"

    def record_failure(code: str, *, installed: bool) -> None:
        failure = {
            "schema_version": "pnc_rca_fresh_install_materialization_failure_v1",
            "observed_at": _utc(now).isoformat(),
            "code": code,
            "configured_database": str(control_path),
            "migration_receipt_raw_sha256": migration_observation["sha256"],
            "installed_before_failure": installed,
        }
        try:
            _write_json_no_clobber(
                evidence_root
                / f"fresh_install_materialization_failure.{uuid.uuid4().hex}.json",
                failure,
            )
        except MigrationDrillError:
            pass
    receipt_present = receipt_path.exists() or receipt_path.is_symlink()
    maintenance_present = maintenance_path.exists() or maintenance_path.is_symlink()
    destination_state = _fresh_destination_state(control_path)
    if not apply:
        return {
            "schema_version": "pnc_rca_fresh_install_materialization_plan_v1",
            "applied": False,
            "idempotent": receipt_present and not maintenance_present,
            "recovery_required": maintenance_present,
            "migration_receipt_raw_sha256": migration_observation["sha256"],
            "configured_database": str(control_path),
            "seed_sha256": seed_artifact["sha256"],
            "candidate_commit": candidate_commit,
            "config_sha256": config_sha256,
            "destination_state": destination_state,
            "receipt_present": receipt_present,
            "maintenance_present": maintenance_present,
            "writer_stop_evidence_sha256": hashlib.sha256(
                _json_payload(stopped)
            ).hexdigest(),
            "initial_process_probe": initial_probe,
            "apply_audit_required": True,
            "required_capacity_identity": capacity_identity,
            "capacity_initialization_receipt_present": (
                capacity_receipt_path.exists() or capacity_receipt_path.is_symlink()
            ),
        }

    if audit is None or capacity_identity is None:
        raise MigrationDrillError("fresh_install_audit_invalid")
    runtime_identity = _fresh_install_runtime_identity()
    parent_fd = -1
    lock_fd = -1
    stage_path: Path | None = None
    installed = control_path.exists()
    marker: dict[str, Any] | None = None
    marker_observation: dict[str, Any] | None = None
    receipt_recovered = False
    try:
        parent_fd = os.open(
            parent,
            os.O_RDONLY
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_NOFOLLOW", 0),
        )
        parent_stat = os.fstat(parent_fd)
        if (
            parent_stat.st_dev != parent_observation["device"]
            or parent_stat.st_ino != parent_observation["inode"]
        ):
            raise MigrationDrillError("fresh_install_parent_changed")
        lock_flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0)
        lock_fd = os.open(lock_path.name, lock_flags, 0o600, dir_fd=parent_fd)
        lock_stat = os.fstat(lock_fd)
        if (
            not stat.S_ISREG(lock_stat.st_mode)
            or lock_stat.st_uid != os.getuid()
            or lock_stat.st_nlink != 1
            or stat.S_IMODE(lock_stat.st_mode) != 0o600
        ):
            raise MigrationDrillError("fresh_install_lock_invalid")
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            raise MigrationDrillError("fresh_install_lock_busy") from exc
        locked_probe = _checked_writer_process_probe(process_probe)
        _resolved_parent, locked_parent = _secure_directory_observation(
            parent,
            code="fresh_install_parent_changed",
        )
        if (
            locked_parent["device"],
            locked_parent["inode"],
        ) != (parent_observation["device"], parent_observation["inode"]):
            raise MigrationDrillError("fresh_install_parent_changed")

        if maintenance_path.exists() or maintenance_path.is_symlink():
            marker, _marker_raw, marker_observation = _read_json_regular_file(
                maintenance_path
            )
            if (
                set(marker)
                != {
                    "schema_version",
                    "created_at",
                    "state",
                    "journal_id",
                    "maintenance_token_sha256",
                    "configured_database",
                    "migration_receipt_raw_sha256",
                    "candidate_commit",
                    "config_sha256",
                    "audit",
                    "capacity_identity",
                    "runtime_identity",
                }
                or marker.get("schema_version")
                != FRESH_INSTALL_MAINTENANCE_SCHEMA_VERSION
                or marker.get("state") != "active"
                or marker.get("configured_database") != str(control_path)
                or marker.get("migration_receipt_raw_sha256")
                != migration_observation["sha256"]
                or marker.get("candidate_commit") != candidate_commit
                or marker.get("config_sha256") != config_sha256
                or marker.get("audit") != audit
                or marker.get("capacity_identity") != capacity_identity
            ):
                raise MigrationDrillError("fresh_install_maintenance_conflict")
            journal_id = str(marker.get("journal_id") or "")
            maintenance_token_sha256 = str(
                marker.get("maintenance_token_sha256") or ""
            )
            if (
                re.fullmatch(r"[0-9a-f]{64}", journal_id) is None
                or re.fullmatch(r"[0-9a-f]{64}", maintenance_token_sha256)
                is None
            ):
                raise MigrationDrillError("fresh_install_maintenance_conflict")
            receipt_recovered = True
        else:
            if receipt_path.exists() or receipt_path.is_symlink():
                validated_existing = _validate_materialization_receipt(
                    receipt_path=receipt_path,
                    control_path=control_path,
                    delivery_path=delivery_path,
                    config_sha256=config_sha256,
                    audit=audit,
                    artifact_path=control_path,
                    artifact_path_may_differ=False,
                    live_present=True,
                    maintenance_present=False,
                    tombstone_present=False,
                )
                return {
                    **validated_existing["receipt"],
                    "applied": False,
                    "idempotent": True,
                    "recovered": False,
                }
            absence = _require_fresh_destination_absent(control_path)
            maintenance_token = uuid.uuid4().hex
            maintenance_token_sha256 = hashlib.sha256(
                maintenance_token.encode("ascii")
            ).hexdigest()
            journal_id = hashlib.sha256(
                _json_payload({
                    "maintenance_token_sha256": maintenance_token_sha256,
                    "migration_receipt_raw_sha256": migration_observation["sha256"],
                    "config_sha256": config_sha256,
                    "audit": audit,
                    "capacity_identity": capacity_identity,
                })
            ).hexdigest()
            marker = {
                "schema_version": FRESH_INSTALL_MAINTENANCE_SCHEMA_VERSION,
                "created_at": started_at.isoformat(),
                "state": "active",
                "journal_id": journal_id,
                "maintenance_token_sha256": maintenance_token_sha256,
                "configured_database": str(control_path),
                "migration_receipt_raw_sha256": migration_observation["sha256"],
                "candidate_commit": candidate_commit,
                "config_sha256": config_sha256,
                "audit": audit,
                "capacity_identity": capacity_identity,
                "runtime_identity": runtime_identity,
            }
            marker_observation = _write_json_no_clobber(maintenance_path, marker)

        if intent_path.exists() or intent_path.is_symlink():
            intent, _intent_raw, intent_observation = _read_json_regular_file(intent_path)
        else:
            intent = {
                "schema_version": "pnc_rca_fresh_install_genesis_intent_v1",
                "created_at": str(marker["created_at"]),
                "db_instance_id": str(uuid.uuid4()),
                "configured_database": str(control_path),
                "migration_receipt_raw_sha256": migration_observation["sha256"],
                "seed_sha256": seed_artifact["sha256"],
                "candidate_commit": candidate_commit,
                "config_sha256": config_sha256,
                "journal_id": journal_id,
                "maintenance_token_sha256": maintenance_token_sha256,
                "audit": audit,
                "capacity_identity": capacity_identity,
            }
            intent_observation = _write_json_no_clobber(intent_path, intent)
        if (
            intent.get("schema_version")
            != "pnc_rca_fresh_install_genesis_intent_v1"
            or intent.get("configured_database") != str(control_path)
            or intent.get("migration_receipt_raw_sha256")
            != migration_observation["sha256"]
            or intent.get("seed_sha256") != seed_artifact["sha256"]
            or intent.get("candidate_commit") != candidate_commit
            or intent.get("config_sha256") != config_sha256
            or intent.get("journal_id") != journal_id
            or intent.get("maintenance_token_sha256")
            != maintenance_token_sha256
            or intent.get("audit") != audit
            or intent.get("capacity_identity") != capacity_identity
        ):
            raise MigrationDrillError("fresh_install_genesis_intent_conflict")
        genesis_intent_sha256 = hashlib.sha256(_json_payload(intent)).hexdigest()
        prepared_path = _fresh_install_journal_path(
            evidence_root, journal_id, "prepared"
        )
        if prepared_path.exists() or prepared_path.is_symlink():
            prepared, _prepared_raw, prepared_observation = (
                _read_json_regular_file(prepared_path)
            )
            absence = prepared.get("destination_absence_before")
            if (
                prepared.get("schema_version")
                != FRESH_INSTALL_JOURNAL_SCHEMA_VERSION
                or prepared.get("phase") != "prepared"
                or prepared.get("journal_id") != journal_id
                or prepared.get("maintenance_token_sha256")
                != maintenance_token_sha256
                or prepared.get("configured_database") != str(control_path)
                or prepared.get("migration_receipt_raw_sha256")
                != migration_observation["sha256"]
                or prepared.get("candidate_commit") != candidate_commit
                or prepared.get("config_sha256") != config_sha256
                or prepared.get("seed_sha256") != seed_artifact["sha256"]
                or prepared.get("genesis_intent_sha256")
                != genesis_intent_sha256
                or prepared.get("db_instance_id") != intent["db_instance_id"]
                or prepared.get("audit") != audit
                or prepared.get("capacity_identity") != capacity_identity
                or not isinstance(absence, Mapping)
            ):
                raise MigrationDrillError("fresh_install_journal_conflict")
        else:
            if control_path.exists() or control_path.is_symlink():
                raise MigrationDrillError("fresh_install_recovery_journal_missing")
            absence = _require_fresh_destination_absent(control_path)
            prepared = {
                "schema_version": FRESH_INSTALL_JOURNAL_SCHEMA_VERSION,
                "phase": "prepared",
                "journal_id": journal_id,
                "maintenance_token_sha256": maintenance_token_sha256,
                "started_at": str(marker["created_at"]),
                "configured_database": str(control_path),
                "migration_receipt_raw_sha256": migration_observation["sha256"],
                "candidate_commit": candidate_commit,
                "config_sha256": config_sha256,
                "seed_sha256": seed_artifact["sha256"],
                "genesis_intent_sha256": genesis_intent_sha256,
                "db_instance_id": intent["db_instance_id"],
                "audit": audit,
                "capacity_identity": capacity_identity,
                "runtime_identity": marker["runtime_identity"],
                "destination_absence_before": absence,
            }
            prepared_observation = _write_json_no_clobber(
                prepared_path, prepared
            )
        stage_path = parent / f".{control_path.name}.genesis-{journal_id}.sqlite3"
        expected_meta = {
            f"{prefix}.{key}": str(value)
            for prefix in ("control", "delivery")
            for key, value in (
                ("fresh_install_db_instance_id", intent["db_instance_id"]),
                (
                    "fresh_install_genesis_intent_sha256",
                    genesis_intent_sha256,
                ),
                ("fresh_install_origin_commit", candidate_commit),
            )
        }
        if not control_path.exists():
            for candidate_path in (
                stage_path,
                Path(f"{stage_path}-wal"),
                Path(f"{stage_path}-shm"),
                Path(f"{stage_path}-journal"),
            ):
                candidate_path.unlink(missing_ok=True)
            _sqlite_backup(seed_path, stage_path)
            with sqlite3.connect(stage_path) as connection:
                for table in ("control_meta", "rca_delivery_meta"):
                    connection.executemany(
                        f"INSERT OR REPLACE INTO {table}(key, value) VALUES (?, ?)",
                        (
                            ("fresh_install_db_instance_id", intent["db_instance_id"]),
                            (
                                "fresh_install_genesis_intent_sha256",
                                genesis_intent_sha256,
                            ),
                            ("fresh_install_origin_commit", candidate_commit),
                        ),
                    )
                connection.commit()
            try:
                RcaControlStore(
                    stage_path, require_current=True
                ).initialize_capacity_transition(
                    release_id=capacity_identity["release_id"],
                    bootstrap_epoch_id=capacity_identity["bootstrap_epoch_id"],
                    now=started_at,
                )
            except (OSError, RuntimeError, sqlite3.Error) as exc:
                raise MigrationDrillError(
                    "fresh_install_capacity_initialization_failed"
                ) from exc
            _checkpoint_restore(stage_path)
            with stage_path.open("rb") as handle:
                os.fsync(handle.fileno())
            stage_validation = inspect_sqlite_read_only(
                stage_path, ("control", "delivery")
            )
            if stage_validation["schemas"] != _expected_target_schemas(
                ("control", "delivery")
            ):
                raise MigrationDrillError("fresh_install_stage_invalid")
            immediate_probe = _checked_writer_process_probe(process_probe)
            validate_writer_stop_evidence(
                writer_stop_evidence,
                now=_utc(now),
                max_age_seconds=max_writer_stop_age_seconds,
            )
            _require_fresh_destination_absent(control_path)
            try:
                os.link(
                    stage_path.name,
                    control_path.name,
                    src_dir_fd=parent_fd,
                    dst_dir_fd=parent_fd,
                    follow_symlinks=False,
                )
            except FileExistsError as exc:
                raise MigrationDrillError("fresh_install_destination_race") from exc
            installed = True
            os.fsync(parent_fd)
            _recover_linked_materialization_stage(
                live_path=control_path,
                stage_path=stage_path,
                seed_path=seed_path,
                expected_genesis_meta=expected_meta,
                parent_fd=parent_fd,
            )
            stage_path = None
        else:
            immediate_probe = _checked_writer_process_probe(process_probe)
            installed = True
            if stage_path.exists() or stage_path.is_symlink():
                validate_writer_stop_evidence(
                    writer_stop_evidence,
                    now=_utc(now),
                    max_age_seconds=max_writer_stop_age_seconds,
                )
                _recover_linked_materialization_stage(
                    live_path=control_path,
                    stage_path=stage_path,
                    seed_path=seed_path,
                    expected_genesis_meta=expected_meta,
                    parent_fd=parent_fd,
                )
            stage_path = None

        state_before_validation = _fresh_destination_state(control_path)
        if any(
            state_before_validation[name]["present"]
            for name in ("wal", "shm", "journal", "tombstone")
        ):
            raise MigrationDrillError("fresh_install_destination_sidecar_present")
        final_probe = _checked_writer_process_probe(process_probe)
        process_probes = {
            "initial": initial_probe,
            "locked": locked_probe,
            "immediate_pre_install": immediate_probe,
            "post_install": final_probe,
        }
        capacity_transition = _capacity_transition_snapshot(
            control_path,
            expected_release_id=capacity_identity["release_id"],
            expected_bootstrap_epoch_id=capacity_identity["bootstrap_epoch_id"],
        )
        if capacity_receipt_path.exists() or capacity_receipt_path.is_symlink():
            capacity_initialization = _validate_capacity_initialization_receipt(
                receipt_path=capacity_receipt_path,
                database_path=control_path,
                configured_databases=configured,
                migration_receipt_observation=migration_observation,
                expected_identity=capacity_identity,
                allowed_operations=frozenset({"fresh_materialization"}),
            )
            capacity_receipt_observation = capacity_initialization[
                "receipt_observation"
            ]
        else:
            _capacity_receipt, capacity_receipt_observation = (
                _write_capacity_initialization_receipt(
                    receipt_path=capacity_receipt_path,
                    operation="fresh_materialization",
                    configured_databases=configured,
                    migration_receipt=migration_observation,
                    migration_receipt_raw_sha256=migration_observation["sha256"],
                    capacity_identity=capacity_identity,
                    capacity_transition=capacity_transition,
                    audit=audit,
                    writer_stop_evidence=stopped,
                    process_probes=process_probes,
                    observed_at=_utc(now),
                )
            )
        destination_artifact = observe_regular_file(control_path)
        _require_exact_artifact(
            control_path,
            destination_artifact,
            allowed_link_counts=frozenset({1}),
            code="fresh_install_destination_link_count_invalid",
        )
        destination_state = _fresh_destination_state(control_path)
        if any(
            destination_state[name]["present"]
            for name in ("wal", "shm", "journal", "tombstone")
        ):
            raise MigrationDrillError("fresh_install_destination_sidecar_present")
        destination_validation = inspect_sqlite_read_only(
            control_path, ("control", "delivery")
        )
        inheritance = compare_sqlite_common_content(seed_path, control_path)
        genesis_meta = _read_genesis_meta(control_path)
        if genesis_meta != expected_meta:
            raise MigrationDrillError("fresh_install_genesis_meta_invalid")
        _parent_path, parent_after = _secure_directory_observation(
            parent,
            code="fresh_install_parent_changed",
        )
        if (parent_after["device"], parent_after["inode"]) != (
            parent_observation["device"],
            parent_observation["inode"],
        ):
            raise MigrationDrillError("fresh_install_parent_changed")
        installed_path = _fresh_install_journal_path(
            evidence_root, journal_id, "installed"
        )
        if installed_path.exists() or installed_path.is_symlink():
            installed_phase, _installed_raw, installed_observation = (
                _read_json_regular_file(installed_path)
            )
            if (
                installed_phase.get("schema_version")
                != FRESH_INSTALL_JOURNAL_SCHEMA_VERSION
                or installed_phase.get("phase") != "installed"
                or installed_phase.get("journal_id") != journal_id
                or installed_phase.get("maintenance_token_sha256")
                != maintenance_token_sha256
                or installed_phase.get("prepared_sha256")
                != prepared_observation["sha256"]
                or installed_phase.get("destination_artifact")
                != destination_artifact
                or installed_phase.get("genesis_intent_sha256")
                != genesis_intent_sha256
                or installed_phase.get("db_instance_id")
                != intent["db_instance_id"]
                or installed_phase.get("capacity_identity") != capacity_identity
                or installed_phase.get("capacity_transition")
                != capacity_transition
                or installed_phase.get("capacity_initialization_receipt")
                != capacity_receipt_observation
            ):
                raise MigrationDrillError("fresh_install_journal_conflict")
            completed_at = _parse_timestamp(
                installed_phase.get("installed_at"),
                code="fresh_install_journal_timestamp_invalid",
            )
            stopped_for_receipt = installed_phase.get("writer_stop_evidence")
            process_probes = installed_phase.get("process_probes")
            completed_runtime_identity = installed_phase.get("runtime_identity")
            if (
                not isinstance(stopped_for_receipt, Mapping)
                or not isinstance(process_probes, Mapping)
                or not isinstance(completed_runtime_identity, Mapping)
            ):
                raise MigrationDrillError("fresh_install_journal_conflict")
        else:
            completed_at = _utc(now)
            stopped_for_receipt = stopped
            completed_runtime_identity = runtime_identity
            installed_phase = {
                "schema_version": FRESH_INSTALL_JOURNAL_SCHEMA_VERSION,
                "phase": "installed",
                "journal_id": journal_id,
                "maintenance_token_sha256": maintenance_token_sha256,
                "prepared_sha256": prepared_observation["sha256"],
                "installed_at": completed_at.isoformat(),
                "destination_artifact": destination_artifact,
                "genesis_intent_sha256": genesis_intent_sha256,
                "db_instance_id": intent["db_instance_id"],
                "capacity_identity": capacity_identity,
                "capacity_transition": capacity_transition,
                "capacity_initialization_receipt": capacity_receipt_observation,
                "writer_stop_evidence": stopped_for_receipt,
                "process_probes": process_probes,
                "runtime_identity": completed_runtime_identity,
            }
            installed_observation = _write_json_no_clobber(
                installed_path, installed_phase
            )
        journal = {
            "schema_version": FRESH_INSTALL_JOURNAL_SCHEMA_VERSION,
            "journal_id": journal_id,
            "maintenance_path": str(maintenance_path),
            "maintenance_token_sha256": maintenance_token_sha256,
            "prepared": prepared_observation,
            "installed": installed_observation,
        }
        material = {
            "strategy": "fresh_install_preserve",
            "migration_receipt_raw_sha256": migration_observation["sha256"],
            "candidate_commit": candidate_commit,
            "config_sha256": config_sha256,
            "configured_databases": dict(configured),
            "genesis_intent_sha256": genesis_intent_sha256,
            "db_instance_id": intent["db_instance_id"],
            "audit": audit,
            "capacity_identity": capacity_identity,
            "capacity_transition": capacity_transition,
            "capacity_initialization_receipt": capacity_receipt_observation,
            "started_at": str(marker["created_at"]),
            "completed_at": completed_at.isoformat(),
            "runtime_identities": {
                "started": marker["runtime_identity"],
                "completed": completed_runtime_identity,
            },
            "materialization_journal": journal,
            "destination": {
                "roles": ["control", "delivery"],
                "configured_path": str(control_path),
                "artifact": destination_artifact,
                "state": destination_state,
                "validation": destination_validation,
                "seed_sha256": seed_artifact["sha256"],
                "seed_inheritance": inheritance,
                "genesis_meta": genesis_meta,
            },
            "rollback_contract": {
                "action": "disable_writers_and_preserve_store",
                "destructive_cleanup_allowed": False,
                "quarantine_before_replacement_required": True,
            },
        }
        proposed_receipt = {
            "schema_version": FRESH_INSTALL_MATERIALIZATION_RECEIPT_SCHEMA_VERSION,
            "observed_at": completed_at.isoformat(),
            "ok": True,
            "migration_receipt": migration_observation,
            "genesis_intent": intent_observation,
            "parent_directory": {
                **parent_after,
            },
            "installation_lock": {
                "path": str(lock_path),
                "device": int(lock_stat.st_dev),
                "inode": int(lock_stat.st_ino),
                "owner_pid": os.getpid(),
                "owner_uid": os.getuid(),
                "mode": f"{stat.S_IMODE(lock_stat.st_mode):04o}",
            },
            "destination_absence_before": absence,
            "writer_stop_evidence": stopped_for_receipt,
            "process_probes": process_probes,
            **material,
            "materialization_binding_sha256": hashlib.sha256(
                _json_payload(material)
            ).hexdigest(),
        }
        if receipt_path.exists() or receipt_path.is_symlink():
            receipt, _receipt_raw, receipt_observation = _read_json_regular_file(
                receipt_path
            )
            if (
                receipt.get("schema_version")
                != FRESH_INSTALL_MATERIALIZATION_RECEIPT_SCHEMA_VERSION
                or receipt.get("ok") is not True
                or receipt.get("migration_receipt_raw_sha256")
                != migration_observation["sha256"]
                or receipt.get("candidate_commit") != candidate_commit
                or receipt.get("config_sha256") != config_sha256
                or receipt.get("configured_databases") != configured
                or receipt.get("audit") != audit
                or receipt.get("capacity_identity") != capacity_identity
                or receipt.get("capacity_transition") != capacity_transition
                or receipt.get("capacity_initialization_receipt")
                != capacity_receipt_observation
                or receipt.get("genesis_intent_sha256")
                != genesis_intent_sha256
                or receipt.get("db_instance_id") != intent["db_instance_id"]
                or receipt.get("materialization_journal") != journal
                or not isinstance(receipt.get("destination"), Mapping)
                or receipt["destination"].get("artifact") != destination_artifact
            ):
                raise MigrationDrillError("fresh_install_existing_receipt_invalid")
            completed_at = _parse_timestamp(
                receipt.get("completed_at"),
                code="fresh_install_existing_receipt_invalid",
            )
        else:
            receipt = proposed_receipt
            receipt_observation = _write_json_no_clobber(receipt_path, receipt)
        receipted_path = _fresh_install_journal_path(
            evidence_root, journal_id, "receipted"
        )
        receipted_phase = {
            "schema_version": FRESH_INSTALL_JOURNAL_SCHEMA_VERSION,
            "phase": "receipted",
            "journal_id": journal_id,
            "maintenance_token_sha256": maintenance_token_sha256,
            "installed_sha256": installed_observation["sha256"],
            "receipt": receipt_observation,
            "receipted_at": completed_at.isoformat(),
            "maintenance_release_required": True,
        }
        _write_json_no_clobber(receipted_path, receipted_phase)
        if marker is None or marker_observation is None:
            raise MigrationDrillError("fresh_install_maintenance_missing")
        validated_receipt = _validate_materialization_receipt(
            receipt_path=receipt_path,
            control_path=control_path,
            delivery_path=delivery_path,
            config_sha256=config_sha256,
            audit=audit,
            artifact_path=control_path,
            artifact_path_may_differ=False,
            live_present=True,
            maintenance_present=True,
            tombstone_present=False,
        )
        receipt = validated_receipt["receipt"]
        _remove_exact_json_file(maintenance_path, expected=marker)
        return {
            **receipt,
            "applied": not receipt_recovered,
            "idempotent": False,
            "recovered": receipt_recovered,
        }
    except MigrationDrillError as exc:
        record_failure(exc.code, installed=installed)
        raise
    finally:
        if stage_path is not None and not installed:
            stage_path.unlink(missing_ok=True)
        if lock_fd >= 0:
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
            finally:
                os.close(lock_fd)
        if parent_fd >= 0:
            os.close(parent_fd)


def _require_shared_absolute_database_paths(
    control_db_path: str | Path,
    delivery_db_path: str | Path,
) -> tuple[Path, Path]:
    raw_control = Path(control_db_path).expanduser()
    raw_delivery = Path(delivery_db_path).expanduser()
    if not raw_control.is_absolute() or not raw_delivery.is_absolute():
        raise MigrationDrillError("fresh_install_configured_database_not_absolute")
    control_path = _canonical_configured_path(raw_control)
    delivery_path = _canonical_configured_path(raw_delivery)
    if control_path != delivery_path:
        raise MigrationDrillError("fresh_install_configured_database_mismatch")
    return control_path, delivery_path


def _materialization_audit_from_receipt(path: Path) -> dict[str, Any]:
    try:
        receipt, _raw, _observation = _read_json_regular_file(path)
    except MigrationDrillError as exc:
        raise MigrationDrillError("fresh_install_materialization_receipt_invalid") from exc
    audit = receipt.get("audit")
    if not isinstance(audit, Mapping) or set(audit) != {
        "release_id_sha256",
        "operator_sha256",
        "reason_sha256",
    }:
        raise MigrationDrillError("fresh_install_materialization_receipt_invalid")
    for value in audit.values():
        _require_sha256(
            value,
            code="fresh_install_materialization_receipt_invalid",
        )
    return dict(audit)


def _rollback_transaction_id(
    *,
    operation: str,
    source_receipt_sha256: str,
    configured_database: Path,
    config_sha256: str,
    audit: Mapping[str, Any],
) -> str:
    return hashlib.sha256(
        _json_payload({
            "operation": operation,
            "source_receipt_sha256": source_receipt_sha256,
            "configured_database": str(configured_database),
            "config_sha256": config_sha256,
            "audit": dict(audit),
        })
    ).hexdigest()


def _read_rollback_phase(
    path: Path,
    *,
    operation: str,
    phase: str,
    transaction_id: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    try:
        body, _raw, observation = _read_json_regular_file(path)
    except MigrationDrillError as exc:
        raise MigrationDrillError("fresh_install_rollback_journal_invalid") from exc
    if (
        body.get("schema_version")
        != FRESH_INSTALL_ROLLBACK_JOURNAL_SCHEMA_VERSION
        or body.get("operation") != operation
        or body.get("phase") != phase
        or body.get("transaction_id") != transaction_id
    ):
        raise MigrationDrillError("fresh_install_rollback_journal_invalid")
    return body, observation


def _validate_quarantine_receipt(
    *,
    receipt_path: Path,
    materialization_receipt_path: Path,
    control_path: Path,
    delivery_path: Path,
    config_sha256: str,
    audit: Mapping[str, Any],
    evidence_root: Path,
    quarantine_root: Path,
    maintenance_present: bool,
    quarantined_state: bool = True,
    allow_transient_quarantine_link: bool = False,
) -> dict[str, Any]:
    try:
        receipt, receipt_raw, receipt_observation = _read_json_regular_file(
            receipt_path
        )
    except MigrationDrillError as exc:
        raise MigrationDrillError("fresh_install_quarantine_receipt_invalid") from exc
    expected_keys = {
        "schema_version",
        "observed_at",
        "completed_at",
        "ok",
        "operation",
        "transaction_id",
        "config_sha256",
        "audit",
        "configured_databases",
        "materialization_receipt",
        "materialization_receipt_raw_sha256",
        "db_instance_id",
        "genesis_intent_sha256",
        "maintenance",
        "writer_stop_evidence",
        "writer_stop_evidence_max_age_seconds",
        "process_probes",
        "quarantine_directory",
        "quarantine_artifact",
        "live_absence_after",
        "tombstone",
        "journal",
        "runtime_identities",
        "quarantine_binding_sha256",
    }
    configured = {
        "control": str(control_path),
        "delivery": str(delivery_path),
        "same_database": True,
    }
    if (
        set(receipt) != expected_keys
        or receipt.get("schema_version")
        != FRESH_INSTALL_QUARANTINE_RECEIPT_SCHEMA_VERSION
        or receipt.get("ok") is not True
        or receipt.get("operation") != "quarantine"
        or receipt.get("config_sha256") != config_sha256
        or receipt.get("audit") != dict(audit)
        or receipt.get("configured_databases") != configured
    ):
        raise MigrationDrillError("fresh_install_quarantine_receipt_invalid")
    transaction_id = _require_sha256(
        receipt.get("transaction_id"),
        code="fresh_install_quarantine_receipt_invalid",
    )
    source_sha256 = _require_sha256(
        receipt.get("materialization_receipt_raw_sha256"),
        code="fresh_install_quarantine_receipt_invalid",
    )
    if transaction_id != _rollback_transaction_id(
        operation="quarantine",
        source_receipt_sha256=source_sha256,
        configured_database=control_path,
        config_sha256=config_sha256,
        audit=audit,
    ):
        raise MigrationDrillError("fresh_install_quarantine_receipt_invalid")
    quarantine_artifact_value = receipt.get("quarantine_artifact")
    if not isinstance(quarantine_artifact_value, Mapping):
        raise MigrationDrillError("fresh_install_quarantine_receipt_invalid")
    quarantine_path = Path(str(quarantine_artifact_value.get("path") or ""))
    try:
        quarantine_path.parent.resolve(strict=True).relative_to(quarantine_root)
    except (OSError, ValueError) as exc:
        raise MigrationDrillError("fresh_install_quarantine_receipt_invalid") from exc
    materialization_audit = _materialization_audit_from_receipt(
        materialization_receipt_path
    )
    materialization = _validate_materialization_receipt(
        receipt_path=materialization_receipt_path,
        control_path=control_path,
        delivery_path=delivery_path,
        config_sha256=config_sha256,
        audit=materialization_audit,
        artifact_path=quarantine_path,
        artifact_path_may_differ=True,
        live_present=False,
        maintenance_present=maintenance_present,
        tombstone_present=True,
        enforce_live_state=quarantined_state,
        allowed_artifact_link_counts=(
            frozenset({1, 2})
            if allow_transient_quarantine_link
            else frozenset({1})
        ),
    )
    if (
        materialization["receipt_observation"]
        != receipt.get("materialization_receipt")
        or hashlib.sha256(materialization["receipt_raw"]).hexdigest()
        != source_sha256
        or materialization["receipt"].get("db_instance_id")
        != receipt.get("db_instance_id")
        or materialization["receipt"].get("genesis_intent_sha256")
        != receipt.get("genesis_intent_sha256")
        or materialization["artifact"] != quarantine_artifact_value
    ):
        raise MigrationDrillError("fresh_install_quarantine_receipt_invalid")
    if receipt.get("quarantine_directory") != _secure_directory_observation(
        quarantine_root,
        code="fresh_install_quarantine_directory_invalid",
    )[1]:
        raise MigrationDrillError("fresh_install_quarantine_receipt_invalid")
    if quarantined_state:
        if control_path.exists() or control_path.is_symlink():
            raise MigrationDrillError("fresh_install_quarantine_live_present")
        state = _require_no_sqlite_sidecars(
            control_path,
            tombstone_present=True,
            maintenance_present=maintenance_present,
            code="fresh_install_quarantine_live_state_invalid",
        )
        if state != receipt.get("live_absence_after"):
            raise MigrationDrillError("fresh_install_quarantine_receipt_invalid")
    else:
        historical_state = receipt.get("live_absence_after")
        if (
            not isinstance(historical_state, Mapping)
            or set(historical_state) != {"database", "wal", "shm", "journal", "tombstone"}
            or historical_state["database"].get("present") is not False
            or any(
                historical_state[name].get("present") is not False
                for name in ("wal", "shm", "journal")
            )
            or historical_state["tombstone"].get("present") is not True
        ):
            raise MigrationDrillError("fresh_install_quarantine_receipt_invalid")
    tombstone_value = receipt.get("tombstone")
    if not isinstance(tombstone_value, Mapping) or set(tombstone_value) != {
        "body",
        "observation",
    }:
        raise MigrationDrillError("fresh_install_quarantine_receipt_invalid")
    tombstone_path = Path(f"{control_path}.pnc-rca-tombstone")
    if quarantined_state:
        try:
            tombstone, _tombstone_raw, tombstone_observation = (
                _read_json_regular_file(tombstone_path)
            )
        except MigrationDrillError as exc:
            raise MigrationDrillError("fresh_install_quarantine_receipt_invalid") from exc
    else:
        tombstone = tombstone_value.get("body")
        tombstone_observation = tombstone_value.get("observation")
    if (
        not isinstance(tombstone, Mapping)
        or not isinstance(tombstone_observation, Mapping)
        or tombstone != tombstone_value["body"]
        or tombstone_observation != tombstone_value["observation"]
        or set(tombstone)
        != {
            "schema_version",
            "state",
            "created_at",
            "transaction_id",
            "configured_database",
            "materialization_receipt_raw_sha256",
            "config_sha256",
            "audit",
            "db_instance_id",
            "genesis_intent_sha256",
            "quarantine_artifact",
        }
        or tombstone.get("schema_version") != FRESH_INSTALL_TOMBSTONE_SCHEMA_VERSION
        or tombstone.get("state") != "quarantined"
        or tombstone.get("transaction_id") != transaction_id
        or tombstone.get("configured_database") != str(control_path)
        or tombstone.get("materialization_receipt_raw_sha256") != source_sha256
        or tombstone.get("config_sha256") != config_sha256
        or tombstone.get("audit") != dict(audit)
        or tombstone.get("quarantine_artifact") != quarantine_artifact_value
    ):
        raise MigrationDrillError("fresh_install_quarantine_receipt_invalid")
    journal = receipt.get("journal")
    expected_phases = ("prepared", "linked", "unlinked", "tombstoned")
    if not isinstance(journal, Mapping) or set(journal) != set(expected_phases):
        raise MigrationDrillError("fresh_install_quarantine_receipt_invalid")
    phase_bodies: dict[str, Any] = {}
    phase_observations: dict[str, Any] = {}
    for phase in expected_phases:
        path = _rollback_journal_path(
            evidence_root, "quarantine", transaction_id, phase
        )
        body, observation = _read_rollback_phase(
            path,
            operation="quarantine",
            phase=phase,
            transaction_id=transaction_id,
        )
        if observation != journal[phase]:
            raise MigrationDrillError("fresh_install_quarantine_receipt_invalid")
        phase_bodies[phase] = body
        phase_observations[phase] = observation
    prepared = phase_bodies["prepared"]
    linked = phase_bodies["linked"]
    unlinked = phase_bodies["unlinked"]
    tombstoned = phase_bodies["tombstoned"]
    maintenance = receipt.get("maintenance")
    runtime_identities = receipt.get("runtime_identities")
    if (
        not isinstance(maintenance, Mapping)
        or set(maintenance)
        != {"path", "maintenance_token_sha256", "observation"}
        or maintenance.get("path") != f"{control_path}.pnc-rca-maintenance"
        or maintenance.get("maintenance_token_sha256")
        != prepared.get("maintenance_token_sha256")
        or not isinstance(maintenance.get("observation"), Mapping)
        or set(maintenance["observation"]) != _FILE_OBSERVATION_KEYS
        or not isinstance(runtime_identities, Mapping)
        or set(runtime_identities) != {"started", "completed"}
    ):
        raise MigrationDrillError("fresh_install_quarantine_receipt_invalid")
    started_identity = _validate_runtime_identity_record(
        runtime_identities["started"],
        code="fresh_install_quarantine_receipt_invalid",
    )
    completed_identity = _validate_runtime_identity_record(
        runtime_identities["completed"],
        code="fresh_install_quarantine_receipt_invalid",
    )
    if (
        prepared.get("materialization_receipt")
        != materialization["receipt_observation"]
        or prepared.get("materialization_receipt_raw_sha256") != source_sha256
        or prepared.get("expected_live_artifact")
        != materialization["receipt"]["destination"]["artifact"]
        or prepared.get("config_sha256") != config_sha256
        or prepared.get("audit") != dict(audit)
        or prepared.get("runtime_identity") != started_identity
        or linked.get("prepared_sha256")
        != phase_observations["prepared"]["sha256"]
        or linked.get("quarantine_artifact") != quarantine_artifact_value
        or unlinked.get("linked_sha256")
        != phase_observations["linked"]["sha256"]
        or unlinked.get("quarantine_artifact") != quarantine_artifact_value
        or tombstoned.get("unlinked_sha256")
        != phase_observations["unlinked"]["sha256"]
        or tombstoned.get("tombstone") != tombstone_observation
        or tombstoned.get("runtime_identity") != completed_identity
    ):
        raise MigrationDrillError("fresh_install_quarantine_receipt_invalid")
    process_probes = _validate_recorded_process_probes(
        receipt.get("process_probes"),
        expected_names={
            "initial",
            "locked",
            "post_maintenance",
            "immediate_pre_unlink",
            "post_unlink",
        },
        code="fresh_install_quarantine_receipt_invalid",
    )
    if (
        prepared.get("process_probes")
        != {
            name: process_probes[name]
            for name in ("initial", "locked", "post_maintenance")
        }
        or unlinked.get("process_probes")
        != {
            name: process_probes[name]
            for name in ("immediate_pre_unlink", "post_unlink")
        }
    ):
        raise MigrationDrillError("fresh_install_quarantine_receipt_invalid")
    completed_at = _parse_timestamp(
        receipt.get("completed_at"),
        code="fresh_install_quarantine_receipt_invalid",
    )
    if receipt.get("observed_at") != completed_at.isoformat():
        raise MigrationDrillError("fresh_install_quarantine_receipt_invalid")
    prepared_at = _parse_timestamp(
        prepared.get("prepared_at"),
        code="fresh_install_quarantine_receipt_invalid",
    )
    unlinked_at = _parse_timestamp(
        unlinked.get("unlinked_at"),
        code="fresh_install_quarantine_receipt_invalid",
    )
    writer_stop_proofs = receipt.get("writer_stop_evidence")
    writer_stop_max_ages = receipt.get("writer_stop_evidence_max_age_seconds")
    if (
        not isinstance(writer_stop_proofs, Mapping)
        or set(writer_stop_proofs) != {"initial", "completion"}
        or not isinstance(writer_stop_max_ages, Mapping)
        or set(writer_stop_max_ages) != {"initial", "completion"}
        or any(
            not isinstance(writer_stop_max_ages[name], int)
            or writer_stop_max_ages[name] < 1
            for name in ("initial", "completion")
        )
        or not isinstance(unlinked.get("writer_stop_max_age_seconds"), int)
        or unlinked["writer_stop_max_age_seconds"] < 1
        or tombstoned.get("tombstoned_at") != completed_at.isoformat()
        or not (prepared_at <= unlinked_at <= completed_at)
    ):
        raise MigrationDrillError("fresh_install_quarantine_receipt_invalid")
    try:
        initial_stopped = validate_writer_stop_evidence(
            writer_stop_proofs["initial"],
            now=prepared_at,
            max_age_seconds=writer_stop_max_ages["initial"],
        )
        action_stopped = validate_writer_stop_evidence(
            unlinked.get("writer_stop_evidence"),
            now=unlinked_at,
            max_age_seconds=unlinked["writer_stop_max_age_seconds"],
        )
        completion_stopped = validate_writer_stop_evidence(
            writer_stop_proofs["completion"],
            now=completed_at,
            max_age_seconds=writer_stop_max_ages["completion"],
        )
    except MigrationDrillError as exc:
        raise MigrationDrillError("fresh_install_quarantine_receipt_invalid") from exc
    if (
        prepared.get("writer_stop_evidence") != initial_stopped
        or tombstoned.get("writer_stop_evidence") != completion_stopped
        or prepared.get("writer_stop_max_age_seconds")
        != writer_stop_max_ages["initial"]
        or tombstoned.get("writer_stop_max_age_seconds")
        != writer_stop_max_ages["completion"]
        or unlinked.get("writer_stop_evidence") != action_stopped
        or _parse_timestamp(
            completion_stopped.get("observed_at"),
            code="fresh_install_quarantine_receipt_invalid",
        )
        < _parse_timestamp(
            initial_stopped.get("observed_at"),
            code="fresh_install_quarantine_receipt_invalid",
        )
    ):
        raise MigrationDrillError("fresh_install_quarantine_receipt_invalid")
    binding_keys = expected_keys - {
        "schema_version",
        "observed_at",
        "ok",
        "operation",
        "quarantine_binding_sha256",
    }
    binding = {key: receipt[key] for key in binding_keys}
    if hashlib.sha256(_json_payload(binding)).hexdigest() != receipt.get(
        "quarantine_binding_sha256"
    ):
        raise MigrationDrillError("fresh_install_quarantine_receipt_invalid")
    receipted_path = _rollback_journal_path(
        evidence_root, "quarantine", transaction_id, "receipted"
    )
    receipted, _receipted_observation = _read_rollback_phase(
        receipted_path,
        operation="quarantine",
        phase="receipted",
        transaction_id=transaction_id,
    )
    if (
        receipted.get("tombstoned_sha256")
        != phase_observations["tombstoned"]["sha256"]
        or receipted.get("receipt") != receipt_observation
        or receipted.get("receipted_at") != completed_at.isoformat()
        or receipted.get("maintenance_release_required") is not True
    ):
        raise MigrationDrillError("fresh_install_quarantine_receipt_invalid")
    return {
        "receipt": receipt,
        "receipt_raw": receipt_raw,
        "receipt_observation": receipt_observation,
        "materialization": materialization,
        "quarantine_artifact": materialization["artifact"],
        "tombstone": tombstone,
        "tombstone_observation": tombstone_observation,
        "journal_bodies": phase_bodies,
        "journal_observations": phase_observations,
    }


def quarantine_fresh_install(
    *,
    materialization_receipt_path: str | Path,
    control_db_path: str | Path,
    delivery_db_path: str | Path,
    config_sha256: str,
    evidence_dir: str | Path,
    quarantine_dir: str | Path,
    writer_stop_evidence: Mapping[str, Any],
    apply: bool = False,
    now: datetime | None = None,
    max_writer_stop_age_seconds: int = 900,
    writer_process_probe: Any | None = None,
    release_id: str | None = None,
    operator: str | None = None,
    reason: str | None = None,
) -> dict[str, Any]:
    """Reversibly quarantine a materialized greenfield DB without replacing it."""
    started_at = _utc(now)
    audit = _fresh_install_audit_hashes(
        release_id=release_id,
        operator=operator,
        reason=reason,
        required=apply,
    )
    if (
        not isinstance(max_writer_stop_age_seconds, int)
        or max_writer_stop_age_seconds < 1
    ):
        raise MigrationDrillError("writer_stop_evidence_max_age_invalid")
    _require_sha256(config_sha256, code="fresh_install_config_sha256_invalid")
    control_path, delivery_path = _require_shared_absolute_database_paths(
        control_db_path, delivery_db_path
    )
    evidence_root, _evidence_observation = _owner_only_directory_observation(
        evidence_dir,
        code="fresh_install_quarantine_evidence_directory_invalid",
    )
    quarantine_root, quarantine_root_observation = (
        _owner_only_directory_observation(
            quarantine_dir,
            code="fresh_install_quarantine_directory_invalid",
        )
    )
    parent, parent_observation = _secure_directory_observation(
        control_path.parent,
        code="fresh_install_directory_invalid",
    )
    if quarantine_root_observation["device"] != parent_observation["device"]:
        raise MigrationDrillError("fresh_install_quarantine_cross_device")
    materialization_path = _absolute(materialization_receipt_path).absolute()
    materialization_audit = _materialization_audit_from_receipt(materialization_path)
    materialization_receipt, materialization_raw, materialization_observation = (
        _read_json_regular_file(materialization_path)
    )
    if materialization_receipt.get("config_sha256") != config_sha256:
        raise MigrationDrillError("fresh_install_quarantine_config_mismatch")
    source_sha256 = hashlib.sha256(materialization_raw).hexdigest()
    if audit is None:
        audit_for_id = {
            name: "0" * 64
            for name in ("release_id_sha256", "operator_sha256", "reason_sha256")
        }
    else:
        audit_for_id = audit
    transaction_id = _rollback_transaction_id(
        operation="quarantine",
        source_receipt_sha256=source_sha256,
        configured_database=control_path,
        config_sha256=config_sha256,
        audit=audit_for_id,
    )
    receipt_path = evidence_root / "fresh_install_quarantine_receipt.json"
    maintenance_path = Path(f"{control_path}.pnc-rca-maintenance")
    tombstone_path = Path(f"{control_path}.pnc-rca-tombstone")
    quarantine_path = quarantine_root / (
        f"{control_path.name}.{transaction_id}.quarantined.sqlite3"
    )
    process_probe = writer_process_probe or _default_writer_process_probe
    stopped = validate_writer_stop_evidence(
        writer_stop_evidence,
        now=started_at,
        max_age_seconds=max_writer_stop_age_seconds,
    )
    initial_probe = _checked_writer_process_probe(process_probe)
    if receipt_path.exists() or receipt_path.is_symlink():
        receipt_preview, _preview_raw, _preview_observation = (
            _read_json_regular_file(receipt_path)
        )
        if audit is None:
            audit_for_id = dict(receipt_preview.get("audit") or {})
        if (
            receipt_preview.get("schema_version")
            != FRESH_INSTALL_QUARANTINE_RECEIPT_SCHEMA_VERSION
            or receipt_preview.get("config_sha256") != config_sha256
            or receipt_preview.get("audit") != audit_for_id
        ):
            raise MigrationDrillError("fresh_install_quarantine_receipt_invalid")
        if maintenance_path.exists() or maintenance_path.is_symlink():
            if not apply:
                return {
                    "schema_version": "pnc_rca_fresh_install_quarantine_plan_v1",
                    "applied": False,
                    "idempotent": False,
                    "recovery_required": True,
                    "transaction_id": receipt_preview.get("transaction_id"),
                    "configured_database": str(control_path),
                    "receipt_present": True,
                    "initial_process_probe": initial_probe,
                }
        else:
            validated = _validate_quarantine_receipt(
                receipt_path=receipt_path,
                materialization_receipt_path=materialization_path,
                control_path=control_path,
                delivery_path=delivery_path,
                config_sha256=config_sha256,
                audit=audit_for_id,
                evidence_root=evidence_root,
                quarantine_root=quarantine_root,
                maintenance_present=False,
            )
        if not apply and not maintenance_path.exists():
            return {
                "schema_version": "pnc_rca_fresh_install_quarantine_plan_v1",
                "applied": False,
                "idempotent": True,
                "recovery_required": maintenance_path.exists(),
                "transaction_id": validated["receipt"]["transaction_id"],
                "configured_database": str(control_path),
                "quarantine_artifact": validated["quarantine_artifact"],
                "initial_process_probe": initial_probe,
            }
        if audit is None or receipt_preview.get("audit") != audit:
            raise MigrationDrillError("fresh_install_quarantine_receipt_conflict")
        if maintenance_path.exists() or maintenance_path.is_symlink():
            # A completed receipt with an owned marker resumes under the lock below.
            pass
        else:
            return {
                **validated["receipt"],
                "applied": False,
                "idempotent": True,
                "recovered": False,
            }
    live_present = control_path.exists() or control_path.is_symlink()
    marker_present = maintenance_path.exists() or maintenance_path.is_symlink()
    artifact_for_validation = control_path if live_present else quarantine_path
    if not artifact_for_validation.exists() or artifact_for_validation.is_symlink():
        raise MigrationDrillError("fresh_install_quarantine_source_missing")
    materialization = _validate_materialization_receipt(
        receipt_path=materialization_path,
        control_path=control_path,
        delivery_path=delivery_path,
        config_sha256=config_sha256,
        audit=materialization_audit,
        artifact_path=artifact_for_validation,
        artifact_path_may_differ=not live_present,
        live_present=live_present,
        maintenance_present=marker_present,
        tombstone_present=(tombstone_path.exists() or tombstone_path.is_symlink()),
        allowed_artifact_link_counts=(
            frozenset({1, 2}) if marker_present else frozenset({1})
        ),
    )
    if not apply:
        return {
            "schema_version": "pnc_rca_fresh_install_quarantine_plan_v1",
            "applied": False,
            "idempotent": False,
            "recovery_required": marker_present,
            "transaction_id": transaction_id,
            "configured_database": str(control_path),
            "materialization_receipt_raw_sha256": source_sha256,
            "expected_live_artifact": materialization["receipt"]["destination"][
                "artifact"
            ],
            "quarantine_path": str(quarantine_path),
            "destination_state": _fresh_destination_state(control_path),
            "initial_process_probe": initial_probe,
            "apply_audit_required": True,
        }
    if audit is None:
        raise MigrationDrillError("fresh_install_quarantine_audit_invalid")
    runtime_identity = _fresh_install_runtime_identity()
    recovered = marker_present
    with _fresh_install_lock(parent, parent_observation) as (_parent_fd, lock_stat):
        locked_probe = _checked_writer_process_probe(process_probe)
        if (
            (receipt_path.exists() or receipt_path.is_symlink())
            and not (maintenance_path.exists() or maintenance_path.is_symlink())
        ):
            validated = _validate_quarantine_receipt(
                receipt_path=receipt_path,
                materialization_receipt_path=materialization_path,
                control_path=control_path,
                delivery_path=delivery_path,
                config_sha256=config_sha256,
                audit=audit,
                evidence_root=evidence_root,
                quarantine_root=quarantine_root,
                maintenance_present=maintenance_path.exists(),
            )
            if not maintenance_path.exists():
                return {
                    **validated["receipt"],
                    "applied": False,
                    "idempotent": True,
                    "recovered": False,
                }
        if maintenance_path.exists() or maintenance_path.is_symlink():
            marker, _marker_raw, marker_observation = _read_json_regular_file(
                maintenance_path
            )
            if (
                marker.get("schema_version")
                != FRESH_INSTALL_ROLLBACK_MAINTENANCE_SCHEMA_VERSION
                or marker.get("operation") != "quarantine"
                or marker.get("state") != "active"
                or marker.get("transaction_id") != transaction_id
                or marker.get("configured_database") != str(control_path)
                or marker.get("source_receipt_raw_sha256") != source_sha256
                or marker.get("config_sha256") != config_sha256
                or marker.get("audit") != audit
            ):
                raise MigrationDrillError("fresh_install_quarantine_maintenance_conflict")
            maintenance_token = _require_sha256(
                marker.get("maintenance_token_sha256"),
                code="fresh_install_quarantine_maintenance_conflict",
            )
            recovered = True
        else:
            if tombstone_path.exists() or tombstone_path.is_symlink():
                raise MigrationDrillError("fresh_install_quarantine_tombstone_conflict")
            maintenance_token = hashlib.sha256(uuid.uuid4().bytes).hexdigest()
            marker = {
                "schema_version": FRESH_INSTALL_ROLLBACK_MAINTENANCE_SCHEMA_VERSION,
                "operation": "quarantine",
                "state": "active",
                "created_at": started_at.isoformat(),
                "transaction_id": transaction_id,
                "maintenance_token_sha256": maintenance_token,
                "configured_database": str(control_path),
                "source_receipt_raw_sha256": source_sha256,
                "config_sha256": config_sha256,
                "audit": audit,
                "runtime_identity": runtime_identity,
            }
            marker_observation = _write_json_no_clobber(maintenance_path, marker)
        post_maintenance_probe = _checked_writer_process_probe(process_probe)
        validate_writer_stop_evidence(
            writer_stop_evidence,
            now=_utc(now),
            max_age_seconds=max_writer_stop_age_seconds,
        )
        live_present = control_path.exists() or control_path.is_symlink()
        source_artifact_path = control_path if live_present else quarantine_path
        materialization = _validate_materialization_receipt(
            receipt_path=materialization_path,
            control_path=control_path,
            delivery_path=delivery_path,
            config_sha256=config_sha256,
            audit=materialization_audit,
            artifact_path=source_artifact_path,
            artifact_path_may_differ=not live_present,
            live_present=live_present,
            maintenance_present=True,
            tombstone_present=(tombstone_path.exists() or tombstone_path.is_symlink()),
            allowed_artifact_link_counts=frozenset({1, 2}),
        )
        expected_live_artifact = materialization["receipt"]["destination"]["artifact"]
        prepared_path = _rollback_journal_path(
            evidence_root, "quarantine", transaction_id, "prepared"
        )
        if prepared_path.exists() or prepared_path.is_symlink():
            prepared, prepared_observation = _read_rollback_phase(
                prepared_path,
                operation="quarantine",
                phase="prepared",
                transaction_id=transaction_id,
            )
            if (
                prepared.get("maintenance_token_sha256") != maintenance_token
                or prepared.get("materialization_receipt")
                != materialization_observation
                or prepared.get("materialization_receipt_raw_sha256")
                != source_sha256
                or prepared.get("expected_live_artifact") != expected_live_artifact
                or prepared.get("quarantine_path") != str(quarantine_path)
                or prepared.get("config_sha256") != config_sha256
                or prepared.get("audit") != audit
                or not isinstance(
                    prepared.get("writer_stop_max_age_seconds"), int
                )
                or prepared["writer_stop_max_age_seconds"] < 1
            ):
                raise MigrationDrillError("fresh_install_rollback_journal_invalid")
        else:
            if not live_present:
                raise MigrationDrillError("fresh_install_quarantine_prepared_missing")
            prepared = {
                "schema_version": FRESH_INSTALL_ROLLBACK_JOURNAL_SCHEMA_VERSION,
                "operation": "quarantine",
                "phase": "prepared",
                "transaction_id": transaction_id,
                "prepared_at": str(marker["created_at"]),
                "maintenance_token_sha256": maintenance_token,
                "configured_database": str(control_path),
                "materialization_receipt": materialization_observation,
                "materialization_receipt_raw_sha256": source_sha256,
                "expected_live_artifact": expected_live_artifact,
                "db_instance_id": materialization["receipt"]["db_instance_id"],
                "genesis_intent_sha256": materialization["receipt"][
                    "genesis_intent_sha256"
                ],
                "quarantine_path": str(quarantine_path),
                "config_sha256": config_sha256,
                "audit": audit,
                "writer_stop_evidence": stopped,
                "writer_stop_max_age_seconds": max_writer_stop_age_seconds,
                "process_probes": {
                    "initial": initial_probe,
                    "locked": locked_probe,
                    "post_maintenance": post_maintenance_probe,
                },
                "runtime_identity": marker["runtime_identity"],
            }
            prepared_observation = _write_json_no_clobber(prepared_path, prepared)
        if quarantine_path.exists() or quarantine_path.is_symlink():
            quarantine_artifact = _require_exact_artifact(
                quarantine_path,
                expected_live_artifact,
                allow_path_mismatch=True,
                allowed_link_counts=(
                    frozenset({2}) if live_present else frozenset({1})
                ),
                code="fresh_install_quarantine_artifact_conflict",
            )
            if live_present:
                live_artifact = _require_exact_artifact(
                    control_path,
                    expected_live_artifact,
                    allowed_link_counts=frozenset({2}),
                    code="fresh_install_quarantine_live_changed",
                )
                if (live_artifact["device"], live_artifact["inode"]) != (
                    quarantine_artifact["device"],
                    quarantine_artifact["inode"],
                ):
                    raise MigrationDrillError("fresh_install_quarantine_link_conflict")
        else:
            if not live_present:
                raise MigrationDrillError("fresh_install_quarantine_artifact_missing")
            _require_no_sqlite_sidecars(
                control_path,
                tombstone_present=False,
                maintenance_present=True,
                code="fresh_install_quarantine_sidecar_present",
            )
            live_artifact = _require_exact_artifact(
                control_path,
                expected_live_artifact,
                allowed_link_counts=frozenset({1}),
                code="fresh_install_quarantine_live_changed",
            )
            try:
                os.link(control_path, quarantine_path, follow_symlinks=False)
                _fsync_directory(quarantine_root)
            except FileExistsError as exc:
                raise MigrationDrillError("fresh_install_quarantine_link_race") from exc
            except OSError as exc:
                raise MigrationDrillError("fresh_install_quarantine_link_failed") from exc
            quarantine_artifact = _require_exact_artifact(
                quarantine_path,
                expected_live_artifact,
                allow_path_mismatch=True,
                allowed_link_counts=frozenset({2}),
                code="fresh_install_quarantine_artifact_invalid",
            )
            if (live_artifact["device"], live_artifact["inode"]) != (
                quarantine_artifact["device"],
                quarantine_artifact["inode"],
            ):
                raise MigrationDrillError("fresh_install_quarantine_link_invalid")
        linked_path = _rollback_journal_path(
            evidence_root, "quarantine", transaction_id, "linked"
        )
        linked_value = {
            "schema_version": FRESH_INSTALL_ROLLBACK_JOURNAL_SCHEMA_VERSION,
            "operation": "quarantine",
            "phase": "linked",
            "transaction_id": transaction_id,
            "maintenance_token_sha256": maintenance_token,
            "prepared_sha256": prepared_observation["sha256"],
            "linked_at": _utc(now).isoformat(),
            "quarantine_artifact": quarantine_artifact,
        }
        if linked_path.exists() or linked_path.is_symlink():
            linked, linked_observation = _read_rollback_phase(
                linked_path,
                operation="quarantine",
                phase="linked",
                transaction_id=transaction_id,
            )
            if (
                linked.get("maintenance_token_sha256") != maintenance_token
                or linked.get("prepared_sha256") != prepared_observation["sha256"]
                or linked.get("quarantine_artifact") != quarantine_artifact
            ):
                raise MigrationDrillError("fresh_install_rollback_journal_invalid")
        else:
            linked = linked_value
            linked_observation = _write_json_no_clobber(linked_path, linked)
        immediate_pre_unlink_probe = _checked_writer_process_probe(process_probe)
        pre_unlink_stopped = validate_writer_stop_evidence(
            writer_stop_evidence,
            now=_utc(now),
            max_age_seconds=max_writer_stop_age_seconds,
        )
        if control_path.exists() or control_path.is_symlink():
            _require_no_sqlite_sidecars(
                control_path,
                tombstone_present=False,
                maintenance_present=True,
                code="fresh_install_quarantine_sidecar_present",
            )
            live_artifact = _require_exact_artifact(
                control_path,
                expected_live_artifact,
                allowed_link_counts=frozenset({2}),
                code="fresh_install_quarantine_live_changed",
            )
            if (live_artifact["device"], live_artifact["inode"]) != (
                quarantine_artifact["device"],
                quarantine_artifact["inode"],
            ):
                raise MigrationDrillError("fresh_install_quarantine_link_changed")
            _unlink_exact_artifact(
                control_path,
                expected=live_artifact,
                code="fresh_install_quarantine_unlink_failed",
            )
        post_unlink_probe = _checked_writer_process_probe(process_probe)
        if control_path.exists() or control_path.is_symlink():
            raise MigrationDrillError("fresh_install_quarantine_live_still_present")
        quarantine_artifact = _require_exact_artifact(
            quarantine_path,
            quarantine_artifact,
            allowed_link_counts=frozenset({1}),
            code="fresh_install_quarantine_artifact_invalid",
        )
        unlinked_path = _rollback_journal_path(
            evidence_root, "quarantine", transaction_id, "unlinked"
        )
        if unlinked_path.exists() or unlinked_path.is_symlink():
            unlinked, unlinked_observation = _read_rollback_phase(
                unlinked_path,
                operation="quarantine",
                phase="unlinked",
                transaction_id=transaction_id,
            )
            if (
                unlinked.get("linked_sha256") != linked_observation["sha256"]
                or unlinked.get("quarantine_artifact") != quarantine_artifact
                or not isinstance(
                    unlinked.get("writer_stop_max_age_seconds"), int
                )
                or unlinked["writer_stop_max_age_seconds"] < 1
            ):
                raise MigrationDrillError("fresh_install_rollback_journal_invalid")
        else:
            unlinked = {
                "schema_version": FRESH_INSTALL_ROLLBACK_JOURNAL_SCHEMA_VERSION,
                "operation": "quarantine",
                "phase": "unlinked",
                "transaction_id": transaction_id,
                "maintenance_token_sha256": maintenance_token,
                "linked_sha256": linked_observation["sha256"],
                "unlinked_at": _utc(now).isoformat(),
                "live_artifact": expected_live_artifact,
                "quarantine_artifact": quarantine_artifact,
                "writer_stop_evidence": pre_unlink_stopped,
                "writer_stop_max_age_seconds": max_writer_stop_age_seconds,
                "process_probes": {
                    "immediate_pre_unlink": immediate_pre_unlink_probe,
                    "post_unlink": post_unlink_probe,
                },
            }
            unlinked_observation = _write_json_no_clobber(unlinked_path, unlinked)
        tombstone = {
            "schema_version": FRESH_INSTALL_TOMBSTONE_SCHEMA_VERSION,
            "state": "quarantined",
            "created_at": str(marker["created_at"]),
            "transaction_id": transaction_id,
            "configured_database": str(control_path),
            "materialization_receipt_raw_sha256": source_sha256,
            "config_sha256": config_sha256,
            "audit": audit,
            "db_instance_id": materialization["receipt"]["db_instance_id"],
            "genesis_intent_sha256": materialization["receipt"][
                "genesis_intent_sha256"
            ],
            "quarantine_artifact": quarantine_artifact,
        }
        if tombstone_path.exists() or tombstone_path.is_symlink():
            existing_tombstone, _raw, tombstone_observation = (
                _read_json_regular_file(tombstone_path)
            )
            if existing_tombstone != tombstone:
                raise MigrationDrillError("fresh_install_quarantine_tombstone_conflict")
        else:
            tombstone_observation = _write_json_no_clobber(tombstone_path, tombstone)
        live_absence_after = _require_no_sqlite_sidecars(
            control_path,
            tombstone_present=True,
            maintenance_present=True,
            code="fresh_install_quarantine_live_state_invalid",
        )
        tombstoned_path = _rollback_journal_path(
            evidence_root, "quarantine", transaction_id, "tombstoned"
        )
        if tombstoned_path.exists() or tombstoned_path.is_symlink():
            tombstoned, tombstoned_observation = _read_rollback_phase(
                tombstoned_path,
                operation="quarantine",
                phase="tombstoned",
                transaction_id=transaction_id,
            )
            if (
                tombstoned.get("unlinked_sha256")
                != unlinked_observation["sha256"]
                or tombstoned.get("tombstone") != tombstone_observation
                or not isinstance(tombstoned.get("writer_stop_evidence"), Mapping)
                or not isinstance(tombstoned.get("runtime_identity"), Mapping)
                or not isinstance(
                    tombstoned.get("writer_stop_max_age_seconds"), int
                )
                or tombstoned["writer_stop_max_age_seconds"] < 1
            ):
                raise MigrationDrillError("fresh_install_rollback_journal_invalid")
            completed_at = _parse_timestamp(
                tombstoned.get("tombstoned_at"),
                code="fresh_install_rollback_journal_invalid",
            )
        else:
            completed_at = _utc(now)
            completion_stopped = validate_writer_stop_evidence(
                writer_stop_evidence,
                now=completed_at,
                max_age_seconds=max_writer_stop_age_seconds,
            )
            completion_runtime_identity = _fresh_install_runtime_identity()
            tombstoned = {
                "schema_version": FRESH_INSTALL_ROLLBACK_JOURNAL_SCHEMA_VERSION,
                "operation": "quarantine",
                "phase": "tombstoned",
                "transaction_id": transaction_id,
                "maintenance_token_sha256": maintenance_token,
                "unlinked_sha256": unlinked_observation["sha256"],
                "tombstoned_at": completed_at.isoformat(),
                "tombstone": tombstone_observation,
                "writer_stop_evidence": completion_stopped,
                "writer_stop_max_age_seconds": max_writer_stop_age_seconds,
                "runtime_identity": completion_runtime_identity,
            }
            tombstoned_observation = _write_json_no_clobber(
                tombstoned_path, tombstoned
            )
        process_probes = {
            **dict(prepared["process_probes"]),
            **dict(unlinked["process_probes"]),
        }
        completion_stopped = dict(tombstoned["writer_stop_evidence"])
        completion_runtime_identity = dict(tombstoned["runtime_identity"])
        maintenance_value = {
            "path": str(maintenance_path),
            "maintenance_token_sha256": maintenance_token,
            "observation": marker_observation,
        }
        journal = {
            "prepared": prepared_observation,
            "linked": linked_observation,
            "unlinked": unlinked_observation,
            "tombstoned": tombstoned_observation,
        }
        binding = {
            "completed_at": completed_at.isoformat(),
            "transaction_id": transaction_id,
            "config_sha256": config_sha256,
            "audit": audit,
            "configured_databases": {
                "control": str(control_path),
                "delivery": str(delivery_path),
                "same_database": True,
            },
            "materialization_receipt": materialization_observation,
            "materialization_receipt_raw_sha256": source_sha256,
            "db_instance_id": materialization["receipt"]["db_instance_id"],
            "genesis_intent_sha256": materialization["receipt"][
                "genesis_intent_sha256"
            ],
            "maintenance": maintenance_value,
            "writer_stop_evidence": {
                "initial": dict(prepared["writer_stop_evidence"]),
                "completion": completion_stopped,
            },
            "writer_stop_evidence_max_age_seconds": {
                "initial": int(prepared["writer_stop_max_age_seconds"]),
                "completion": int(tombstoned["writer_stop_max_age_seconds"]),
            },
            "process_probes": process_probes,
            "quarantine_directory": quarantine_root_observation,
            "quarantine_artifact": quarantine_artifact,
            "live_absence_after": live_absence_after,
            "tombstone": {
                "body": tombstone,
                "observation": tombstone_observation,
            },
            "journal": journal,
            "runtime_identities": {
                "started": marker["runtime_identity"],
                "completed": completion_runtime_identity,
            },
        }
        proposed_receipt = {
            "schema_version": FRESH_INSTALL_QUARANTINE_RECEIPT_SCHEMA_VERSION,
            "observed_at": completed_at.isoformat(),
            "ok": True,
            "operation": "quarantine",
            **binding,
            "quarantine_binding_sha256": hashlib.sha256(
                _json_payload(binding)
            ).hexdigest(),
        }
        if receipt_path.exists() or receipt_path.is_symlink():
            receipt, _receipt_raw, receipt_observation = _read_json_regular_file(
                receipt_path
            )
            if receipt != proposed_receipt:
                raise MigrationDrillError("fresh_install_quarantine_receipt_conflict")
        else:
            receipt = proposed_receipt
            receipt_observation = _write_json_no_clobber(receipt_path, receipt)
        receipted_path = _rollback_journal_path(
            evidence_root, "quarantine", transaction_id, "receipted"
        )
        receipted = {
            "schema_version": FRESH_INSTALL_ROLLBACK_JOURNAL_SCHEMA_VERSION,
            "operation": "quarantine",
            "phase": "receipted",
            "transaction_id": transaction_id,
            "maintenance_token_sha256": maintenance_token,
            "tombstoned_sha256": tombstoned_observation["sha256"],
            "receipted_at": completed_at.isoformat(),
            "receipt": receipt_observation,
            "maintenance_release_required": True,
        }
        _write_json_no_clobber(receipted_path, receipted)
        _remove_exact_json_file(maintenance_path, expected=marker)
        return {
            **receipt,
            "applied": not recovered,
            "idempotent": False,
            "recovered": recovered,
        }


def _validate_restore_receipt(
    *,
    receipt_path: Path,
    quarantine_receipt_path: Path,
    control_path: Path,
    delivery_path: Path,
    config_sha256: str,
    audit: Mapping[str, Any],
    evidence_root: Path,
    quarantine_root: Path,
    maintenance_present: bool,
) -> dict[str, Any]:
    try:
        receipt, receipt_raw, receipt_observation = _read_json_regular_file(
            receipt_path
        )
    except MigrationDrillError as exc:
        raise MigrationDrillError("fresh_install_restore_receipt_invalid") from exc
    expected_keys = {
        "schema_version",
        "observed_at",
        "completed_at",
        "ok",
        "operation",
        "transaction_id",
        "config_sha256",
        "audit",
        "configured_databases",
        "quarantine_receipt",
        "quarantine_receipt_raw_sha256",
        "quarantine_artifact",
        "restored_live_artifact",
        "db_instance_id",
        "genesis_intent_sha256",
        "capacity_transition",
        "maintenance",
        "writer_stop_evidence",
        "writer_stop_evidence_max_age_seconds",
        "process_probes",
        "journal",
        "runtime_identities",
        "restore_binding_sha256",
    }
    configured = {
        "control": str(control_path),
        "delivery": str(delivery_path),
        "same_database": True,
    }
    if (
        set(receipt) != expected_keys
        or receipt.get("schema_version") != FRESH_INSTALL_RESTORE_RECEIPT_SCHEMA_VERSION
        or receipt.get("ok") is not True
        or receipt.get("operation") != "restore"
        or receipt.get("config_sha256") != config_sha256
        or receipt.get("audit") != dict(audit)
        or receipt.get("configured_databases") != configured
    ):
        raise MigrationDrillError("fresh_install_restore_receipt_invalid")
    source_sha256 = _require_sha256(
        receipt.get("quarantine_receipt_raw_sha256"),
        code="fresh_install_restore_receipt_invalid",
    )
    transaction_id = _require_sha256(
        receipt.get("transaction_id"),
        code="fresh_install_restore_receipt_invalid",
    )
    if transaction_id != _rollback_transaction_id(
        operation="restore",
        source_receipt_sha256=source_sha256,
        configured_database=control_path,
        config_sha256=config_sha256,
        audit=audit,
    ):
        raise MigrationDrillError("fresh_install_restore_receipt_invalid")
    try:
        quarantine_preview, quarantine_raw, quarantine_observation = (
            _read_json_regular_file(quarantine_receipt_path)
        )
    except MigrationDrillError as exc:
        raise MigrationDrillError("fresh_install_restore_receipt_invalid") from exc
    if (
        quarantine_observation != receipt.get("quarantine_receipt")
        or hashlib.sha256(quarantine_raw).hexdigest() != source_sha256
        or quarantine_preview.get("config_sha256") != config_sha256
    ):
        raise MigrationDrillError("fresh_install_restore_receipt_invalid")
    quarantine_audit = quarantine_preview.get("audit")
    materialization_value = quarantine_preview.get("materialization_receipt")
    if not isinstance(quarantine_audit, Mapping) or not isinstance(
        materialization_value, Mapping
    ):
        raise MigrationDrillError("fresh_install_restore_receipt_invalid")
    quarantine = _validate_quarantine_receipt(
        receipt_path=quarantine_receipt_path,
        materialization_receipt_path=Path(str(materialization_value.get("path") or "")),
        control_path=control_path,
        delivery_path=delivery_path,
        config_sha256=config_sha256,
        audit=quarantine_audit,
        evidence_root=evidence_root,
        quarantine_root=quarantine_root,
        maintenance_present=maintenance_present,
        quarantined_state=False,
    )
    if (
        receipt.get("quarantine_artifact") != quarantine["quarantine_artifact"]
        or receipt.get("db_instance_id")
        != quarantine["receipt"].get("db_instance_id")
        or receipt.get("genesis_intent_sha256")
        != quarantine["receipt"].get("genesis_intent_sha256")
    ):
        raise MigrationDrillError("fresh_install_restore_receipt_invalid")
    restored_value = receipt.get("restored_live_artifact")
    if not isinstance(restored_value, Mapping):
        raise MigrationDrillError("fresh_install_restore_receipt_invalid")
    restored = _require_exact_artifact(
        control_path,
        restored_value,
        allowed_link_counts=frozenset({1}),
        code="fresh_install_restored_database_changed",
    )
    restored_metadata = control_path.lstat()
    if (
        restored_metadata.st_nlink != 1
        or restored["sha256"] != quarantine["quarantine_artifact"]["sha256"]
        or restored["size_bytes"]
        != quarantine["quarantine_artifact"]["size_bytes"]
        or (restored["device"], restored["inode"])
        == (
            quarantine["quarantine_artifact"]["device"],
            quarantine["quarantine_artifact"]["inode"],
        )
    ):
        raise MigrationDrillError("fresh_install_restored_database_invalid")
    validation = inspect_sqlite_read_only(control_path, ("control", "delivery"))
    genesis = _read_genesis_meta(control_path)
    materialization = quarantine["materialization"]
    if (
        validation != materialization["validation"]
        or validation.get("schemas")
        != {
            "control": CONTROL_STORE_SCHEMA_VERSION,
            "delivery": DELIVERY_STORE_SCHEMA_VERSION,
        }
        or genesis != materialization["genesis_meta"]
    ):
        raise MigrationDrillError("fresh_install_restored_database_invalid")
    preserved_identity = materialization["receipt"].get("capacity_identity")
    if not isinstance(preserved_identity, Mapping):
        raise MigrationDrillError("fresh_install_restored_capacity_invalid")
    restored_capacity = _capacity_transition_snapshot(
        control_path,
        expected_release_id=str(preserved_identity.get("release_id") or ""),
        expected_bootstrap_epoch_id=str(
            preserved_identity.get("bootstrap_epoch_id") or ""
        ),
    )
    if (
        restored_capacity != materialization.get("capacity_transition")
        or receipt.get("capacity_transition") != restored_capacity
    ):
        raise MigrationDrillError("fresh_install_restored_capacity_invalid")
    state = _require_no_sqlite_sidecars(
        control_path,
        tombstone_present=False,
        maintenance_present=maintenance_present,
        code="fresh_install_restored_database_fenced",
    )
    if not state["database"]["present"]:
        raise MigrationDrillError("fresh_install_restored_database_missing")
    process_probes = _validate_recorded_process_probes(
        receipt.get("process_probes"),
        expected_names={
            "initial",
            "locked",
            "post_maintenance",
            "immediate_pre_install",
            "post_install",
        },
        code="fresh_install_restore_receipt_invalid",
    )
    completed_at = _parse_timestamp(
        receipt.get("completed_at"),
        code="fresh_install_restore_receipt_invalid",
    )
    if receipt.get("observed_at") != completed_at.isoformat():
        raise MigrationDrillError("fresh_install_restore_receipt_invalid")
    journal = receipt.get("journal")
    expected_phases = (
        "prepared",
        "staged",
        "copied",
        "installed",
        "verified",
        "unfenced",
    )
    if not isinstance(journal, Mapping) or set(journal) != set(expected_phases):
        raise MigrationDrillError("fresh_install_restore_receipt_invalid")
    phase_bodies: dict[str, Any] = {}
    phase_observations: dict[str, Any] = {}
    for phase in expected_phases:
        path = _rollback_journal_path(
            evidence_root, "restore", transaction_id, phase
        )
        body, observation = _read_rollback_phase(
            path,
            operation="restore",
            phase=phase,
            transaction_id=transaction_id,
        )
        if observation != journal[phase]:
            raise MigrationDrillError("fresh_install_restore_receipt_invalid")
        phase_bodies[phase] = body
        phase_observations[phase] = observation
    prepared = phase_bodies["prepared"]
    staged = phase_bodies["staged"]
    copied = phase_bodies["copied"]
    installed = phase_bodies["installed"]
    verified = phase_bodies["verified"]
    unfenced = phase_bodies["unfenced"]
    expected_stage_path = quarantine_root / (
        f".{control_path.name}.{transaction_id}.restore-hardlink.sqlite3"
    )
    expected_copy_path = control_path.parent / (
        f".{control_path.name}.{transaction_id}.restore-copy.sqlite3"
    )
    expected_copy_temporary_path = control_path.parent / (
        f".{control_path.name}.{transaction_id}.restore-copy.partial.sqlite3"
    )
    expected_transient_absence = {
        "stage": {"path": str(expected_stage_path), "present": False},
        "copy": {"path": str(expected_copy_path), "present": False},
        "copy_temporary": {
            "path": str(expected_copy_temporary_path),
            "present": False,
        },
    }
    maintenance = receipt.get("maintenance")
    runtime_identities = receipt.get("runtime_identities")
    if (
        not isinstance(maintenance, Mapping)
        or set(maintenance)
        != {"path", "maintenance_token_sha256", "observation"}
        or maintenance.get("path") != f"{control_path}.pnc-rca-maintenance"
        or maintenance.get("maintenance_token_sha256")
        != prepared.get("maintenance_token_sha256")
        or not isinstance(maintenance.get("observation"), Mapping)
        or set(maintenance["observation"]) != _FILE_OBSERVATION_KEYS
        or not isinstance(runtime_identities, Mapping)
        or set(runtime_identities) != {"started", "completed"}
    ):
        raise MigrationDrillError("fresh_install_restore_receipt_invalid")
    started_identity = _validate_runtime_identity_record(
        runtime_identities["started"],
        code="fresh_install_restore_receipt_invalid",
    )
    completed_identity = _validate_runtime_identity_record(
        runtime_identities["completed"],
        code="fresh_install_restore_receipt_invalid",
    )
    if (
        prepared.get("quarantine_receipt") != quarantine_observation
        or prepared.get("quarantine_receipt_raw_sha256") != source_sha256
        or prepared.get("quarantine_artifact")
        != quarantine["quarantine_artifact"]
        or prepared.get("stage_path") != str(expected_stage_path)
        or prepared.get("copy_path") != str(expected_copy_path)
        or prepared.get("copy_temporary_path")
        != str(expected_copy_temporary_path)
        or prepared.get("transient_absence_before")
        != expected_transient_absence
        or prepared.get("runtime_identity") != started_identity
        or prepared.get("process_probes")
        != {
            name: process_probes[name]
            for name in ("initial", "locked", "post_maintenance")
        }
        or staged.get("prepared_sha256")
        != phase_observations["prepared"]["sha256"]
        or not isinstance(staged.get("stage_artifact"), Mapping)
        or staged["stage_artifact"].get("path") != str(expected_stage_path)
        or copied.get("staged_sha256")
        != phase_observations["staged"]["sha256"]
        or not isinstance(copied.get("copy_artifact"), Mapping)
        or copied["copy_artifact"].get("path") != str(expected_copy_path)
        or installed.get("copied_sha256")
        != phase_observations["copied"]["sha256"]
        or installed.get("restored_live_artifact") != restored
        or installed.get("process_probes")
        != {
            name: process_probes[name]
            for name in ("immediate_pre_install", "post_install")
        }
        or verified.get("installed_sha256")
        != phase_observations["installed"]["sha256"]
        or verified.get("restored_live_artifact") != restored
        or verified.get("validation") != validation
        or verified.get("genesis_meta") != genesis
        or verified.get("capacity_transition") != restored_capacity
        or verified.get("single_link") is not True
        or unfenced.get("verified_sha256")
        != phase_observations["verified"]["sha256"]
        or unfenced.get("tombstone_removed")
        != quarantine["tombstone_observation"]
        or unfenced.get("runtime_identity") != completed_identity
    ):
        raise MigrationDrillError("fresh_install_restore_receipt_invalid")
    prepared_at = _parse_timestamp(
        prepared.get("prepared_at"),
        code="fresh_install_restore_receipt_invalid",
    )
    installed_at = _parse_timestamp(
        installed.get("installed_at"),
        code="fresh_install_restore_receipt_invalid",
    )
    verified_at = _parse_timestamp(
        verified.get("verified_at"),
        code="fresh_install_restore_receipt_invalid",
    )
    if (
        unfenced.get("unfenced_at") != completed_at.isoformat()
        or not (prepared_at <= installed_at <= verified_at <= completed_at)
    ):
        raise MigrationDrillError("fresh_install_restore_receipt_invalid")
    writer_stop_proofs = receipt.get("writer_stop_evidence")
    writer_stop_max_ages = receipt.get("writer_stop_evidence_max_age_seconds")
    if not isinstance(writer_stop_proofs, Mapping) or set(writer_stop_proofs) != {
        "initial",
        "completion",
    } or not isinstance(writer_stop_max_ages, Mapping) or set(
        writer_stop_max_ages
    ) != {"initial", "completion"} or any(
        not isinstance(writer_stop_max_ages[name], int)
        or writer_stop_max_ages[name] < 1
        for name in ("initial", "completion")
    ) or not isinstance(installed.get("writer_stop_max_age_seconds"), int) or (
        installed["writer_stop_max_age_seconds"] < 1
    ):
        raise MigrationDrillError("fresh_install_restore_receipt_invalid")
    try:
        initial_stopped = validate_writer_stop_evidence(
            writer_stop_proofs["initial"],
            now=prepared_at,
            max_age_seconds=writer_stop_max_ages["initial"],
        )
        action_stopped = validate_writer_stop_evidence(
            installed.get("writer_stop_evidence"),
            now=installed_at,
            max_age_seconds=installed["writer_stop_max_age_seconds"],
        )
        completion_stopped = validate_writer_stop_evidence(
            writer_stop_proofs["completion"],
            now=completed_at,
            max_age_seconds=writer_stop_max_ages["completion"],
        )
    except MigrationDrillError as exc:
        raise MigrationDrillError("fresh_install_restore_receipt_invalid") from exc
    if (
        prepared.get("writer_stop_evidence") != initial_stopped
        or installed.get("writer_stop_evidence") != action_stopped
        or unfenced.get("writer_stop_evidence") != completion_stopped
        or prepared.get("writer_stop_max_age_seconds")
        != writer_stop_max_ages["initial"]
        or unfenced.get("writer_stop_max_age_seconds")
        != writer_stop_max_ages["completion"]
        or _parse_timestamp(
            completion_stopped.get("observed_at"),
            code="fresh_install_restore_receipt_invalid",
        )
        < _parse_timestamp(
            initial_stopped.get("observed_at"),
            code="fresh_install_restore_receipt_invalid",
        )
    ):
        raise MigrationDrillError("fresh_install_restore_receipt_invalid")
    binding_keys = expected_keys - {
        "schema_version",
        "observed_at",
        "ok",
        "operation",
        "restore_binding_sha256",
    }
    binding = {key: receipt[key] for key in binding_keys}
    if hashlib.sha256(_json_payload(binding)).hexdigest() != receipt.get(
        "restore_binding_sha256"
    ):
        raise MigrationDrillError("fresh_install_restore_receipt_invalid")
    receipted_path = _rollback_journal_path(
        evidence_root, "restore", transaction_id, "receipted"
    )
    receipted, _receipted_observation = _read_rollback_phase(
        receipted_path,
        operation="restore",
        phase="receipted",
        transaction_id=transaction_id,
    )
    if (
        receipted.get("unfenced_sha256")
        != phase_observations["unfenced"]["sha256"]
        or receipted.get("receipt") != receipt_observation
        or receipted.get("receipted_at") != completed_at.isoformat()
        or receipted.get("maintenance_release_required") is not True
    ):
        raise MigrationDrillError("fresh_install_restore_receipt_invalid")
    for transient in (
        expected_stage_path,
        expected_copy_path,
        expected_copy_temporary_path,
    ):
        if transient.exists() or transient.is_symlink():
            raise MigrationDrillError("fresh_install_restore_transient_present")
    return {
        "receipt": receipt,
        "receipt_raw": receipt_raw,
        "receipt_observation": receipt_observation,
        "quarantine": quarantine,
        "restored_live_artifact": restored,
        "validation": validation,
        "genesis_meta": genesis,
        "journal_bodies": phase_bodies,
        "journal_observations": phase_observations,
    }


def restore_fresh_install_from_quarantine(
    *,
    quarantine_receipt_path: str | Path,
    control_db_path: str | Path,
    delivery_db_path: str | Path,
    config_sha256: str,
    evidence_dir: str | Path,
    quarantine_dir: str | Path,
    writer_stop_evidence: Mapping[str, Any],
    apply: bool = False,
    now: datetime | None = None,
    max_writer_stop_age_seconds: int = 900,
    writer_process_probe: Any | None = None,
    release_id: str | None = None,
    operator: str | None = None,
    reason: str | None = None,
) -> dict[str, Any]:
    """Restore an exact quarantined v10/v6 DB through a no-clobber install."""
    started_at = _utc(now)
    audit = _fresh_install_audit_hashes(
        release_id=release_id,
        operator=operator,
        reason=reason,
        required=apply,
    )
    if (
        not isinstance(max_writer_stop_age_seconds, int)
        or max_writer_stop_age_seconds < 1
    ):
        raise MigrationDrillError("writer_stop_evidence_max_age_invalid")
    _require_sha256(config_sha256, code="fresh_install_config_sha256_invalid")
    control_path, delivery_path = _require_shared_absolute_database_paths(
        control_db_path, delivery_db_path
    )
    evidence_root, _evidence_observation = _owner_only_directory_observation(
        evidence_dir,
        code="fresh_install_restore_evidence_directory_invalid",
    )
    quarantine_root, quarantine_root_observation = (
        _owner_only_directory_observation(
            quarantine_dir,
            code="fresh_install_quarantine_directory_invalid",
        )
    )
    parent, parent_observation = _secure_directory_observation(
        control_path.parent,
        code="fresh_install_directory_invalid",
    )
    if quarantine_root_observation["device"] != parent_observation["device"]:
        raise MigrationDrillError("fresh_install_quarantine_cross_device")
    quarantine_receipt_path = _absolute(quarantine_receipt_path).absolute()
    try:
        quarantine_receipt_path.parent.resolve(strict=True).relative_to(evidence_root)
    except (OSError, ValueError) as exc:
        raise MigrationDrillError("fresh_install_quarantine_receipt_outside_evidence") from exc
    quarantine_preview, quarantine_raw, quarantine_observation = (
        _read_json_regular_file(quarantine_receipt_path)
    )
    if quarantine_preview.get("config_sha256") != config_sha256:
        raise MigrationDrillError("fresh_install_restore_config_mismatch")
    quarantine_audit = quarantine_preview.get("audit")
    materialization_value = quarantine_preview.get("materialization_receipt")
    if not isinstance(quarantine_audit, Mapping) or not isinstance(
        materialization_value, Mapping
    ):
        raise MigrationDrillError("fresh_install_quarantine_receipt_invalid")
    maintenance_path = Path(f"{control_path}.pnc-rca-maintenance")
    tombstone_path = Path(f"{control_path}.pnc-rca-tombstone")
    source_sha256 = hashlib.sha256(quarantine_raw).hexdigest()
    audit_for_id = audit or {
        name: "0" * 64
        for name in ("release_id_sha256", "operator_sha256", "reason_sha256")
    }
    transaction_id = _rollback_transaction_id(
        operation="restore",
        source_receipt_sha256=source_sha256,
        configured_database=control_path,
        config_sha256=config_sha256,
        audit=audit_for_id,
    )
    receipt_path = evidence_root / "fresh_install_restore_receipt.json"
    process_probe = writer_process_probe or _default_writer_process_probe
    stopped = validate_writer_stop_evidence(
        writer_stop_evidence,
        now=started_at,
        max_age_seconds=max_writer_stop_age_seconds,
    )
    initial_probe = _checked_writer_process_probe(process_probe)
    if receipt_path.exists() or receipt_path.is_symlink():
        preview, _raw, _observation = _read_json_regular_file(receipt_path)
        receipt_audit = preview.get("audit")
        if not isinstance(receipt_audit, Mapping):
            raise MigrationDrillError("fresh_install_restore_receipt_invalid")
        if (
            preview.get("schema_version") != FRESH_INSTALL_RESTORE_RECEIPT_SCHEMA_VERSION
            or preview.get("config_sha256") != config_sha256
        ):
            raise MigrationDrillError("fresh_install_restore_receipt_invalid")
        if maintenance_path.exists() or maintenance_path.is_symlink():
            if not apply:
                return {
                    "schema_version": "pnc_rca_fresh_install_restore_plan_v1",
                    "applied": False,
                    "idempotent": False,
                    "recovery_required": True,
                    "transaction_id": preview.get("transaction_id"),
                    "configured_database": str(control_path),
                    "receipt_present": True,
                    "initial_process_probe": initial_probe,
                }
        else:
            validated = _validate_restore_receipt(
                receipt_path=receipt_path,
                quarantine_receipt_path=quarantine_receipt_path,
                control_path=control_path,
                delivery_path=delivery_path,
                config_sha256=config_sha256,
                audit=receipt_audit,
                evidence_root=evidence_root,
                quarantine_root=quarantine_root,
                maintenance_present=False,
            )
        if not apply and not maintenance_path.exists():
            return {
                "schema_version": "pnc_rca_fresh_install_restore_plan_v1",
                "applied": False,
                "idempotent": True,
                "recovery_required": maintenance_path.exists(),
                "transaction_id": validated["receipt"]["transaction_id"],
                "configured_database": str(control_path),
                "restored_live_artifact": validated["restored_live_artifact"],
                "initial_process_probe": initial_probe,
            }
        if audit is None or dict(receipt_audit) != audit:
            raise MigrationDrillError("fresh_install_restore_receipt_conflict")
        if not maintenance_path.exists():
            return {
                **validated["receipt"],
                "applied": False,
                "idempotent": True,
                "recovered": False,
            }
    live_present = control_path.exists() or control_path.is_symlink()
    quarantine = _validate_quarantine_receipt(
        receipt_path=quarantine_receipt_path,
        materialization_receipt_path=Path(str(materialization_value.get("path") or "")),
        control_path=control_path,
        delivery_path=delivery_path,
        config_sha256=config_sha256,
        audit=quarantine_audit,
        evidence_root=evidence_root,
        quarantine_root=quarantine_root,
        maintenance_present=(maintenance_path.exists() or maintenance_path.is_symlink()),
        quarantined_state=not live_present,
        allow_transient_quarantine_link=(
            maintenance_path.exists() or maintenance_path.is_symlink()
        ),
    )
    if not apply:
        return {
            "schema_version": "pnc_rca_fresh_install_restore_plan_v1",
            "applied": False,
            "idempotent": False,
            "recovery_required": maintenance_path.exists(),
            "transaction_id": transaction_id,
            "configured_database": str(control_path),
            "quarantine_receipt_raw_sha256": source_sha256,
            "quarantine_artifact": quarantine["quarantine_artifact"],
            "destination_state": _fresh_destination_state(control_path),
            "initial_process_probe": initial_probe,
            "apply_audit_required": True,
        }
    if audit is None:
        raise MigrationDrillError("fresh_install_restore_audit_invalid")
    runtime_identity = _fresh_install_runtime_identity()
    recovered = maintenance_path.exists() or maintenance_path.is_symlink()
    stage_path = quarantine_root / (
        f".{control_path.name}.{transaction_id}.restore-hardlink.sqlite3"
    )
    copy_path = parent / f".{control_path.name}.{transaction_id}.restore-copy.sqlite3"
    copy_temporary_path = parent / (
        f".{control_path.name}.{transaction_id}.restore-copy.partial.sqlite3"
    )
    with _fresh_install_lock(parent, parent_observation) as (_parent_fd, lock_stat):
        locked_probe = _checked_writer_process_probe(process_probe)
        if (
            (receipt_path.exists() or receipt_path.is_symlink())
            and not (maintenance_path.exists() or maintenance_path.is_symlink())
        ):
            validated = _validate_restore_receipt(
                receipt_path=receipt_path,
                quarantine_receipt_path=quarantine_receipt_path,
                control_path=control_path,
                delivery_path=delivery_path,
                config_sha256=config_sha256,
                audit=audit,
                evidence_root=evidence_root,
                quarantine_root=quarantine_root,
                maintenance_present=maintenance_path.exists(),
            )
            if not maintenance_path.exists():
                return {
                    **validated["receipt"],
                    "applied": False,
                    "idempotent": True,
                    "recovered": False,
                }
        if maintenance_path.exists() or maintenance_path.is_symlink():
            marker, _marker_raw, marker_observation = _read_json_regular_file(
                maintenance_path
            )
            if (
                marker.get("schema_version")
                != FRESH_INSTALL_ROLLBACK_MAINTENANCE_SCHEMA_VERSION
                or marker.get("operation") != "restore"
                or marker.get("state") != "active"
                or marker.get("transaction_id") != transaction_id
                or marker.get("configured_database") != str(control_path)
                or marker.get("source_receipt_raw_sha256") != source_sha256
                or marker.get("config_sha256") != config_sha256
                or marker.get("audit") != audit
            ):
                raise MigrationDrillError("fresh_install_restore_maintenance_conflict")
            maintenance_token = _require_sha256(
                marker.get("maintenance_token_sha256"),
                code="fresh_install_restore_maintenance_conflict",
            )
            recovered = True
        else:
            if not tombstone_path.exists() and not receipt_path.exists():
                raise MigrationDrillError("fresh_install_restore_tombstone_missing")
            maintenance_token = hashlib.sha256(uuid.uuid4().bytes).hexdigest()
            marker = {
                "schema_version": FRESH_INSTALL_ROLLBACK_MAINTENANCE_SCHEMA_VERSION,
                "operation": "restore",
                "state": "active",
                "created_at": started_at.isoformat(),
                "transaction_id": transaction_id,
                "maintenance_token_sha256": maintenance_token,
                "configured_database": str(control_path),
                "source_receipt_raw_sha256": source_sha256,
                "config_sha256": config_sha256,
                "audit": audit,
                "runtime_identity": runtime_identity,
            }
            marker_observation = _write_json_no_clobber(maintenance_path, marker)
        post_maintenance_probe = _checked_writer_process_probe(process_probe)
        validate_writer_stop_evidence(
            writer_stop_evidence,
            now=_utc(now),
            max_age_seconds=max_writer_stop_age_seconds,
        )
        live_present = control_path.exists() or control_path.is_symlink()
        quarantine = _validate_quarantine_receipt(
            receipt_path=quarantine_receipt_path,
            materialization_receipt_path=Path(
                str(materialization_value.get("path") or "")
            ),
            control_path=control_path,
            delivery_path=delivery_path,
            config_sha256=config_sha256,
            audit=quarantine_audit,
            evidence_root=evidence_root,
            quarantine_root=quarantine_root,
            maintenance_present=True,
            quarantined_state=not live_present,
            allow_transient_quarantine_link=True,
        )
        quarantine_artifact = quarantine["quarantine_artifact"]
        prepared_path = _rollback_journal_path(
            evidence_root, "restore", transaction_id, "prepared"
        )
        if prepared_path.exists() or prepared_path.is_symlink():
            prepared, prepared_observation = _read_rollback_phase(
                prepared_path,
                operation="restore",
                phase="prepared",
                transaction_id=transaction_id,
            )
            if (
                prepared.get("maintenance_token_sha256") != maintenance_token
                or prepared.get("quarantine_receipt") != quarantine_observation
                or prepared.get("quarantine_receipt_raw_sha256") != source_sha256
                or prepared.get("quarantine_artifact") != quarantine_artifact
                or prepared.get("config_sha256") != config_sha256
                or prepared.get("audit") != audit
                or prepared.get("stage_path") != str(stage_path)
                or prepared.get("copy_path") != str(copy_path)
                or prepared.get("copy_temporary_path")
                != str(copy_temporary_path)
                or not isinstance(
                    prepared.get("writer_stop_max_age_seconds"), int
                )
                or prepared["writer_stop_max_age_seconds"] < 1
                or prepared.get("transient_absence_before")
                != {
                    "stage": {"path": str(stage_path), "present": False},
                    "copy": {"path": str(copy_path), "present": False},
                    "copy_temporary": {
                        "path": str(copy_temporary_path),
                        "present": False,
                    },
                }
            ):
                raise MigrationDrillError("fresh_install_rollback_journal_invalid")
        else:
            if live_present:
                raise MigrationDrillError("fresh_install_restore_prepared_missing")
            if (
                stage_path.exists()
                or stage_path.is_symlink()
                or copy_path.exists()
                or copy_path.is_symlink()
                or copy_temporary_path.exists()
                or copy_temporary_path.is_symlink()
            ):
                raise MigrationDrillError("fresh_install_restore_transient_conflict")
            absent_state = _require_no_sqlite_sidecars(
                control_path,
                tombstone_present=True,
                maintenance_present=True,
                code="fresh_install_restore_destination_not_empty",
            )
            if absent_state["database"]["present"]:
                raise MigrationDrillError("fresh_install_restore_destination_not_empty")
            prepared = {
                "schema_version": FRESH_INSTALL_ROLLBACK_JOURNAL_SCHEMA_VERSION,
                "operation": "restore",
                "phase": "prepared",
                "transaction_id": transaction_id,
                "prepared_at": str(marker["created_at"]),
                "maintenance_token_sha256": maintenance_token,
                "configured_database": str(control_path),
                "quarantine_receipt": quarantine_observation,
                "quarantine_receipt_raw_sha256": source_sha256,
                "quarantine_artifact": quarantine_artifact,
                "db_instance_id": quarantine["receipt"]["db_instance_id"],
                "genesis_intent_sha256": quarantine["receipt"][
                    "genesis_intent_sha256"
                ],
                "config_sha256": config_sha256,
                "audit": audit,
                "destination_absence_before": absent_state,
                "stage_path": str(stage_path),
                "copy_path": str(copy_path),
                "copy_temporary_path": str(copy_temporary_path),
                "transient_absence_before": {
                    "stage": {"path": str(stage_path), "present": False},
                    "copy": {"path": str(copy_path), "present": False},
                    "copy_temporary": {
                        "path": str(copy_temporary_path),
                        "present": False,
                    },
                },
                "writer_stop_evidence": stopped,
                "writer_stop_max_age_seconds": max_writer_stop_age_seconds,
                "process_probes": {
                    "initial": initial_probe,
                    "locked": locked_probe,
                    "post_maintenance": post_maintenance_probe,
                },
                "runtime_identity": marker["runtime_identity"],
            }
            prepared_observation = _write_json_no_clobber(prepared_path, prepared)
        quarantine_path = Path(str(quarantine_artifact["path"]))
        if stage_path.exists() or stage_path.is_symlink():
            stage_artifact = _require_exact_artifact(
                stage_path,
                quarantine_artifact,
                allow_path_mismatch=True,
                allowed_link_counts=frozenset({2}),
                code="fresh_install_restore_stage_conflict",
            )
        else:
            try:
                os.link(quarantine_path, stage_path, follow_symlinks=False)
                _fsync_directory(quarantine_root)
            except FileExistsError as exc:
                raise MigrationDrillError("fresh_install_restore_stage_race") from exc
            except OSError as exc:
                raise MigrationDrillError("fresh_install_restore_stage_failed") from exc
            stage_artifact = _require_exact_artifact(
                stage_path,
                quarantine_artifact,
                allow_path_mismatch=True,
                allowed_link_counts=frozenset({2}),
                code="fresh_install_restore_stage_invalid",
            )
        if (stage_artifact["device"], stage_artifact["inode"]) != (
            quarantine_artifact["device"],
            quarantine_artifact["inode"],
        ):
            raise MigrationDrillError("fresh_install_restore_stage_invalid")
        staged_path = _rollback_journal_path(
            evidence_root, "restore", transaction_id, "staged"
        )
        if staged_path.exists() or staged_path.is_symlink():
            staged, staged_observation = _read_rollback_phase(
                staged_path,
                operation="restore",
                phase="staged",
                transaction_id=transaction_id,
            )
            if (
                staged.get("prepared_sha256") != prepared_observation["sha256"]
                or staged.get("stage_artifact") != stage_artifact
            ):
                raise MigrationDrillError("fresh_install_rollback_journal_invalid")
        else:
            staged = {
                "schema_version": FRESH_INSTALL_ROLLBACK_JOURNAL_SCHEMA_VERSION,
                "operation": "restore",
                "phase": "staged",
                "transaction_id": transaction_id,
                "maintenance_token_sha256": maintenance_token,
                "prepared_sha256": prepared_observation["sha256"],
                "staged_at": _utc(now).isoformat(),
                "stage_artifact": stage_artifact,
            }
            staged_observation = _write_json_no_clobber(staged_path, staged)
        if not live_present:
            copy_artifact = _copy_artifact_no_clobber(
                stage_path,
                copy_path,
                temporary=copy_temporary_path,
                expected=quarantine_artifact,
            )
            if (copy_artifact["device"], copy_artifact["inode"]) == (
                quarantine_artifact["device"],
                quarantine_artifact["inode"],
            ):
                raise MigrationDrillError("fresh_install_restore_copy_not_detached")
        elif copy_temporary_path.exists() or copy_temporary_path.is_symlink():
            raise MigrationDrillError("fresh_install_restore_copy_conflict")
        elif copy_path.exists() or copy_path.is_symlink():
            copy_artifact = _require_content_equivalent_artifact(
                copy_path,
                quarantine_artifact,
                require_single_link=False,
                code="fresh_install_restore_copy_conflict",
            )
        else:
            copied_path = _rollback_journal_path(
                evidence_root, "restore", transaction_id, "copied"
            )
            if not copied_path.exists():
                raise MigrationDrillError("fresh_install_restore_copy_missing")
            copied_existing, _copied_observation = _read_rollback_phase(
                copied_path,
                operation="restore",
                phase="copied",
                transaction_id=transaction_id,
            )
            copy_artifact = copied_existing.get("copy_artifact")
            if not isinstance(copy_artifact, Mapping):
                raise MigrationDrillError("fresh_install_restore_copy_missing")
        copied_path = _rollback_journal_path(
            evidence_root, "restore", transaction_id, "copied"
        )
        if copied_path.exists() or copied_path.is_symlink():
            copied, copied_observation = _read_rollback_phase(
                copied_path,
                operation="restore",
                phase="copied",
                transaction_id=transaction_id,
            )
            if (
                copied.get("staged_sha256") != staged_observation["sha256"]
                or copied.get("copy_artifact") != copy_artifact
            ):
                raise MigrationDrillError("fresh_install_rollback_journal_invalid")
        else:
            copied = {
                "schema_version": FRESH_INSTALL_ROLLBACK_JOURNAL_SCHEMA_VERSION,
                "operation": "restore",
                "phase": "copied",
                "transaction_id": transaction_id,
                "maintenance_token_sha256": maintenance_token,
                "staged_sha256": staged_observation["sha256"],
                "copied_at": _utc(now).isoformat(),
                "copy_artifact": copy_artifact,
            }
            copied_observation = _write_json_no_clobber(copied_path, copied)
        immediate_pre_install_probe = _checked_writer_process_probe(process_probe)
        pre_install_stopped = validate_writer_stop_evidence(
            writer_stop_evidence,
            now=_utc(now),
            max_age_seconds=max_writer_stop_age_seconds,
        )
        if control_path.exists() or control_path.is_symlink():
            restored_live_artifact = _require_content_equivalent_artifact(
                control_path,
                copy_artifact,
                require_single_link=not copy_path.exists(),
                code="fresh_install_restore_destination_conflict",
            )
            if copy_path.exists() and (
                restored_live_artifact["device"], restored_live_artifact["inode"]
            ) != (copy_artifact["device"], copy_artifact["inode"]):
                raise MigrationDrillError("fresh_install_restore_destination_conflict")
        else:
            _require_no_sqlite_sidecars(
                control_path,
                tombstone_present=True,
                maintenance_present=True,
                code="fresh_install_restore_destination_not_empty",
            )
            try:
                os.link(copy_path, control_path, follow_symlinks=False)
                _fsync_directory(parent)
            except FileExistsError as exc:
                raise MigrationDrillError("fresh_install_restore_destination_race") from exc
            except OSError as exc:
                raise MigrationDrillError("fresh_install_restore_install_failed") from exc
            restored_live_artifact = _require_content_equivalent_artifact(
                control_path,
                copy_artifact,
                require_single_link=False,
                code="fresh_install_restore_install_invalid",
            )
            if (restored_live_artifact["device"], restored_live_artifact["inode"]) != (
                copy_artifact["device"],
                copy_artifact["inode"],
            ):
                raise MigrationDrillError("fresh_install_restore_install_invalid")
        post_install_probe = _checked_writer_process_probe(process_probe)
        installed_path = _rollback_journal_path(
            evidence_root, "restore", transaction_id, "installed"
        )
        if installed_path.exists() or installed_path.is_symlink():
            installed, installed_observation = _read_rollback_phase(
                installed_path,
                operation="restore",
                phase="installed",
                transaction_id=transaction_id,
            )
            if (
                installed.get("copied_sha256") != copied_observation["sha256"]
                or installed.get("restored_live_artifact")
                != restored_live_artifact
                or not isinstance(installed.get("writer_stop_evidence"), Mapping)
                or not isinstance(
                    installed.get("writer_stop_max_age_seconds"), int
                )
                or installed["writer_stop_max_age_seconds"] < 1
            ):
                raise MigrationDrillError("fresh_install_rollback_journal_invalid")
        else:
            installed = {
                "schema_version": FRESH_INSTALL_ROLLBACK_JOURNAL_SCHEMA_VERSION,
                "operation": "restore",
                "phase": "installed",
                "transaction_id": transaction_id,
                "maintenance_token_sha256": maintenance_token,
                "copied_sha256": copied_observation["sha256"],
                "installed_at": _utc(now).isoformat(),
                "restored_live_artifact": restored_live_artifact,
                "writer_stop_evidence": pre_install_stopped,
                "writer_stop_max_age_seconds": max_writer_stop_age_seconds,
                "process_probes": {
                    "immediate_pre_install": immediate_pre_install_probe,
                    "post_install": post_install_probe,
                },
            }
            installed_observation = _write_json_no_clobber(
                installed_path, installed
            )
        if copy_path.exists() or copy_path.is_symlink():
            _unlink_exact_artifact(
                copy_path,
                expected=copy_artifact,
                code="fresh_install_restore_copy_release_failed",
            )
        if copy_temporary_path.exists() or copy_temporary_path.is_symlink():
            raise MigrationDrillError("fresh_install_restore_transient_present")
        if stage_path.exists() or stage_path.is_symlink():
            _unlink_exact_artifact(
                stage_path,
                expected=stage_artifact,
                code="fresh_install_restore_stage_release_failed",
            )
        quarantine_artifact = _require_exact_artifact(
            quarantine_path,
            quarantine_artifact,
            allowed_link_counts=frozenset({1}),
            code="fresh_install_quarantine_artifact_invalid",
        )
        restored_live_artifact = _require_content_equivalent_artifact(
            control_path,
            quarantine_artifact,
            require_single_link=True,
            code="fresh_install_restored_database_invalid",
        )
        validation = inspect_sqlite_read_only(control_path, ("control", "delivery"))
        genesis = _read_genesis_meta(control_path)
        if (
            validation != quarantine["materialization"]["validation"]
            or validation.get("schemas")
            != {
                "control": CONTROL_STORE_SCHEMA_VERSION,
                "delivery": DELIVERY_STORE_SCHEMA_VERSION,
            }
            or genesis != quarantine["materialization"]["genesis_meta"]
        ):
            raise MigrationDrillError("fresh_install_restored_database_invalid")
        materialization_receipt = quarantine["materialization"]["receipt"]
        preserved_identity = materialization_receipt.get("capacity_identity")
        if not isinstance(preserved_identity, Mapping):
            raise MigrationDrillError("fresh_install_restored_capacity_invalid")
        restored_capacity = _capacity_transition_snapshot(
            control_path,
            expected_release_id=str(preserved_identity.get("release_id") or ""),
            expected_bootstrap_epoch_id=str(
                preserved_identity.get("bootstrap_epoch_id") or ""
            ),
        )
        if restored_capacity != quarantine["materialization"].get(
            "capacity_transition"
        ):
            raise MigrationDrillError("fresh_install_restored_capacity_invalid")
        verified_path = _rollback_journal_path(
            evidence_root, "restore", transaction_id, "verified"
        )
        tombstone_still_present = (
            tombstone_path.exists() or tombstone_path.is_symlink()
        )
        if not tombstone_still_present and not (
            verified_path.exists() or verified_path.is_symlink()
        ):
            raise MigrationDrillError("fresh_install_restore_tombstone_missing")
        _require_no_sqlite_sidecars(
            control_path,
            tombstone_present=tombstone_still_present,
            maintenance_present=True,
            code="fresh_install_restored_database_fenced",
        )
        if verified_path.exists() or verified_path.is_symlink():
            verified, verified_observation = _read_rollback_phase(
                verified_path,
                operation="restore",
                phase="verified",
                transaction_id=transaction_id,
            )
            if (
                verified.get("installed_sha256")
                != installed_observation["sha256"]
                or verified.get("restored_live_artifact")
                != restored_live_artifact
                or verified.get("validation") != validation
                or verified.get("genesis_meta") != genesis
                or verified.get("capacity_transition") != restored_capacity
                or verified.get("single_link") is not True
            ):
                raise MigrationDrillError("fresh_install_rollback_journal_invalid")
            completed_at = _parse_timestamp(
                verified.get("verified_at"),
                code="fresh_install_rollback_journal_invalid",
            )
        else:
            completed_at = _utc(now)
            verified = {
                "schema_version": FRESH_INSTALL_ROLLBACK_JOURNAL_SCHEMA_VERSION,
                "operation": "restore",
                "phase": "verified",
                "transaction_id": transaction_id,
                "maintenance_token_sha256": maintenance_token,
                "installed_sha256": installed_observation["sha256"],
                "verified_at": completed_at.isoformat(),
                "restored_live_artifact": restored_live_artifact,
                "validation": validation,
                "genesis_meta": genesis,
                "capacity_transition": restored_capacity,
                "single_link": True,
            }
            verified_observation = _write_json_no_clobber(verified_path, verified)
        expected_tombstone = quarantine["tombstone"]
        expected_tombstone_observation = quarantine["tombstone_observation"]
        unfenced_path = _rollback_journal_path(
            evidence_root, "restore", transaction_id, "unfenced"
        )
        if unfenced_path.exists() or unfenced_path.is_symlink():
            unfenced, unfenced_observation = _read_rollback_phase(
                unfenced_path,
                operation="restore",
                phase="unfenced",
                transaction_id=transaction_id,
            )
            if (
                unfenced.get("verified_sha256")
                != verified_observation["sha256"]
                or unfenced.get("tombstone_removed")
                != expected_tombstone_observation
                or not isinstance(unfenced.get("writer_stop_evidence"), Mapping)
                or not isinstance(unfenced.get("runtime_identity"), Mapping)
                or not isinstance(
                    unfenced.get("writer_stop_max_age_seconds"), int
                )
                or unfenced["writer_stop_max_age_seconds"] < 1
            ):
                raise MigrationDrillError("fresh_install_rollback_journal_invalid")
            completed_at = _parse_timestamp(
                unfenced.get("unfenced_at"),
                code="fresh_install_rollback_journal_invalid",
            )
        else:
            completed_at = _utc(now)
            completion_stopped = validate_writer_stop_evidence(
                writer_stop_evidence,
                now=completed_at,
                max_age_seconds=max_writer_stop_age_seconds,
            )
            completion_runtime_identity = _fresh_install_runtime_identity()
            if tombstone_path.exists() or tombstone_path.is_symlink():
                _remove_exact_json_file(tombstone_path, expected=expected_tombstone)
            if tombstone_path.exists() or tombstone_path.is_symlink():
                raise MigrationDrillError("fresh_install_restore_tombstone_release_failed")
            unfenced = {
                "schema_version": FRESH_INSTALL_ROLLBACK_JOURNAL_SCHEMA_VERSION,
                "operation": "restore",
                "phase": "unfenced",
                "transaction_id": transaction_id,
                "maintenance_token_sha256": maintenance_token,
                "verified_sha256": verified_observation["sha256"],
                "unfenced_at": completed_at.isoformat(),
                "tombstone_removed": expected_tombstone_observation,
                "writer_stop_evidence": completion_stopped,
                "writer_stop_max_age_seconds": max_writer_stop_age_seconds,
                "runtime_identity": completion_runtime_identity,
            }
            unfenced_observation = _write_json_no_clobber(unfenced_path, unfenced)
        _require_no_sqlite_sidecars(
            control_path,
            tombstone_present=False,
            maintenance_present=True,
            code="fresh_install_restored_database_fenced",
        )
        process_probes = {
            **dict(prepared["process_probes"]),
            **dict(installed["process_probes"]),
        }
        completion_stopped = dict(unfenced["writer_stop_evidence"])
        completion_runtime_identity = dict(unfenced["runtime_identity"])
        journal = {
            "prepared": prepared_observation,
            "staged": staged_observation,
            "copied": copied_observation,
            "installed": installed_observation,
            "verified": verified_observation,
            "unfenced": unfenced_observation,
        }
        binding = {
            "completed_at": completed_at.isoformat(),
            "transaction_id": transaction_id,
            "config_sha256": config_sha256,
            "audit": audit,
            "configured_databases": {
                "control": str(control_path),
                "delivery": str(delivery_path),
                "same_database": True,
            },
            "quarantine_receipt": quarantine_observation,
            "quarantine_receipt_raw_sha256": source_sha256,
            "quarantine_artifact": quarantine_artifact,
            "restored_live_artifact": restored_live_artifact,
            "db_instance_id": quarantine["receipt"]["db_instance_id"],
            "genesis_intent_sha256": quarantine["receipt"][
                "genesis_intent_sha256"
            ],
            "capacity_transition": restored_capacity,
            "maintenance": {
                "path": str(maintenance_path),
                "maintenance_token_sha256": maintenance_token,
                "observation": marker_observation,
            },
            "writer_stop_evidence": {
                "initial": dict(prepared["writer_stop_evidence"]),
                "completion": completion_stopped,
            },
            "writer_stop_evidence_max_age_seconds": {
                "initial": int(prepared["writer_stop_max_age_seconds"]),
                "completion": int(unfenced["writer_stop_max_age_seconds"]),
            },
            "process_probes": process_probes,
            "journal": journal,
            "runtime_identities": {
                "started": marker["runtime_identity"],
                "completed": completion_runtime_identity,
            },
        }
        proposed_receipt = {
            "schema_version": FRESH_INSTALL_RESTORE_RECEIPT_SCHEMA_VERSION,
            "observed_at": completed_at.isoformat(),
            "ok": True,
            "operation": "restore",
            **binding,
            "restore_binding_sha256": hashlib.sha256(
                _json_payload(binding)
            ).hexdigest(),
        }
        if receipt_path.exists() or receipt_path.is_symlink():
            receipt, _receipt_raw, receipt_observation = _read_json_regular_file(
                receipt_path
            )
            if receipt != proposed_receipt:
                raise MigrationDrillError("fresh_install_restore_receipt_conflict")
        else:
            receipt = proposed_receipt
            receipt_observation = _write_json_no_clobber(receipt_path, receipt)
        receipted_path = _rollback_journal_path(
            evidence_root, "restore", transaction_id, "receipted"
        )
        receipted = {
            "schema_version": FRESH_INSTALL_ROLLBACK_JOURNAL_SCHEMA_VERSION,
            "operation": "restore",
            "phase": "receipted",
            "transaction_id": transaction_id,
            "maintenance_token_sha256": maintenance_token,
            "unfenced_sha256": unfenced_observation["sha256"],
            "receipted_at": completed_at.isoformat(),
            "receipt": receipt_observation,
            "maintenance_release_required": True,
        }
        _write_json_no_clobber(receipted_path, receipted)
        _remove_exact_json_file(maintenance_path, expected=marker)
        return {
            **receipt,
            "applied": not recovered,
            "idempotent": False,
            "recovered": recovered,
        }


def initialize_existing_capacity_transition(
    *,
    migration_receipt_path: str | Path,
    control_db_path: str | Path,
    delivery_db_path: str | Path,
    evidence_dir: str | Path,
    writer_stop_evidence: Mapping[str, Any],
    release_id: str | None,
    bootstrap_epoch_id: str | None,
    apply: bool = False,
    now: datetime | None = None,
    max_writer_stop_age_seconds: int = 900,
    writer_process_probe: Any | None = None,
    operator: str | None = None,
    reason: str | None = None,
) -> dict[str, Any]:
    """Migrate an existing stopped store and initialize its capacity latch once."""
    current = _utc(now)
    audit = _fresh_install_audit_hashes(
        release_id=release_id,
        operator=operator,
        reason=reason,
        required=apply,
    )
    identity = _capacity_identity(
        release_id=release_id,
        bootstrap_epoch_id=bootstrap_epoch_id,
        required=apply,
    )
    migration, _migration_raw, migration_observation = _read_json_regular_file(
        migration_receipt_path
    )
    configured = migration.get("configured_databases")
    if (
        migration.get("schema_version") != STORE_MIGRATION_RECEIPT_SCHEMA_VERSION
        or migration.get("ok") is not True
        or migration.get("mode") != "existing"
        or migration.get("migration_state")
        not in {"migration_required", "already_current"}
        or not isinstance(configured, Mapping)
    ):
        raise MigrationDrillError("existing_capacity_migration_receipt_invalid")
    control_path = _canonical_configured_path(control_db_path)
    delivery_path = _canonical_configured_path(delivery_db_path)
    expected_configured = {
        "control": str(control_path),
        "delivery": str(delivery_path),
        "same_database": control_path == delivery_path,
    }
    if configured != expected_configured or control_path != delivery_path:
        raise MigrationDrillError("existing_capacity_database_layout_invalid")
    if not control_path.exists():
        raise MigrationDrillError("existing_capacity_database_missing")
    stopped = validate_writer_stop_evidence(
        writer_stop_evidence,
        now=current,
        max_age_seconds=max_writer_stop_age_seconds,
    )
    process_probe = writer_process_probe or _default_writer_process_probe
    initial_probe = _checked_writer_process_probe(process_probe)
    evidence_root, _evidence_observation = _secure_directory_observation(
        evidence_dir,
        code="existing_capacity_evidence_directory_invalid",
    )
    parent, parent_observation = _secure_directory_observation(
        control_path.parent,
        code="existing_capacity_parent_invalid",
    )
    receipt_path = evidence_root / "capacity_transition_initialization_receipt.json"
    maintenance_path = Path(f"{control_path}.pnc-rca-maintenance")
    if not apply:
        return {
            "schema_version": "pnc_rca_existing_capacity_initialization_plan_v1",
            "applied": False,
            "configured_databases": expected_configured,
            "migration_receipt_raw_sha256": migration_observation["sha256"],
            "required_capacity_identity": identity,
            "receipt_present": receipt_path.exists() or receipt_path.is_symlink(),
            "maintenance_present": (
                maintenance_path.exists() or maintenance_path.is_symlink()
            ),
            "initial_process_probe": initial_probe,
            "apply_audit_required": True,
        }
    if audit is None or identity is None:
        raise MigrationDrillError("existing_capacity_audit_invalid")
    marker = {
        "schema_version": "pnc_rca_existing_capacity_maintenance_v1",
        "state": "active",
        "created_at": current.isoformat(),
        "configured_databases": expected_configured,
        "migration_receipt": migration_observation,
        "capacity_identity": identity,
        "audit": audit,
    }
    with _fresh_install_lock(parent, parent_observation):
        locked_probe = _checked_writer_process_probe(process_probe)
        recovered = maintenance_path.exists() or maintenance_path.is_symlink()
        if (receipt_path.exists() or receipt_path.is_symlink()) and not recovered:
            existing_receipt, _existing_raw, _existing_observation = (
                _read_json_regular_file(receipt_path)
            )
            origin_identity = existing_receipt.get("capacity_identity")
            if not isinstance(origin_identity, Mapping):
                raise MigrationDrillError("capacity_initialization_receipt_invalid")
            validated = _validate_capacity_initialization_receipt(
                receipt_path=receipt_path,
                database_path=control_path,
                configured_databases=expected_configured,
                migration_receipt_observation=migration_observation,
                expected_identity={
                    "release_id": str(origin_identity.get("release_id") or ""),
                    "bootstrap_epoch_id": str(
                        origin_identity.get("bootstrap_epoch_id") or ""
                    ),
                },
                allowed_operations=frozenset({"existing_migration"}),
            )
            return {
                **validated["receipt"],
                "applied": False,
                "idempotent": True,
                "recovered": False,
                "current_release_identity": identity,
            }
        existing_origin = _existing_capacity_identity(control_path)
        if (
            existing_origin is not None
            and existing_origin.get("state") == "STEADY_ACTIVE"
            and not recovered
        ):
            raise MigrationDrillError(
                "existing_capacity_original_receipt_missing"
            )
        if existing_origin is not None and (
            existing_origin.get("release_id") != identity["release_id"]
            or existing_origin.get("bootstrap_epoch_id")
            != identity["bootstrap_epoch_id"]
        ):
            raise MigrationDrillError("capacity_transition_identity_conflict")
        if recovered:
            existing_marker, _marker_raw, marker_observation = (
                _read_json_regular_file(maintenance_path)
            )
            if (
                existing_marker.get("schema_version")
                != "pnc_rca_existing_capacity_maintenance_v1"
                or existing_marker.get("state") != "active"
                or existing_marker.get("configured_databases")
                != expected_configured
                or existing_marker.get("migration_receipt")
                != migration_observation
                or existing_marker.get("capacity_identity") != identity
                or existing_marker.get("audit") != audit
            ):
                raise MigrationDrillError("existing_capacity_maintenance_conflict")
            marker = existing_marker
        else:
            marker_observation = _write_json_no_clobber(
                maintenance_path, marker
            )
        post_maintenance_probe = _checked_writer_process_probe(process_probe)
        if receipt_path.exists() or receipt_path.is_symlink():
            validated = _validate_capacity_initialization_receipt(
                receipt_path=receipt_path,
                database_path=control_path,
                configured_databases=expected_configured,
                migration_receipt_observation=migration_observation,
                expected_identity=identity,
                allowed_operations=frozenset({"existing_migration"}),
            )
            _remove_exact_json_file(maintenance_path, expected=marker)
            return {
                **validated["receipt"],
                "applied": False,
                "idempotent": True,
                "recovered": recovered,
            }
        validate_writer_stop_evidence(
            writer_stop_evidence,
            now=_utc(now),
            max_age_seconds=max_writer_stop_age_seconds,
        )
        immediate_probe = _checked_writer_process_probe(process_probe)
        try:
            control = RcaControlStore(control_path)
            RcaDeliveryStore(delivery_path)
            control.initialize_capacity_transition(
                release_id=identity["release_id"],
                bootstrap_epoch_id=identity["bootstrap_epoch_id"],
                now=current,
            )
        except (OSError, RuntimeError, sqlite3.Error) as exc:
            raise MigrationDrillError(
                "existing_capacity_initialization_failed"
            ) from exc
        _checkpoint_restore(control_path)
        final_probe = _checked_writer_process_probe(process_probe)
        validation = inspect_sqlite_read_only(
            control_path, ("control", "delivery")
        )
        if validation.get("schemas") != {
            "control": CONTROL_STORE_SCHEMA_VERSION,
            "delivery": DELIVERY_STORE_SCHEMA_VERSION,
        }:
            raise MigrationDrillError("existing_capacity_schema_migration_failed")
        transition = _capacity_transition_snapshot(
            control_path,
            expected_release_id=identity["release_id"],
            expected_bootstrap_epoch_id=identity["bootstrap_epoch_id"],
        )
        process_probes = {
            "initial": initial_probe,
            "locked": locked_probe,
            "post_maintenance": post_maintenance_probe,
            "immediate_pre_migration": immediate_probe,
            "post_migration": final_probe,
        }
        receipt, _receipt_observation = _write_capacity_initialization_receipt(
            receipt_path=receipt_path,
            operation="existing_migration",
            configured_databases=expected_configured,
            migration_receipt=migration_observation,
            migration_receipt_raw_sha256=migration_observation["sha256"],
            capacity_identity=identity,
            capacity_transition=transition,
            audit=audit,
            writer_stop_evidence=stopped,
            process_probes=process_probes,
            observed_at=_utc(now),
        )
        validated = _validate_capacity_initialization_receipt(
            receipt_path=receipt_path,
            database_path=control_path,
            configured_databases=expected_configured,
            migration_receipt_observation=migration_observation,
            expected_identity=identity,
            allowed_operations=frozenset({"existing_migration"}),
        )
        if validated["receipt"] != receipt:
            raise MigrationDrillError("existing_capacity_receipt_conflict")
        if marker_observation.get("path") != str(maintenance_path):
            raise MigrationDrillError("existing_capacity_maintenance_conflict")
        _remove_exact_json_file(maintenance_path, expected=marker)
        return {
            **receipt,
            "applied": not recovered,
            "idempotent": False,
            "recovered": recovered,
        }


def _run_database_drill(
    *,
    configured_path: Path,
    roles: Sequence[str],
    run_root: Path,
    writer_process_probe: Any,
    predecessor_validator: Mapping[str, Any] | None,
    observed_at: datetime,
) -> dict[str, Any]:
    started = time.monotonic()
    sorted_roles = tuple(sorted(roles))
    key = "shared" if len(sorted_roles) == 2 else sorted_roles[0]
    source_exists = configured_path.exists()
    source_bundle_before = _require_sidecar_free_sqlite_bundle(
        configured_path,
        expected_database_present=source_exists,
    )
    source_before: dict[str, Any] | None = None
    predecessor_fixture: dict[str, Any] | None = None
    if source_exists:
        source_before = observe_regular_file(configured_path)
        input_path = configured_path
        pre_validation = inspect_sqlite_read_only(input_path, sorted_roles)
        _validate_supported_pre_schema(pre_validation, sorted_roles)
        source_mode = "existing"
    else:
        input_path = run_root / f"{key}.predecessor.sqlite3"
        _create_predecessor_fixture(input_path, sorted_roles)
        pre_validation = inspect_sqlite_read_only(input_path, sorted_roles)
        if pre_validation["schemas"] != _expected_predecessor_schemas(sorted_roles):
            raise MigrationDrillError("migration_predecessor_schema_invalid")
        _validate_predecessor_fixture_structure(pre_validation, sorted_roles)
        predecessor_fixture = {
            "artifact": observe_regular_file(input_path),
            "validation": pre_validation,
            "schema_transitions": _schema_transitions(sorted_roles),
        }
        source_mode = "fresh_create"
    migration_state = (
        "fresh_install"
        if source_mode == "fresh_create"
        else _migration_state(pre_validation, sorted_roles)
    )
    if migration_state == "migration_required":
        _validate_predecessor_fixture_structure(pre_validation, sorted_roles)

    backup_path = run_root / f"{key}.backup.sqlite3"
    restore_path = run_root / f"{key}.restore.sqlite3"
    rollback_path = run_root / f"{key}.rollback.sqlite3"
    if (
        _require_sidecar_free_sqlite_bundle(
            configured_path,
            expected_database_present=source_exists,
        )
        != source_bundle_before
    ):
        raise MigrationDrillError("migration_source_changed")
    writer_probe_before_backup = _checked_writer_process_probe(writer_process_probe)
    _sqlite_backup(input_path, backup_path)
    writer_probe_after_backup = _checked_writer_process_probe(writer_process_probe)
    if (
        _require_sidecar_free_sqlite_bundle(
            configured_path,
            expected_database_present=source_exists,
        )
        != source_bundle_before
    ):
        raise MigrationDrillError("migration_source_changed")
    backup_validation = inspect_sqlite_read_only(backup_path, sorted_roles)
    if backup_validation["schemas"] != pre_validation["schemas"]:
        raise MigrationDrillError("migration_backup_schema_mismatch")
    _sqlite_backup(backup_path, restore_path)
    migration_observations: dict[str, Any] = {}
    try:
        if "control" in sorted_roles:
            control = RcaControlStore(restore_path)
            migration_observations["control"] = control.initialization_observation()
        if "delivery" in sorted_roles:
            RcaDeliveryStore(restore_path)
            migration_observations["delivery"] = {"mode": "candidate_store_initialize"}
    except (OSError, RuntimeError, sqlite3.Error) as exc:
        raise MigrationDrillError("migration_candidate_upgrade_failed") from exc
    _checkpoint_restore(restore_path)
    restore_validation = inspect_sqlite_read_only(restore_path, sorted_roles)
    if restore_validation["schemas"] != _expected_target_schemas(sorted_roles):
        raise MigrationDrillError("migration_target_schema_mismatch")
    if (
        "control" in sorted_roles
        and set(restore_validation["structure"]["control_v8_indexes"])
        != CONTROL_V8_BASELINE_INDEXES
    ):
        raise MigrationDrillError("migration_control_v8_indexes_missing")
    if (
        "control" in sorted_roles
        and restore_validation["structure"].get("control_v9_activation")
        != _expected_control_activation_structure(present=True)
    ):
        raise MigrationDrillError("migration_control_v9_activation_invalid")
    if (
        "delivery" in sorted_roles
        and restore_validation["structure"]["delivery_task_id_notnull"] != 0
    ):
        raise MigrationDrillError("migration_delivery_v5_watch_invalid")
    expected_transition_structure = _expected_host_runtime_transition_structure(
        present=True
    )
    if (
        restore_validation["structure"].get("host_runtime_transitions")
        != expected_transition_structure
    ):
        raise MigrationDrillError("migration_host_runtime_transition_invalid")
    data_inheritance = compare_sqlite_common_content(backup_path, restore_path)
    rollback_source_path = (
        restore_path if migration_state == "fresh_install" else backup_path
    )
    _sqlite_backup(rollback_source_path, rollback_path)
    rollback_validation = inspect_sqlite_read_only(rollback_path, sorted_roles)
    expected_rollback_validation = (
        restore_validation if migration_state == "fresh_install" else backup_validation
    )
    if rollback_validation != expected_rollback_validation:
        raise MigrationDrillError("migration_rollback_schema_mismatch")
    predecessor_execution = None
    if migration_state == "migration_required" and predecessor_validator is not None:
        predecessor_execution = _run_predecessor_validator(
            validator=predecessor_validator,
            rollback_path=rollback_path,
            roles=sorted_roles,
            run_root=run_root,
            observed_at=observed_at,
        )
    readiness_blockers: list[str] = []
    if migration_state == "already_current":
        readiness_blockers.append("migration_source_already_current")
    if migration_state == "fresh_install":
        readiness_blockers.append("fresh_install_materialization_required")
    if migration_state == "migration_required" and predecessor_validator is None:
        readiness_blockers.append("predecessor_validator_not_configured")
    rollback_ready = (
        (
            migration_state == "migration_required"
            and source_mode == "existing"
            and predecessor_execution is not None
        )
    ) and not readiness_blockers
    rollback_strategy = {
        "migration_required": "restore_predecessor_snapshot_v1",
        "fresh_install": "disable_writers_preserve_current_store_v1",
        "already_current": "idempotency_only_v1",
    }[migration_state]

    source_bundle_after = _require_sidecar_free_sqlite_bundle(
        configured_path,
        expected_database_present=source_exists,
    )
    if source_bundle_after != source_bundle_before:
        raise MigrationDrillError("migration_source_changed")

    return {
        "drill_id": key,
        "roles": list(sorted_roles),
        "migration_state": migration_state,
        "rollback_strategy": rollback_strategy,
        "materialization_required": migration_state == "fresh_install",
        "rollback_ready": rollback_ready,
        "readiness_blockers": sorted(readiness_blockers),
        "schema_transitions": _schema_transitions(sorted_roles),
        "source": {
            "configured_path": str(configured_path),
            "mode": source_mode,
            "exists": source_exists,
            "identity": source_before,
            "bundle_before": source_bundle_before,
            "bundle_after": source_bundle_after,
            "pre_validation": pre_validation,
            "unchanged": True,
        },
        "backup_writer_probe": {
            "process_probe": WRITER_PROCESS_PROBE,
            "before": writer_probe_before_backup,
            "after": writer_probe_after_backup,
        },
        "predecessor_fixture": predecessor_fixture,
        "installation_seed": (
            {
                "artifact": observe_regular_file(restore_path),
                "validation": restore_validation,
            }
            if migration_state == "fresh_install"
            else None
        ),
        "backup": {
            "artifact": observe_regular_file(backup_path),
            "validation": backup_validation,
        },
        "restore": {
            "artifact": observe_regular_file(restore_path),
            "source_backup_sha256": observe_regular_file(backup_path)["sha256"],
            "validation": restore_validation,
            "migration_observations": migration_observations,
            "data_inheritance": data_inheritance,
        },
        "rollback": {
            "artifact": observe_regular_file(rollback_path),
            "source_artifact_sha256": observe_regular_file(rollback_source_path)[
                "sha256"
            ],
            "validation": rollback_validation,
            "strategy": rollback_strategy,
            "compatibility_probe": (
                PREDECESSOR_COMPATIBILITY_PROBE
                if predecessor_execution is not None
                else None
            ),
            "read_only_restore_proven": predecessor_execution is not None,
            "predecessor_validator_execution": predecessor_execution,
            "rollback_ready": rollback_ready,
            "preserve_store_proven": False,
        },
        "duration_ms": round((time.monotonic() - started) * 1000, 3),
    }


def run_migration_drill(
    *,
    control_db_path: str | Path,
    delivery_db_path: str | Path,
    work_dir: str | Path,
    evidence_dir: str | Path,
    writer_stop_evidence: Mapping[str, Any],
    receipt_path: str | Path | None = None,
    repo_root: Path = REPO_ROOT,
    now: datetime | None = None,
    max_writer_stop_age_seconds: int = 900,
    writer_process_probe: Any | None = None,
    predecessor_validator_path: str | Path | None = None,
) -> dict[str, Any]:
    started = time.monotonic()
    current = _utc(now)
    if max_writer_stop_age_seconds < 1:
        raise MigrationDrillError("writer_stop_evidence_max_age_invalid")
    control_path = _canonical_configured_path(control_db_path)
    delivery_path = _canonical_configured_path(delivery_db_path)
    if (
        control_path != delivery_path
        and control_path.exists()
        and delivery_path.exists()
    ):
        control_identity = observe_regular_file(control_path)
        delivery_identity = observe_regular_file(delivery_path)
        if (
            control_identity["device"],
            control_identity["inode"],
        ) == (
            delivery_identity["device"],
            delivery_identity["inode"],
        ):
            raise MigrationDrillError("migration_configured_database_alias")
    work_root = _ensure_directory(work_dir)
    evidence_root = _ensure_directory(evidence_dir)
    for configured in {control_path, delivery_path}:
        try:
            configured.relative_to(work_root)
        except ValueError:
            pass
        else:
            raise MigrationDrillError("migration_source_inside_work_dir")
    run_root = work_root / f"store-migration-{uuid.uuid4().hex}"
    run_root.mkdir(mode=0o700)
    candidate = _candidate_provenance(repo_root)
    predecessor_validator = (
        _git_committed_executable(
            predecessor_validator_path,
            candidate=candidate,
        )
        if predecessor_validator_path is not None
        else None
    )
    stopped = validate_writer_stop_evidence(
        writer_stop_evidence,
        now=current,
        max_age_seconds=max_writer_stop_age_seconds,
    )
    process_probe = writer_process_probe or _default_writer_process_probe
    probe_started_at = current
    _checked_writer_process_probe(process_probe)
    grouped: dict[Path, list[str]] = {}
    grouped.setdefault(control_path, []).append("control")
    grouped.setdefault(delivery_path, []).append("delivery")
    drills = [
        _run_database_drill(
            configured_path=path,
            roles=roles,
            run_root=run_root,
            writer_process_probe=process_probe,
            predecessor_validator=predecessor_validator,
            observed_at=current,
        )
        for path, roles in sorted(grouped.items(), key=lambda item: str(item[0]))
    ]
    final_process_probe = _checked_writer_process_probe(process_probe)
    probe_completed_at = current if now is not None else _utc()
    stopped = {
        **stopped,
        "process_probe": WRITER_PROCESS_PROBE,
        "probe_started_at": probe_started_at.isoformat(),
        "probe_completed_at": probe_completed_at.isoformat(),
        "services": {
            label: {
                **service,
                "process_probe": WRITER_PROCESS_PROBE,
                **final_process_probe[label],
            }
            for label, service in stopped["services"].items()
        },
    }
    source_modes = {drill["source"]["mode"] for drill in drills}
    overall_mode = next(iter(source_modes)) if len(source_modes) == 1 else "mixed"
    migration_states = {drill["migration_state"] for drill in drills}
    overall_migration_state = (
        next(iter(migration_states)) if len(migration_states) == 1 else "mixed"
    )
    rollback_strategies = {drill["rollback_strategy"] for drill in drills}
    overall_rollback_strategy = (
        next(iter(rollback_strategies))
        if len(rollback_strategies) == 1
        else "mixed_v1"
    )
    blockers = sorted({
        blocker
        for drill in drills
        for blocker in drill["readiness_blockers"]
    })
    rollback_ready = bool(drills) and all(
        drill["rollback_ready"] is True for drill in drills
    )
    receipt = {
        "schema_version": STORE_MIGRATION_RECEIPT_SCHEMA_VERSION,
        "observed_at": probe_completed_at.isoformat(),
        "ok": True,
        "mode": overall_mode,
        "migration_state": overall_migration_state,
        "rollback_strategy": overall_rollback_strategy,
        "materialization_required": overall_migration_state == "fresh_install",
        "rollback_ready": rollback_ready,
        "blockers": blockers,
        "work_dir": str(run_root.resolve(strict=True)),
        "configured_databases": {
            "control": str(control_path),
            "delivery": str(delivery_path),
            "same_database": control_path == delivery_path,
        },
        "candidate": candidate,
        "writer_stop_evidence": stopped,
        "database_drills": drills,
        "schema_transitions": _schema_transitions(("control", "delivery")),
        "compatibility_probe": PREDECESSOR_COMPATIBILITY_PROBE,
        "predecessor_validator": (
            predecessor_validator
            if predecessor_validator is not None
            else {
                "status": "not_configured",
                "protocol": PREDECESSOR_COMPATIBILITY_PROBE,
            }
        ),
        "duration_ms": round((time.monotonic() - started) * 1000, 3),
    }
    destination = (
        _absolute(receipt_path)
        if receipt_path is not None
        else evidence_root / "store_migration_receipt.json"
    )
    try:
        destination.parent.resolve(strict=True).relative_to(evidence_root)
    except (OSError, ValueError) as exc:
        raise MigrationDrillError("migration_receipt_outside_evidence_dir") from exc
    write_receipt_atomic(destination, receipt)
    return receipt


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--control-db", type=Path, required=True)
    parser.add_argument("--delivery-db", type=Path, required=True)
    parser.add_argument("--work-dir", type=Path, required=True)
    parser.add_argument("--evidence-dir", type=Path, required=True)
    parser.add_argument("--writer-stop-evidence", type=Path, required=True)
    parser.add_argument("--predecessor-validator", type=Path)
    parser.add_argument("--materialize-fresh-from-receipt", type=Path)
    parser.add_argument("--initialize-existing-from-receipt", type=Path)
    parser.add_argument("--materialization-config-sha256")
    parser.add_argument(
        "--quarantine-fresh-from-materialization-receipt",
        type=Path,
    )
    parser.add_argument(
        "--restore-fresh-from-quarantine-receipt",
        type=Path,
    )
    parser.add_argument("--fresh-rollback-quarantine-dir", type=Path)
    parser.add_argument("--fresh-rollback-config-sha256")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--release-id")
    parser.add_argument("--bootstrap-epoch-id")
    parser.add_argument("--operator")
    parser.add_argument("--reason")
    parser.add_argument("--receipt", type=Path)
    parser.add_argument(
        "--max-writer-stop-age-seconds",
        type=int,
        default=900,
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        writer_stop = _load_writer_stop_evidence(args.writer_stop_evidence)
        selected_modes = sum(
            value is not None
            for value in (
                args.materialize_fresh_from_receipt,
                args.initialize_existing_from_receipt,
                args.quarantine_fresh_from_materialization_receipt,
                args.restore_fresh_from_quarantine_receipt,
            )
        )
        if selected_modes > 1:
            raise MigrationDrillError("fresh_install_operation_mode_conflict")
        if args.materialize_fresh_from_receipt is not None:
            if not args.materialization_config_sha256:
                raise MigrationDrillError(
                    "fresh_install_config_sha256_invalid"
                )
            result = materialize_fresh_install(
                migration_receipt_path=args.materialize_fresh_from_receipt,
                control_db_path=args.control_db,
                delivery_db_path=args.delivery_db,
                config_sha256=args.materialization_config_sha256,
                evidence_dir=args.evidence_dir,
                writer_stop_evidence=writer_stop,
                apply=args.apply,
                max_writer_stop_age_seconds=args.max_writer_stop_age_seconds,
                release_id=args.release_id,
                bootstrap_epoch_id=args.bootstrap_epoch_id,
                operator=args.operator,
                reason=args.reason,
            )
            print(json.dumps(result, ensure_ascii=True, sort_keys=True))
            return 0
        if args.initialize_existing_from_receipt is not None:
            if (
                args.materialization_config_sha256
                or args.fresh_rollback_config_sha256
                or args.fresh_rollback_quarantine_dir
            ):
                raise MigrationDrillError("existing_capacity_operation_mode_invalid")
            result = initialize_existing_capacity_transition(
                migration_receipt_path=args.initialize_existing_from_receipt,
                control_db_path=args.control_db,
                delivery_db_path=args.delivery_db,
                evidence_dir=args.evidence_dir,
                writer_stop_evidence=writer_stop,
                release_id=args.release_id,
                bootstrap_epoch_id=args.bootstrap_epoch_id,
                apply=args.apply,
                max_writer_stop_age_seconds=args.max_writer_stop_age_seconds,
                operator=args.operator,
                reason=args.reason,
            )
            print(json.dumps(result, ensure_ascii=True, sort_keys=True))
            return 0
        if args.quarantine_fresh_from_materialization_receipt is not None:
            if (
                not args.fresh_rollback_config_sha256
                or args.fresh_rollback_quarantine_dir is None
                or args.materialization_config_sha256
                or args.bootstrap_epoch_id
            ):
                raise MigrationDrillError("fresh_install_quarantine_mode_invalid")
            result = quarantine_fresh_install(
                materialization_receipt_path=(
                    args.quarantine_fresh_from_materialization_receipt
                ),
                control_db_path=args.control_db,
                delivery_db_path=args.delivery_db,
                config_sha256=args.fresh_rollback_config_sha256,
                evidence_dir=args.evidence_dir,
                quarantine_dir=args.fresh_rollback_quarantine_dir,
                writer_stop_evidence=writer_stop,
                apply=args.apply,
                max_writer_stop_age_seconds=args.max_writer_stop_age_seconds,
                release_id=args.release_id,
                operator=args.operator,
                reason=args.reason,
            )
            print(json.dumps(result, ensure_ascii=True, sort_keys=True))
            return 0
        if args.restore_fresh_from_quarantine_receipt is not None:
            if (
                not args.fresh_rollback_config_sha256
                or args.fresh_rollback_quarantine_dir is None
                or args.materialization_config_sha256
                or args.bootstrap_epoch_id
            ):
                raise MigrationDrillError("fresh_install_restore_mode_invalid")
            result = restore_fresh_install_from_quarantine(
                quarantine_receipt_path=(
                    args.restore_fresh_from_quarantine_receipt
                ),
                control_db_path=args.control_db,
                delivery_db_path=args.delivery_db,
                config_sha256=args.fresh_rollback_config_sha256,
                evidence_dir=args.evidence_dir,
                quarantine_dir=args.fresh_rollback_quarantine_dir,
                writer_stop_evidence=writer_stop,
                apply=args.apply,
                max_writer_stop_age_seconds=args.max_writer_stop_age_seconds,
                release_id=args.release_id,
                operator=args.operator,
                reason=args.reason,
            )
            print(json.dumps(result, ensure_ascii=True, sort_keys=True))
            return 0
        if (
            args.apply
            or args.materialization_config_sha256
            or args.fresh_rollback_config_sha256
            or args.fresh_rollback_quarantine_dir
            or args.release_id
            or args.bootstrap_epoch_id
            or args.operator
            or args.reason
        ):
            raise MigrationDrillError("fresh_install_materialization_mode_invalid")
        receipt = run_migration_drill(
            control_db_path=args.control_db,
            delivery_db_path=args.delivery_db,
            work_dir=args.work_dir,
            evidence_dir=args.evidence_dir,
            writer_stop_evidence=writer_stop,
            receipt_path=args.receipt,
            max_writer_stop_age_seconds=args.max_writer_stop_age_seconds,
            predecessor_validator_path=args.predecessor_validator,
        )
    except MigrationDrillError as exc:
        print(json.dumps({"ok": False, "code": exc.code}, sort_keys=True))
        return 2
    print(json.dumps(receipt, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
