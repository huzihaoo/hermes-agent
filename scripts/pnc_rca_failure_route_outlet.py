#!/usr/bin/env python3
"""Durable internal-only outlet for RCA failure routes.

The outlet deliberately uses a sidecar ledger.  The main delivery database has
an externally attested exact-schema migration contract, so outlet delivery state
must not silently extend that schema.  Route rows remain the source of truth;
this ledger only owns local claim, retry, and receipt-delivery state.
"""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import stat
import sys
import tempfile
from typing import Any, Callable, Mapping
import uuid

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from gateway.pnc_rca_delivery_store import RcaDeliveryStore


OUTLET_SCHEMA_VERSION = "pnc_rca_failure_route_outlet_v1"
OUTLET_INSPECTION_SCHEMA_VERSION = "pnc_rca_failure_route_outlet_inspection_v1"
RECEIPT_SCHEMA_VERSION = "pnc_rca_internal_failure_route_receipt_v1"
INTERNAL_ROUTE_KINDS = frozenset({"internal_backlog", "internal_alert"})
INTERNAL_ROUTE_STATUSES = frozenset({"backlog_pending", "alert_pending"})
OUTLET_TERMINAL_STATUSES = frozenset({"settled", "resolved", "quarantined"})
DEFAULT_RETRY_DELAYS_SECONDS = (2, 5, 10, 30, 60, 300)
OUTLET_TABLE_COLUMNS = frozenset({
    "route_key",
    "outlet_kind",
    "submission_key",
    "business_key",
    "generation",
    "task_id",
    "terminal_error_code",
    "lane",
    "route_owner",
    "route_status",
    "audit_json",
    "route_payload_json",
    "status",
    "attempt",
    "next_attempt_at",
    "lease_token",
    "lease_owner",
    "lease_expires_at",
    "receipt_path",
    "receipt_sha256",
    "last_error_code",
    "last_error_detail",
    "created_at",
    "updated_at",
    "completed_at",
})
_HEX = frozenset("0123456789abcdef")


class FailureRouteOutletError(RuntimeError):
    """The internal route cannot be handled without weakening its contract."""


class FailureRouteOutletSchemaError(FailureRouteOutletError):
    """The source or sidecar schema is unavailable or incompatible."""


class FailureRouteOutletPermanentError(FailureRouteOutletError):
    """The route is malformed and must be quarantined instead of retried."""


class StaleFailureRouteOutletLeaseError(FailureRouteOutletError):
    """A sidecar mutation used an expired or superseded lease token."""


@dataclass(frozen=True)
class FailureRouteOutletClaim:
    route_key: str
    outlet_kind: str
    submission_key: str
    business_key: str
    generation: int
    task_id: str
    terminal_error_code: str
    lane: str
    route_owner: str
    route_status: str
    audit: dict[str, Any]
    route_payload: dict[str, Any]
    attempt: int
    lease_token: str
    lease_owner: str
    lease_expires_at: str


@dataclass(frozen=True)
class FailureRouteOutletReceipt:
    path: str
    sha256: str


@dataclass(frozen=True)
class FailureRouteOutletMutation:
    route_key: str
    status: str
    attempt: int
    next_attempt_at: str = ""


ReceiptSink = Callable[[FailureRouteOutletClaim], FailureRouteOutletReceipt]


def _utc_datetime(value: datetime | None = None) -> datetime:
    selected = value or datetime.now(timezone.utc)
    if selected.tzinfo is None:
        raise ValueError("failure route outlet time must be timezone-aware")
    return selected.astimezone(timezone.utc)


def _iso(value: datetime | None = None) -> str:
    return _utc_datetime(value).isoformat()


def _canonical_bytes(value: Mapping[str, Any]) -> bytes:
    try:
        encoded = json.dumps(
            dict(value),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise FailureRouteOutletPermanentError(
            "failure_route_outlet_payload_not_canonical"
        ) from exc
    return encoded + b"\n"


def _json_object(value: Any, *, error_code: str) -> dict[str, Any]:
    try:
        parsed = json.loads(str(value))
    except (TypeError, ValueError) as exc:
        raise FailureRouteOutletPermanentError(error_code) from exc
    if not isinstance(parsed, dict):
        raise FailureRouteOutletPermanentError(error_code)
    return parsed


def _hex64(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    if len(normalized) != 64 or any(char not in _HEX for char in normalized):
        raise ValueError("failure route outlet receipt hash is invalid")
    return normalized


def _external_effect_marker(value: Any) -> bool:
    if isinstance(value, Mapping):
        for raw_key, child in value.items():
            key = str(raw_key or "").strip().lower()
            if "feishu" in key or key.startswith("effect_") or key == "effect_kind":
                return True
            if _external_effect_marker(child):
                return True
    elif isinstance(value, (list, tuple)):
        return any(_external_effect_marker(child) for child in value)
    return False


class FailureRouteOutlet:
    """Synchronize internal routes into a leased local receipt outlet."""

    @staticmethod
    def _inspection_paths(
        route_store: RcaDeliveryStore | str | Path,
        outlet_root: str | Path | None,
        outlet_db_path: str | Path | None,
    ) -> tuple[Path, Path, Path]:
        delivery_db_path = Path(
            route_store.db_path
            if isinstance(route_store, RcaDeliveryStore)
            else route_store
        ).expanduser().absolute()
        selected_root = (
            Path(outlet_root).expanduser()
            if outlet_root is not None
            else delivery_db_path.parent / "failure-route-outlet"
        ).absolute()
        selected_db = (
            Path(outlet_db_path).expanduser().absolute()
            if outlet_db_path is not None
            else selected_root / "outlet.sqlite3"
        )
        return delivery_db_path, selected_root, selected_db

    @staticmethod
    def _permission_ready(path: Path, *, directory: bool) -> bool:
        observed = path.lstat()
        permission_bits = stat.S_IMODE(observed.st_mode)
        required_modes = [0o444, 0o222]
        if directory:
            required_modes.append(0o111)
        required_access = os.R_OK | os.W_OK | (os.X_OK if directory else 0)
        return all(permission_bits & mode for mode in required_modes) and os.access(
            path,
            required_access,
        )

    @classmethod
    def _inspection_payload(
        cls,
        route_store: RcaDeliveryStore | str | Path,
        outlet_root: str | Path | None,
        outlet_db_path: str | Path | None,
    ) -> dict[str, Any]:
        delivery_db_path, selected_root, selected_db = cls._inspection_paths(
            route_store, outlet_root, outlet_db_path
        )
        return {
            "schema_version": OUTLET_INSPECTION_SCHEMA_VERSION,
            "status": "invalid",
            "ready": False,
            "initialized": False,
            "observed_at": _iso(),
            "delivery_db_path": str(delivery_db_path),
            "root": str(selected_root),
            "db_path": str(selected_db),
            "outlet_schema_version": "",
            "read_only": True,
            "external_writes": False,
            "error": "",
        }

    @classmethod
    def inspect(
        cls,
        route_store: RcaDeliveryStore | str | Path,
        outlet_root: str | Path | None = None,
        *,
        outlet_db_path: str | Path | None = None,
        enabled: bool = True,
    ) -> dict[str, Any]:
        """Return a read-only readiness receipt without materializing the outlet."""
        payload = cls._inspection_payload(
            route_store, outlet_root, outlet_db_path
        )
        if not enabled:
            return {**payload, "status": "disabled", "ready": True}
        try:
            return cls.validate(
                route_store,
                outlet_root,
                outlet_db_path=outlet_db_path,
            )
        except (FailureRouteOutletError, OSError, sqlite3.Error, ValueError) as exc:
            error = str(exc).strip() or "failure_route_outlet_inspection_failed"
            return {
                **payload,
                "error": error[:120],
                "detail": f"{type(exc).__name__}: {exc}"[:500],
            }

    @classmethod
    def validate(
        cls,
        route_store: RcaDeliveryStore | str | Path,
        outlet_root: str | Path | None = None,
        *,
        outlet_db_path: str | Path | None = None,
    ) -> dict[str, Any]:
        """Validate outlet readiness using filesystem metadata and read-only SQLite."""
        payload = cls._inspection_payload(
            route_store, outlet_root, outlet_db_path
        )
        delivery_db_path, selected_root, selected_db = cls._inspection_paths(
            route_store, outlet_root, outlet_db_path
        )
        if selected_db == delivery_db_path:
            raise FailureRouteOutletSchemaError(
                "failure_route_outlet_database_not_sidecar"
            )
        if selected_db.parent != selected_root:
            raise FailureRouteOutletSchemaError(
                "failure_route_outlet_database_parent_invalid"
            )
        try:
            root_stat = selected_root.lstat()
        except FileNotFoundError:
            parent = selected_root.parent
            while True:
                try:
                    parent_stat = parent.lstat()
                    break
                except FileNotFoundError:
                    if parent == parent.parent:
                        raise FailureRouteOutletSchemaError(
                            "failure_route_outlet_root_invalid"
                        ) from None
                    parent = parent.parent
            if stat.S_ISLNK(parent_stat.st_mode) or not stat.S_ISDIR(
                parent_stat.st_mode
            ):
                raise FailureRouteOutletSchemaError(
                    "failure_route_outlet_root_invalid"
                )
            if not cls._permission_ready(parent, directory=True):
                raise FailureRouteOutletSchemaError(
                    "failure_route_outlet_root_permission_denied"
                )
            return {
                **payload,
                "status": "uninitialized",
                "ready": True,
                "error": "",
            }
        if stat.S_ISLNK(root_stat.st_mode) or not stat.S_ISDIR(root_stat.st_mode):
            raise FailureRouteOutletSchemaError("failure_route_outlet_root_invalid")
        if not cls._permission_ready(selected_root, directory=True):
            raise FailureRouteOutletSchemaError(
                "failure_route_outlet_root_permission_denied"
            )
        try:
            db_stat = selected_db.lstat()
        except FileNotFoundError:
            return {
                **payload,
                "status": "uninitialized",
                "ready": True,
                "error": "",
            }
        if stat.S_ISLNK(db_stat.st_mode) or not stat.S_ISREG(db_stat.st_mode):
            raise FailureRouteOutletSchemaError("failure_route_outlet_db_invalid")
        if not cls._permission_ready(selected_db, directory=False):
            raise FailureRouteOutletSchemaError(
                "failure_route_outlet_db_permission_denied"
            )
        wal_path = selected_db.with_name(selected_db.name + "-wal")
        shm_path = selected_db.with_name(selected_db.name + "-shm")
        try:
            wal_stat = wal_path.lstat()
        except FileNotFoundError:
            wal_stat = None
        if wal_stat is None:
            uri = f"{selected_db.as_uri()}?mode=ro&immutable=1"
        else:
            if stat.S_ISLNK(wal_stat.st_mode) or not stat.S_ISREG(wal_stat.st_mode):
                raise FailureRouteOutletSchemaError(
                    "failure_route_outlet_wal_invalid"
                )
            try:
                shm_stat = shm_path.lstat()
            except FileNotFoundError:
                raise FailureRouteOutletSchemaError(
                    "failure_route_outlet_wal_readonly_unavailable"
                ) from None
            if stat.S_ISLNK(shm_stat.st_mode) or not stat.S_ISREG(shm_stat.st_mode):
                raise FailureRouteOutletSchemaError(
                    "failure_route_outlet_wal_invalid"
                )
            uri = f"{selected_db.as_uri()}?mode=ro"
        try:
            conn = sqlite3.connect(uri, uri=True, timeout=5)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA query_only=ON")
            marker = conn.execute(
                "SELECT value FROM failure_route_outlet_meta "
                "WHERE key = 'schema_version'"
            ).fetchone()
            columns = {
                str(row[1])
                for row in conn.execute(
                    "PRAGMA table_info(failure_route_outlets)"
                )
            }
        except sqlite3.Error as exc:
            raise FailureRouteOutletSchemaError(
                "failure_route_outlet_schema_unavailable"
            ) from exc
        finally:
            if "conn" in locals():
                conn.close()
        if (wal_stat is None) != (not wal_path.exists()):
            raise FailureRouteOutletSchemaError(
                "failure_route_outlet_inspection_raced"
            )
        if marker is None or str(marker["value"]) != OUTLET_SCHEMA_VERSION:
            raise FailureRouteOutletSchemaError(
                "failure_route_outlet_schema_not_current"
            )
        if columns != OUTLET_TABLE_COLUMNS:
            raise FailureRouteOutletSchemaError(
                "failure_route_outlet_schema_invalid"
            )
        return {
            **payload,
            "status": "ready",
            "ready": True,
            "initialized": True,
            "outlet_schema_version": OUTLET_SCHEMA_VERSION,
            "error": "",
        }

    def __init__(
        self,
        route_store: RcaDeliveryStore | str | Path,
        outlet_root: str | Path | None = None,
        *,
        outlet_db_path: str | Path | None = None,
        lease_owner: str = "rca-failure-route-outlet",
        lease_seconds: int = 60,
        max_attempts: int = 5,
        retry_delays_seconds: tuple[int, ...] = DEFAULT_RETRY_DELAYS_SECONDS,
        now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
        receipt_sink: ReceiptSink | None = None,
    ):
        self.route_store = (
            route_store
            if isinstance(route_store, RcaDeliveryStore)
            else RcaDeliveryStore(route_store, require_current=True)
        )
        self.delivery_db_path = self.route_store.db_path.expanduser().absolute()
        selected_root = (
            Path(outlet_root).expanduser()
            if outlet_root is not None
            else self.delivery_db_path.parent / "failure-route-outlet"
        )
        self.outlet_root = selected_root.absolute()
        selected_db = (
            Path(outlet_db_path).expanduser().absolute()
            if outlet_db_path is not None
            else self.outlet_root / "outlet.sqlite3"
        )
        self.outlet_db_path = selected_db
        self.lease_owner = str(lease_owner or "").strip()
        self.lease_seconds = int(lease_seconds)
        self.max_attempts = int(max_attempts)
        self.retry_delays_seconds = tuple(int(item) for item in retry_delays_seconds)
        self.now = now
        self.receipt_sink = receipt_sink or self._write_local_receipt
        if not self.lease_owner or len(self.lease_owner.encode("utf-8")) > 160:
            raise ValueError("failure route outlet lease owner is invalid")
        if self.lease_seconds < 1 or self.max_attempts < 1:
            raise ValueError("failure route outlet limits are invalid")
        if not self.retry_delays_seconds or any(
            delay < 0 for delay in self.retry_delays_seconds
        ):
            raise ValueError("failure route outlet retry delays are invalid")
        if self.outlet_db_path == self.delivery_db_path:
            raise ValueError("failure route outlet database must be a sidecar")
        self._ensure_root()
        self._initialize()

    def _ensure_root(self) -> None:
        self.outlet_root.mkdir(parents=True, exist_ok=True, mode=0o700)
        observed = self.outlet_root.lstat()
        if stat.S_ISLNK(observed.st_mode) or not stat.S_ISDIR(observed.st_mode):
            raise FailureRouteOutletSchemaError("failure_route_outlet_root_invalid")
        if self.outlet_db_path.parent != self.outlet_root:
            self.outlet_db_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        try:
            db_stat = self.outlet_db_path.lstat()
        except FileNotFoundError:
            return
        if stat.S_ISLNK(db_stat.st_mode) or not stat.S_ISREG(db_stat.st_mode):
            raise FailureRouteOutletSchemaError("failure_route_outlet_db_invalid")

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.outlet_db_path, timeout=5, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=FULL")
        return conn

    def _initialize(self) -> None:
        conn = self._connect()
        try:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS failure_route_outlet_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS failure_route_outlets (
                    route_key TEXT PRIMARY KEY,
                    outlet_kind TEXT NOT NULL CHECK (
                        outlet_kind IN ('internal_backlog', 'internal_alert')
                    ),
                    submission_key TEXT NOT NULL,
                    business_key TEXT NOT NULL,
                    generation INTEGER NOT NULL CHECK (generation >= 1),
                    task_id TEXT NOT NULL,
                    terminal_error_code TEXT NOT NULL,
                    lane TEXT NOT NULL CHECK (
                        lane IN ('needs_human_input', 'hard_defect')
                    ),
                    route_owner TEXT NOT NULL,
                    route_status TEXT NOT NULL,
                    audit_json TEXT NOT NULL,
                    route_payload_json TEXT NOT NULL,
                    status TEXT NOT NULL CHECK (
                        status IN (
                            'pending', 'claimed', 'retry_wait', 'settled',
                            'resolved', 'quarantined'
                        )
                    ),
                    attempt INTEGER NOT NULL DEFAULT 0 CHECK (attempt >= 0),
                    next_attempt_at TEXT NOT NULL,
                    lease_token TEXT,
                    lease_owner TEXT,
                    lease_expires_at TEXT,
                    receipt_path TEXT NOT NULL DEFAULT '',
                    receipt_sha256 TEXT NOT NULL DEFAULT '',
                    last_error_code TEXT NOT NULL DEFAULT '',
                    last_error_detail TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    completed_at TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_failure_route_outlets_due
                    ON failure_route_outlets(
                        status, next_attempt_at, lease_expires_at, route_key
                    );
                """
            )
            marker = conn.execute(
                "SELECT value FROM failure_route_outlet_meta "
                "WHERE key = 'schema_version'"
            ).fetchone()
            if marker is None:
                conn.execute(
                    "INSERT INTO failure_route_outlet_meta(key, value) VALUES(?, ?)",
                    ("schema_version", OUTLET_SCHEMA_VERSION),
                )
            elif str(marker["value"]) != OUTLET_SCHEMA_VERSION:
                raise FailureRouteOutletSchemaError(
                    "failure_route_outlet_schema_not_current"
                )
            columns = {
                str(row[1])
                for row in conn.execute("PRAGMA table_info(failure_route_outlets)")
            }
            if columns != OUTLET_TABLE_COLUMNS:
                raise FailureRouteOutletSchemaError(
                    "failure_route_outlet_schema_invalid"
                )
        finally:
            conn.close()
        os.chmod(self.outlet_db_path, 0o600)

    def _source_route(self, route_key: str) -> dict[str, Any]:
        selected = str(route_key or "").strip()
        uri = f"{self.delivery_db_path.resolve().as_uri()}?mode=ro"
        try:
            conn = sqlite3.connect(uri, uri=True, timeout=5)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA query_only=ON")
            row = conn.execute(
                """
                SELECT route_key, submission_key, business_key, generation,
                       task_id, terminal_error_code, lane, route_kind, owner,
                       status, audit_json, route_payload_json, created_at
                  FROM rca_failure_routes
                 WHERE route_key = ?
                """,
                (selected,),
            ).fetchone()
        except sqlite3.Error as exc:
            raise FailureRouteOutletSchemaError(
                "failure_route_source_schema_unavailable"
            ) from exc
        finally:
            if "conn" in locals():
                conn.close()
        if row is None:
            raise FailureRouteOutletPermanentError("failure_route_source_missing")
        route = dict(row)
        route_kind = str(route["route_kind"])
        lane = str(route["lane"])
        expected_status = {
            "internal_backlog": "backlog_pending",
            "internal_alert": "alert_pending",
        }.get(route_kind)
        if route_kind not in INTERNAL_ROUTE_KINDS or expected_status is None:
            raise FailureRouteOutletPermanentError(
                "failure_route_outlet_external_route_forbidden"
            )
        if (route_kind == "internal_backlog" and lane != "needs_human_input") or (
            route_kind == "internal_alert" and lane != "hard_defect"
        ):
            raise FailureRouteOutletPermanentError(
                "failure_route_outlet_lane_mismatch"
            )
        if str(route["status"]) not in {
            expected_status,
            "terminal_fallback",
            "resolved",
        }:
            raise FailureRouteOutletPermanentError(
                "failure_route_outlet_status_mismatch"
            )
        expected_key, _dedupe = RcaDeliveryStore._failure_route_identity(
            submission_key=str(route["submission_key"]),
            terminal_error_code=str(route["terminal_error_code"]),
            lane=lane,
            route_kind=route_kind,
        )
        if selected != expected_key:
            raise FailureRouteOutletPermanentError(
                "failure_route_outlet_identity_mismatch"
            )
        route["audit"] = _json_object(
            route["audit_json"], error_code="failure_route_outlet_audit_invalid"
        )
        route["route_payload"] = _json_object(
            route["route_payload_json"],
            error_code="failure_route_outlet_payload_invalid",
        )
        if _external_effect_marker(route["audit"]) or _external_effect_marker(
            route["route_payload"]
        ):
            raise FailureRouteOutletPermanentError(
                "failure_route_outlet_external_effect_forbidden"
            )
        return route

    def sync_route(
        self, route_key: str, *, now: datetime | None = None
    ) -> bool:
        route = self._source_route(route_key)
        current = _iso(now or self.now())
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            existing = conn.execute(
                "SELECT status FROM failure_route_outlets WHERE route_key = ?",
                (route_key,),
            ).fetchone()
            conn.execute(
                """
                INSERT INTO failure_route_outlets(
                    route_key, outlet_kind, submission_key, business_key,
                    generation, task_id, terminal_error_code, lane, route_owner,
                    route_status, audit_json, route_payload_json, status,
                    next_attempt_at, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?)
                ON CONFLICT(route_key) DO UPDATE SET
                    route_status = excluded.route_status,
                    audit_json = CASE
                        WHEN failure_route_outlets.status IN (
                            'pending', 'claimed', 'retry_wait'
                        ) THEN excluded.audit_json
                        ELSE failure_route_outlets.audit_json
                    END,
                    route_payload_json = CASE
                        WHEN failure_route_outlets.status IN (
                            'pending', 'claimed', 'retry_wait'
                        ) THEN excluded.route_payload_json
                        ELSE failure_route_outlets.route_payload_json
                    END,
                    updated_at = excluded.updated_at
                """,
                (
                    str(route["route_key"]),
                    str(route["route_kind"]),
                    str(route["submission_key"]),
                    str(route["business_key"]),
                    int(route["generation"]),
                    str(route["task_id"]),
                    str(route["terminal_error_code"]),
                    str(route["lane"]),
                    str(route["owner"]),
                    str(route["status"]),
                    json.dumps(route["audit"], sort_keys=True, separators=(",", ":")),
                    json.dumps(
                        route["route_payload"], sort_keys=True, separators=(",", ":")
                    ),
                    current,
                    current,
                    current,
                ),
            )
            conn.commit()
            return existing is None
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def claim(
        self,
        *,
        route_key: str | None = None,
        now: datetime | None = None,
    ) -> FailureRouteOutletClaim | None:
        current_dt = _utc_datetime(now or self.now())
        current = _iso(current_dt)
        expires = _iso(current_dt + timedelta(seconds=self.lease_seconds))
        token = uuid.uuid4().hex
        selected_key = str(route_key or "").strip()
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                """
                SELECT * FROM failure_route_outlets
                 WHERE (? = '' OR route_key = ?)
                   AND (
                        (status IN ('pending', 'retry_wait') AND next_attempt_at <= ?)
                        OR (
                            status = 'claimed' AND lease_expires_at IS NOT NULL
                            AND lease_expires_at <= ?
                        )
                   )
                 ORDER BY next_attempt_at, created_at, route_key
                 LIMIT 1
                """,
                (selected_key, selected_key, current, current),
            ).fetchone()
            if row is None:
                conn.commit()
                return None
            attempt = int(row["attempt"]) + 1
            updated = conn.execute(
                """
                UPDATE failure_route_outlets
                   SET status = 'claimed', attempt = ?, lease_token = ?,
                       lease_owner = ?, lease_expires_at = ?, updated_at = ?
                 WHERE route_key = ?
                   AND (
                        (status IN ('pending', 'retry_wait') AND next_attempt_at <= ?)
                        OR (
                            status = 'claimed' AND lease_expires_at IS NOT NULL
                            AND lease_expires_at <= ?
                        )
                   )
                """,
                (
                    attempt,
                    token,
                    self.lease_owner,
                    expires,
                    current,
                    row["route_key"],
                    current,
                    current,
                ),
            )
            if updated.rowcount != 1:
                conn.rollback()
                return None
            conn.commit()
            return FailureRouteOutletClaim(
                route_key=str(row["route_key"]),
                outlet_kind=str(row["outlet_kind"]),
                submission_key=str(row["submission_key"]),
                business_key=str(row["business_key"]),
                generation=int(row["generation"]),
                task_id=str(row["task_id"]),
                terminal_error_code=str(row["terminal_error_code"]),
                lane=str(row["lane"]),
                route_owner=str(row["route_owner"]),
                route_status=str(row["route_status"]),
                audit=_json_object(
                    row["audit_json"], error_code="failure_route_outlet_audit_invalid"
                ),
                route_payload=_json_object(
                    row["route_payload_json"],
                    error_code="failure_route_outlet_payload_invalid",
                ),
                attempt=attempt,
                lease_token=token,
                lease_owner=self.lease_owner,
                lease_expires_at=expires,
            )
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    @staticmethod
    def _current_claim(
        conn: sqlite3.Connection,
        claim: FailureRouteOutletClaim,
        current: str,
    ) -> sqlite3.Row:
        row = conn.execute(
            """
            SELECT * FROM failure_route_outlets
             WHERE route_key = ? AND status = 'claimed'
               AND lease_token = ? AND lease_owner = ?
               AND lease_expires_at IS NOT NULL AND lease_expires_at > ?
            """,
            (
                claim.route_key,
                claim.lease_token,
                claim.lease_owner,
                current,
            ),
        ).fetchone()
        if row is None:
            raise StaleFailureRouteOutletLeaseError(
                f"stale failure-route outlet lease for {claim.route_key}"
            )
        return row

    def retry(
        self,
        claim: FailureRouteOutletClaim,
        *,
        error_code: str,
        error_detail: str,
        delay_seconds: int,
        now: datetime | None = None,
    ) -> FailureRouteOutletMutation:
        if delay_seconds < 0:
            raise ValueError("failure route outlet retry delay is invalid")
        current_dt = _utc_datetime(now or self.now())
        current = _iso(current_dt)
        retry_at = _iso(current_dt + timedelta(seconds=delay_seconds))
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            self._current_claim(conn, claim, current)
            conn.execute(
                """
                UPDATE failure_route_outlets
                   SET status = 'retry_wait', next_attempt_at = ?,
                       lease_token = NULL, lease_owner = NULL,
                       lease_expires_at = NULL, last_error_code = ?,
                       last_error_detail = ?, updated_at = ?
                 WHERE route_key = ? AND lease_token = ?
                """,
                (
                    retry_at,
                    str(error_code or "failure_route_outlet_sink_failed")[:120],
                    str(error_detail or "")[:1000],
                    current,
                    claim.route_key,
                    claim.lease_token,
                ),
            )
            conn.commit()
            return FailureRouteOutletMutation(
                claim.route_key, "retry_wait", claim.attempt, retry_at
            )
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def settle(
        self,
        claim: FailureRouteOutletClaim,
        receipt: FailureRouteOutletReceipt,
        *,
        now: datetime | None = None,
    ) -> FailureRouteOutletMutation:
        receipt_path = Path(receipt.path).expanduser()
        receipt_sha = _hex64(receipt.sha256)
        if not receipt_path.is_absolute():
            raise ValueError("failure route outlet receipt path must be absolute")
        current = _iso(now or self.now())
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            self._current_claim(conn, claim, current)
            conn.execute(
                """
                UPDATE failure_route_outlets
                   SET status = 'settled', receipt_path = ?, receipt_sha256 = ?,
                       next_attempt_at = ?, lease_token = NULL, lease_owner = NULL,
                       lease_expires_at = NULL, last_error_code = '',
                       last_error_detail = '', completed_at = ?, updated_at = ?
                 WHERE route_key = ? AND lease_token = ?
                """,
                (
                    str(receipt_path),
                    receipt_sha,
                    current,
                    current,
                    current,
                    claim.route_key,
                    claim.lease_token,
                ),
            )
            conn.commit()
            return FailureRouteOutletMutation(
                claim.route_key, "settled", claim.attempt, current
            )
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def resolve(
        self, route_key: str, *, now: datetime | None = None
    ) -> FailureRouteOutletMutation:
        current = _iso(now or self.now())
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT attempt FROM failure_route_outlets "
                "WHERE route_key = ? AND status IN ('settled', 'resolved')",
                (route_key,),
            ).fetchone()
            if row is None:
                raise FailureRouteOutletError(
                    "failure_route_outlet_not_settled"
                )
            conn.execute(
                "UPDATE failure_route_outlets SET status = 'resolved', "
                "updated_at = ? WHERE route_key = ? AND status = 'settled'",
                (current, route_key),
            )
            conn.commit()
            return FailureRouteOutletMutation(
                str(route_key), "resolved", int(row["attempt"]), current
            )
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def quarantine(
        self,
        claim: FailureRouteOutletClaim,
        *,
        error_code: str,
        error_detail: str,
        now: datetime | None = None,
    ) -> FailureRouteOutletMutation:
        current = _iso(now or self.now())
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            self._current_claim(conn, claim, current)
            conn.execute(
                """
                UPDATE failure_route_outlets
                   SET status = 'quarantined', next_attempt_at = ?,
                       lease_token = NULL, lease_owner = NULL,
                       lease_expires_at = NULL, last_error_code = ?,
                       last_error_detail = ?, completed_at = ?, updated_at = ?
                 WHERE route_key = ? AND lease_token = ?
                """,
                (
                    current,
                    str(error_code or "failure_route_outlet_quarantined")[:120],
                    str(error_detail or "")[:1000],
                    current,
                    current,
                    claim.route_key,
                    claim.lease_token,
                ),
            )
            conn.commit()
            return FailureRouteOutletMutation(
                claim.route_key, "quarantined", claim.attempt, current
            )
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _retry_delay(self, attempt: int) -> int:
        index = min(max(1, int(attempt)) - 1, len(self.retry_delays_seconds) - 1)
        return self.retry_delays_seconds[index]

    def process_route(
        self, route_key: str, *, now: datetime | None = None
    ) -> dict[str, Any]:
        current = _utc_datetime(now or self.now())
        created = self.sync_route(route_key, now=current)
        claim = self.claim(route_key=route_key, now=current)
        if claim is None:
            row = self.row(route_key)
            return {
                "route_key": route_key,
                "status": str(row.get("status") or "queued"),
                "created": created,
                "attempt": int(row.get("attempt") or 0),
                "external_effects": 0,
            }
        try:
            receipt = self.receipt_sink(claim)
            mutation = self.settle(claim, receipt, now=current)
        except StaleFailureRouteOutletLeaseError:
            raise
        except FailureRouteOutletPermanentError as exc:
            mutation = self.quarantine(
                claim,
                error_code=str(exc),
                error_detail=type(exc).__name__,
                now=current,
            )
        except Exception as exc:
            code = f"failure_route_outlet_sink_failed:{type(exc).__name__}"[:120]
            if claim.attempt >= self.max_attempts:
                mutation = self.quarantine(
                    claim,
                    error_code=code,
                    error_detail=str(exc),
                    now=current,
                )
            else:
                mutation = self.retry(
                    claim,
                    error_code=code,
                    error_detail=str(exc),
                    delay_seconds=self._retry_delay(claim.attempt),
                    now=current,
                )
        return {
            **asdict(mutation),
            "created": created,
            "external_effects": 0,
        }

    def _write_local_receipt(
        self, claim: FailureRouteOutletClaim
    ) -> FailureRouteOutletReceipt:
        if _external_effect_marker(claim.audit) or _external_effect_marker(
            claim.route_payload
        ):
            raise FailureRouteOutletPermanentError(
                "failure_route_outlet_external_effect_forbidden"
            )
        payload = {
            "schema_version": RECEIPT_SCHEMA_VERSION,
            "route_key": claim.route_key,
            "outlet_kind": claim.outlet_kind,
            "delivery_mode": "internal_durable_receipt_only",
            "external_effects": 0,
            "submission_key": claim.submission_key,
            "business_key": claim.business_key,
            "generation": claim.generation,
            "task_id": claim.task_id,
            "terminal_error_code": claim.terminal_error_code,
            "lane": claim.lane,
            "route_owner": claim.route_owner,
            "route_status": claim.route_status,
            "audit": claim.audit,
            "route_payload": claim.route_payload,
        }
        raw = _canonical_bytes(payload)
        receipt_sha = hashlib.sha256(raw).hexdigest()
        destination_dir = self.outlet_root / claim.outlet_kind
        destination_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        directory_stat = destination_dir.lstat()
        if stat.S_ISLNK(directory_stat.st_mode) or not stat.S_ISDIR(
            directory_stat.st_mode
        ):
            raise FailureRouteOutletPermanentError(
                "failure_route_outlet_receipt_directory_invalid"
            )
        destination = destination_dir / f"{claim.route_key}.json"
        if destination.exists() or destination.is_symlink():
            return self._validate_existing_receipt(
                destination, claim=claim, expected_raw=raw
            )
        descriptor, temporary = tempfile.mkstemp(
            prefix=f".{claim.route_key}.", dir=destination_dir
        )
        temporary_path = Path(temporary)
        try:
            os.fchmod(descriptor, 0o600)
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(raw)
                handle.flush()
                os.fsync(handle.fileno())
            try:
                os.link(temporary_path, destination)
            except FileExistsError:
                return self._validate_existing_receipt(
                    destination, claim=claim, expected_raw=raw
                )
            directory_fd = os.open(destination_dir, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        finally:
            try:
                temporary_path.unlink()
            except FileNotFoundError:
                pass
        return FailureRouteOutletReceipt(str(destination.absolute()), receipt_sha)

    @staticmethod
    def _validate_existing_receipt(
        path: Path,
        *,
        claim: FailureRouteOutletClaim,
        expected_raw: bytes,
    ) -> FailureRouteOutletReceipt:
        observed = path.lstat()
        if (
            stat.S_ISLNK(observed.st_mode)
            or not stat.S_ISREG(observed.st_mode)
            or observed.st_nlink != 1
        ):
            raise FailureRouteOutletPermanentError(
                "failure_route_outlet_receipt_identity_invalid"
            )
        raw = path.read_bytes()
        if raw != expected_raw:
            raise FailureRouteOutletPermanentError(
                "failure_route_outlet_receipt_conflict"
            )
        try:
            payload = json.loads(raw)
        except (TypeError, ValueError) as exc:
            raise FailureRouteOutletPermanentError(
                "failure_route_outlet_receipt_invalid"
            ) from exc
        if (
            not isinstance(payload, Mapping)
            or payload.get("schema_version") != RECEIPT_SCHEMA_VERSION
            or payload.get("route_key") != claim.route_key
            or payload.get("outlet_kind") != claim.outlet_kind
            or payload.get("external_effects") != 0
        ):
            raise FailureRouteOutletPermanentError(
                "failure_route_outlet_receipt_invalid"
            )
        return FailureRouteOutletReceipt(
            str(path.absolute()), hashlib.sha256(raw).hexdigest()
        )

    def row(self, route_key: str) -> dict[str, Any]:
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT * FROM failure_route_outlets WHERE route_key = ?",
                (route_key,),
            ).fetchone()
            return dict(row) if row is not None else {}
        finally:
            conn.close()

    def list_rows(self) -> list[dict[str, Any]]:
        conn = self._connect()
        try:
            return [
                dict(row)
                for row in conn.execute(
                    "SELECT * FROM failure_route_outlets ORDER BY created_at, route_key"
                )
            ]
        finally:
            conn.close()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Drain durable RCA routes into internal-only local receipts"
    )
    parser.add_argument("--delivery-db", type=Path, required=True)
    parser.add_argument("--outlet-root", type=Path, required=True)
    parser.add_argument("--route-key", required=True)
    parser.add_argument("--lease-owner", default="rca-failure-route-outlet-cli")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        route_store = RcaDeliveryStore(
            args.delivery_db,
            require_current=True,
            allow_successor_write=True,
        )
        outlet = FailureRouteOutlet(
            route_store,
            args.outlet_root,
            lease_owner=args.lease_owner,
        )
        result = outlet.process_route(args.route_key)
    except (FailureRouteOutletError, OSError, sqlite3.Error, ValueError) as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False))
        return 2
    print(json.dumps({"ok": True, **result}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
