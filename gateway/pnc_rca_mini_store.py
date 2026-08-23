"""Independent SQLite inbox, trigger and outbox for the direct Kafka path."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import base64
import hashlib
import json
from pathlib import Path
import sqlite3
from typing import Any, Iterable, Iterator, Mapping, Sequence

from gateway.pnc_rca_admission import build_rca_trigger_context
from gateway.pnc_rca_kafka_contract import (
    WorkflowEventPolicy,
    build_event_admission,
    classify_workflow_event,
)


MINI_STORE_SCHEMA_VERSION = "pnc_rca_mini_store_v1"
MINI_OUTBOX_SCHEMA_VERSION = "pnc_rca_mini_outbox_v1"
DEFAULT_BUSY_TIMEOUT_MS = 5_000
MAX_ERROR_DETAIL = 500

SCHEMA_TABLES = (
    "mini_store_meta",
    "kafka_inbox",
    "kafka_partition_progress",
    "business_triggers",
    "rca_outbox",
)
SCHEMA_COLUMNS = {
    "mini_store_meta": tuple("key value".split()),
    "kafka_inbox": tuple(
        "event_uid topic partition_id offset_id kafka_timestamp_ms record_key raw_value "
        "raw_size_bytes raw_sha256 headers_json policy_json creation_rule_version "
        "received_at decision reason normalized_json business_key submission_key "
        "generation processed_at processing_attempts last_error_code "
        "last_error_detail processing_failed_at".split()
    ),
    "kafka_partition_progress": tuple(
        "topic partition_id first_offset durable_next_offset last_event_uid updated_at".split()
    ),
    "business_triggers": tuple(
        "business_key generation submission_key creation_rule_version work_item_id "
        "project_key project_simple_name work_item_type_key origin_source_id "
        "source_event_id source_topic source_partition source_offset normalized_json "
        "state created_at updated_at".split()
    ),
    "rca_outbox": tuple(
        "outbox_id action business_key submission_key creation_rule_version generation "
        "source_event_id source_topic source_partition source_offset payload_json status "
        "created_at updated_at".split()
    ),
}


class MiniStoreError(RuntimeError):
    """Base error for the additive store."""


class MiniStoreSchemaError(MiniStoreError):
    """Raised when an existing file is not the exact supported schema."""


class MiniRecordConflictError(MiniStoreError):
    """Raised when one transport coordinate is reused for different bytes."""


class MiniRecordNotFoundError(MiniStoreError):
    """Raised when a processing operation names an unknown inbox row."""


@dataclass(frozen=True)
class MiniKafkaRecord:
    """Transport-neutral record accepted by :class:`MiniStore`."""

    topic: str
    partition: int
    offset: int
    value: bytes | bytearray | memoryview | str
    key: bytes | bytearray | memoryview | str | None = None
    timestamp_ms: int | None = None
    headers: tuple[tuple[str, bytes | None], ...] = ()

    def __post_init__(self) -> None:
        topic = str(self.topic or "").strip()
        if not topic:
            raise ValueError("topic must not be empty")
        if (
            isinstance(self.partition, bool)
            or not isinstance(self.partition, int)
            or self.partition < 0
        ):
            raise ValueError("partition must be a non-negative integer")
        if (
            isinstance(self.offset, bool)
            or not isinstance(self.offset, int)
            or self.offset < 0
        ):
            raise ValueError("offset must be a non-negative integer")
        object.__setattr__(self, "topic", topic)
        object.__setattr__(self, "value", _as_bytes(self.value))
        if self.key is not None:
            object.__setattr__(self, "key", _as_bytes(self.key))
        object.__setattr__(self, "headers", tuple(self.headers or ()))

    @property
    def event_uid(self) -> str:
        return f"{self.topic}:{self.partition}:{self.offset}"


@dataclass(frozen=True)
class MiniRawPersistResult:
    event_uid: str
    inserted: bool


@dataclass(frozen=True)
class MiniIngestResult:
    event_uid: str
    decision: str
    reason: str
    raw_inserted: bool = False
    transport_duplicate: bool = False
    trigger_created: bool = False
    outbox_created: bool = False
    business_key: str = ""
    submission_key: str = ""
    generation: int = 0
    ack_safe: bool = False


def _as_bytes(value: bytes | bytearray | memoryview | str) -> bytes:
    if isinstance(value, bytes):
        return value
    if isinstance(value, (bytearray, memoryview)):
        return bytes(value)
    if isinstance(value, str):
        return value.encode("utf-8")
    raise ValueError("record value must be bytes or text")


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _policy(value: WorkflowEventPolicy | Mapping[str, Any]) -> WorkflowEventPolicy:
    if isinstance(value, WorkflowEventPolicy):
        return value
    if isinstance(value, Mapping):
        return WorkflowEventPolicy.from_mapping(value)
    raise TypeError("policy must be a WorkflowEventPolicy or mapping")


def _error_detail(exc: Exception) -> tuple[str, str]:
    return type(exc).__name__[:100], str(exc)[:MAX_ERROR_DETAIL]


def _normalize_record(record: Any) -> MiniKafkaRecord:
    timestamp = getattr(record, "timestamp_ms", None)
    if timestamp is None:
        timestamp = getattr(record, "timestamp", None)
    if isinstance(timestamp, bool) or not isinstance(timestamp, int) or timestamp < 0:
        timestamp = None
    headers = []
    for item in getattr(record, "headers", ()) or ():
        if not isinstance(item, Sequence) or len(item) != 2:
            raise ValueError("record headers must contain name/value pairs")
        name, value = item
        headers.append((str(name), None if value is None else _as_bytes(value)))
    key = getattr(record, "key", None)
    return MiniKafkaRecord(
        topic=str(getattr(record, "topic", "")),
        partition=int(getattr(record, "partition")),
        offset=int(getattr(record, "offset")),
        value=_as_bytes(getattr(record, "value")),
        key=None if key is None else _as_bytes(key),
        timestamp_ms=timestamp,
        headers=tuple(headers),
    )


def _headers_json(headers: Iterable[tuple[str, bytes | None]]) -> str:
    return _canonical_json([
        {
            "name": str(name),
            "value_b64": None
            if value is None
            else base64.b64encode(value).decode("ascii"),
        }
        for name, value in headers
    ])


_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS mini_store_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS kafka_inbox (
 event_uid TEXT PRIMARY KEY, topic TEXT NOT NULL,
 partition_id INTEGER NOT NULL CHECK(partition_id >= 0),
 offset_id INTEGER NOT NULL CHECK(offset_id >= 0), kafka_timestamp_ms INTEGER,
 record_key BLOB, raw_value BLOB NOT NULL, raw_size_bytes INTEGER NOT NULL CHECK(raw_size_bytes >= 0),
 raw_sha256 TEXT NOT NULL CHECK(length(raw_sha256)=64 AND raw_sha256 NOT GLOB '*[^0-9a-f]*'),
 headers_json TEXT NOT NULL, policy_json TEXT NOT NULL, creation_rule_version TEXT NOT NULL,
 received_at TEXT NOT NULL, decision TEXT NOT NULL DEFAULT 'pending'
   CHECK(decision IN ('pending','accepted','filtered','invalid','deduped')),
 reason TEXT NOT NULL DEFAULT '', normalized_json TEXT, business_key TEXT, submission_key TEXT,
 generation INTEGER CHECK(generation IS NULL OR generation >= 1), processed_at TEXT,
 processing_attempts INTEGER NOT NULL DEFAULT 0 CHECK(processing_attempts >= 0),
 last_error_code TEXT NOT NULL DEFAULT '', last_error_detail TEXT NOT NULL DEFAULT '',
 processing_failed_at TEXT, UNIQUE(topic, partition_id, offset_id)
);
CREATE TABLE IF NOT EXISTS kafka_partition_progress (
 topic TEXT NOT NULL, partition_id INTEGER NOT NULL CHECK(partition_id >= 0),
 first_offset INTEGER NOT NULL CHECK(first_offset >= 0),
 durable_next_offset INTEGER NOT NULL CHECK(durable_next_offset > 0),
 last_event_uid TEXT NOT NULL, updated_at TEXT NOT NULL,
 PRIMARY KEY(topic, partition_id), FOREIGN KEY(last_event_uid) REFERENCES kafka_inbox(event_uid)
);
CREATE TABLE IF NOT EXISTS business_triggers (
 business_key TEXT NOT NULL, generation INTEGER NOT NULL CHECK(generation >= 1),
 submission_key TEXT NOT NULL UNIQUE, creation_rule_version TEXT NOT NULL, work_item_id TEXT NOT NULL,
 project_key TEXT NOT NULL, project_simple_name TEXT NOT NULL, work_item_type_key TEXT NOT NULL,
 origin_source_id TEXT NOT NULL, source_event_id TEXT NOT NULL, source_topic TEXT NOT NULL,
 source_partition INTEGER NOT NULL CHECK(source_partition >= 0), source_offset INTEGER NOT NULL CHECK(source_offset >= 0),
 normalized_json TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'pending'
   CHECK(state IN ('pending','claimed','completed','failed','quarantined')),
 created_at TEXT NOT NULL, updated_at TEXT NOT NULL, PRIMARY KEY(business_key, generation)
);
CREATE TABLE IF NOT EXISTS rca_outbox (
 outbox_id INTEGER PRIMARY KEY AUTOINCREMENT, action TEXT NOT NULL, business_key TEXT NOT NULL,
 submission_key TEXT NOT NULL UNIQUE, creation_rule_version TEXT NOT NULL, generation INTEGER NOT NULL CHECK(generation >= 1),
 source_event_id TEXT NOT NULL, source_topic TEXT NOT NULL, source_partition INTEGER NOT NULL CHECK(source_partition >= 0),
 source_offset INTEGER NOT NULL CHECK(source_offset >= 0), payload_json TEXT NOT NULL,
 status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','claimed','completed','quarantined')),
 created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
 FOREIGN KEY(business_key, generation) REFERENCES business_triggers(business_key, generation)
);
CREATE INDEX IF NOT EXISTS idx_mini_inbox_pending ON kafka_inbox(decision, received_at, topic, partition_id, offset_id);
CREATE INDEX IF NOT EXISTS idx_mini_outbox_due ON rca_outbox(status, outbox_id);
CREATE INDEX IF NOT EXISTS idx_mini_trigger_issue ON business_triggers(project_key, work_item_type_key, work_item_id);
"""


class MiniStore:
    """Strict five-table SQLite store; no implicit migration is supported."""

    def __init__(
        self, db_path: str | Path, *, busy_timeout_ms: int = DEFAULT_BUSY_TIMEOUT_MS
    ) -> None:
        if isinstance(busy_timeout_ms, bool) or int(busy_timeout_ms) < 1:
            raise ValueError("busy_timeout_ms must be positive")
        self.db_path = Path(db_path).expanduser()
        self.busy_timeout_ms = int(busy_timeout_ms)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=self.busy_timeout_ms / 1000)
        conn.row_factory = sqlite3.Row
        conn.execute(f"PRAGMA busy_timeout = {self.busy_timeout_ms}")
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.commit()
        except Exception:
            if conn.in_transaction:
                conn.rollback()
            raise
        finally:
            conn.close()

    def _initialize(self) -> None:
        if self.db_path.is_file() and self.db_path.stat().st_size:
            conn = self._connect()
            try:
                tables = {
                    str(row["name"])
                    for row in conn.execute(
                        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
                    )
                }
                if tables and tables != set(SCHEMA_TABLES):
                    raise MiniStoreSchemaError("database is not an additive mini store")
            finally:
                conn.close()
        with self._transaction() as conn:
            conn.executescript(_SCHEMA_SQL)
            marker = conn.execute(
                "SELECT value FROM mini_store_meta WHERE key='schema_version'"
            ).fetchone()
            if marker is None:
                conn.execute(
                    "INSERT INTO mini_store_meta(key,value) VALUES('schema_version',?)",
                    (MINI_STORE_SCHEMA_VERSION,),
                )
                conn.execute(
                    "INSERT INTO mini_store_meta(key,value) VALUES('created_at',?)",
                    (_now_iso(),),
                )
            elif str(marker["value"]) != MINI_STORE_SCHEMA_VERSION:
                raise MiniStoreSchemaError(
                    f"unsupported mini store schema: {marker['value']}"
                )
        self._validate_schema()

    def _validate_schema(self) -> None:
        conn = self._connect()
        try:
            actual = tuple(
                str(row["name"])
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
                )
            )
            if set(actual) != set(SCHEMA_TABLES):
                raise MiniStoreSchemaError(f"unexpected mini store tables: {actual!r}")
            for table in SCHEMA_TABLES:
                columns = tuple(
                    str(row["name"])
                    for row in conn.execute(f'PRAGMA table_info("{table}")')
                )
                if columns != SCHEMA_COLUMNS[table]:
                    raise MiniStoreSchemaError(
                        f"unexpected columns for {table}: {columns!r}"
                    )
        finally:
            conn.close()

    @property
    def schema_version(self) -> str:
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT value FROM mini_store_meta WHERE key='schema_version'"
            ).fetchone()
            if row is None:
                raise MiniStoreSchemaError("schema marker is missing")
            return str(row["value"])
        finally:
            conn.close()

    def _record_values(
        self, record: MiniKafkaRecord, policy: WorkflowEventPolicy
    ) -> tuple[Any, ...]:
        return (
            record.event_uid,
            record.topic,
            record.partition,
            record.offset,
            record.timestamp_ms,
            record.key,
            record.value,
            len(record.value),
            hashlib.sha256(record.value).hexdigest(),
            _headers_json(record.headers),
            _canonical_json(policy.to_dict()),
            policy.policy_version,
            _now_iso(),
        )

    def persist_raw(
        self, record: Any, *, policy: WorkflowEventPolicy | Mapping[str, Any]
    ) -> MiniRawPersistResult:
        record, policy = _normalize_record(record), _policy(policy)
        values, event_uid = self._record_values(record, policy), record.event_uid
        with self._transaction() as conn:
            existing = conn.execute(
                "SELECT raw_sha256 FROM kafka_inbox WHERE event_uid=?", (event_uid,)
            ).fetchone()
            if existing is not None:
                if str(existing["raw_sha256"]) != str(values[8]):
                    raise MiniRecordConflictError(
                        f"payload hash conflict for {event_uid}"
                    )
                return MiniRawPersistResult(event_uid, False)
            conn.execute(
                """
                INSERT INTO kafka_inbox(event_uid,topic,partition_id,offset_id,kafka_timestamp_ms,record_key,raw_value,raw_size_bytes,raw_sha256,headers_json,policy_json,creation_rule_version,received_at)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
                values,
            )
            return MiniRawPersistResult(event_uid, True)

    def ingest_record(
        self, record: Any, *, policy: WorkflowEventPolicy | Mapping[str, Any]
    ) -> MiniIngestResult:
        raw = self.persist_raw(record, policy=policy)
        try:
            result = self.process_event(raw.event_uid)
        except Exception as exc:
            self._record_processing_failure(raw.event_uid, exc)
            raise
        return MiniIngestResult(
            result.event_uid,
            result.decision,
            result.reason,
            raw.inserted,
            result.transport_duplicate,
            result.trigger_created,
            result.outbox_created,
            result.business_key,
            result.submission_key,
            result.generation,
            result.ack_safe,
        )

    def _record_processing_failure(self, event_uid: str, exc: Exception) -> None:
        code, detail = _error_detail(exc)
        with self._transaction() as conn:
            row = conn.execute(
                "SELECT processing_attempts FROM kafka_inbox WHERE event_uid=?",
                (event_uid,),
            ).fetchone()
            if row is None:
                return
            conn.execute(
                """
                UPDATE kafka_inbox SET processing_attempts=?,last_error_code=?,last_error_detail=?,processing_failed_at=?
                WHERE event_uid=? AND decision='pending'
            """,
                (
                    int(row["processing_attempts"]) + 1,
                    code,
                    detail,
                    _now_iso(),
                    event_uid,
                ),
            )

    def _create_business_tx(
        self, conn: sqlite3.Connection, row: sqlite3.Row, normalized: Any, now: str
    ) -> tuple[str, str, int, str, bool, bool]:
        admission = build_event_admission(
            normalized,
            topic=str(row["topic"]),
            partition=int(row["partition_id"]),
            offset=int(row["offset_id"]),
        )
        business_key, submission_key, generation = (
            admission.business_key,
            admission.submission_key,
            int(admission.generation),
        )
        existing = conn.execute(
            "SELECT submission_key FROM business_triggers WHERE business_key=? AND generation=?",
            (business_key, generation),
        ).fetchone()
        if existing is not None:
            if str(existing["submission_key"]) != submission_key:
                raise MiniRecordConflictError("business identity conflict")
            return (
                business_key,
                submission_key,
                generation,
                "business_trigger_exists",
                False,
                False,
            )
        context = build_rca_trigger_context(
            source_kind="kafka_workflow_event",
            project_key=normalized.project_key,
            project_simple_name=normalized.project_simple_name,
            work_item_type_key=normalized.work_item_type_key,
            work_item_id=normalized.work_item_id,
            rule_version=normalized.creation_rule_version,
            issue_url=normalized.issue_url,
            title=normalized.title,
        )
        normalized_json = _canonical_json(normalized.to_dict())
        source_id, topic, partition, offset = (
            str(row["event_uid"]),
            str(row["topic"]),
            int(row["partition_id"]),
            int(row["offset_id"]),
        )
        conn.execute(
            """
            INSERT INTO business_triggers(business_key,generation,submission_key,creation_rule_version,work_item_id,project_key,project_simple_name,work_item_type_key,origin_source_id,source_event_id,source_topic,source_partition,source_offset,normalized_json,state,created_at,updated_at)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?, 'pending', ?, ?)
        """,
            (
                business_key,
                generation,
                submission_key,
                normalized.creation_rule_version,
                normalized.work_item_id,
                normalized.project_key,
                normalized.project_simple_name,
                normalized.work_item_type_key,
                source_id,
                source_id,
                topic,
                partition,
                offset,
                normalized_json,
                now,
                now,
            ),
        )
        payload = _canonical_json({
            "schema_version": MINI_OUTBOX_SCHEMA_VERSION,
            "business_key": business_key,
            "submission_key": submission_key,
            "generation": generation,
            "source_event_id": source_id,
            "topic": topic,
            "partition": partition,
            "offset": offset,
            "admission": admission.to_dict(),
            "trigger_context": context.to_dict(),
            "normalized_event": normalized.to_dict(),
        })
        conn.execute(
            """
            INSERT INTO rca_outbox(action,business_key,submission_key,creation_rule_version,generation,source_event_id,source_topic,source_partition,source_offset,payload_json,status,created_at,updated_at)
            VALUES('submit_rca_issue_intake',?,?,?,?,?,?,?,?,?,?,?,?)
        """,
            (
                business_key,
                submission_key,
                normalized.creation_rule_version,
                generation,
                source_id,
                topic,
                partition,
                offset,
                payload,
                "pending",
                now,
                now,
            ),
        )
        return business_key, submission_key, generation, "", True, True

    def process_event(self, event_uid: str) -> MiniIngestResult:
        with self._transaction() as conn:
            row = conn.execute(
                "SELECT * FROM kafka_inbox WHERE event_uid=?", (event_uid,)
            ).fetchone()
            if row is None:
                raise MiniRecordNotFoundError(event_uid)
            if str(row["decision"]) != "pending":
                return self._result_from_row(row, True, True)
            policy = WorkflowEventPolicy.from_mapping(
                json.loads(str(row["policy_json"]))
            )
            classified = classify_workflow_event(
                topic=str(row["topic"]), value=bytes(row["raw_value"]), policy=policy
            )
            decision, reason, normalized_json = (
                str(classified.decision),
                str(classified.reason),
                None,
            )
            business_key = submission_key = ""
            generation = 0
            trigger_created = outbox_created = False
            now = _now_iso()
            if classified.normalized is not None:
                normalized_json = _canonical_json(classified.normalized.to_dict())
                (
                    business_key,
                    submission_key,
                    generation,
                    reason,
                    trigger_created,
                    outbox_created,
                ) = self._create_business_tx(conn, row, classified.normalized, now)
                decision = "deduped" if not trigger_created else "accepted"
                if trigger_created:
                    reason = "creation_policy_matched"
            conn.execute(
                """
                UPDATE kafka_inbox SET decision=?,reason=?,normalized_json=?,business_key=?,submission_key=?,generation=?,processed_at=?,last_error_code='',last_error_detail='',processing_failed_at=NULL
                WHERE event_uid=? AND decision='pending'
            """,
                (
                    decision,
                    reason,
                    normalized_json,
                    business_key or None,
                    submission_key or None,
                    generation or None,
                    now,
                    event_uid,
                ),
            )
            self._advance_partition_progress_tx(
                conn,
                str(row["topic"]),
                int(row["partition_id"]),
                int(row["offset_id"]),
                event_uid,
                now,
            )
            return MiniIngestResult(
                event_uid,
                decision,
                reason,
                trigger_created=trigger_created,
                outbox_created=outbox_created,
                business_key=business_key,
                submission_key=submission_key,
                generation=generation,
                ack_safe=True,
            )

    @staticmethod
    def _advance_partition_progress_tx(
        conn: sqlite3.Connection,
        topic: str,
        partition: int,
        offset: int,
        event_uid: str,
        updated_at: str,
    ) -> None:
        next_offset = offset + 1
        current = conn.execute(
            "SELECT durable_next_offset FROM kafka_partition_progress WHERE topic=? AND partition_id=?",
            (topic, partition),
        ).fetchone()
        if current is None:
            conn.execute(
                "INSERT INTO kafka_partition_progress(topic,partition_id,first_offset,durable_next_offset,last_event_uid,updated_at) VALUES(?,?,?,?,?,?)",
                (topic, partition, offset, next_offset, event_uid, updated_at),
            )
        elif next_offset > int(current["durable_next_offset"]):
            conn.execute(
                "UPDATE kafka_partition_progress SET durable_next_offset=?,last_event_uid=?,updated_at=? WHERE topic=? AND partition_id=?",
                (next_offset, event_uid, updated_at, topic, partition),
            )

    def partition_progress(
        self, *, topic: str, partitions: Iterable[int]
    ) -> dict[int, int]:
        parts = sorted({int(partition) for partition in partitions})
        if any(partition < 0 for partition in parts):
            raise ValueError("partitions must be non-negative")
        if not parts:
            return {}
        placeholders = ",".join("?" for _ in parts)
        conn = self._connect()
        try:
            rows = conn.execute(
                f"SELECT partition_id,durable_next_offset FROM kafka_partition_progress WHERE topic=? AND partition_id IN ({placeholders})",
                (str(topic), *parts),
            ).fetchall()
            return {
                int(row["partition_id"]): int(row["durable_next_offset"])
                for row in rows
            }
        finally:
            conn.close()

    def ack_safe(self, event_uid: str) -> bool:
        conn = self._connect()
        try:
            row = conn.execute(
                """
                SELECT i.decision,i.partition_id,i.offset_id,p.durable_next_offset
                FROM kafka_inbox i LEFT JOIN kafka_partition_progress p
                  ON p.topic=i.topic AND p.partition_id=i.partition_id WHERE i.event_uid=?
            """,
                (event_uid,),
            ).fetchone()
            return bool(
                row
                and row["decision"] != "pending"
                and row["durable_next_offset"] is not None
                and int(row["durable_next_offset"]) >= int(row["offset_id"]) + 1
            )
        finally:
            conn.close()

    is_ack_safe = ack_safe

    def pending_event_uids(self, *, limit: int = 1_000) -> list[str]:
        if isinstance(limit, bool) or int(limit) < 1:
            raise ValueError("limit must be positive")
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT event_uid FROM kafka_inbox WHERE decision='pending' ORDER BY received_at,topic,partition_id,offset_id LIMIT ?",
                (int(limit),),
            ).fetchall()
            return [str(row["event_uid"]) for row in rows]
        finally:
            conn.close()

    def process_pending(self, *, limit: int = 1_000) -> list[MiniIngestResult]:
        results = []
        for event_uid in self.pending_event_uids(limit=limit):
            try:
                results.append(self.process_event(event_uid))
            except Exception as exc:
                self._record_processing_failure(event_uid, exc)
                raise
        return results

    def get_inbox(self, event_uid: str) -> dict[str, Any] | None:
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT * FROM kafka_inbox WHERE event_uid=?", (event_uid,)
            ).fetchone()
            return None if row is None else dict(row)
        finally:
            conn.close()

    def list_rows(self, table: str) -> list[dict[str, Any]]:
        if table not in SCHEMA_TABLES:
            raise ValueError(f"unknown mini store table: {table}")
        conn = self._connect()
        try:
            return [dict(row) for row in conn.execute(f'SELECT * FROM "{table}"')]
        finally:
            conn.close()

    @staticmethod
    def _result_from_row(
        row: sqlite3.Row, transport_duplicate: bool, ack_safe: bool
    ) -> MiniIngestResult:
        return MiniIngestResult(
            str(row["event_uid"]),
            str(row["decision"]),
            str(row["reason"]),
            transport_duplicate=transport_duplicate,
            business_key=str(row["business_key"] or ""),
            submission_key=str(row["submission_key"] or ""),
            generation=int(row["generation"] or 0),
            ack_safe=ack_safe,
        )


RcaMiniStore = MiniStore
