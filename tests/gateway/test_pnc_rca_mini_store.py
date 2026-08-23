"""Focused contract tests for the additive MiniStore."""

from __future__ import annotations

import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from gateway.pnc_rca_kafka_contract import WorkflowEventPolicy, WorkflowTransition
from gateway.pnc_rca_mini_store import (
    MINI_OUTBOX_SCHEMA_VERSION,
    MINI_STORE_SCHEMA_VERSION,
    SCHEMA_COLUMNS,
    SCHEMA_TABLES,
    MiniKafkaRecord,
    MiniRecordConflictError,
    MiniRecordNotFoundError,
    MiniStore,
    MiniStoreSchemaError,
)


TOPIC = "feishu-project-workflow-event"


def _policy() -> WorkflowEventPolicy:
    return WorkflowEventPolicy(
        topic=TOPIC,
        policy_version="issue-created-v1",
        project_keys=frozenset({"project-key"}),
        project_simple_names=frozenset({"g1q3"}),
        work_item_type_keys=frozenset({"problem-type"}),
        status_change_types=frozenset({"Reached"}),
        transitions=(WorkflowTransition("new-problem-state", 1, 2),),
    )


def _value(
    *, work_item_id: int = 7041712812, title: str = "ACC braking issue"
) -> bytes:
    return json.dumps(
        {
            "id": work_item_id,
            "name": title,
            "nodes": [
                {
                    "state_key": "new-problem-state",
                    "node_name": "diagnostic only",
                    "pre_status": 1,
                    "cur_status": 2,
                }
            ],
            "project_key": "project-key",
            "project_simple_name": "g1q3",
            "status_change_type": "Reached",
            "updated_at": 1783650000000,
            "work_item_type_key": "problem-type",
        },
        separators=(",", ":"),
    ).encode()


def _record(
    offset: int = 10,
    *,
    partition: int = 0,
    value: bytes | str | None = None,
    work_item_id: int = 7041712812,
) -> MiniKafkaRecord:
    return MiniKafkaRecord(
        topic=TOPIC,
        partition=partition,
        offset=offset,
        value=value if value is not None else _value(work_item_id=work_item_id),
        key=b"source-key",
        timestamp_ms=1783650000000,
        headers=(("trace", b"t-1"), ("empty", None)),
    )


@pytest.fixture()
def store(tmp_path: Path) -> MiniStore:
    return MiniStore(tmp_path / "mini.sqlite3")


def test_schema_is_exact_and_has_no_gate_columns(store: MiniStore):
    assert store.schema_version == MINI_STORE_SCHEMA_VERSION
    with sqlite3.connect(store.db_path) as conn:
        tables = tuple(
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name NOT LIKE 'sqlite_%' ORDER BY name"
            )
        )
        assert tables == tuple(sorted(SCHEMA_TABLES))
        for table, expected in SCHEMA_COLUMNS.items():
            columns = tuple(
                row[1] for row in conn.execute(f'PRAGMA table_info("{table}")')
            )
            assert columns == expected
            assert not {column.lower() for column in expected} & {
                "activation",
                "release",
                "w3",
            }


def test_existing_unrelated_database_is_rejected_before_ddl(tmp_path: Path):
    path = tmp_path / "old.sqlite3"
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE old_data(value TEXT)")
        conn.commit()

    with pytest.raises(MiniStoreSchemaError, match="additive mini store"):
        MiniStore(path)

    with sqlite3.connect(path) as conn:
        assert conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='old_data'"
        ).fetchone()
        assert (
            conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='kafka_inbox'"
            ).fetchone()
            is None
        )


def test_existing_partial_mini_schema_is_not_implicitly_completed(tmp_path: Path):
    path = tmp_path / "partial.sqlite3"
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE mini_store_meta(key TEXT PRIMARY KEY, value TEXT)")
        conn.commit()

    with pytest.raises(MiniStoreSchemaError, match="additive mini store"):
        MiniStore(path)

    with sqlite3.connect(path) as conn:
        tables = tuple(
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name NOT LIKE 'sqlite_%' ORDER BY name"
            )
        )
    assert tables == ("mini_store_meta",)


def test_schema_marker_mismatch_is_fail_closed(tmp_path: Path):
    path = tmp_path / "mini.sqlite3"
    first = MiniStore(path)
    del first
    with sqlite3.connect(path) as conn:
        conn.execute(
            "UPDATE mini_store_meta SET value='pnc_rca_mini_store_v0' "
            "WHERE key='schema_version'"
        )
        conn.commit()
    with pytest.raises(MiniStoreSchemaError, match="unsupported mini store schema"):
        MiniStore(path)


def test_persist_raw_is_durable_before_processing(store: MiniStore):
    raw = store.persist_raw(_record(10), policy=_policy())
    assert raw.inserted is True
    row = store.get_inbox(raw.event_uid)
    assert row is not None
    assert row["decision"] == "pending"
    assert row["raw_size_bytes"] == len(_value())
    assert row["raw_sha256"]
    assert store.list_rows("business_triggers") == []
    assert store.list_rows("rca_outbox") == []
    assert store.partition_progress(topic=TOPIC, partitions=[0]) == {}
    assert store.ack_safe(raw.event_uid) is False


def test_accepted_event_writes_trigger_outbox_and_progress_atomically(store: MiniStore):
    result = store.ingest_record(_record(10), policy=_policy())

    assert result.decision == "accepted"
    assert result.reason == "creation_policy_matched"
    assert result.raw_inserted is True
    assert result.trigger_created is True
    assert result.outbox_created is True
    assert result.generation == 1
    assert result.ack_safe is True
    assert store.ack_safe(result.event_uid) is True
    assert store.is_ack_safe(result.event_uid) is True
    assert store.partition_progress(topic=TOPIC, partitions=[0]) == {0: 11}

    inbox = store.get_inbox(result.event_uid)
    triggers = store.list_rows("business_triggers")
    outbox = store.list_rows("rca_outbox")
    assert inbox is not None
    assert inbox["decision"] == "accepted"
    assert inbox["business_key"] == result.business_key
    assert len(triggers) == len(outbox) == 1
    assert triggers[0]["submission_key"] == result.submission_key
    assert outbox[0]["status"] == "pending"
    payload = json.loads(outbox[0]["payload_json"])
    assert payload["schema_version"] == MINI_OUTBOX_SCHEMA_VERSION
    assert payload["submission_key"] == result.submission_key


def test_same_transport_duplicate_is_idempotent(store: MiniStore):
    first = store.ingest_record(_record(10), policy=_policy())
    second = store.ingest_record(_record(10), policy=_policy())

    assert second.raw_inserted is False
    assert second.transport_duplicate is True
    assert second.decision == first.decision
    assert second.trigger_created is False
    assert second.outbox_created is False
    assert len(store.list_rows("business_triggers")) == 1
    assert len(store.list_rows("rca_outbox")) == 1


def test_payload_hash_conflict_does_not_replace_raw(store: MiniStore):
    first = _record(10)
    store.persist_raw(first, policy=_policy())

    conflicting = _record(10, value=_value(title="changed bytes"))
    with pytest.raises(MiniRecordConflictError, match="payload hash conflict"):
        store.persist_raw(conflicting, policy=_policy())

    row = store.get_inbox(first.event_uid)
    assert row is not None
    assert bytes(row["raw_value"]) == first.value
    assert row["decision"] == "pending"


def test_business_duplicate_at_new_offset_creates_one_trigger(store: MiniStore):
    first = store.ingest_record(_record(10), policy=_policy())
    second = store.ingest_record(_record(11), policy=_policy())

    assert second.transport_duplicate is False
    assert second.decision == "deduped"
    assert second.reason == "business_trigger_exists"
    assert second.business_key == first.business_key
    assert second.submission_key == first.submission_key
    assert second.ack_safe is True
    assert len(store.list_rows("business_triggers")) == 1
    assert len(store.list_rows("rca_outbox")) == 1
    assert store.partition_progress(topic=TOPIC, partitions=[0]) == {0: 12}


@pytest.mark.parametrize(
    ("value", "decision", "reason"),
    [
        (b"not-json", "invalid", "invalid_json"),
        (
            json.dumps({"id": 1, "name": "x"}).encode(),
            "filtered",
            "unsupported_message_shape",
        ),
    ],
)
def test_filtered_or_invalid_rows_are_ack_safe_and_make_no_trigger(
    store: MiniStore,
    value: bytes,
    decision: str,
    reason: str,
):
    result = store.ingest_record(_record(10, value=value), policy=_policy())
    assert result.decision == decision
    assert result.reason == reason
    assert result.trigger_created is False
    assert result.outbox_created is False
    assert result.ack_safe is True
    assert store.ack_safe(result.event_uid) is True
    assert store.partition_progress(topic=TOPIC, partitions=[0]) == {0: 11}
    assert store.list_rows("business_triggers") == []


def test_pending_recovery_processes_raw_rows_before_new_poll(store: MiniStore):
    raw = store.persist_raw(_record(7), policy=_policy())
    assert store.pending_event_uids() == [raw.event_uid]

    recovered = store.process_pending()
    assert len(recovered) == 1
    assert recovered[0].event_uid == raw.event_uid
    assert recovered[0].ack_safe is True
    assert store.pending_event_uids() == []
    assert store.get_inbox(raw.event_uid)["decision"] == "accepted"


def test_processing_failure_keeps_raw_pending_and_records_attempt(
    store: MiniStore,
    monkeypatch: pytest.MonkeyPatch,
):
    raw = store.persist_raw(_record(10), policy=_policy())
    import gateway.pnc_rca_mini_store as module

    original = module.classify_workflow_event
    monkeypatch.setattr(
        module,
        "classify_workflow_event",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("synthetic failure")),
    )
    with pytest.raises(RuntimeError, match="synthetic failure"):
        store.ingest_record(_record(10), policy=_policy())
    row = store.get_inbox(raw.event_uid)
    assert row is not None
    assert row["decision"] == "pending"
    assert row["processing_attempts"] == 1
    assert store.list_rows("business_triggers") == []
    monkeypatch.setattr(module, "classify_workflow_event", original)
    assert store.process_pending()[0].decision == "accepted"


def test_atomic_failure_rolls_back_trigger_outbox_and_progress(
    store: MiniStore,
    monkeypatch: pytest.MonkeyPatch,
):
    raw = store.persist_raw(_record(10), policy=_policy())
    import gateway.pnc_rca_mini_store as module

    original = module.MiniStore._advance_partition_progress_tx

    def fail(*_args, **_kwargs):
        raise RuntimeError("progress write failed")

    monkeypatch.setattr(module.MiniStore, "_advance_partition_progress_tx", fail)
    with pytest.raises(RuntimeError, match="progress write failed"):
        store.process_event(raw.event_uid)
    assert store.get_inbox(raw.event_uid)["decision"] == "pending"
    assert store.list_rows("business_triggers") == []
    assert store.list_rows("rca_outbox") == []
    assert store.partition_progress(topic=TOPIC, partitions=[0]) == {}

    monkeypatch.setattr(
        module.MiniStore,
        "_advance_partition_progress_tx",
        staticmethod(original),
    )
    recovered = store.process_pending()
    assert recovered[0].ack_safe is True


def test_progress_does_not_cross_an_unprocessed_offset_gap(store: MiniStore):
    first = store.ingest_record(_record(10, work_item_id=1), policy=_policy())
    assert first.ack_safe is True
    later = store.ingest_record(_record(14, work_item_id=2), policy=_policy())
    earlier = store.ingest_record(_record(12, work_item_id=3), policy=_policy())

    assert later.ack_safe is False
    assert earlier.ack_safe is False
    assert store.partition_progress(topic=TOPIC, partitions=[0]) == {0: 11}
    progress = store.list_rows("kafka_partition_progress")[0]
    assert progress["first_offset"] == 10
    assert progress["durable_next_offset"] == 11

    missing_11 = store.ingest_record(_record(11, work_item_id=4), policy=_policy())
    assert missing_11.ack_safe is True
    assert store.partition_progress(topic=TOPIC, partitions=[0]) == {0: 13}
    assert store.ack_safe(earlier.event_uid) is True
    assert store.ack_safe(later.event_uid) is False

    missing_13 = store.ingest_record(_record(13, work_item_id=5), policy=_policy())
    assert missing_13.ack_safe is True
    assert store.partition_progress(topic=TOPIC, partitions=[0]) == {0: 15}
    assert store.ack_safe(later.event_uid) is True


def test_terminal_duplicate_rechecks_durable_progress(store: MiniStore):
    store.ingest_record(_record(10, work_item_id=1), policy=_policy())
    first = store.ingest_record(_record(12, work_item_id=2), policy=_policy())
    duplicate = store.ingest_record(_record(12, work_item_id=2), policy=_policy())

    assert first.ack_safe is False
    assert duplicate.transport_duplicate is True
    assert duplicate.ack_safe is False


def test_partition_progress_rejects_negative_partition(store: MiniStore):
    with pytest.raises(ValueError, match="non-negative"):
        store.partition_progress(topic=TOPIC, partitions=[-1])


def test_unknown_event_cannot_authorize_ack(store: MiniStore):
    with pytest.raises(MiniRecordNotFoundError):
        store.process_event("missing:0:1")
    assert store.ack_safe("missing:0:1") is False


def test_legacy_record_shape_is_accepted(store: MiniStore):
    record = SimpleNamespace(
        topic=TOPIC,
        partition=0,
        offset=10,
        value=_value(),
        key=None,
        timestamp=1783650000000,
        headers=[],
    )
    result = store.ingest_record(record, policy=_policy())
    assert result.decision == "accepted"
    assert result.ack_safe is True


def test_outbox_payload_is_canonical_and_contains_only_source_contract(
    store: MiniStore,
):
    result = store.ingest_record(_record(10), policy=_policy())
    payload_text = store.list_rows("rca_outbox")[0]["payload_json"]
    assert payload_text == json.dumps(
        json.loads(payload_text),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    assert result.submission_key in payload_text
    assert "raw_value" not in payload_text
