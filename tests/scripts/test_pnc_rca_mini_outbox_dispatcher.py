from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sqlite3

import pytest

from gateway.pnc_rca_kafka_contract import WorkflowEventPolicy, WorkflowTransition
from gateway.pnc_rca_mini_store import MiniKafkaRecord, MiniStore, _stable_source_id
from scripts.pnc_rca_mini_outbox_dispatcher import (
    MiniOutboxDispatcher,
    PermanentDispatchError,
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


def _record(offset: int = 10) -> MiniKafkaRecord:
    value = json.dumps(
        {
            "id": 7041712812,
            "name": "ACC braking issue",
            "nodes": [
                {"state_key": "new-problem-state", "pre_status": 1, "cur_status": 2}
            ],
            "project_key": "project-key",
            "project_simple_name": "g1q3",
            "status_change_type": "Reached",
            "updated_at": 1783650000000,
            "work_item_type_key": "problem-type",
        },
        separators=(",", ":"),
    ).encode()
    return MiniKafkaRecord(TOPIC, 0, offset, value)


def _store(tmp_path: Path) -> MiniStore:
    store = MiniStore(tmp_path / "mini.sqlite3")
    store.ingest_record(_record(), policy=_policy())
    return store


def _existing_status(claim, *, state: str = "running", **identity):
    expected = {
        "submission_key": claim.submission_key,
        "business_key": claim.business_key,
        "generation": claim.generation,
        "origin_source_id": claim.origin_source_id,
    }
    expected.update(identity)
    return {"state": state, "identity": expected}


def _request(_payload, _claim):
    return {"schema_version": "g1q3_rca_execution_request_v2"}


def test_dispatch_requires_status_and_create_callbacks(tmp_path: Path):
    store = MiniStore(tmp_path / "mini.sqlite3")
    with pytest.raises(ValueError, match="status callback"):
        MiniOutboxDispatcher(store, lease_owner="owner", create=lambda *_: {})
    with pytest.raises(ValueError, match="create callback"):
        MiniOutboxDispatcher(store, lease_owner="owner", status=lambda *_: {})


def test_exact_missing_freezes_then_creates_and_post_reconciles(tmp_path: Path):
    store = _store(tmp_path)
    status_calls: list[str] = []
    created: list[str] = []

    def status(task_id, claim):
        status_calls.append(task_id)
        if len(status_calls) == 1:
            return {"state": "missing"}
        return _existing_status(claim)

    def create(request_json, claim):
        row = store.list_rows("rca_outbox")[0]
        assert row["request_json"] == request_json
        assert row["request_sha256"]
        created.append(claim.submission_key)
        return {"success": True, "task_id": claim.submission_key, "state": "created"}

    dispatcher = MiniOutboxDispatcher(
        store,
        lease_owner="test-owner",
        build_request=_request,
        status=status,
        create=create,
    )

    result = dispatcher.dispatch_one()

    assert result.status == "completed"
    assert status_calls == [result.submission_key, result.submission_key]
    assert created == [result.submission_key]
    row = store.list_rows("rca_outbox")[0]
    assert row["status"] == "completed"
    assert row["request_json"]
    assert row["request_sha256"]
    assert json.loads(row["result_json"])["status"] == "reconciled"


def test_pre_status_matching_deduplicates_without_create(tmp_path: Path):
    store = _store(tmp_path)
    dispatcher = MiniOutboxDispatcher(
        store,
        lease_owner="test-owner",
        build_request=lambda *_: pytest.fail("matching status built a request"),
        status=lambda _task_id, claim: _existing_status(claim, state="completed"),
        create=lambda *_: pytest.fail("matching status called create"),
    )

    result = dispatcher.dispatch_one()

    assert result.status == "deduped"
    row = store.list_rows("rca_outbox")[0]
    assert row["status"] == "completed"
    assert row["request_json"] == ""
    assert json.loads(row["result_json"])["status"] == "deduped"


@pytest.mark.parametrize("state", ["unknown", "not_found", "absent"])
def test_pre_status_unknown_retries_without_create(tmp_path: Path, state: str):
    store = _store(tmp_path)
    dispatcher = MiniOutboxDispatcher(
        store,
        lease_owner="test-owner",
        build_request=_request,
        status=lambda *_: {"state": state},
        create=lambda *_: pytest.fail("unknown status called create"),
    )

    result = dispatcher.dispatch_one()

    assert result.status == "retry"
    assert result.error_code == "status_unknown"
    row = store.list_rows("rca_outbox")[0]
    assert row["status"] == "pending"
    assert row["attempt_count"] == 1


def test_pre_status_read_error_retries_without_create(tmp_path: Path):
    store = _store(tmp_path)

    def status(*_args):
        raise RuntimeError("status transport unavailable")

    result = MiniOutboxDispatcher(
        store,
        lease_owner="test-owner",
        build_request=_request,
        status=status,
        create=lambda *_: pytest.fail("read error called create"),
    ).dispatch_one()

    assert result.status == "retry"
    assert result.error_code == "status_read_error"
    assert store.list_rows("rca_outbox")[0]["status"] == "pending"


@pytest.mark.parametrize(
    ("identity_field", "bad_value"),
    [
        ("submission_key", "another-submission"),
        ("business_key", "another-business"),
        ("generation", 99),
        ("origin_source_id", "another-source"),
    ],
)
def test_pre_status_identity_mismatch_quarantines_without_create(
    tmp_path: Path,
    identity_field: str,
    bad_value,
):
    store = _store(tmp_path)

    result = MiniOutboxDispatcher(
        store,
        lease_owner="test-owner",
        build_request=_request,
        status=lambda _task_id, claim: _existing_status(
            claim, **{identity_field: bad_value}
        ),
        create=lambda *_: pytest.fail("mismatched status called create"),
    ).dispatch_one()

    assert result.status == "quarantined"
    assert result.error_code == "identity_mismatch"
    assert store.list_rows("rca_outbox")[0]["status"] == "quarantined"


@pytest.mark.parametrize(
    ("response", "error_code"),
    [
        ({"state": "maybe"}, "status_schema_error"),
        ({"state": "missing", "task_id": "contradictory"}, "identity_mismatch"),
        ({"state": "unknown", "identity": 123}, "status_schema_error"),
    ],
)
def test_status_schema_errors_quarantine_without_create(
    tmp_path: Path, response, error_code: str
):
    store = _store(tmp_path)

    result = MiniOutboxDispatcher(
        store,
        lease_owner="test-owner",
        build_request=_request,
        status=lambda *_: response,
        create=lambda *_: pytest.fail("invalid status called create"),
    ).dispatch_one()

    assert result.status == "quarantined"
    assert result.error_code == error_code


def test_existing_status_without_explicit_identity_is_quarantined(tmp_path: Path):
    store = _store(tmp_path)

    result = MiniOutboxDispatcher(
        store,
        lease_owner="test-owner",
        build_request=lambda *_: pytest.fail("unbound status built a request"),
        status=lambda *_: {"state": "running"},
        create=lambda *_: pytest.fail("unbound status called create"),
    ).dispatch_one()

    assert result.status == "quarantined"
    assert result.error_code == "status_schema_error"
    assert store.list_rows("rca_outbox")[0]["status"] == "quarantined"


def test_conflicting_nested_status_identity_is_quarantined(tmp_path: Path):
    store = _store(tmp_path)

    result = MiniOutboxDispatcher(
        store,
        lease_owner="test-owner",
        build_request=lambda *_: pytest.fail("conflicting status built a request"),
        status=lambda _task_id, claim: {
            "state": "running",
            "submission_key": claim.submission_key,
            "identity": {"submission_key": "another-submission"},
        },
        create=lambda *_: pytest.fail("conflicting status called create"),
    ).dispatch_one()

    assert result.status == "quarantined"
    assert result.error_code == "status_schema_error"
    assert store.list_rows("rca_outbox")[0]["status"] == "quarantined"


def test_create_return_always_reconciles_and_missing_retries_same_key(tmp_path: Path):
    store = _store(tmp_path)
    status_calls: list[str] = []
    create_calls: list[str] = []

    def status(task_id, _claim):
        status_calls.append(task_id)
        return {"state": "missing"}

    def create(_request, claim):
        create_calls.append(claim.submission_key)
        return {"success": True, "task_id": claim.submission_key}

    result = MiniOutboxDispatcher(
        store,
        lease_owner="test-owner",
        build_request=_request,
        status=status,
        create=create,
    ).dispatch_one()

    assert result.status == "retry"
    assert result.error_code == "post_status_missing"
    assert status_calls == [result.submission_key, result.submission_key]
    assert create_calls == [result.submission_key]
    row = store.list_rows("rca_outbox")[0]
    assert row["status"] == "pending"
    assert row["submission_key"] == result.submission_key
    assert row["request_json"]


def test_create_exception_post_matching_completes(tmp_path: Path):
    store = _store(tmp_path)
    status_calls = 0

    def status(_task_id, claim):
        nonlocal status_calls
        status_calls += 1
        return {"state": "missing"} if status_calls == 1 else _existing_status(claim)

    def create(*_args):
        raise TimeoutError("create response lost")

    result = MiniOutboxDispatcher(
        store,
        lease_owner="test-owner",
        build_request=_request,
        status=status,
        create=create,
    ).dispatch_one()

    assert result.status == "completed"
    assert status_calls == 2
    stored = json.loads(store.list_rows("rca_outbox")[0]["result_json"])
    assert stored["create_error_code"] == "TimeoutError"


def test_create_exception_post_unknown_retries_same_key(tmp_path: Path):
    store = _store(tmp_path)
    calls: list[str] = []

    def status(task_id, _claim):
        calls.append(task_id)
        return {"state": "missing"} if len(calls) == 1 else {"state": "unknown"}

    result = MiniOutboxDispatcher(
        store,
        lease_owner="test-owner",
        build_request=_request,
        status=status,
        create=lambda *_: (_ for _ in ()).throw(RuntimeError("create failed")),
    ).dispatch_one()

    assert result.status == "retry"
    assert calls == [result.submission_key, result.submission_key]
    assert store.list_rows("rca_outbox")[0]["status"] == "pending"


def test_create_return_post_identity_mismatch_quarantines(tmp_path: Path):
    store = _store(tmp_path)
    calls = 0

    def status(_task_id, claim):
        nonlocal calls
        calls += 1
        if calls == 1:
            return {"state": "missing"}
        return _existing_status(claim, submission_key="another-submission")

    result = MiniOutboxDispatcher(
        store,
        lease_owner="test-owner",
        build_request=_request,
        status=status,
        create=lambda _request, claim: {
            "success": True,
            "task_id": claim.submission_key,
        },
    ).dispatch_one()

    assert calls == 2
    assert result.status == "quarantined"
    assert result.error_code == "identity_mismatch"


def test_permanent_create_error_still_post_checks_then_retries_if_unknown(
    tmp_path: Path,
):
    store = _store(tmp_path)
    calls = 0

    def status(*_args):
        nonlocal calls
        calls += 1
        return {"state": "missing"} if calls == 1 else {"state": "unknown"}

    result = MiniOutboxDispatcher(
        store,
        lease_owner="test-owner",
        build_request=_request,
        status=status,
        create=lambda _request, _claim: {"success": True, "task_id": "wrong"},
    ).dispatch_one()

    assert calls == 2
    assert result.status == "retry"
    assert result.error_code == "status_unknown"


def test_create_rejected_post_matching_completes(tmp_path: Path):
    store = _store(tmp_path)
    calls = 0

    def status(_task_id, claim):
        nonlocal calls
        calls += 1
        return {"state": "missing"} if calls == 1 else _existing_status(claim)

    result = MiniOutboxDispatcher(
        store,
        lease_owner="test-owner",
        build_request=_request,
        status=status,
        create=lambda _request, claim: {
            "success": False,
            "task_id": claim.submission_key,
            "error": "remote rejected after accepting the task",
        },
    ).dispatch_one()

    assert result.status == "completed"
    assert calls == 2
    stored = json.loads(store.list_rows("rca_outbox")[0]["result_json"])
    assert stored["create_error_code"] == "create_rejected"


@pytest.mark.parametrize(
    "create_result",
    [None, {"success": True}],
)
def test_malformed_create_result_post_matching_completes(tmp_path: Path, create_result):
    store = _store(tmp_path)
    calls = 0

    def status(_task_id, claim):
        nonlocal calls
        calls += 1
        return {"state": "missing"} if calls == 1 else _existing_status(claim)

    result = MiniOutboxDispatcher(
        store,
        lease_owner="test-owner",
        build_request=_request,
        status=status,
        create=lambda *_: create_result,
    ).dispatch_one()

    assert result.status == "completed"
    assert calls == 2
    stored = json.loads(store.list_rows("rca_outbox")[0]["result_json"])
    assert stored["create_error_code"] == "create_schema_error"


def test_permanent_create_exception_post_matching_completes(tmp_path: Path):
    store = _store(tmp_path)
    calls = 0

    def status(_task_id, claim):
        nonlocal calls
        calls += 1
        return {"state": "missing"} if calls == 1 else _existing_status(claim)

    def create(*_args):
        raise PermanentDispatchError("create contract rejected locally")

    result = MiniOutboxDispatcher(
        store,
        lease_owner="test-owner",
        build_request=_request,
        status=status,
        create=create,
    ).dispatch_one()

    assert result.status == "completed"
    assert calls == 2


def test_invalid_admission_is_immediately_quarantined(tmp_path: Path):
    store = _store(tmp_path)
    row = store.list_rows("rca_outbox")[0]
    payload = json.loads(row["payload_json"])
    payload["admission"]["schema_version"] = "unsupported"
    with sqlite3.connect(store.db_path) as connection:
        connection.execute(
            "UPDATE rca_outbox SET payload_json = ?",
            (json.dumps(payload, sort_keys=True, separators=(",", ":")),),
        )

    result = MiniOutboxDispatcher(
        store,
        lease_owner="test-owner",
        build_request=_request,
        status=lambda *_: pytest.fail("invalid admission read status"),
        create=lambda *_: pytest.fail("invalid admission called create"),
    ).dispatch_one()

    assert result.status == "quarantined"
    assert result.error_code == "permanent_dispatch_error"


def test_permanent_builder_error_quarantines_without_create(tmp_path: Path):
    store = _store(tmp_path)

    def build_request(*_args):
        raise PermanentDispatchError("request schema mismatch")

    result = MiniOutboxDispatcher(
        store,
        lease_owner="test-owner",
        build_request=build_request,
        status=lambda *_: {"state": "missing"},
        create=lambda *_: pytest.fail("invalid request called create"),
    ).dispatch_one()

    assert result.status == "quarantined"
    assert result.error_code == "permanent_dispatch_error"


def test_origin_source_id_uses_g1q3_hash(tmp_path: Path):
    store = MiniStore(tmp_path / "mini.sqlite3")
    store.ingest_record(_record(), policy=_policy())
    trigger = store.list_rows("business_triggers")[0]
    assert trigger["origin_source_id"].startswith("g1q3-rca-source-v1-")
    assert trigger["source_event_id"] == "feishu-project-workflow-event:0:10"


def test_stale_lease_cannot_complete(tmp_path: Path):
    store = MiniStore(tmp_path / "mini.sqlite3")
    store.ingest_record(_record(), policy=_policy())
    claim = store.claim_outbox(lease_owner="owner-a")[0]
    with pytest.raises(Exception):
        store.complete_outbox(
            claim.outbox_id, lease_owner="owner-b", result={"ok": True}
        )
    assert store.list_rows("rca_outbox")[0]["status"] == "claimed"


def test_frozen_request_is_byte_stable(tmp_path: Path):
    store = MiniStore(tmp_path / "mini.sqlite3")
    store.ingest_record(_record(), policy=_policy())
    claim = store.claim_outbox(lease_owner="owner-a")[0]
    first, digest = store.freeze_execution_request(
        claim.outbox_id, lease_owner="owner-a", request={"b": 2, "a": 1}
    )
    second, same_digest = store.freeze_execution_request(
        claim.outbox_id, lease_owner="owner-a", request='{"a":1,"b":2}'
    )
    assert first == second
    assert digest == same_digest


def test_retrigger_source_identity_includes_generation():
    event_uid = "feishu-project-workflow-event:0:10"
    assert _stable_source_id(event_uid, 2, "kafka_retrigger") != _stable_source_id(
        event_uid, 1, "issue_created"
    )


def test_expired_lease_cannot_complete(tmp_path: Path):
    store = MiniStore(tmp_path / "mini.sqlite3")
    store.ingest_record(_record(), policy=_policy())
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    claim = store.claim_outbox(lease_owner="owner-a", lease_seconds=1, now=start)[0]
    with pytest.raises(Exception):
        store.complete_outbox(
            claim.outbox_id,
            lease_owner="owner-a",
            result={"ok": True},
            now=start + timedelta(seconds=2),
        )
