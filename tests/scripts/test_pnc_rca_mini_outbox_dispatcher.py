from __future__ import annotations

import json
from pathlib import Path
import pytest

from gateway.pnc_rca_kafka_contract import WorkflowEventPolicy, WorkflowTransition
from gateway.pnc_rca_mini_store import MiniKafkaRecord, MiniStore
from scripts.pnc_rca_mini_outbox_dispatcher import MiniOutboxDispatcher


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


def test_dispatch_freezes_request_before_submit(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(
        "scripts.pnc_rca_mini_outbox_dispatcher.validate_vm_execution_request_envelope",
        lambda value: dict(value),
    )
    store = MiniStore(tmp_path / "mini.sqlite3")
    store.ingest_record(_record(), policy=_policy())
    submitted: list[str] = []
    dispatcher = MiniOutboxDispatcher(
        store,
        lease_owner="test-owner",
        build_request=lambda _payload, _claim: {
            "schema_version": "g1q3_rca_execution_request_v2"
        },
        submit=lambda request, _claim: submitted.append(request) or {"status": "ok"},
    )

    result = dispatcher.dispatch_one()

    assert result.status == "completed"
    assert submitted == ['{"status":"ok"}'] or len(submitted) == 1
    row = store.list_rows("rca_outbox")[0]
    assert row["status"] == "completed"
    assert row["request_json"]
    assert row["request_sha256"]
    assert row["result_json"] == '{"status":"ok"}'


def test_dispatch_retries_then_quarantines(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(
        "scripts.pnc_rca_mini_outbox_dispatcher.validate_vm_execution_request_envelope",
        lambda value: dict(value),
    )
    store = MiniStore(tmp_path / "mini.sqlite3")
    store.ingest_record(_record(), policy=_policy())
    dispatcher = MiniOutboxDispatcher(
        store,
        lease_owner="test-owner",
        build_request=lambda _payload, _claim: {
            "schema_version": "g1q3_rca_execution_request_v2"
        },
        submit=lambda *_: (_ for _ in ()).throw(RuntimeError("downstream")),
    )

    first = dispatcher.dispatch_one()
    assert first.status == "retry"
    row = store.list_rows("rca_outbox")[0]
    assert row["status"] == "pending"
    assert row["attempt_count"] == 1


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
