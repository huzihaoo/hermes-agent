from __future__ import annotations

import json
from pathlib import Path

from gateway.pnc_rca_kafka_contract import WorkflowEventPolicy, WorkflowTransition
from gateway.pnc_issue_context import G1Q3IssueReadResult
from gateway.pnc_rca_mini_store import MiniKafkaRecord, MiniStore
from gateway.pnc_rca_schema import RcaIssueContext
from gateway.pnc_rca_direct_execution_builder import build_direct_execution_request
from scripts.pnc_rca_mini_outbox_dispatcher import (
    DirectVmDispatcherBoundary,
    MiniOutboxDispatcher,
    MiniOutboxDispatcherConfig,
    build_mini_outbox_dispatcher,
    build_host_preread_execution_request,
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


def _store(tmp_path: Path) -> MiniStore:
    return _ingest(MiniStore(tmp_path / "mini.sqlite3"))


def _ingest(store: MiniStore) -> MiniStore:
    event = {
        "id": 7041712812,
        "name": "ACC braking issue",
        "nodes": [{"state_key": "new-problem-state", "pre_status": 1, "cur_status": 2}],
        "project_key": "project-key",
        "project_simple_name": "g1q3",
        "status_change_type": "Reached",
        "updated_at": 1783650000000,
        "work_item_type_key": "problem-type",
    }
    store.ingest_record(
        MiniKafkaRecord(TOPIC, 0, 10, json.dumps(event).encode()),
        policy=_policy(),
    )
    return store


def _reader(_project: str, _item: str) -> RcaIssueContext:
    return RcaIssueContext(
        project_key="project-key",
        work_item_type="problem-type",
        work_item_id="7041712812",
        url="https://project.feishu.cn/g1q3/issue/detail/7041712812",
        title="ACC braking issue",
        source_quality="partial",
        pdcl_download_cmd="mdi download event -u demo -s ./",
    )


class _FakeTransport:
    def __init__(self) -> None:
        self.created: list[dict[str, object]] = []

    def status(self, task_id: str):
        if not self.created:
            return {"state": "missing", "task_id": task_id}
        request = self.created[-1]
        return {
            "state": "existing",
            "task_id": task_id,
            "submission_key": request["submission_key"],
            "identity_sha256": request["identity_sha256"],
        }

    def create(self, request):
        self.created.append(request)
        return {
            "accepted": True,
            "task_id": request["task_id"],
            "submission_key": request["submission_key"],
            "identity_sha256": request["identity_sha256"],
        }


def test_host_preread_builder_closes_mini_store_to_status_first_create(tmp_path: Path):
    store = _store(tmp_path)
    transport = _FakeTransport()
    config = MiniOutboxDispatcherConfig(
        enabled=True,
        submit_enabled=True,
        request_builder="host_preread",
        db_path=tmp_path / "mini.sqlite3",
        health_path=tmp_path / "health.json",
    )
    boundary = DirectVmDispatcherBoundary(
        config,
        transport=transport,
        request_builder=lambda payload, claim: build_direct_execution_request(
            payload, claim, reader=_reader
        ),
    )
    dispatcher = MiniOutboxDispatcher(
        store,
        lease_owner="integration-test",
        status=boundary.status,
        create=boundary.create,
        build_request=lambda payload, claim: build_direct_execution_request(
            payload, claim, reader=_reader
        ),
        request_check=lambda request, claim: None,
    )

    result = dispatcher.dispatch_one()

    assert result.status == "completed"
    assert len(transport.created) == 1
    envelope = transport.created[0]
    assert envelope["create_once"] is True
    assert envelope["allow_download"] is False
    assert envelope["execution_request"]["schema_version"] == (
        "g1q3_rca_execution_request_v2"
    )
    row = store.list_rows("rca_outbox")[0]
    assert row["status"] == "completed"
    assert row["request_sha256"]


def test_host_preread_builder_wrapper_is_explicit_and_not_default():
    config = MiniOutboxDispatcherConfig()
    assert config.request_builder == "prebuilt"
    assert callable(build_host_preread_execution_request)


def test_factory_wires_host_preread_builder_without_legacy_dispatcher(tmp_path: Path):
    runtime_root = (
        tmp_path / ".hermes" / "runtime" / "pnc_agent" / "feishu_issue_kafka_rca_direct"
    )
    runtime_root.mkdir(parents=True)
    runtime_root.chmod(0o700)
    direct_store = _ingest(MiniStore(runtime_root / "mini.sqlite3"))
    transport = _FakeTransport()
    config = MiniOutboxDispatcherConfig(
        enabled=True,
        submit_enabled=True,
        request_builder="host_preread",
        db_path=runtime_root / "mini.sqlite3",
        health_path=runtime_root / "outbox_dispatcher_health.json",
    )
    dispatcher = build_mini_outbox_dispatcher(
        config,
        transport=transport,
        request_reader=_reader,
    )

    result = dispatcher.dispatch_one()

    assert result.status == "completed"
    assert len(transport.created) == 1


def test_host_preread_unavailable_retries_without_create(tmp_path: Path):
    runtime_root = (
        tmp_path / ".hermes" / "runtime" / "pnc_agent" / "feishu_issue_kafka_rca_direct"
    )
    runtime_root.mkdir(parents=True)
    runtime_root.chmod(0o700)
    store = _ingest(MiniStore(runtime_root / "mini.sqlite3"))
    transport = _FakeTransport()
    config = MiniOutboxDispatcherConfig(
        enabled=True,
        submit_enabled=True,
        request_builder="host_preread",
        db_path=runtime_root / "mini.sqlite3",
        health_path=runtime_root / "outbox_dispatcher_health.json",
    )
    unavailable = G1Q3IssueReadResult(
        status="read_failed",
        blocker={"kind": "host_meegle_preread_timeout", "retryable": True},
        source="meegle",
    )
    dispatcher = build_mini_outbox_dispatcher(
        config,
        transport=transport,
        request_reader=lambda *_: unavailable,
    )

    result = dispatcher.dispatch_one()

    assert result.status == "retry"
    assert result.error_code == "host_meegle_preread_timeout"
    assert transport.created == []
    assert store.list_rows("rca_outbox")[0]["status"] == "pending"
