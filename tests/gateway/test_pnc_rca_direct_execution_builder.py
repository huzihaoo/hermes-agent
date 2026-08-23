from __future__ import annotations

import json
from pathlib import Path

import pytest

from gateway.pnc_rca_direct_execution_builder import (
    DirectExecutionBuildError,
    DirectExecutionEvidenceRequired,
    DirectExecutionIdentityError,
    build_direct_execution_request,
)
from gateway.pnc_issue_context import G1Q3IssueReadResult
from gateway.pnc_rca_mini_store import MiniKafkaRecord, MiniStore
from gateway.pnc_rca_schema import RcaIssueContext
from gateway.pnc_rca_kafka_contract import WorkflowEventPolicy, WorkflowTransition


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


def _store(tmp_path: Path):
    store = MiniStore(tmp_path / "mini.sqlite3")
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
    result = store.ingest_record(
        MiniKafkaRecord(TOPIC, 0, 10, json.dumps(event).encode()),
        policy=_policy(),
    )
    assert result.outbox_created is True
    claim = store.claim_outbox(lease_owner="builder-test")[0]
    payload = json.loads(claim.payload_json)
    return store, claim, payload


def _context(*, project_key: str = "project-key", work_item_id: str = "7041712812"):
    return RcaIssueContext(
        project_key=project_key,
        work_item_type="problem-type",
        work_item_id=work_item_id,
        url=f"https://project.feishu.cn/g1q3/issue/detail/{work_item_id}",
        title="ACC braking issue",
        source_quality="partial",
        pdcl_download_cmd="mdi download event -u demo -s ./",
    )


def test_builds_direct_safe_request_from_typed_preread(tmp_path: Path):
    _store_obj, claim, payload = _store(tmp_path)
    request = build_direct_execution_request(
        payload,
        claim,
        reader=lambda _project, _item: _context(),
    )

    assert request["schema_version"] == "g1q3_rca_execution_request_v2"
    assert request["request_kind"] == "issue_intake"
    assert request["data"]["data_access"]["references"]
    assert request["execution_policy"]["allow_download"] is False
    assert request["execution_policy"]["input_materialization"] == "forbidden"
    refs = request["source_refs"]
    assert refs["submission_key"] == claim.submission_key
    assert refs["origin_source_id"] == claim.origin_source_id
    encoded = json.dumps(request, ensure_ascii=False, sort_keys=True)
    assert "resource_class" not in encoded
    assert "derived_capacity" not in encoded
    assert "w3_execution_snapshot" not in encoded


def test_builder_rejects_missing_preread_without_synthesizing_request(tmp_path: Path):
    _store_obj, claim, payload = _store(tmp_path)

    with pytest.raises(DirectExecutionEvidenceRequired) as exc_info:
        build_direct_execution_request(
            payload,
            claim,
            reader=lambda _project, _item: {"source": "meegle", "context_text": ""},
        )
    assert exc_info.value.retryable is True
    assert exc_info.value.code == "direct_execution_preread_unavailable"


def test_reader_without_context_is_retryable(tmp_path: Path):
    _store_obj, claim, payload = _store(tmp_path)

    result = G1Q3IssueReadResult(
        blocker={"kind": "host_meegle_preread_timeout", "retryable": True},
        source="meegle",
        status="read_failed",
    )

    with pytest.raises(DirectExecutionEvidenceRequired) as exc_info:
        build_direct_execution_request(payload, claim, reader=lambda *_: result)
    assert exc_info.value.code == "host_meegle_preread_timeout"
    assert exc_info.value.retryable is True


def test_typed_unavailable_context_is_retryable(tmp_path: Path):
    _store_obj, claim, payload = _store(tmp_path)
    context = RcaIssueContext(
        project_key="project-key",
        work_item_type="problem-type",
        work_item_id="7041712812",
        url="https://project.feishu.cn/g1q3/issue/detail/7041712812",
        source_quality="unavailable",
    )

    with pytest.raises(DirectExecutionEvidenceRequired) as exc_info:
        build_direct_execution_request(payload, claim, reader=lambda *_: context)
    assert exc_info.value.code == "direct_execution_preread_unavailable"
    assert exc_info.value.retryable is True


def test_reader_identity_mismatch_is_permanent(tmp_path: Path):
    _store_obj, claim, payload = _store(tmp_path)

    with pytest.raises(DirectExecutionIdentityError) as exc_info:
        build_direct_execution_request(
            payload,
            claim,
            reader=lambda _project, _item: _context(project_key="other-project"),
        )
    assert exc_info.value.code == "direct_execution_context_project_mismatch"
    assert exc_info.value.retryable is False


def test_invalid_remote_reference_does_not_create_blocked_request(tmp_path: Path):
    _store_obj, claim, payload = _store(tmp_path)
    context = _context()
    context = RcaIssueContext(**{
        **context.__dict__,
        "pdcl_download_cmd": "not-a-pdcl-address",
    })

    with pytest.raises(DirectExecutionEvidenceRequired) as exc_info:
        build_direct_execution_request(payload, claim, reader=lambda *_: context)
    assert exc_info.value.code == "issue_field_invalid_remote_data_reference"


def test_tampered_event_identity_is_rejected(tmp_path: Path):
    _store_obj, claim, payload = _store(tmp_path)
    payload["normalized_event"]["work_item_id"] = "999999"

    with pytest.raises(DirectExecutionIdentityError) as exc_info:
        build_direct_execution_request(
            payload,
            claim,
            reader=lambda *_: _context(),
        )
    assert exc_info.value.code == "direct_execution_payload_claim_mismatch"
