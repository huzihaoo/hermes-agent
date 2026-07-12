import json

import pytest

from gateway.pnc_rca_schema import RcaIssueContext
from gateway.pnc_rca_state_machine import new_intake_state, transition, write_rca_intake_state


def _read_jsonl(path):
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def test_state_receipts_are_append_only_and_ordered(tmp_path):
    state = new_intake_state(
        task_id="g1q3_rca_issue_intake_7008267126",
        group_binding_id="gb_g1q3_rca_feishu_group",
        source={"platform": "feishu", "chat_id": "oc_x", "thread_id": "topic_x", "message_id": "om_x"},
        request_text_excerpt="分析这个问题",
    )
    path = write_rca_intake_state(tmp_path, state)
    enriched = transition(
        state,
        "issue_enriched",
        issue_context=RcaIssueContext(work_item_id="7008267126", source_quality="partial"),
    )
    path2 = write_rca_intake_state(tmp_path, enriched)

    assert path == path2
    records = _read_jsonl(path)
    assert [record["stage"] for record in records] == ["admitted", "issue_enriched"]
    assert records[0]["event_type"] == "rca_intake_state"
    assert records[1]["issue_context"]["work_item_id"] == "7008267126"


def test_blocker_receipt_persists_when_enrichment_fails(tmp_path):
    state = new_intake_state(task_id="task_001")
    blocked = transition(
        state,
        "issue_enrichment_blocked",
        blocker={"kind": "feishu_permission_denied", "message": "permission denied", "retry_after": "auth"},
        retryable=True,
    )
    path = write_rca_intake_state(tmp_path, blocked)

    [record] = _read_jsonl(path)
    assert record["stage"] == "issue_enrichment_blocked"
    assert record["blocker"]["kind"] == "feishu_permission_denied"
    assert record["retryable"] is True


def test_receipts_strip_sensitive_source_and_bound_large_text(tmp_path):
    state = new_intake_state(
        task_id="task_001",
        source={"platform": "feishu", "token": "secret-token", "nested": {"secret": "hidden", "message_id": "om_x"}},
        request_text_excerpt="x" * 1400,
        issue_context={"raw_payload": "should disappear", "work_item_id": "7008267126", "description_markdown": "y" * 2500},
    )
    path = write_rca_intake_state(tmp_path, state)
    payload = path.read_text(encoding="utf-8")
    [record] = _read_jsonl(path)

    assert "secret-token" not in payload
    assert "should disappear" not in payload
    assert record["source"] == {"nested": {"message_id": "om_x"}, "platform": "feishu"}
    assert record["request_text_excerpt"].endswith("...")
    assert record["issue_context"]["description_markdown"].endswith("...")


def test_transition_rejects_unknown_stage_and_fields():
    state = new_intake_state(task_id="task_001")

    with pytest.raises(ValueError, match="unknown RCA intake stage"):
        transition(state, "not_a_stage")
    with pytest.raises(ValueError, match="unknown RCA intake state field"):
        transition(state, "issue_enriched", not_a_field=True)
