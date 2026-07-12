import json

from pathlib import Path

from gateway.pnc_pdcl_contract import classify_invalid_pdcl, parse_pdcl_command
from gateway.pnc_rca_schema import (
    RCA_EXECUTION_REQUEST_SCHEMA_VERSION,
    RCA_INTAKE_STATE_SCHEMA_VERSION,
    RcaIntakeState,
    RcaIssueContext,
    build_execution_request,
    issue_context_from_compact_text,
    is_valid_pdcl_download_cmd,
    validate_issue_context_fields,
    to_dict,
    to_json,
)


def test_issue_context_defaults_are_safe_and_privacy_light():
    ctx = RcaIssueContext()
    data = to_dict(ctx)

    assert data["project_key"] == ""
    assert data["work_item_type"] == "issue"
    assert data["source_quality"] == "unavailable"
    assert data["comments_timeline"] == []
    assert data["blockers"] == []


def test_intake_state_serializes_deterministically_with_source_routing():
    state = RcaIntakeState(
        task_id="task_001",
        stage="issue_enriched",
        group_binding_id="gb_g1q3_rca_feishu_group",
        source={"platform": "feishu", "chat_id": "oc_x", "thread_id": "topic_x", "message_id": "om_x"},
        request_text_excerpt="分析这个问题",
        issue_context=RcaIssueContext(work_item_id="7008267126", source_quality="partial"),
        created_at="2026-06-08T00:00:00+00:00",
    )

    payload = json.loads(to_json(state))
    assert payload["schema_version"] == RCA_INTAKE_STATE_SCHEMA_VERSION
    assert payload["source"]["platform"] == "feishu"
    assert payload["source"]["message_id"] == "om_x"
    assert payload["issue_context"]["work_item_id"] == "7008267126"
    assert to_json(state) == to_json(state)


def test_serialization_drops_raw_payload_and_token_keys():
    state = RcaIntakeState(
        task_id="task_001",
        issue_context={
            "work_item_id": "7008267126",
            "raw_feishu_payload": {"secret": "should_not_persist"},
            "token": "secret-token",
        },
        created_at="2026-06-08T00:00:00+00:00",
    )

    payload_text = to_json(state)
    assert "raw_feishu_payload" not in payload_text
    assert "secret-token" not in payload_text
    assert "7008267126" in payload_text


def test_issue_context_from_compact_text_extracts_known_fields_without_raw_payload():
    ctx = issue_context_from_compact_text(
        project_key="t03o4q",
        work_item_id="7008267126",
        url="https://project.feishu.cn/t03o4q/issue/detail/7008267126",
        compact_text="\n".join(
            [
                "## Feishu issue 已解析字段（主控侧读取）",
                "- title: G1Q3_6351 ACC",
                "- 当前状态: OPEN",
                "- 当前负责人: 张三, 李四",
                "- 所属项目: G1Q3",
                "- frame_id: 318153",
                "- 数据地址: mdi download event -u demo -s ./",
                "- 根因分析字段: 目标误识别",
            ]
        ),
        source_quality="full",
    )

    assert ctx.project_key == "t03o4q"
    assert ctx.title == "G1Q3_6351 ACC"
    assert ctx.status == "OPEN"
    assert ctx.owners == ["张三", "李四"]
    assert ctx.frame_id == "318153"
    assert ctx.source_quality == "full"
    assert ctx.blockers == []


def test_unavailable_issue_context_gets_structured_blocker():
    ctx = issue_context_from_compact_text(project_key="t03o4q", work_item_id="7008267126", compact_text="")

    assert ctx.source_quality == "unavailable"
    assert ctx.blockers == [{"kind": "host_preread_unavailable", "message": "Feishu issue preread unavailable"}]


def test_build_execution_request_has_policy_defaults_and_survives_fields():
    ctx = RcaIssueContext(
        project_key="t03o4q",
        work_item_id="7008267126",
        url="https://project.feishu.cn/t03o4q/issue/detail/7008267126",
        title="G1Q3_6351 ACC",
        status="OPEN",
        owners=["张三"],
        project_label="G1Q3",
        frame_id="318153",
        pdcl_download_cmd="mdi download event -u demo -s ./",
        root_cause_text="目标误识别",
        description_markdown="compact issue context",
        source_quality="partial",
    )

    request = build_execution_request(
        request_kind="issue_intake",
        task_id="g1q3_rca_issue_intake_7008267126",
        issue_context=ctx,
        request_text_excerpt="分析这个问题",
        source_group_id="oc_group",
        source_message_id="om_msg",
        artifact_root="/mnt/tmp/g1q3_rca_issue_intake_7008267126/",
        artifact_cifs_root="//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/g1q3_rca_issue_intake_7008267126/",
    )

    payload = json.loads(to_json(request))
    assert payload["schema_version"] == RCA_EXECUTION_REQUEST_SCHEMA_VERSION
    assert payload["work_item"]["work_item_id"] == "7008267126"
    assert payload["case"]["frame_id"] == "318153"
    assert payload["data"]["pdcl_download_cmd"].startswith("mdi download")
    assert payload["evidence"]["source_quality"] == "partial"
    assert payload["execution_policy"] == {
        "allow_download": False,
        "allow_feishu_writeback": False,
        "artifact_root": "/mnt/tmp/g1q3_rca_issue_intake_7008267126/",
        "group_response_cap": "L1",
        "mode": "readonly_status_first",
        "translate_baseline": "production",
        "translate_contract_path": "",
    }
    assert payload["source_refs"]["source_group_id"] == "oc_group"
    assert payload["source_refs"]["source_message_id"] == "om_msg"


def test_build_execution_request_carries_translate_baseline_candidate():
    ctx = RcaIssueContext(work_item_id="7026690721", source_quality="partial")
    request = build_execution_request(
        request_kind="issue_intake",
        task_id="g1q3_rca_issue_intake_7026690721",
        issue_context=ctx,
        translate_baseline="candidate",
        translate_contract_path="api/g1q3_rca/translate_contract.candidate.json",
    )

    payload = json.loads(to_json(request))
    assert payload["execution_policy"]["translate_baseline"] == "candidate"
    assert payload["execution_policy"]["translate_contract_path"] == "api/g1q3_rca/translate_contract.candidate.json"


def test_validate_issue_context_fields_distinguishes_missing_and_invalid_pdcl():
    missing_ctx, missing_blocker = validate_issue_context_fields(
        RcaIssueContext(work_item_id="7015689036", source_quality="partial")
    )
    invalid_ctx, invalid_blocker = validate_issue_context_fields(
        RcaIssueContext(work_item_id="7015689036", source_quality="partial", pdcl_download_cmd="mdi refresh -f /home/mini/map.txt")
    )
    valid_ctx, valid_blocker = validate_issue_context_fields(
        RcaIssueContext(work_item_id="7015689036", source_quality="partial", pdcl_download_cmd="mdi download event -u demo -s ./")
    )

    assert missing_ctx.is_pdcl_format is False
    assert missing_blocker["kind"] == "issue_field_missing_pdcl_download_cmd"
    assert missing_blocker["sub_kind"] == "empty"
    assert invalid_ctx.is_pdcl_format is False
    assert invalid_blocker["kind"] == "issue_field_invalid_pdcl_download_cmd"
    assert invalid_blocker["sub_kind"] == "bad_mdi_form"
    assert valid_ctx.is_pdcl_format is True
    assert valid_blocker is None
    assert is_valid_pdcl_download_cmd("mdi download clip -u abc -s ./") is True


# Host and VM must keep tests/gateway/data/pdcl_command_vectors.json and
# api/g1q3_rca/tests/data/pdcl_command_vectors.json byte-for-byte aligned.
def test_pdcl_command_vectors_drive_host_contract():
    vectors = json.loads((Path(__file__).parent / "data" / "pdcl_command_vectors.json").read_text(encoding="utf-8"))
    assert len(vectors) >= 14
    for vector in vectors:
        parsed = parse_pdcl_command(vector["cmd"])
        assert (parsed is not None) is vector["valid"], vector["cmd"]
        assert is_valid_pdcl_download_cmd(vector["cmd"]) is vector["valid"]
        if parsed is not None:
            for key in ("verb", "ticket_ids", "event_ids", "clip_ukeys", "raw_refs"):
                if key in vector:
                    assert parsed[key] == vector[key]


def test_trigger_card_refresh_command_is_pdcl_format_without_blocker():
    ctx, blocker = validate_issue_context_fields(RcaIssueContext(
        work_item_id="7020647953",
        source_quality="full",
        pdcl_download_cmd="mdi refresh -t 019eac96-ce14-7439-5112-ed9506fe4126 -e  019e8ddb-684f-7bd0-460d-d06210a06b62  -s ./",
    ))
    assert ctx.is_pdcl_format is True
    assert blocker is None


def test_classify_invalid_pdcl_samples_without_widening_allowlist():
    assert classify_invalid_pdcl("/media/nas/G1Q3_RCA/7025391597/demo.record") == "nas_path"
    assert classify_invalid_pdcl("cyber_recorder play -f /media/nas/G1Q3_RCA/7025391597/demo.record") == "replay_cmd"
    assert classify_invalid_pdcl("") == "empty"
    assert classify_invalid_pdcl("/mnt/tmp/foo") == "nas_path"
    assert classify_invalid_pdcl("https://example.com/data") == "non_mdi"
    assert classify_invalid_pdcl("mdi refresh -f /home/mini/map.txt") == "bad_mdi_form"
    assert is_valid_pdcl_download_cmd("mdi download event -u demo -s ./") is True
    assert is_valid_pdcl_download_cmd("mdi refresh -t ticket -e event -s ./") is True
