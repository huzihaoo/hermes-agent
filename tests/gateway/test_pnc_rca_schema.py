import json

from pathlib import Path

import pytest

from gateway.pnc_pdcl_contract import (
    classify_invalid_pdcl,
    is_valid_pdcl_download_cmd,
    parse_pdcl_command,
)
from gateway.pnc_rca_schema import (
    RCA_EXECUTION_REQUEST_SCHEMA_VERSION,
    RCA_INTAKE_STATE_SCHEMA_VERSION,
    RcaIntakeState,
    RcaIssueContext,
    build_execution_request,
    issue_context_from_compact_text,
    to_dict,
    to_json,
    validate_issue_context_fields,
    validate_vm_execution_request_envelope,
)
from gateway.pnc_rca_issue_focus import (
    ANALYSIS_CAPABILITY_UNSUPPORTED,
    ANALYSIS_INSUFFICIENT_STATEMENT,
    build_issue_focus_plan,
)


def _nested_mapping(depth):
    value = "leaf"
    for _index in range(depth):
        value = {"n": value}
    return value


@pytest.mark.parametrize(
    ("payload", "error"),
    [
        (
            {
                "toolchain": {},
                "evidence": _nested_mapping(32),
            },
            "rca_vm_request_json_shape_exceeded",
        ),
        (
            {"toolchain": {}, "evidence": {"nodes": [0] * 50_001}},
            "rca_vm_request_json_shape_exceeded",
        ),
        (
            {"toolchain": {}, "evidence": {"text": "x" * (1024 * 1024)}},
            "rca_vm_request_json_bytes_exceeded",
        ),
    ],
)
def test_vm_execution_request_envelope_matches_fixed_service_limits(payload, error):
    with pytest.raises(ValueError, match=error):
        validate_vm_execution_request_envelope(payload)


def test_vm_execution_request_envelope_supports_dispatcher_headroom_limit():
    payload = {"toolchain": {}, "evidence": {"text": "x" * 256}}

    with pytest.raises(ValueError, match="rca_vm_request_json_bytes_exceeded"):
        validate_vm_execution_request_envelope(payload, max_bytes=128)


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
                "- function_category: 研发中心/规划部 | 规划部",
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
    assert ctx.function_category == "研发中心/规划部 | 规划部"
    assert ctx.source_quality == "full"
    assert ctx.blockers == []


def test_issue_context_compact_text_promotes_bounded_comment_timeline():
    ctx = issue_context_from_compact_text(
        project_key="t03o4q",
        work_item_id="7008267126",
        compact_text=(
            "- title: ACC-前车减速过晚\n"
            "## 最近评论摘录\n"
            "- 2026-08-07T10:00:00Z: 目标 ID=7，减速晚\n"
            "- 2026-08-07T10:01:00Z: https://project.feishu.cn/file/stream/download/x"
        ),
        source_quality="full",
    )

    assert [row["created_at"] for row in ctx.comments_timeline] == [
        "2026-08-07T10:00:00Z",
        "2026-08-07T10:01:00Z",
    ]
    assert ctx.comments_timeline[0]["content"].startswith("目标 ID=7")


@pytest.mark.parametrize(
    ("title", "status"),
    [
        ("ACC-车速60限速60跟停减速太晚", "planned"),
        ("HMI-S弯", ANALYSIS_INSUFFICIENT_STATEMENT),
        ("AEB-双闪无能力", ANALYSIS_CAPABILITY_UNSUPPORTED),
    ],
)
def test_issue_focus_plan_is_title_bound_and_fail_closed(title, status):
    plan = build_issue_focus_plan(title=title)

    assert plan["schema_version"] == "g1q3_issue_focus_plan_v1"
    assert plan["analysis_status"] == status
    assert plan["title_sha256"]
    assert plan["plan_sha256"]
    if status == ANALYSIS_CAPABILITY_UNSUPPORTED:
        assert plan["unsupported_capabilities"] == ["vehicle_signal_chain"]
        assert plan["stop_reason"]
    if status == ANALYSIS_INSUFFICIENT_STATEMENT:
        assert plan["missing_requirements"] == ["statement:problem_statement"]


def test_issue_context_carries_canonical_frame_lookup_into_execution_request():
    frame_lookup = {
        "kind": "front_camera_timestamp",
        "management_timestamp": 1_783_841_476_000_000,
        "management_timestamp_unit": "microseconds_since_unix_epoch",
        "timezone": "Asia/Shanghai",
        "max_delta_us": 100_000,
        "topic_priority": ["front_120", "camera1"],
    }
    ctx = issue_context_from_compact_text(
        project_key="t03o4q",
        work_item_id="7049071505",
        compact_text="- frame_lookup: "
        + json.dumps(frame_lookup, ensure_ascii=False, sort_keys=True)
        + "\n- 数据地址: mdi download event -u demo -s ./",
        source_quality="partial",
    )

    request = build_execution_request(
        request_kind="issue_intake",
        task_id="g1q3_rca_issue_intake_7049071505",
        issue_context=ctx,
    )

    assert ctx.frame_id == ""
    assert ctx.frame_lookup == frame_lookup
    assert to_dict(request)["case"]["frame_lookup"] == frame_lookup


def test_build_execution_request_promotes_compound_occurrence_time():
    context = RcaIssueContext(
        work_item_id="7068819154",
        pdcl_download_cmd="mdi download event -u demo -s ./",
        description_markdown=(
            "【发生时间】：20260728112713_2026-07-28_11-26-43_"
            "2026-07-28_11-28-33"
        ),
        source_quality="partial",
    )

    payload = to_dict(build_execution_request(
        request_kind="issue_intake",
        task_id="g1q3_rca_issue_intake_7068819154",
        issue_context=context,
    ))

    assert payload["case"]["issue_time_s"] == 1785209233.0
    assert payload["case"]["issue_time_source"] == (
        "description_occurrence_time_asia_shanghai"
    )


def test_build_execution_request_rejects_midnight_occurrence_placeholder():
    context = RcaIssueContext(
        work_item_id="7068819154",
        pdcl_download_cmd="mdi download event -u demo -s ./",
        description_markdown="【发生时间】：2026-07-28 00:00:00",
        source_quality="partial",
    )

    payload = to_dict(build_execution_request(
        request_kind="issue_intake",
        task_id="g1q3_rca_issue_intake_7068819154_midnight",
        issue_context=context,
    ))

    assert "issue_time_s" not in payload["case"]
    assert "issue_time_source" not in payload["case"]


def test_unavailable_issue_context_gets_structured_blocker():
    ctx = issue_context_from_compact_text(project_key="t03o4q", work_item_id="7008267126", compact_text="")

    assert ctx.source_quality == "unavailable"
    assert ctx.blockers == [{"kind": "host_preread_unavailable", "message": "Feishu issue preread unavailable"}]


def test_build_execution_request_has_policy_defaults_and_survives_fields():
    signed_url = (
        "https://project.feishu.cn/goapi/v5/platform/file/stream/download/"
        "temporary-token?signature=secret"
    )
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
        root_cause_text=f"目标误识别 {signed_url}",
        description_markdown=(
            "compact issue context\n"
            "- 数据地址: mdi download event -u demo -s ./\n"
            f"- 证据: {signed_url}<!--private-token-->"
        ),
        comments_timeline=[
            {
                "text": (
                    "copied: mdi download event -u demo -s ./ "
                    f"attachment={signed_url}"
                )
            }
        ],
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
    assert "pdcl_download_cmd" not in payload["data"]
    assert payload["data"]["data_access"]["references"] == [
        {
            "event_uuid": "demo",
            "kind": "event",
            "reader_class": "RemoteEventReader",
        }
    ]
    assert payload["data"]["data_access"]["reader_contract"] == {
        "completeness": "full_requested_scope",
        "distribution": "pdcl_pyclip",
        "fallback": "forbidden",
        "mdi_download_allowed": False,
        "required_version": "0.1.6+rca.2",
    }
    assert payload["evidence"]["source_quality"] == "partial"
    assert "mdi download" not in json.dumps(payload["evidence"], ensure_ascii=False)
    assert "[remote data reference redacted]" in payload["evidence"]["description_markdown"]
    assert "[attachment]" in json.dumps(payload["evidence"], ensure_ascii=False)
    assert "temporary-token" not in json.dumps(payload, ensure_ascii=False)
    assert "private-token" not in json.dumps(payload, ensure_ascii=False)
    assert "signature=secret" not in json.dumps(payload, ensure_ascii=False)
    assert payload["evidence"]["issue_focus_plan"]["schema_version"] == "g1q3_issue_focus_plan_v1"
    assert payload["evidence"]["issue_focus_plan"]["source"]["comment_count"] == 1
    assert payload["execution_policy"] == {
        "allow_download": False,
        "allow_feishu_writeback": False,
        "artifact_root": "/mnt/tmp/g1q3_rca_issue_intake_7008267126/",
        "data_access_mode": "remote_read",
        "derived_artifacts_allowed": True,
        "group_response_cap": "L1",
        "input_materialization": "forbidden",
        "mode": "remote_read",
        "translate_baseline": "production",
        "translate_contract_path": "",
    }
    assert payload["source_refs"]["source_group_id"] == "oc_group"
    assert payload["source_refs"]["source_message_id"] == "om_msg"


def test_build_execution_request_carries_translate_baseline_candidate():
    ctx = RcaIssueContext(
        work_item_id="7026690721",
        source_quality="partial",
        pdcl_download_cmd="mdi download clip -u clip-7026690721 -s ./",
    )
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
    assert missing_blocker["kind"] == "issue_field_missing_remote_data_reference"
    assert missing_blocker["sub_kind"] == "empty"
    assert invalid_ctx.is_pdcl_format is False
    assert invalid_blocker["kind"] == "issue_field_invalid_remote_data_reference"
    assert invalid_blocker["sub_kind"] == "remote_data_reference_invalid"
    assert valid_ctx.is_pdcl_format is True
    assert valid_blocker is None
    assert is_valid_pdcl_download_cmd("mdi download clip -u abc -s ./") is True


def test_fallback_raw_text_cannot_be_used_as_a_remote_reference():
    context, blocker = validate_issue_context_fields(
        RcaIssueContext(
            work_item_id="7015689036",
            source_quality="fallback_raw_text",
            pdcl_download_cmd="mdi download event -u default-fallback -s ./",
        )
    )

    assert context.is_pdcl_format is False
    assert blocker["kind"] == "issue_field_untrusted_remote_data_reference"
    assert blocker["sub_kind"] == "fallback_raw_text"


def test_fallback_raw_text_build_request_is_blocked_even_when_text_parses():
    context = RcaIssueContext(
        work_item_id="7015689036",
        source_quality="fallback_raw_text",
        pdcl_download_cmd="mdi download event -u default-fallback -s ./",
        blockers=[{"kind": "issue_field_untrusted_remote_data_reference"}],
    )

    request = build_execution_request(
        request_kind="issue_intake",
        task_id="g1q3_rca_issue_intake_fallback",
        issue_context=context,
    )

    assert request.data["data_access"]["status"] == "blocked"
    assert request.data["data_access"]["blocker"]["kind"] == (
        "issue_field_untrusted_remote_data_reference"
    )


def test_business_profile_is_parsed_bound_and_fail_closed_before_data_resolution():
    profile = {
        "status": "matched",
        "profile_id": "mdrive4",
        "execution_readiness": "input_adapter_pending",
        "resource_class": "rca_prod",
        "artifact_kind": "mdrive4_ct_evaluation",
        "artifact_namespace": "rca/mdrive4",
        "routing_field_key": "field_052f23",
    }
    ctx = issue_context_from_compact_text(
        project_key="t03o4q",
        work_item_id="7044346306",
        compact_text=(
            "- business_profile_contract: "
            + json.dumps(profile, ensure_ascii=False, sort_keys=True)
            + "\n- title: recorder packet issue"
        ),
        source_quality="partial",
    )

    _, blocker = validate_issue_context_fields(ctx)

    assert ctx.business_profile == profile
    assert blocker["kind"] == "business_profile_adapter_not_ready"
    assert blocker["retryable"] is False
    assert "不会回退" in blocker["message"]


def test_validate_issue_context_fields_rejects_invalid_frame_reference():
    context, blocker = validate_issue_context_fields(
        RcaIssueContext(
            work_item_id="7049071505",
            source_quality="partial",
            pdcl_download_cmd="mdi download event -u demo -s ./",
            frame_reference_error="frame_reference_format_invalid",
        )
    )

    assert context.is_pdcl_format is True
    assert blocker["kind"] == "issue_field_invalid_frame_reference"
    assert blocker["sub_kind"] == "frame_reference_format_invalid"


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
