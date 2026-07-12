import json

import pytest

from gateway import pnc_issue_context


def test_compact_issue_context_handles_structured_result_and_comments():
    context = pnc_issue_context.compact_g1q3_issue_context(
        work_item_brief={
            "work_item_attribute": {
                "work_item_id": "7008267126",
                "work_item_name": "G1Q3_6351 ACC 旁车道其他车辆连续切入自车道ACC制动感强",
                "work_item_status": {"name": "待处理（OPEN）"},
            },
            "work_item_fields": [
                {"key": "field_052f23", "name": "所属项目", "value": {"label": "G1Q3"}},
                {"name": "当前负责人", "value": [{"name": "张三"}, {"name": "李四"}]},
                {"name": "问题发生frameid", "value": "318153"},
                {"name": "问题数据地址_PDCL", "value": "mdi download event -u demo -s ./"},
                {"name": "问题根本原因分析", "value": "目标误识别为CBLA法规目标"},
            ],
        },
        comments=[{"created_at": "2026-06-05", "content": "已回放\n\n\n优化后通过 ![图](http://x)<!--token-->"}],
    )

    assert "title: G1Q3_6351 ACC" in context
    assert "work_item_id: 7008267126" in context
    assert "所属项目: G1Q3" in context
    assert "当前负责人: 张三, 李四" in context
    assert "frame_id: 318153" in context
    assert "数据地址: mdi download event" in context
    assert "根因分析字段: 目标误识别为CBLA法规目标" in context
    assert "已回放\n\n优化后通过 [image]" in context


def test_compact_issue_context_treats_work_item_id_only_payload_as_empty():
    # Regression (Feishu issue 7025381565, 2026-06-23): a read that returns
    # nothing usable except the work_item_id (which is derived from the request
    # URL, NOT the API) must not look like a successful field read.  Otherwise
    # the intake reports source_quality=partial and falsely blames a missing
    # 问题数据地址_PDCL when the real problem is an empty/failed read.
    context = pnc_issue_context.compact_g1q3_issue_context(
        work_item_brief={
            "work_item_attribute": {
                "work_item_id": "7025381565",
                "work_item_name": "",
                "work_item_status": {"name": ""},
            },
            "work_item_fields": [],
        },
        comments=[],
    )
    assert context == ""


def test_compact_issue_context_keeps_real_partial_read_when_only_title_present():
    # Guard the other side: a genuine (if minimal) API read that surfaced the
    # title is a real field read and must stay non-empty -> fields_extracted.
    context = pnc_issue_context.compact_g1q3_issue_context(
        work_item_brief={
            "work_item_attribute": {
                "work_item_id": "7025381565",
                "work_item_name": "LCC-弯道LCC异常退出",
                "work_item_status": {"name": ""},
            },
            "work_item_fields": [],
        },
        comments=[],
    )
    assert "title: LCC-弯道LCC异常退出" in context


def test_meegle_workitem_id_only_payload_is_read_empty_not_fields_extracted():
    # Authenticated read whose payload carries only the work_item_id must be
    # classified read_empty (a non-PDCL, retryable preread blocker), NOT
    # fields_extracted -> spurious issue_field_missing_pdcl_download_cmd.
    def empty_meegle(args):
        if args[:2] == ["auth", "status"]:
            return 0, json.dumps({"authenticated": True}), ""
        if args[:2] == ["workitem", "get"]:
            return 0, json.dumps({"data": {"work_item_id": "7025381565"}}), ""
        if args[:2] == ["comment", "list"]:
            return 0, json.dumps({"data": []}), ""
        raise AssertionError(f"unexpected meegle call: {args}")

    result = pnc_issue_context.fetch_g1q3_issue_context_result_via_meegle(
        project_key="t03o4q",
        work_item_id="7025381565",
        runner=empty_meegle,
    )

    assert result.context_text == ""
    assert result.status == "read_empty"
    assert result.source_quality == "unavailable"
    assert result.blocker["kind"] == "host_meegle_preread_empty"
    assert "问题数据地址_PDCL" not in result.blocker["message"]


def test_mcp_result_payload_parses_json_string_result():
    raw = {"result": json.dumps({"comments": [{"content": "ok"}]}, ensure_ascii=False)}

    assert pnc_issue_context.mcp_result_payload(raw) == {"comments": [{"content": "ok"}]}


def test_fetch_issue_context_via_mcp_calls_registered_tools_and_handles_string_result():
    calls = []

    def fake_call_tool(name, args):
        calls.append((name, args))
        if name == "mcp_feishu_project_get_workitem_brief":
            return {
                "result": json.dumps(
                    {
                        "work_item_attribute": {
                            "work_item_id": "7008267126",
                            "work_item_name": "G1Q3_6351 ACC case",
                            "work_item_status": {"name": "OPEN"},
                        },
                        "work_item_fields": [
                            {"name": "问题发生frameid", "value": "318153"},
                            {"name": "问题根本原因分析", "value": "目标误识别"},
                        ],
                    },
                    ensure_ascii=False,
                )
            }
        return {"result": json.dumps({"comments": [{"created_at": "2026-06-05", "content": "已回放"}]}, ensure_ascii=False)}

    context = pnc_issue_context.fetch_g1q3_issue_context_result_via_mcp(
        project_key="t03o4q",
        work_item_id="7008267126",
        tool_caller=fake_call_tool,
        now_ms=123456,
    ).context_text

    assert calls[0][0] == "mcp_feishu_project_get_workitem_brief"
    assert calls[0][1]["project_key"] == "t03o4q"
    assert calls[0][1]["work_item_id"] == "7008267126"
    assert calls[1][0] == "mcp_feishu_project_list_workitem_comments"
    assert calls[1][1]["end_time"] == 123456
    assert "title: G1Q3_6351 ACC case" in context
    assert "frame_id: 318153" in context
    assert "根因分析字段: 目标误识别" in context
    assert "已回放" in context


def test_fetch_issue_context_via_mcp_tool_error_returns_empty_context():
    def broken_call_tool(name, args):
        raise RuntimeError("permission denied")

    assert pnc_issue_context.fetch_g1q3_issue_context_result_via_mcp(
        project_key="t03o4q",
        work_item_id="7008267126",
        tool_caller=broken_call_tool,
    ).context_text == ""


def test_fetch_issue_context_mcp_fallback_distinguishes_preread_failure_from_field_missing():
    def broken_call_tool(name, args):
        raise RuntimeError("permission denied")

    def unauth_meegle(args):
        if args[:2] == ["auth", "status"]:
            return 1, json.dumps({"authenticated": False, "reason": "no local token"}), ""
        return 1, "", "should not run"

    result = pnc_issue_context.fetch_g1q3_issue_context_result(
        project_key="t03o4q",
        work_item_id="7008267126",
        tool_caller=broken_call_tool,
        use_mcp_fallback=True,
        meegle_runner=unauth_meegle,
    )

    assert result.context_text == ""
    assert result.status == "read_failed"
    # Unauthenticated Meegle keeps its specific kind so group notices and
    # operator alerts can still tell "请重新授权" from a generic read failure.
    assert result.blocker["kind"] == "host_meegle_preread_unauthenticated"
    assert "不代表 问题数据地址_PDCL 缺失" in result.blocker["message"]
    assert "mcp_feishu_project_get_workitem_brief" in result.blocker["failed_tools"]
    assert "meegle auth status" in result.blocker["failed_tools"]


@pytest.mark.parametrize(
    ("text", "work_item_id", "expected"),
    [
        ("https://project.feishu.cn/t03o4q/issue/detail/7008267126", "7008267126", "t03o4q"),
        ("https://project.feishu.cn/t03o4q/issue/detail/7008267126", "other", ""),
        ("no url", "", ""),
    ],
)
def test_extract_feishu_issue_project_key(text, work_item_id, expected):
    assert pnc_issue_context.extract_feishu_issue_project_key(text, work_item_id=work_item_id) == expected


def test_fixed_g1q3_group_fallback_project_key_when_url_missing():
    assert pnc_issue_context.resolve_feishu_issue_project_key(
        "@胡子豪的小助手 分析这个问题",
        work_item_id="7008267126",
        source_group_id=pnc_issue_context.G1Q3_RCA_GROUP_ID,
    ) == "t03o4q"


def test_url_project_key_wins_over_fixed_group_fallback():
    assert pnc_issue_context.resolve_feishu_issue_project_key(
        "https://project.feishu.cn/other_space/issue/detail/7008267126",
        work_item_id="7008267126",
        source_group_id=pnc_issue_context.G1Q3_RCA_GROUP_ID,
    ) == "other_space"

def test_no_context_fallback_empty_inputs():
    assert pnc_issue_context.compact_g1q3_issue_context(work_item_brief={}, comments=[]) == ""
    assert pnc_issue_context.fetch_g1q3_issue_context(project_key="t03o4q", work_item_id="") == ""


def test_fetch_issue_context_uses_meegle_primary_without_mcp():
    calls = []

    def should_not_call_mcp(_name, _args):
        raise AssertionError("MCP must not be called by default for Feishu Project issue preread")

    def fake_meegle(args):
        calls.append(args)
        joined = " ".join(args)
        if joined.startswith("auth status"):
            return 0, json.dumps({"authenticated": True, "host": "project.feishu.cn"}), ""
        if joined.startswith("workitem get"):
            return 0, json.dumps({
                "id": "7015689036",
                "name": "G1Q3_0938-AWB case",
                "status": {"name": "待处理（OPEN）"},
                "fields": {
                    "问题发生frameid": "336215",
                    "问题数据地址_PDCL": "mdi download event -u demo -s ./",
                    "问题根本原因分析": "mono3d测距波动大",
                },
            }, ensure_ascii=False), ""
        if joined.startswith("comment list"):
            return 0, json.dumps({"comments": [{"created_at": "2026-06-11", "content": "原因分析：mono3d测距波动大"}]}, ensure_ascii=False), ""
        return 1, "", "unexpected"

    result = pnc_issue_context.fetch_g1q3_issue_context_result(
        project_key="t03o4q",
        work_item_id="7015689036",
        tool_caller=should_not_call_mcp,
        meegle_runner=fake_meegle,
    )

    assert result.status == "fields_extracted"
    assert "title: G1Q3_0938-AWB case" in result.context_text
    assert "frame_id: 336215" in result.context_text
    assert "数据地址: mdi download event -u demo -s ./" in result.context_text
    assert any(call[:2] == ["workitem", "get"] for call in calls)
    assert any(call[:2] == ["comment", "list"] for call in calls)


def test_meegle_unauthenticated_auto_degrades_to_mcp_and_succeeds(monkeypatch):
    monkeypatch.delenv("HERMES_G1Q3_MCP_FALLBACK", raising=False)
    monkeypatch.delenv("HERMES_G1Q3_MCP_AUTODEGRADE", raising=False)
    mcp_calls = []

    def working_mcp(name, args):
        mcp_calls.append(name)
        if name == "mcp_feishu_project_get_workitem_brief":
            return {"result": json.dumps({
                "work_item_attribute": {"work_item_id": "7015689036", "work_item_name": "G1Q3 case", "work_item_status": {"name": "OPEN"}},
                "work_item_fields": [{"key": "field_93aa63", "name": "问题数据地址_PDCL", "value": "mdi download event -u demo -s ./"}],
            }, ensure_ascii=False)}
        return {"result": json.dumps({"comments": []})}

    def unauth_meegle(args):
        if args[:2] == ["auth", "status"]:
            return 0, json.dumps({"authenticated": False, "reason": "token expired"}), ""
        return 1, "", "should not run"

    result = pnc_issue_context.fetch_g1q3_issue_context_result(
        project_key="t03o4q",
        work_item_id="7015689036",
        tool_caller=working_mcp,
        meegle_runner=unauth_meegle,
    )

    assert result.status == "fields_extracted"
    assert result.source == "mcp_auto_degraded"
    assert "mdi download event" in result.context_text
    assert "mcp_feishu_project_get_workitem_brief" in mcp_calls


def test_meegle_unauthenticated_with_mcp_also_down_keeps_unauthenticated_blocker(monkeypatch):
    monkeypatch.delenv("HERMES_G1Q3_MCP_FALLBACK", raising=False)
    monkeypatch.delenv("HERMES_G1Q3_MCP_AUTODEGRADE", raising=False)

    def broken_mcp(name, args):
        return {"error": f"Unknown tool: {name}"}

    def unauth_meegle(args):
        if args[:2] == ["auth", "status"]:
            return 0, json.dumps({"authenticated": False, "reason": "token expired"}), ""
        return 1, "", "should not run"

    result = pnc_issue_context.fetch_g1q3_issue_context_result(
        project_key="t03o4q",
        work_item_id="7015689036",
        tool_caller=broken_mcp,
        meegle_runner=unauth_meegle,
    )

    assert result.status == "read_failed"
    assert result.blocker["kind"] == "host_meegle_preread_unauthenticated"
    assert "meegle auth login" in result.blocker["message"]
    assert "不代表 问题数据地址_PDCL 缺失" in result.blocker["message"]


def test_meegle_auto_degrade_can_be_disabled_by_env(monkeypatch):
    monkeypatch.delenv("HERMES_G1Q3_MCP_FALLBACK", raising=False)
    monkeypatch.setenv("HERMES_G1Q3_MCP_AUTODEGRADE", "0")
    mcp_calls = []

    def recording_mcp(name, args):
        mcp_calls.append(name)
        return {}

    def unauth_meegle(args):
        if args[:2] == ["auth", "status"]:
            return 0, json.dumps({"authenticated": False, "reason": "token expired"}), ""
        return 1, "", "should not run"

    result = pnc_issue_context.fetch_g1q3_issue_context_result(
        project_key="t03o4q",
        work_item_id="7015689036",
        tool_caller=recording_mcp,
        meegle_runner=unauth_meegle,
    )

    assert result.status == "read_failed"
    assert result.blocker["kind"] == "host_meegle_preread_unauthenticated"
    assert mcp_calls == []


def test_meegle_read_empty_does_not_auto_degrade(monkeypatch):
    monkeypatch.delenv("HERMES_G1Q3_MCP_FALLBACK", raising=False)
    monkeypatch.delenv("HERMES_G1Q3_MCP_AUTODEGRADE", raising=False)
    mcp_calls = []

    def recording_mcp(name, args):
        mcp_calls.append(name)
        return {}

    def empty_meegle(args):
        if args[:2] == ["auth", "status"]:
            return 0, json.dumps({"authenticated": True}), ""
        return 0, "[]", ""

    result = pnc_issue_context.fetch_g1q3_issue_context_result(
        project_key="t03o4q",
        work_item_id="7015689036",
        tool_caller=recording_mcp,
        meegle_runner=empty_meegle,
    )

    assert result.status == "read_empty"
    assert mcp_calls == []


def test_check_meegle_auth_status_authenticated_with_expiry():
    def fake_runner(args):
        assert args == ["auth", "status", "--format", "json"]
        return 0, json.dumps({"authenticated": True, "expires_in_minutes": 96, "host": "project.feishu.cn"}), ""

    status = pnc_issue_context.check_meegle_auth_status(runner=fake_runner)

    assert status["ok"] is True
    assert status["authenticated"] is True
    assert status["expires_in_minutes"] == 96
    assert status["host"] == "project.feishu.cn"
    assert status["error"] == ""


def test_check_meegle_auth_status_unauthenticated_is_explicit():
    def fake_runner(args):
        return 0, json.dumps({"authenticated": False, "reason": "token expired"}), ""

    status = pnc_issue_context.check_meegle_auth_status(runner=fake_runner)

    assert status["ok"] is False
    assert status["authenticated"] is False
    assert "token expired" in status["error"]


def test_check_meegle_auth_status_cli_missing_is_inconclusive_not_unauthenticated():
    def fake_runner(args):
        return 127, "", "meegle CLI not found"

    status = pnc_issue_context.check_meegle_auth_status(runner=fake_runner)

    assert status["ok"] is False
    assert status["authenticated"] is None
    assert "not found" in status["error"]


def test_check_meegle_auth_status_never_raises_on_runner_crash():
    def fake_runner(args):
        raise RuntimeError("boom")

    status = pnc_issue_context.check_meegle_auth_status(runner=fake_runner)

    assert status["ok"] is False
    assert status["authenticated"] is None
    assert "RuntimeError" in status["error"]


def test_meegle_unauthenticated_blocker_message_names_relogin_not_pdcl():
    def unauth_meegle(args):
        if args[:2] == ["auth", "status"]:
            return 0, json.dumps({"authenticated": False, "reason": "token expired"}), ""
        return 1, "", "should not run"

    result = pnc_issue_context.fetch_g1q3_issue_context_result_via_meegle(
        project_key="t03o4q",
        work_item_id="7015689036",
        runner=unauth_meegle,
    )

    assert result.blocker["kind"] == "host_meegle_preread_unauthenticated"
    assert "meegle auth login" in result.blocker["message"]
    assert "不代表 问题数据地址_PDCL 缺失" in result.blocker["message"]


def test_meegle_success_writes_portable_capture_to_configured_root(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_G1Q3_ISSUE_CAPTURE_ROOT", str(tmp_path / "captures"))
    monkeypatch.delenv("HERMES_G1Q3_DISABLE_ISSUE_CAPTURE", raising=False)

    def fake_meegle(args):
        joined = " ".join(args)
        if joined.startswith("auth status"):
            return 0, json.dumps({"authenticated": True}), ""
        if joined.startswith("workitem get"):
            return 0, json.dumps({
                "id": "7015689036",
                "name": "G1Q3 capture case",
                "fields": {
                    "问题发生frameid": "336215",
                    "问题数据地址_PDCL": "mdi download event -u demo -s ./",
                    "token": "must_not_leak",
                    "open_id": "ou_x",
                },
            }, ensure_ascii=False), ""
        if joined.startswith("comment list"):
            return 0, json.dumps({"comments": [{"created_at": "2026-06-11", "content": "ok secret should stay out"}]}, ensure_ascii=False), ""
        return 1, "", "unexpected"

    result = pnc_issue_context.fetch_g1q3_issue_context_result(
        project_key="t03o4q",
        work_item_id="7015689036",
        meegle_runner=fake_meegle,
        tool_caller=lambda *_: (_ for _ in ()).throw(AssertionError("no mcp")),
    )

    capture_path = tmp_path / "captures" / "7015689036" / "issue_capture.json"
    capture = json.loads(capture_path.read_text(encoding="utf-8"))
    assert result.status == "fields_extracted"
    assert capture["schema_version"] == "g1q3_rca_issue_capture_v1"
    assert capture["read_source"] == "meegle"
    assert capture["issue_context_sanitized"]["work_item_id"] == "7015689036"
    assert capture["issue_context_sanitized"]["pdcl_download_cmd"] == "mdi download event -u demo -s ./"
    text = json.dumps(capture, ensure_ascii=False).lower()
    assert "token" not in text
    assert "open_id" not in text
    assert "user_key" not in text


def test_capture_failure_never_breaks_issue_preread(monkeypatch):
    monkeypatch.setenv("HERMES_G1Q3_ISSUE_CAPTURE_ROOT", "/dev/null/not-a-dir")

    def fake_meegle(args):
        joined = " ".join(args)
        if joined.startswith("auth status"):
            return 0, json.dumps({"authenticated": True}), ""
        if joined.startswith("workitem get"):
            return 0, json.dumps({"id": "7015689037", "name": "G1Q3 capture failure", "fields": {"问题数据地址_PDCL": "mdi download event -u demo -s ./"}}, ensure_ascii=False), ""
        if joined.startswith("comment list"):
            return 0, json.dumps({"comments": []}), ""
        return 1, "", "unexpected"

    result = pnc_issue_context.fetch_g1q3_issue_context_result(
        project_key="t03o4q",
        work_item_id="7015689037",
        meegle_runner=fake_meegle,
    )

    assert result.status == "fields_extracted"
    assert "mdi download event" in result.context_text
