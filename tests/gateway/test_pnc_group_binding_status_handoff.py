from types import SimpleNamespace
import json

import pytest

from gateway import pnc_issue_context
from gateway import run as gateway_run
from gateway.config import Platform
from gateway.platforms.base import MessageEvent, MessageType
from gateway.session import SessionSource
from gateway.tasks.store import TaskStore
from hermes_constants import reset_hermes_home_override, set_hermes_home_override


G1Q3_GROUP_ID = "oc_6cfc782212009ff4cd815349909dd423"


def make_runner(receipt_dir=None):
    runner = object.__new__(gateway_run.GatewayRunner)
    runner.adapters = {}
    runner.config = SimpleNamespace()
    runner.session_store = SimpleNamespace()
    runner.pairing_store = SimpleNamespace()
    if receipt_dir is not None:
        runner._pnc_group_binding_receipt_dir = receipt_dir
    return runner


def make_feishu_event(text: str, *, chat_id: str = G1Q3_GROUP_ID) -> MessageEvent:
    return MessageEvent(
        text=text,
        message_type=MessageType.TEXT,
        source=SessionSource(
            platform=Platform.FEISHU,
            user_id="ou_test_user",
            user_name="测试用户",
            chat_id=chat_id,
            chat_name="G1Q3 归因群",
            chat_type="group",
            thread_id="topic:om_request_topic",
            message_id="om_test_message",
        ),
        message_id="om_test_message",
    )


@pytest.mark.asyncio
async def test_g1q3_status_accepted_submits_shared_state_handoff(monkeypatch, tmp_path):
    runner = make_runner(receipt_dir=tmp_path)
    event = make_feishu_event("帮我看一下 case G1Q3-042 现在归因做到哪一步了")
    calls = []

    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    monkeypatch.setattr(gateway_run.GatewayRunner, "_is_user_authorized", lambda self, source: True)

    def fake_submit(**kwargs):
        from gateway.session_context import get_session_env
        assert get_session_env("HERMES_SESSION_PLATFORM") == "feishu"
        assert get_session_env("HERMES_SESSION_CHAT_ID") == G1Q3_GROUP_ID
        assert get_session_env("HERMES_SESSION_THREAD_ID") == "topic:om_request_topic"
        assert get_session_env("HERMES_SESSION_MESSAGE_ID") == "om_test_message"
        calls.append(kwargs)
        return {
            "success": True,
            "task": {"task_id": "pnc_status_001"},
            "routing": {"host_state": "host-created"},
            "notify_process": {"started": True, "session_id": "proc_001"},
        }

    monkeypatch.setattr(gateway_run, "_submit_g1q3_rca_status_handoff", fake_submit)

    response = await gateway_run.GatewayRunner._handle_message(runner, event)

    assert response == ""
    assert len(calls) == 1
    call = calls[0]
    assert call["template_id"] == "rca_case_status_check"
    assert call["case_id"] == "042"
    assert call["requester"] == "ou_test_user"
    assert call["source_group_id"] == G1Q3_GROUP_ID
    assert call["message_id"] == "om_test_message"
    assert call["work_item_id"] == ""
    from gateway.session_context import get_session_env
    assert get_session_env("HERMES_SESSION_PLATFORM") == ""
    assert get_session_env("HERMES_SESSION_CHAT_ID") == ""
    assert get_session_env("HERMES_SESSION_THREAD_ID") == ""
    assert get_session_env("HERMES_SESSION_MESSAGE_ID") == ""

    records = [json.loads(line) for path in tmp_path.glob("*.jsonl") for line in path.read_text().splitlines()]
    assert records[-2]["decision"] == "accepted"
    assert records[-2]["decision_snapshot"]["handoff_contract"]["case_id"] == "042"
    assert records[-1]["event_type"] == "handoff_submission"
    assert records[-1]["handoff_success"] is True
    assert records[-1]["task_id"] == "pnc_status_001"


@pytest.mark.asyncio
async def test_g1q3_issue_card_uses_metadata_link_for_handoff(monkeypatch, tmp_path):
    runner = make_runner(receipt_dir=tmp_path)
    event = make_feishu_event(
        "@胡子豪的小助手 分析这个问题 [【08】问题管理] LCC-临停区方向盘调节明显",
        chat_id="oc_16614f4ba25b8c88b69c0b8e9ebc2fb5",
    )
    event.metadata["feishu"] = {
        "link_urls": ["https://project.feishu.cn/t03o4q/issue/detail/7003183096?openScene=4"],
    }
    calls = []

    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    monkeypatch.setattr(gateway_run.GatewayRunner, "_is_user_authorized", lambda self, source: True)
    monkeypatch.setattr(
        gateway_run,
        "_submit_g1q3_rca_status_handoff",
        lambda **kwargs: calls.append(kwargs) or {
            "success": True,
            "task": {"task_id": "20260623-112927-g1q3-rca-issue-intake-7003183096"},
            "notify_process": {"started": True},
        },
    )

    response = await gateway_run.GatewayRunner._handle_message(runner, event)

    assert response == ""
    assert calls[0]["template_id"] == "rca_issue_intake"
    assert calls[0]["work_item_id"] == "7003183096"
    assert "issue/detail/7003183096" in calls[0]["request_text"]


@pytest.mark.asyncio
async def test_g1q3_governance_dispatch_receipt_has_task_id_and_probe(monkeypatch, tmp_path):
    runner = make_runner(receipt_dir=tmp_path)
    event = make_feishu_event("分析这个问题 https://project.feishu.cn/t03o4q/issue/detail/7003183096")

    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    monkeypatch.setattr(gateway_run.GatewayRunner, "_is_user_authorized", lambda self, source: True)
    monkeypatch.setattr(
        gateway_run,
        "_submit_g1q3_rca_status_handoff",
        lambda **kwargs: {
            "success": True,
            "dispatched": "governance_datapipe",
            "task": {"task_id": "20260623-112927-g1q3-rca-issue-intake-7003183096"},
            "notify_process": {"started": True, "session_id": "proc_probe"},
        },
    )

    response = await gateway_run.GatewayRunner._handle_message(runner, event)

    assert response == ""
    records = [json.loads(line) for path in tmp_path.glob("*.jsonl") for line in path.read_text().splitlines()]
    assert records[-1]["event_type"] == "handoff_submission"
    assert records[-1]["task_id"] == "20260623-112927-g1q3-rca-issue-intake-7003183096"
    assert records[-1]["completion_probe_started"] is True


@pytest.mark.asyncio
async def test_g1q3_status_handoff_persists_task_observability(monkeypatch, tmp_path):
    runner = make_runner(receipt_dir=tmp_path)
    event = make_feishu_event("帮我看一下 case G1Q3-042 现在归因做到哪一步了")

    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    monkeypatch.setattr(gateway_run.GatewayRunner, "_is_user_authorized", lambda self, source: True)
    monkeypatch.setattr(
        gateway_run,
        "_submit_g1q3_rca_status_handoff",
        lambda **kwargs: {"success": True, "task": {"task_id": "pnc_status_001"}},
    )

    response = await gateway_run.GatewayRunner._handle_message(runner, event)

    assert response == ""
    task = TaskStore(tmp_path / "analytics" / "tasks.db").get("pnc_status_001")
    assert task is not None
    assert task.platform == "feishu"
    assert task.chat_id == G1Q3_GROUP_ID
    assert task.chat_type == "group"
    assert task.thread_id == "topic:om_request_topic"
    assert task.message_id == "om_test_message"
    assert task.agent_route == "g1q3-rca"
    assert task.vm_task_id == "pnc_status_001"
    assert "G1Q3-042" in (task.request_summary or "")


def test_gateway_thread_metadata_for_feishu_carries_reply_anchor():
    runner = make_runner()
    source = make_feishu_event("帮我看一下 case G1Q3-042 现在归因做到哪一步了").source

    meta = gateway_run.GatewayRunner._thread_metadata_for_source(runner, source, "om_test_message")

    assert meta == {
        "thread_id": "topic:om_request_topic",
        "reply_to_message_id": "om_test_message",
    }


@pytest.mark.asyncio
async def test_g1q3_issue_url_intake_submits_work_item_handoff(monkeypatch, tmp_path):
    runner = make_runner(receipt_dir=tmp_path)
    event = make_feishu_event("@胡子豪的小助手 分析这个问题 https://project.feishu.cn/t03o4q/issue/detail/7008267126")
    calls = []

    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    monkeypatch.setattr(gateway_run.GatewayRunner, "_is_user_authorized", lambda self, source: True)

    def fake_submit(**kwargs):
        from gateway.session_context import get_session_env
        assert get_session_env("HERMES_SESSION_PLATFORM") == "feishu"
        assert get_session_env("HERMES_SESSION_CHAT_ID") == G1Q3_GROUP_ID
        assert get_session_env("HERMES_SESSION_THREAD_ID") == "topic:om_request_topic"
        assert get_session_env("HERMES_SESSION_MESSAGE_ID") == "om_test_message"
        calls.append(kwargs)
        return {"success": True, "task": {"task_id": "pnc_intake_001"}}

    monkeypatch.setattr(gateway_run, "_submit_g1q3_rca_status_handoff", fake_submit)

    response = await gateway_run.GatewayRunner._handle_message(runner, event)

    assert response == ""
    assert len(calls) == 1
    call = calls[0]
    assert call["template_id"] == "rca_issue_intake"
    assert call["case_id"] == ""
    assert call["work_item_id"] == "7008267126"
    assert call["message_id"] == "om_test_message"
    assert "7008267126" in call["request_text"]
    from gateway.session_context import get_session_env
    assert get_session_env("HERMES_SESSION_PLATFORM") == ""
    assert get_session_env("HERMES_SESSION_CHAT_ID") == ""
    assert get_session_env("HERMES_SESSION_THREAD_ID") == ""
    assert get_session_env("HERMES_SESSION_MESSAGE_ID") == ""
    records = [json.loads(line) for path in tmp_path.glob("*.jsonl") for line in path.read_text().splitlines()]
    assert records[-1]["event_type"] == "handoff_submission"
    assert records[-1]["handoff_success"] is True
    assert records[-1]["task_id"] == "pnc_intake_001"
    assert records[-1]["template_id"] == "rca_issue_intake"
    assert records[-1]["contract_version"] == "g1q3_rca_group_handoff_v2"
    assert records[-1]["case_id"] == ""
    assert records[-1]["work_item_id"] == "7008267126"


@pytest.mark.asyncio
async def test_status_handoff_submit_uses_scheduler_safe_metadata(monkeypatch):
    calls = []

    def fake_vm_task_submit(**kwargs):
        calls.append(kwargs)
        return {"success": True, "task": {"task_id": "pnc_status_001"}}

    monkeypatch.setattr("tools.vm_task_tool.vm_task_submit", fake_vm_task_submit)

    result = gateway_run._submit_g1q3_rca_status_handoff(
        template_id="rca_case_status_check",
        case_id="042",
        requester="ou_test_user",
        source_group_id=G1Q3_GROUP_ID,
        message_id="om_test_message",
    )

    assert result["success"] is True
    assert len(calls) == 1
    call = calls[0]
    assert call["repo_scope"] == "unknown"
    assert call["workspace_scope"] == "none"
    assert call["risk_class"] == "normal"
    assert call["executor_type"] == "governed_tool"


def test_vm_task_submit_carries_feishu_topic_reply_route(monkeypatch):
    calls = []

    class Proc:
        returncode = 0
        stdout = '{"task_id":"g1q3_rca_issue_intake_7008267126"}'
        stderr = ""

    def fake_run(cmd, text, capture_output, timeout):
        calls.append(cmd)
        return Proc()

    monkeypatch.setattr("tools.vm_task_tool.subprocess.run", fake_run)
    monkeypatch.setattr("tools.vm_task_tool._spawn_completion_probe_background", lambda task_id: {"started": True})
    monkeypatch.setattr("tools.vm_task_tool._check_vm_task_permission", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("tools.vm_task_tool._create_task_script", lambda: SimpleNamespace(exists=lambda: True, __str__=lambda self: "/tmp/create_task_v2.py"))

    from gateway.session_context import clear_session_vars, set_session_vars
    from tools.vm_task_tool import vm_task_submit

    tokens = set_session_vars(
        platform="feishu",
        chat_id=G1Q3_GROUP_ID,
        chat_name="G1Q3-RCA业务群",
        thread_id="topic:om_request_topic",
        user_id="ou_test_user",
        user_name="测试用户",
        session_key="feishu:test",
        message_id="om_test_message",
    )
    try:
        result = vm_task_submit(
            title="G1Q3 RCA issue intake: 7008267126",
            goal="分析 issue 7008267126",
            user_id="ou_test_user",
            lane="standard",
            resource_class="pnc_data",
            repo_scope="unknown",
            workspace_scope="none",
            risk_class="normal",
            artifact_root="/mnt/tmp/g1q3_rca_issue_intake_7008267126/",
            artifact_cifs_root="//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/g1q3_rca_issue_intake_7008267126/",
            executor_type="governed_tool",
            agent_backend="codex",
        )
    finally:
        clear_session_vars(tokens)

    assert result["success"] is True
    meta = json.loads(calls[0][calls[0].index("--meta") + 1])
    assert meta["platform"] == "feishu"
    assert meta["chat_id"] == G1Q3_GROUP_ID
    assert meta["chat_name"] == "G1Q3-RCA业务群"
    assert meta["thread_id"] == "topic:om_request_topic"
    assert meta["message_id"] == "om_test_message"
    assert meta["session_key"] == "feishu:test"


def test_issue_intake_handoff_goal_includes_work_item_id(monkeypatch):
    calls = []

    def fake_vm_task_submit(**kwargs):
        calls.append(kwargs)
        return {"success": True, "task": {"task_id": "pnc_intake_001"}}

    monkeypatch.setattr("tools.vm_task_tool.vm_task_submit", fake_vm_task_submit)
    monkeypatch.setattr(gateway_run, "_fetch_g1q3_issue_context_result", lambda **kwargs: pnc_issue_context.G1Q3IssueReadResult(status="read_empty", blocker={"kind": "host_issue_preread_empty", "message": "主控侧飞书 issue 读取返回空结果，不能据此判定字段缺失", "retryable": True}))

    result = gateway_run._submit_g1q3_rca_status_handoff(
        template_id="rca_issue_intake",
        case_id="",
        work_item_id="7008267126",
        requester="ou_test_user",
        source_group_id=G1Q3_GROUP_ID,
        message_id="om_test_message",
        request_text="@胡子豪的小助手 分析这个问题 https://project.feishu.cn/t03o4q/issue/detail/7008267126",
    )

    assert result["success"] is True
    assert len(calls) == 1
    call = calls[0]
    assert "work_item_id: 7008267126" in call["goal"]
    assert "待从飞书问题字段解析" in call["goal"]
    assert "issue_preread_blocked" in call["goal"]
    assert "schema_version: g1q3_rca_execution_request_v1" in call["goal"]
    assert "execution_request_path: /mnt/tmp/g1q3_rca_issue_intake_7008267126_8be9b8/rca_execution_request.json" in call["goal"]
    assert "VM command suggestion: python3 /home/mini/data3/yj-evaluation-server/api/g1q3_rca/scripts/run_rca_execution_request.py" in call["goal"]
    assert "RcaExecutionRequest JSON" in call["goal"]
    assert result["execution_request"]["schema_version"] == "g1q3_rca_execution_request_v1"
    assert result["execution_request"]["work_item"]["work_item_id"] == "7008267126"
    assert result["execution_request"]["work_item"]["project_key"] == "t03o4q"
    assert result["execution_request"]["evidence"]["source_quality"] == "unavailable"
    assert result["execution_request"]["evidence"]["blockers"][0]["kind"] == "host_issue_preread_empty"
    assert "原始请求摘录" in call["goal"]
    assert "issue/detail/7008267126" in call["goal"]
    assert call["artifact_root"] == "/mnt/tmp/g1q3_rca_issue_intake_7008267126_8be9b8/"
    assert call["title"] == "G1Q3 RCA issue intake: 7008267126"


def test_issue_intake_handoff_goal_injects_gateway_issue_context(monkeypatch):
    calls = []
    fetch_calls = []

    def fake_vm_task_submit(**kwargs):
        calls.append(kwargs)
        return {"success": True, "task": {"task_id": "pnc_intake_001"}}

    def fake_fetch(**kwargs):
        fetch_calls.append(kwargs)
        return "- title: G1Q3_6351 ACC 旁车道其他车辆连续切入自车道ACC制动感强\n- frame_id: 318153\n- 根因分析字段: 目标误识别为CBLA法规目标"

    monkeypatch.setattr("tools.vm_task_tool.vm_task_submit", fake_vm_task_submit)
    monkeypatch.setattr(gateway_run, "_fetch_g1q3_issue_context_result", lambda **kwargs: pnc_issue_context.G1Q3IssueReadResult(context_text=fake_fetch(**kwargs), status="fields_extracted"))

    result = gateway_run._submit_g1q3_rca_status_handoff(
        template_id="rca_issue_intake",
        case_id="",
        work_item_id="7008267126",
        requester="ou_test_user",
        source_group_id=G1Q3_GROUP_ID,
        message_id="om_test_message",
        request_text="@胡子豪的小助手 分析这个问题 https://project.feishu.cn/t03o4q/issue/detail/7008267126",
    )

    assert result["success"] is True
    assert fetch_calls == [{"project_key": "t03o4q", "work_item_id": "7008267126"}]
    goal = calls[0]["goal"]
    assert "Feishu issue context（主控侧预读取）" in goal
    assert "title: G1Q3_6351 ACC" in goal
    assert "frame_id: 318153" in goal
    assert "根因分析字段: 目标误识别为CBLA法规目标" in goal
    assert result["execution_request"]["work_item"]["project_key"] == "t03o4q"
    assert result["execution_request"]["work_item"]["title"].startswith("G1Q3_6351 ACC")
    assert result["execution_request"]["case"]["frame_id"] == "318153"
    assert result["execution_request"]["evidence"]["root_cause_text"] == "目标误识别为CBLA法规目标"
    assert result["execution_request"]["evidence"]["source_quality"] == "partial"
    assert "execution_request_path: /mnt/tmp/g1q3_rca_issue_intake_7008267126_8be9b8/rca_execution_request.json" in goal


def test_fetch_issue_context_calls_registered_mcp_tools(monkeypatch):
    calls = []

    def fake_call_tool(name, args):
        calls.append((name, args))
        if name == "mcp_feishu_project_get_workitem_brief":
            return {
                "result": {
                    "work_item_attribute": {
                        "work_item_id": "7008267126",
                        "work_item_name": "G1Q3_6351 ACC 旁车道其他车辆连续切入自车道ACC制动感强",
                        "work_item_status": {"name": "待处理（OPEN）"},
                    },
                    "work_item_fields": [
                        {"name": "问题发生frameid", "value": "318153"},
                        {"name": "问题数据地址_PDCL", "value": "mdi download event -u demo -s ./"},
                        {"name": "问题根本原因分析", "value": "目标误识别为CBLA法规目标"},
                    ],
                }
            }
        return {"result": {"comments": [{"created_at": "2026-06-05", "content": "已回放，优化后通过"}]}}

    monkeypatch.setattr(pnc_issue_context, "call_gateway_tool", fake_call_tool)

    context = pnc_issue_context.fetch_g1q3_issue_context_result_via_mcp(
        project_key="t03o4q",
        work_item_id="7008267126",
        tool_caller=fake_call_tool,
    ).context_text

    assert calls[0][0] == "mcp_feishu_project_get_workitem_brief"
    assert calls[0][1]["project_key"] == "t03o4q"
    assert calls[0][1]["work_item_id"] == "7008267126"
    assert calls[1][0] == "mcp_feishu_project_list_workitem_comments"
    assert "title: G1Q3_6351 ACC" in context
    assert "frame_id: 318153" in context
    assert "数据地址: mdi download event" in context
    assert "根因分析字段: 目标误识别为CBLA法规目标" in context
    assert "已回放，优化后通过" in context


def test_issue_intake_handoff_writes_rca_state_receipts(monkeypatch, tmp_path):
    calls = []

    def fake_vm_task_submit(**kwargs):
        calls.append(kwargs)
        return {"success": True, "task": {"task_id": "pnc_intake_001"}}

    monkeypatch.setattr("tools.vm_task_tool.vm_task_submit", fake_vm_task_submit)
    monkeypatch.setattr(gateway_run, "_fetch_g1q3_issue_context_result", lambda **kwargs: pnc_issue_context.G1Q3IssueReadResult(context_text="- title: G1Q3_6351 ACC\n- frame_id: 318153\n- 数据地址: mdi download event -u demo -s ./", status="fields_extracted"))

    result = gateway_run._submit_g1q3_rca_status_handoff(
        template_id="rca_issue_intake",
        case_id="",
        work_item_id="7008267126",
        requester="ou_test_user",
        source_group_id=G1Q3_GROUP_ID,
        message_id="om_test_message",
        request_text="@胡子豪的小助手 分析这个问题",
        receipt_dir=tmp_path,
    )

    assert result["success"] is True
    records = [
        json.loads(line)
        for path in tmp_path.glob("*rca-intake.jsonl")
        for line in path.read_text(encoding="utf-8").splitlines()
    ]
    assert [record["stage"] for record in records] == [
        "admitted",
        "issue_enrichment_started",
        "issue_fields_extracted",
        "vm_submitted",
    ]
    assert records[-1]["vm_task_id"] == "pnc_intake_001"
    assert records[-1]["issue_context"]["work_item_id"] == "7008267126"
    assert records[2]["execution_request_path"] == "/mnt/tmp/g1q3_rca_issue_intake_7008267126_8be9b8/rca_execution_request.json"


@pytest.mark.asyncio
async def test_g1q3_evidence_followup_submits_handoff_but_report_remains_dry_run(monkeypatch):
    runner = make_runner()
    submitted = []

    monkeypatch.setattr(gateway_run.GatewayRunner, "_is_user_authorized", lambda self, source: True)

    def fake_submit(**kwargs):
        submitted.append(kwargs)
        return {"success": True, "task": {"task_id": "pnc_evidence_001"}}

    monkeypatch.setattr(gateway_run, "_submit_g1q3_rca_status_handoff", fake_submit)

    evidence_response = await gateway_run.GatewayRunner._handle_message(
        runner,
        make_feishu_event("汇总一下 G1Q3-105 当前已有证据，还缺什么"),
    )
    report_response = await gateway_run.GatewayRunner._handle_message(
        runner,
        make_feishu_event("给 G1Q3-088 生成一版 RCA 摘要报告"),
    )

    assert evidence_response == ""
    assert submitted[0]["template_id"] == "rca_case_evidence_summary"
    assert submitted[0]["case_id"] == "105"
    assert "G1Q3 RCA dry-run" in report_response
    assert "template: rca_report_generate" in report_response
    assert len(submitted) == 1


@pytest.mark.asyncio
async def test_g1q3_evidence_followup_ack_uses_missing_evidence_copy(monkeypatch):
    runner = make_runner()
    event = make_feishu_event("飞书问题 7013527412 缺少什么")

    monkeypatch.setattr(gateway_run.GatewayRunner, "_is_user_authorized", lambda self, source: True)
    monkeypatch.setattr(
        gateway_run,
        "_submit_g1q3_rca_status_handoff",
        lambda **kwargs: {"success": True, "task": {"task_id": "pnc_evidence_701"}},
    )

    response = await gateway_run.GatewayRunner._handle_message(runner, event)

    assert response == ""


@pytest.mark.asyncio
async def test_g1q3_status_handoff_submission_failure_is_reported(monkeypatch):
    runner = make_runner()
    event = make_feishu_event("帮我看一下 case G1Q3-042 现在归因做到哪一步了")

    monkeypatch.setattr(gateway_run.GatewayRunner, "_is_user_authorized", lambda self, source: True)
    monkeypatch.setattr(
        gateway_run,
        "_submit_g1q3_rca_status_handoff",
        lambda **kwargs: {"success": False, "error": "permission denied"},
    )

    response = await gateway_run.GatewayRunner._handle_message(runner, event)

    assert response is not None
    assert "G1Q3 RCA 状态查询暂时没有接单成功" in response
    assert "permission denied" in response


def test_issue_intake_handoff_blocks_field_validation_after_successful_preread(monkeypatch):
    calls = []

    def fake_vm_task_submit(**kwargs):
        calls.append(kwargs)
        return {"success": True, "task": {"task_id": "pnc_intake_001"}}

    monkeypatch.setattr("tools.vm_task_tool.vm_task_submit", fake_vm_task_submit)
    monkeypatch.setattr(
        gateway_run,
        "_fetch_g1q3_issue_context_result",
        lambda **kwargs: pnc_issue_context.G1Q3IssueReadResult(
            context_text="- title: G1Q3_0938 AWB\n- frame_id: 938",
            status="fields_extracted",
        ),
    )

    result = gateway_run._submit_g1q3_rca_status_handoff(
        template_id="rca_issue_intake",
        case_id="",
        work_item_id="7015689036",
        requester="ou_test_user",
        source_group_id=G1Q3_GROUP_ID,
        message_id="om_test_message",
        request_text="@胡子豪的小助手 分析这个问题 https://project.feishu.cn/t03o4q/issue/detail/7015689036",
    )

    assert result["success"] is True
    payload = result["execution_request"]
    assert payload["evidence"]["source_quality"] == "partial"
    assert payload["data"]["pdcl_download_cmd"] == ""
    assert payload["data"]["is_pdcl_format"] is False
    assert payload["evidence"]["blockers"][0]["kind"] == "issue_field_missing_pdcl_download_cmd"


@pytest.mark.asyncio
async def test_g1q3_intake_ack_surfaces_meegle_unauthenticated_notice(monkeypatch, tmp_path):
    runner = make_runner(receipt_dir=tmp_path)
    event = make_feishu_event("@胡子豪的小助手 分析这个问题 https://project.feishu.cn/t03o4q/issue/detail/7008267126")

    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    monkeypatch.setattr(gateway_run.GatewayRunner, "_is_user_authorized", lambda self, source: True)
    monkeypatch.setattr(
        gateway_run,
        "_submit_g1q3_rca_status_handoff",
        lambda **kwargs: {
            "success": True,
            "task": {"task_id": "pnc_intake_002"},
            "issue_preread": {
                "status": "read_failed",
                "source_quality": "unavailable",
                "blocker": {
                    "kind": "host_meegle_preread_unauthenticated",
                    "message": "Meegle 未登录或授权已过期",
                    "retryable": True,
                },
            },
        },
    )

    response = await gateway_run.GatewayRunner._handle_message(runner, event)

    assert response == ""


@pytest.mark.asyncio
async def test_g1q3_intake_ack_surfaces_field_missing_pdcl_notice(monkeypatch, tmp_path):
    runner = make_runner(receipt_dir=tmp_path)
    event = make_feishu_event("@胡子豪的小助手 分析这个问题 https://project.feishu.cn/t03o4q/issue/detail/7008267126")

    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    monkeypatch.setattr(gateway_run.GatewayRunner, "_is_user_authorized", lambda self, source: True)
    monkeypatch.setattr(
        gateway_run,
        "_submit_g1q3_rca_status_handoff",
        lambda **kwargs: {
            "success": True,
            "task": {"task_id": "pnc_intake_003"},
            "issue_preread": {
                "status": "fields_extracted",
                "source_quality": "partial",
                "blocker": {
                    "kind": "issue_field_missing_pdcl_download_cmd",
                    "message": "问题数据地址_PDCL 缺失",
                    "retryable": True,
                },
            },
        },
    )

    response = await gateway_run.GatewayRunner._handle_message(runner, event)

    assert response == ""


@pytest.mark.asyncio
async def test_g1q3_intake_ack_has_no_notice_when_preread_clean(monkeypatch, tmp_path):
    runner = make_runner(receipt_dir=tmp_path)
    event = make_feishu_event("@胡子豪的小助手 分析这个问题 https://project.feishu.cn/t03o4q/issue/detail/7008267126")

    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    monkeypatch.setattr(gateway_run.GatewayRunner, "_is_user_authorized", lambda self, source: True)
    monkeypatch.setattr(
        gateway_run,
        "_submit_g1q3_rca_status_handoff",
        lambda **kwargs: {
            "success": True,
            "task": {"task_id": "pnc_intake_004"},
            "issue_preread": {"status": "fields_extracted", "source_quality": "partial", "blocker": None},
        },
    )

    response = await gateway_run.GatewayRunner._handle_message(runner, event)

    assert response == ""


def test_issue_intake_submit_result_carries_issue_preread_blocker(monkeypatch):
    monkeypatch.setattr(
        "tools.vm_task_tool.vm_task_submit",
        lambda **kwargs: {"success": True, "task": {"task_id": "pnc_intake_005"}},
    )
    monkeypatch.setattr(
        gateway_run,
        "_fetch_g1q3_issue_context_result",
        lambda **kwargs: pnc_issue_context.G1Q3IssueReadResult(
            status="read_failed",
            blocker={
                "kind": "host_meegle_preread_unauthenticated",
                "message": "Meegle 未登录或授权已过期",
                "retryable": True,
            },
        ),
    )

    result = gateway_run._submit_g1q3_rca_status_handoff(
        template_id="rca_issue_intake",
        case_id="",
        work_item_id="7008267126",
        requester="ou_test_user",
        source_group_id=G1Q3_GROUP_ID,
        message_id="om_test_message",
        request_text="@胡子豪的小助手 分析这个问题 https://project.feishu.cn/t03o4q/issue/detail/7008267126",
    )

    assert result["success"] is True
    preread = result["issue_preread"]
    assert preread["status"] == "read_failed"
    assert preread["source_quality"] == "unavailable"
    assert preread["blocker"]["kind"] == "host_meegle_preread_unauthenticated"


@pytest.mark.asyncio
async def test_g1q3_slow_handoff_does_not_block_event_loop(monkeypatch, tmp_path):
    import asyncio
    import time

    runner = make_runner(receipt_dir=tmp_path)
    event = make_feishu_event("帮我看一下 case G1Q3-042 现在归因做到哪一步了")

    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    monkeypatch.setattr(gateway_run.GatewayRunner, "_is_user_authorized", lambda self, source: True)

    def slow_submit(**kwargs):
        time.sleep(0.4)
        return {"success": True, "task": {"task_id": "pnc_slow_001"}}

    monkeypatch.setattr(gateway_run, "_submit_g1q3_rca_status_handoff", slow_submit)

    handle_task = asyncio.ensure_future(gateway_run.GatewayRunner._handle_message(runner, event))
    await asyncio.sleep(0)
    started = time.monotonic()
    await asyncio.sleep(0.05)
    loop_latency = time.monotonic() - started

    response = await handle_task

    assert response == ""
    # If the submit ran synchronously on the loop, this sleep could not
    # resume until the 0.4s handoff finished.
    assert loop_latency < 0.3, f"event loop blocked for {loop_latency:.2f}s during handoff"


@pytest.mark.asyncio
async def test_meegle_auth_alert_sends_to_home_channel_and_rate_limits():
    sent = []

    class FakeAdapter:
        async def send(self, chat_id, msg, metadata=None):
            sent.append((chat_id, msg))

    runner = make_runner()
    runner.adapters = {type("FakePlatform", (), {"value": "feishu"})(): FakeAdapter()}
    runner.config = SimpleNamespace(
        get_home_channel=lambda platform: SimpleNamespace(chat_id="oc_ops_home", thread_id=None)
    )

    preread = {
        "status": "read_failed",
        "source": "",
        "blocker": {"kind": "host_meegle_preread_unauthenticated", "message": "expired"},
    }
    await gateway_run.GatewayRunner._notify_g1q3_meegle_auth_alert(runner, preread)
    await gateway_run.GatewayRunner._notify_g1q3_meegle_auth_alert(runner, preread)

    assert len(sent) == 1
    chat_id, msg = sent[0]
    assert chat_id == "oc_ops_home"
    assert "Meegle 登录已过期/未授权" in msg
    assert "meegle auth login" in msg


@pytest.mark.asyncio
async def test_meegle_auth_alert_mentions_auto_degrade_success():
    sent = []

    class FakeAdapter:
        async def send(self, chat_id, msg, metadata=None):
            sent.append(msg)

    runner = make_runner()
    runner.adapters = {type("FakePlatform", (), {"value": "feishu"})(): FakeAdapter()}
    runner.config = SimpleNamespace(
        get_home_channel=lambda platform: SimpleNamespace(chat_id="oc_ops_home", thread_id=None)
    )

    await gateway_run.GatewayRunner._notify_g1q3_meegle_auth_alert(
        runner,
        {"status": "fields_extracted", "source": "mcp_auto_degraded", "blocker": None},
    )

    assert len(sent) == 1
    assert "自动降级到 MCP 兜底" in sent[0]


@pytest.mark.asyncio
async def test_g1q3_duplicate_issue_trigger_within_window_is_deduped(monkeypatch, tmp_path):
    runner = make_runner(receipt_dir=tmp_path)
    submit_calls = []

    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    monkeypatch.setattr(gateway_run.GatewayRunner, "_is_user_authorized", lambda self, source: True)

    def fake_submit(**kwargs):
        submit_calls.append(kwargs)
        return {"success": True, "task": {"task_id": "pnc_dedup_001"}}

    monkeypatch.setattr(gateway_run, "_submit_g1q3_rca_status_handoff", fake_submit)

    text = "@胡子豪的小助手 分析这个问题 https://project.feishu.cn/t03o4q/issue/detail/7008267126"
    first = await gateway_run.GatewayRunner._handle_message(runner, make_feishu_event(text))
    second = await gateway_run.GatewayRunner._handle_message(runner, make_feishu_event(text))

    assert first == ""
    assert len(submit_calls) == 1
    assert not second.startswith("已接单")
    assert "不再重复建任务" in second
    assert "pnc_dedup_001" in second


@pytest.mark.asyncio
async def test_g1q3_dedup_releases_reservation_on_submit_failure(monkeypatch, tmp_path):
    runner = make_runner(receipt_dir=tmp_path)
    submit_calls = []

    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    monkeypatch.setattr(gateway_run.GatewayRunner, "_is_user_authorized", lambda self, source: True)

    def flaky_submit(**kwargs):
        submit_calls.append(kwargs)
        if len(submit_calls) == 1:
            return {"success": False, "error": "vm bridge down"}
        return {"success": True, "task": {"task_id": "pnc_retry_002"}}

    monkeypatch.setattr(gateway_run, "_submit_g1q3_rca_status_handoff", flaky_submit)

    text = "@胡子豪的小助手 分析这个问题 https://project.feishu.cn/t03o4q/issue/detail/7008267126"
    first = await gateway_run.GatewayRunner._handle_message(runner, make_feishu_event(text))
    second = await gateway_run.GatewayRunner._handle_message(runner, make_feishu_event(text))

    assert "没有接单成功" in first
    assert second == ""
    assert len(submit_calls) == 2


@pytest.mark.asyncio
async def test_g1q3_different_issues_are_not_deduped(monkeypatch, tmp_path):
    runner = make_runner(receipt_dir=tmp_path)
    submit_calls = []

    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    monkeypatch.setattr(gateway_run.GatewayRunner, "_is_user_authorized", lambda self, source: True)

    def fake_submit(**kwargs):
        submit_calls.append(kwargs)
        return {"success": True, "task": {"task_id": f"pnc_multi_{len(submit_calls):03d}"}}

    monkeypatch.setattr(gateway_run, "_submit_g1q3_rca_status_handoff", fake_submit)

    first = await gateway_run.GatewayRunner._handle_message(
        runner, make_feishu_event("@胡子豪的小助手 分析这个问题 https://project.feishu.cn/t03o4q/issue/detail/7008267126"))
    second = await gateway_run.GatewayRunner._handle_message(
        runner, make_feishu_event("@胡子豪的小助手 分析这个问题 https://project.feishu.cn/t03o4q/issue/detail/7015689036"))

    assert first == ""
    assert second == ""
    assert len(submit_calls) == 2


def test_issue_intake_grants_download_within_quota(monkeypatch, tmp_path):
    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    monkeypatch.setenv("HERMES_G1Q3_AUTO_DOWNLOAD_DAILY_QUOTA", "1")
    submitted_goals = []

    def fake_vm_task_submit(**kwargs):
        submitted_goals.append(kwargs["goal"])
        return {"success": True, "task": {"task_id": f"pnc_quota_{len(submitted_goals):03d}"}}

    monkeypatch.setattr("tools.vm_task_tool.vm_task_submit", fake_vm_task_submit)
    monkeypatch.setattr(
        gateway_run,
        "_fetch_g1q3_issue_context_result",
        lambda **kwargs: pnc_issue_context.G1Q3IssueReadResult(
            context_text=(
                "## Feishu issue 已解析字段（主控侧读取）\n"
                "- title: G1Q3_6351 ACC case\n"
                "- work_item_id: 7008267126\n"
                "- frame_id: 318153\n"
                "- 数据地址: mdi download event -u demo -s ./\n"
            ),
            status="fields_extracted",
            source="meegle",
        ),
    )

    def submit(message_id):
        return gateway_run._submit_g1q3_rca_status_handoff(
            template_id="rca_issue_intake",
            case_id="",
            work_item_id="7008267126",
            requester="ou_real_owner",
            source_group_id=G1Q3_GROUP_ID,
            message_id=message_id,
            request_text="分析这个问题 https://project.feishu.cn/t03o4q/issue/detail/7008267126",
        )

    first = submit("om_msg_1")
    second = submit("om_msg_2")

    assert first["download_grant"]["granted"] is True
    assert first["execution_request"]["execution_policy"]["allow_download"] is True
    assert first["execution_request"]["execution_policy"]["mode"] == "materialize_when_allowed"
    assert "run_rca_auto_pipeline.py" in submitted_goals[0]
    assert "auto_download: granted" in submitted_goals[0]

    assert second["download_grant"]["granted"] is False
    assert second["download_grant"]["reason"] == "daily_quota_exhausted"
    assert second["execution_request"]["execution_policy"]["allow_download"] is False
    assert second["execution_request"]["execution_policy"]["mode"] == "readonly_status_first"
    assert "run_rca_execution_request.py" in submitted_goals[1]
    assert "auto_download: not granted" in submitted_goals[1]



def test_issue_intake_synthetic_trigger_does_not_spend_download_quota(monkeypatch, tmp_path):
    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    monkeypatch.setenv("HERMES_G1Q3_AUTO_DOWNLOAD_DAILY_QUOTA", "1")
    monkeypatch.setattr(
        "tools.vm_task_tool.vm_task_submit",
        lambda **kwargs: {"success": True, "task": {"task_id": "pnc_synthetic_quota"}},
    )
    monkeypatch.setattr(
        gateway_run,
        "_fetch_g1q3_issue_context_result",
        lambda **kwargs: pnc_issue_context.G1Q3IssueReadResult(
            context_text=(
                "## Feishu issue 已解析字段（主控侧读取）\n"
                "- title: G1Q3 synthetic quota guard\n"
                "- work_item_id: 7008267126\n"
                "- frame_id: 318153\n"
                "- 数据地址: mdi download event -u demo -s ./\n"
            ),
            status="fields_extracted",
            source="meegle",
        ),
    )

    result = gateway_run._submit_g1q3_rca_status_handoff(
        template_id="rca_issue_intake",
        case_id="",
        work_item_id="7008267126",
        requester="ou_test_user",
        source_group_id=G1Q3_GROUP_ID,
        message_id="om_test_message",
        request_text="分析这个问题 https://project.feishu.cn/t03o4q/issue/detail/7008267126",
    )

    assert result["download_grant"]["granted"] is False
    assert result["download_grant"]["reason"] == "synthetic_trigger_no_quota_spend"
    assert result["execution_request"]["execution_policy"]["allow_download"] is False
    assert list((tmp_path / "pnc_agent" / "quota").glob("*.json")) == []

def test_issue_intake_does_not_spend_quota_on_blocked_intake(monkeypatch, tmp_path):
    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    monkeypatch.setenv("HERMES_G1Q3_AUTO_DOWNLOAD_DAILY_QUOTA", "5")

    monkeypatch.setattr(
        "tools.vm_task_tool.vm_task_submit",
        lambda **kwargs: {"success": True, "task": {"task_id": "pnc_quota_blocked"}},
    )
    monkeypatch.setattr(
        gateway_run,
        "_fetch_g1q3_issue_context_result",
        lambda **kwargs: pnc_issue_context.G1Q3IssueReadResult(
            status="read_failed",
            blocker={"kind": "host_meegle_preread_unauthenticated", "message": "expired", "retryable": True},
        ),
    )

    result = gateway_run._submit_g1q3_rca_status_handoff(
        template_id="rca_issue_intake",
        case_id="",
        work_item_id="7008267126",
        requester="ou_test_user",
        source_group_id=G1Q3_GROUP_ID,
        message_id="om_msg_blocked",
        request_text="分析这个问题 https://project.feishu.cn/t03o4q/issue/detail/7008267126",
    )

    assert result["download_grant"]["granted"] is False
    assert result["download_grant"]["reason"] == "not_eligible"
    assert result["execution_request"]["execution_policy"]["allow_download"] is False
    assert list((tmp_path / "pnc_agent" / "quota").glob("*.json")) == []


def test_issue_intake_field_blocker_triggers_field_gap_plan(monkeypatch, tmp_path):
    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    monkeypatch.delenv("HERMES_G1Q3_FIELD_GAP_COMMENT", raising=False)
    monkeypatch.setattr(
        "tools.vm_task_tool.vm_task_submit",
        lambda **kwargs: {"success": True, "task": {"task_id": "pnc_gap_001"}},
    )
    monkeypatch.setattr(
        gateway_run,
        "_fetch_g1q3_issue_context_result",
        lambda **kwargs: pnc_issue_context.G1Q3IssueReadResult(
            context_text=(
                "## Feishu issue 已解析字段（主控侧读取）\n"
                "- title: ACC-旁车道切入自车道，ACC未减速\n"
                "- work_item_id: 7015828844\n"
                "- 当前负责人: 邵祖钦\n"
            ),
            status="fields_extracted",
            source="meegle",
        ),
    )

    result = gateway_run._submit_g1q3_rca_status_handoff(
        template_id="rca_issue_intake",
        case_id="",
        work_item_id="7015828844",
        requester="ou_test_user",
        source_group_id=G1Q3_GROUP_ID,
        message_id="om_gap_msg",
        request_text="分析这个问题 https://project.feishu.cn/t03o4q/issue/detail/7015828844",
    )

    gap = result["field_gap_comment"]
    assert gap["action"] == "planned"
    assert gap["blocker_kind"] == "issue_field_missing_pdcl_download_cmd"
    assert "问题数据地址_PDCL" in gap["comment_content"]


@pytest.mark.asyncio
async def test_g1q3_status_handoff_writes_initial_task_card_sidecar(monkeypatch, tmp_path):
    token = set_hermes_home_override(tmp_path)
    try:
        runner = make_runner(receipt_dir=tmp_path)
        event = make_feishu_event("帮我看一下 case G1Q3-042 现在归因做到哪一步了")

        monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
        monkeypatch.setattr(gateway_run.GatewayRunner, "_is_user_authorized", lambda self, source: True)
        monkeypatch.setattr(
            gateway_run,
            "_submit_g1q3_rca_status_handoff",
            lambda **kwargs: {"success": True, "task": {"task_id": "pnc_status_card_001"}},
        )

        response = await gateway_run.GatewayRunner._handle_message(runner, event)
        body = json.loads((tmp_path / "task-state" / "pnc_status_card_001.json").read_text(encoding="utf-8"))
    finally:
        reset_hermes_home_override(token)

    assert response == ""
    assert body["task_card"]["user_state"] == "host-created"
    assert body["task_card"]["task_id"] == "pnc_status_card_001"
    assert body["task_card"].get("one_card_policy") is True
    assert body["task_card"]["chat_id"] == G1Q3_GROUP_ID
    assert body["task_card"]["thread_id"] == "topic:om_request_topic"
    assert body["task_card"]["status_line"]
    assert "last_sent_hash" not in body["task_card"]


def test_issue_intake_auth_blocker_notice_is_not_pdcl_gap_copy(monkeypatch, tmp_path):
    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    monkeypatch.setattr(
        "tools.vm_task_tool.vm_task_submit",
        lambda **kwargs: {"success": True, "task": {"task_id": "pnc_auth_blocked"}},
    )
    monkeypatch.setattr(
        gateway_run,
        "_fetch_g1q3_issue_context_result",
        lambda **kwargs: pnc_issue_context.G1Q3IssueReadResult(
            status="read_failed",
            blocker={"kind": "host_meegle_preread_unauthenticated", "message": "Meegle 授权过期", "retryable": True},
        ),
    )

    result = gateway_run._submit_g1q3_rca_status_handoff(
        template_id="rca_issue_intake",
        case_id="",
        work_item_id="7008267126",
        requester="ou_test_user",
        source_group_id=G1Q3_GROUP_ID,
        message_id="om_auth_blocked",
        request_text="分析这个问题 https://project.feishu.cn/t03o4q/issue/detail/7008267126",
    )

    blocker = result["issue_preread"]["blocker"]
    assert "授权过期" in blocker["message"]
    rendered = "Meegle 授权过期，主控暂时读不到飞书 issue 字段，正在续期/已通知管理员；请稍候再让我重试。本提示不要求你补数据地址。"
    assert "授权过期" in rendered
    assert "补 问题数据地址_PDCL" not in rendered
    assert result["field_gap_comment"] if "field_gap_comment" in result else True
