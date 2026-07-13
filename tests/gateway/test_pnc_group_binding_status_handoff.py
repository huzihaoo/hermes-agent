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


@pytest.mark.parametrize(
    "flag",
    [None, "true", "1", "invalid", "false", "0", "no", "off"],
)
def test_issue_intake_legacy_entry_fails_closed_before_side_effects(
    monkeypatch, flag
):
    if flag is None:
        monkeypatch.delenv("HERMES_RCA_LEGACY_AUTO_EXECUTION_DISABLED", raising=False)
    else:
        monkeypatch.setenv("HERMES_RCA_LEGACY_AUTO_EXECUTION_DISABLED", flag)
    monkeypatch.setattr(
        pnc_issue_context,
        "fetch_g1q3_issue_context_result",
        lambda **kwargs: pytest.fail("disabled legacy intake must not preread the issue"),
    )
    monkeypatch.setattr(
        "tools.vm_task_tool.vm_task_submit",
        lambda **kwargs: pytest.fail("disabled legacy intake must not create a VM task"),
    )

    result = gateway_run._submit_g1q3_rca_status_handoff(
        template_id="rca_issue_intake",
        case_id="",
        work_item_id="7008267126",
        requester="ou_test_user",
        source_group_id=G1Q3_GROUP_ID,
        message_id="om_test_message",
        request_text="分析这个问题",
    )

    assert result == {
        "success": False,
        "error_code": "g1q3_rca_legacy_chat_handoff_retired",
        "error": (
            "旧群聊旁路已退役，不会创建任务。生产入口仅包括 Kafka 自动受理，"
            "以及固定 RCA 群内真实 @、明确动作和完整问题链接的手工控制面。"
        ),
        "retryable": False,
        "intake": "durable_rca_control_plane",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("chat_id", "text"),
    [
        (G1Q3_GROUP_ID, "普通消息也不能在专用 RCA 群绕过策略"),
        (
            gateway_run.PNC_ALL_BUSINESS_TEST_GROUP_ID,
            "分析 G1Q3 RCA 飞书问题 7008267126",
        ),
    ],
)
async def test_g1q3_policy_failure_never_falls_through_to_generic_agent(
    monkeypatch, chat_id, text
):
    from gateway import pnc_group_binding

    runner = make_runner()
    event = make_feishu_event(text, chat_id=chat_id)
    monkeypatch.setattr(
        gateway_run.GatewayRunner,
        "_is_user_authorized",
        lambda self, source: True,
    )
    monkeypatch.setattr(
        pnc_group_binding,
        "evaluate_pnc_group_request",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("policy unavailable")),
    )

    response = await gateway_run.GatewayRunner._handle_message(runner, event)

    assert "路由策略暂时不可用" in response
    assert "不会进入通用 Agent 或创建 VM 任务" in response


@pytest.mark.asyncio
async def test_metadata_only_issue_card_policy_failure_is_fail_closed(
    monkeypatch,
):
    from gateway import pnc_group_binding

    runner = make_runner()
    event = make_feishu_event(
        "@胡子豪的小助手 分析这个问题 [【08】问题管理] ACC case",
        chat_id=gateway_run.PNC_ALL_BUSINESS_TEST_GROUP_ID,
    )
    event.metadata["feishu"] = {
        "link_urls": [
            "https://project.feishu.cn/t03o4q/issue/detail/7003183096?openScene=4"
        ]
    }
    monkeypatch.setattr(
        gateway_run.GatewayRunner,
        "_is_user_authorized",
        lambda self, source: True,
    )
    monkeypatch.setattr(
        pnc_group_binding,
        "evaluate_pnc_group_request",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("policy unavailable")),
    )

    response = await gateway_run.GatewayRunner._handle_message(runner, event)

    assert "路由策略暂时不可用" in response
    assert "不会进入通用 Agent 或创建 VM 任务" in response


def test_shared_group_policy_failure_guard_does_not_claim_unrelated_business():
    assert gateway_run._looks_like_g1q3_rca_request_for_business_routing(
        "https://project.feishu.cn/t03o4q/issue/detail/7003183096"
    ) is True
    assert gateway_run._looks_like_g1q3_rca_request_for_business_routing(
        "请跑一下 mcap 转换并给我结果"
    ) is False


@pytest.mark.asyncio
async def test_g1q3_case_status_is_read_only_and_never_submits(monkeypatch, tmp_path):
    runner = make_runner(receipt_dir=tmp_path)
    event = make_feishu_event("帮我看一下 case G1Q3-042 现在归因做到哪一步了")
    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    monkeypatch.setattr(gateway_run.GatewayRunner, "_is_user_authorized", lambda self, source: True)
    monkeypatch.setattr(
        gateway_run,
        "_submit_g1q3_rca_status_handoff",
        lambda **_kwargs: pytest.fail("status lookup must never submit"),
    )

    response = await gateway_run.GatewayRunner._handle_message(runner, event)

    assert "不能安全映射到唯一 Kafka 问题单" in response
    assert "不会为状态查询新建任务" in response

    records = [json.loads(line) for path in tmp_path.glob("*.jsonl") for line in path.read_text().splitlines()]
    assert records[-1]["decision"] == "accepted"
    assert records[-1]["decision_snapshot"]["handoff_contract"]["case_id"] == "042"
    assert records[-1]["decision_snapshot"]["handoff_contract"]["read_only"] is True
    assert all(record.get("event_type") != "handoff_submission" for record in records)


@pytest.mark.asyncio
async def test_g1q3_status_lookup_does_not_create_task_observability(monkeypatch, tmp_path):
    runner = make_runner(receipt_dir=tmp_path)
    event = make_feishu_event("帮我看一下 case G1Q3-042 现在归因做到哪一步了")

    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    monkeypatch.setattr(gateway_run.GatewayRunner, "_is_user_authorized", lambda self, source: True)
    monkeypatch.setattr(gateway_run, "_submit_g1q3_rca_status_handoff", lambda **_kwargs: pytest.fail("must not submit"))

    response = await gateway_run.GatewayRunner._handle_message(runner, event)

    assert "不会为状态查询新建任务" in response
    task = TaskStore(tmp_path / "analytics" / "tasks.db").get("pnc_status_001")
    assert task is None


def test_gateway_thread_metadata_for_feishu_carries_reply_anchor():
    runner = make_runner()
    source = make_feishu_event("帮我看一下 case G1Q3-042 现在归因做到哪一步了").source

    meta = gateway_run.GatewayRunner._thread_metadata_for_source(runner, source, "om_test_message")

    assert meta == {
        "thread_id": "topic:om_request_topic",
        "reply_to_message_id": "om_test_message",
    }


@pytest.mark.asyncio
async def test_status_handoff_submit_boundary_suppresses_all_side_effects(monkeypatch):
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

    assert result["success"] is False
    assert result["error_code"] == "g1q3_rca_status_query_read_only"
    assert result["read_only"] is True
    assert result["side_effects_suppressed"] is True
    assert calls == []


def test_feishu_topic_context_cannot_bypass_rca_service_boundary(monkeypatch):
    monkeypatch.setattr(
        "tools.vm_task_tool.subprocess.run",
        lambda *_args, **_kwargs: pytest.fail(
            "reserved RCA public submit must not create a task"
        ),
    )
    monkeypatch.setattr("tools.vm_task_tool._check_vm_task_permission", lambda *_args, **_kwargs: None)

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

    assert result["success"] is False
    assert result["error_code"] == "g1q3_rca_service_boundary_required"


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


@pytest.mark.asyncio
async def test_g1q3_evidence_followup_is_read_only_and_report_remains_dry_run(monkeypatch):
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

    assert "不能安全映射到唯一 Kafka 问题单" in evidence_response
    assert "不会为状态查询新建任务" in evidence_response
    assert "G1Q3 RCA dry-run" in report_response
    assert "template: rca_report_generate" in report_response
    assert submitted == []


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

    assert "尚未查询到对应 RCA 任务" in response
    assert "不会手工创建、重跑任务或进入通用 Agent" in response


@pytest.mark.asyncio
async def test_g1q3_status_lookup_never_reaches_submission_failure_path(monkeypatch):
    runner = make_runner()
    event = make_feishu_event("帮我看一下 case G1Q3-042 现在归因做到哪一步了")

    monkeypatch.setattr(gateway_run.GatewayRunner, "_is_user_authorized", lambda self, source: True)
    monkeypatch.setattr(
        gateway_run,
        "_submit_g1q3_rca_status_handoff",
        lambda **_kwargs: pytest.fail("read-only lookup must not submit"),
    )

    response = await gateway_run.GatewayRunner._handle_message(runner, event)

    assert "不能安全映射到唯一 Kafka 问题单" in response
    assert "不会为状态查询新建任务" in response


@pytest.mark.asyncio
async def test_g1q3_slow_control_store_lookup_does_not_block_event_loop(monkeypatch, tmp_path):
    import asyncio
    import time

    runner = make_runner(receipt_dir=tmp_path)
    event = make_feishu_event("飞书问题 7013527412 状态怎么样")

    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    monkeypatch.setattr(gateway_run.GatewayRunner, "_is_user_authorized", lambda self, source: True)

    def slow_lookup(_project_key, _work_item_type_key, _work_item_id):
        time.sleep(0.4)
        return None

    monkeypatch.setattr(
        gateway_run,
        "_resolve_g1q3_issue_identity",
        lambda _handoff: ("project-key", "problem-type", "7013527412"),
    )
    monkeypatch.setattr(
        gateway_run, "_find_g1q3_rca_task_by_issue_identity", slow_lookup
    )
    monkeypatch.setattr(gateway_run, "_submit_g1q3_rca_status_handoff", lambda **_kwargs: pytest.fail("must not submit"))

    handle_task = asyncio.ensure_future(gateway_run.GatewayRunner._handle_message(runner, event))
    await asyncio.sleep(0)
    started = time.monotonic()
    await asyncio.sleep(0.05)
    loop_latency = time.monotonic() - started

    response = await handle_task

    assert "尚未查询到对应 RCA 任务" in response
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
async def test_g1q3_status_lookup_does_not_write_task_card_sidecar(monkeypatch, tmp_path):
    token = set_hermes_home_override(tmp_path)
    try:
        runner = make_runner(receipt_dir=tmp_path)
        event = make_feishu_event("帮我看一下 case G1Q3-042 现在归因做到哪一步了")

        monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
        monkeypatch.setattr(gateway_run.GatewayRunner, "_is_user_authorized", lambda self, source: True)
        monkeypatch.setattr(
            gateway_run,
            "_submit_g1q3_rca_status_handoff",
            lambda **_kwargs: pytest.fail("must not submit"),
        )

        response = await gateway_run.GatewayRunner._handle_message(runner, event)
    finally:
        reset_hermes_home_override(token)

    assert "不会为状态查询新建任务" in response
    assert not (tmp_path / "task-state" / "pnc_status_card_001.json").exists()
