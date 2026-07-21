from types import SimpleNamespace
from datetime import datetime
import hashlib
import json
from pathlib import Path
import tempfile
import threading

import pytest

from gateway import run as gateway_run
from gateway.admission.controller import AdmissionController
from gateway.admission.worker import QueueWorker
from gateway.config import Platform
from gateway.pnc_rca_kafka_contract import WorkflowEventPolicy, WorkflowTransition
from gateway.pnc_rca_policy_config import ManualRcaAdmissionRuntimeConfig
from gateway.platforms.base import MessageEvent, MessageType, SendResult
from gateway.platforms.feishu import FeishuAdapter
from gateway.pnc_group_binding import (
    G1Q3_RCA_GROUP_BINDING_ID,
    PncGroupBindingDecision,
    pnc_group_binding_receipt_filename,
    write_pnc_group_binding_receipt,
)
from gateway.session import SessionSource
from hermes_constants import reset_hermes_home_override, set_hermes_home_override


G1Q3_GROUP_ID = "oc_6cfc782212009ff4cd815349909dd423"


def _gateway_runtime_identity() -> dict:
    return {
        "service_label": "ai.hermes.gateway",
        "pid": 41000,
        "process_create_time": 1_783_650_000.0,
        "boot_time": 1_783_000_000.0,
        "executable": "/candidate/.venv/bin/python",
        "script": "/candidate/gateway/run.py",
        "cwd": "/candidate",
        "script_sha256": "a" * 64,
        "runtime_files_sha256": "b" * 64,
        "public_config_sha256": "c" * 64,
        "loaded_runtime_sha256": "d" * 64,
    }


def _manual_admission_runtime_config(
    *,
    policy_version: str = "issue-created-v2",
    outbox_high_watermark: int = 7,
) -> ManualRcaAdmissionRuntimeConfig:
    return ManualRcaAdmissionRuntimeConfig(
        active_policy=WorkflowEventPolicy(
            topic="feishu-project-workflow-event",
            policy_version=policy_version,
            project_keys=frozenset({"project-key"}),
            project_simple_names=frozenset({"g1q3"}),
            work_item_type_keys=frozenset({"problem-type"}),
            status_change_types=frozenset({"Reached"}),
            transitions=(
                WorkflowTransition(
                    state_key="new-problem-state",
                    pre_status=1,
                    cur_status=2,
                ),
            ),
        ),
        outbox_high_watermark=outbox_high_watermark,
    )


def make_runner(receipt_dir=None):
    runner = object.__new__(gateway_run.GatewayRunner)
    runner.adapters = {}
    runner.config = SimpleNamespace()
    runner.session_store = SimpleNamespace()
    runner.pairing_store = SimpleNamespace()
    runner._rca_gateway_runtime_identity = _gateway_runtime_identity()
    runner._rca_manual_admission_runtime_config = (
        _manual_admission_runtime_config()
    )
    if receipt_dir is None:
        temporary_receipts = tempfile.TemporaryDirectory(
            prefix="pnc-group-binding-test-",
            dir=Path(tempfile.gettempdir()).resolve(strict=True),
        )
        runner._pnc_group_binding_test_receipts = temporary_receipts
        receipt_dir = Path(temporary_receipts.name)
    runner._pnc_group_binding_receipt_dir = receipt_dir
    return runner


def make_feishu_event(
    text: str,
    *,
    chat_id: str = G1Q3_GROUP_ID,
    self_mentioned: bool | None = None,
    self_mention_command_directed: bool | None = None,
    is_bot: bool = False,
    reply_to_text: str | None = None,
) -> MessageEvent:
    mentioned = (
        text.lstrip().startswith("@")
        if self_mentioned is None
        else self_mentioned
    )
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
            thread_id="topic:om_test_message",
            is_bot=is_bot,
        ),
        message_id="om_test_message",
        reply_to_message_id="om_parent" if reply_to_text else None,
        reply_to_text=reply_to_text,
        metadata={
            "feishu": {
                "self_mentioned": mentioned,
                "self_mention_command_directed": (
                    mentioned
                    if self_mention_command_directed is None
                    else self_mention_command_directed
                ),
                "mention_required": False,
            }
        },
    )


@pytest.mark.asyncio
async def test_g1q3_rca_case_status_is_read_only_without_agent_or_task(monkeypatch, tmp_path):
    token = set_hermes_home_override(tmp_path)
    runner = make_runner()
    event = make_feishu_event("帮我看一下 case G1Q3-042 现在归因做到哪一步了")

    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    monkeypatch.setattr(gateway_run.GatewayRunner, "_is_user_authorized", lambda self, source: True)
    async def _should_not_run_agent(*args, **kwargs):  # pragma: no cover - failure path
        raise AssertionError("G1Q3 handoff must not enter generic agent execution")
    monkeypatch.setattr(gateway_run.GatewayRunner, "_handle_message_with_agent", _should_not_run_agent, raising=False)
    monkeypatch.setattr(
        gateway_run,
        "_submit_g1q3_rca_status_handoff",
        lambda **_kwargs: pytest.fail("read-only status must not submit"),
    )

    try:
        response = await gateway_run.GatewayRunner._handle_message(runner, event)
    finally:
        reset_hermes_home_override(token)

    assert "不能安全映射到唯一 Kafka 问题单" in response
    assert "不会手工创建、重跑任务" in response
    assert event.metadata["pnc_group_binding"]["decision"] == "accepted"
    assert "pnc_group_handoff" not in event.metadata
    assert not (tmp_path / "task-state").exists()


@pytest.mark.asyncio
async def test_g1q3_cross_business_line_request_returns_policy_rejection(monkeypatch):
    runner = make_runner()
    event = make_feishu_event("帮我看下这次评测门禁为什么没过")

    monkeypatch.setattr(gateway_run.GatewayRunner, "_is_user_authorized", lambda self, source: True)
    async def _should_not_run_agent(*args, **kwargs):  # pragma: no cover - failure path
        raise AssertionError("G1Q3 rejection must not enter agent execution")
    monkeypatch.setattr(gateway_run.GatewayRunner, "_handle_message_with_agent", _should_not_run_agent, raising=False)

    response = await gateway_run.GatewayRunner._handle_message(runner, event)

    assert response is not None
    assert "只处理 G1Q3 RCA" in response
    assert event.metadata["pnc_group_binding"]["decision"] == "reject"
    assert event.metadata["pnc_group_binding"]["reason"] == "cross_business_line"


@pytest.mark.asyncio
async def test_g1q3_policy_evaluator_exception_sets_retryable_transport_marker(
    monkeypatch,
):
    runner = make_runner()
    event = make_feishu_event("@PNC-Agent 分析这个问题")
    monkeypatch.setattr(
        "gateway.pnc_group_binding.evaluate_pnc_group_request",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("policy unavailable")),
    )
    monkeypatch.setattr(
        gateway_run.GatewayRunner,
        "_is_user_authorized",
        lambda self, source: True,
    )

    response = await gateway_run.GatewayRunner._handle_message(runner, event)

    assert "路由策略暂时不可用" in response
    assert event.metadata["pnc_group_binding_error"] == {
        "schema_version": "pnc_group_binding_error_v1",
        "code": "policy_evaluation_failed",
        "retryable": True,
    }
    assert "pnc_group_binding" not in event.metadata


@pytest.mark.asyncio
async def test_explicit_issue_action_uses_manual_control_store_and_acks_current_topic(
    monkeypatch,
):
    runner = make_runner()
    issue_url = "https://project.feishu.cn/g1q3/issue/detail/7013527412"
    event = make_feishu_event(f"@PNC-Agent 分析这个问题 {issue_url}")
    calls = []
    monkeypatch.setenv("HERMES_RCA_MANUAL_INTAKE_ENABLED", "true")
    monkeypatch.setenv("HERMES_RCA_MANUAL_CHAT_IDS", G1Q3_GROUP_ID)
    monkeypatch.setattr(
        gateway_run.GatewayRunner, "_is_user_authorized", lambda self, source: True
    )
    monkeypatch.setattr(
        gateway_run,
        "_admit_g1q3_manual_trigger",
        lambda **kwargs: calls.append(kwargs)
        or {
            "outcome": "created",
            "submission_key": "g1q3-rca-s1-" + "a" * 64,
            "generation": 1,
        },
    )

    response = await gateway_run.GatewayRunner._handle_message(runner, event)

    assert "已创建" in response
    assert "成功时为 HTML 报告，失败时为终态说明" in response
    assert calls == [
        {
            "issue_url": issue_url,
            "mode": "run_or_join",
            "chat_id": G1Q3_GROUP_ID,
            "thread_id": "topic:om_test_message",
            "message_id": "om_test_message",
            "requester_id": "ou_test_user",
            "submit_enabled": True,
            "operator_authorized": True,
            "operator_rate_limit": 3,
            "operator_rate_window_seconds": 600,
            "allowed_chat_ids": (G1Q3_GROUP_ID,),
            "admission_runtime_config": (
                runner._rca_manual_admission_runtime_config
            ),
        }
    ]
    assert event.metadata["pnc_manual_rca_admission"]["outcome"] == "created"
    authorization = event.metadata["pnc_manual_authorization"]
    assert authorization["schema_version"] == "pnc_rca_manual_authorization_v2"
    assert authorization["manual_operator_rate_limit"] == 3
    assert authorization["manual_operator_rate_window_seconds"] == 600
    assert authorization["manual_intake_enabled"] is True
    assert authorization["manual_chat_allowlist_valid"] is True
    assert authorization["chat_allowed"] is True
    assert authorization["mention_verified"] is True
    assert authorization["debug_requested"] is False
    assert authorization["requester_allowed"] is True
    assert authorization["authorized"] is True
    assert len(authorization["debug_user_allowlist_sha256"]) == 64
    assert authorization["manual_chat_allowlist_sha256"] == hashlib.sha256(
        json.dumps([G1Q3_GROUP_ID], separators=(",", ":")).encode("utf-8")
    ).hexdigest()


@pytest.mark.asyncio
async def test_admission_enabled_feishu_queue_reaches_manual_control_store_from_reply(
    monkeypatch,
    tmp_path,
):
    runner = make_runner(receipt_dir=tmp_path / "receipts")
    issue_url = "https://project.feishu.cn/g1q3/issue/detail/7013527412"
    event = make_feishu_event(
        "分析这个问题",
        self_mentioned=True,
        self_mention_command_directed=True,
        reply_to_text=f"replied Feishu issue card\n{issue_url}",
    )
    event.metadata["feishu"].update(
        {
            "message_id": event.message_id,
            "root_id": "om_test_message",
            "parent_id": "om_parent",
            "thread_id": event.source.thread_id,
            "sender_id": event.source.user_id,
            "sender_type": "user",
            "is_bot_sender": False,
            "raw_container_id": event.source.chat_id,
            "ingress_source": "event_callback",
            "link_urls": None,
        }
    )
    calls = []
    monkeypatch.setenv("HERMES_RCA_MANUAL_INTAKE_ENABLED", "true")
    monkeypatch.setenv("HERMES_RCA_MANUAL_CHAT_IDS", G1Q3_GROUP_ID)
    monkeypatch.setattr(
        gateway_run.GatewayRunner, "_is_user_authorized", lambda self, source: True
    )
    monkeypatch.setattr(
        gateway_run,
        "_admit_g1q3_manual_trigger",
        lambda **kwargs: calls.append(kwargs)
        or {
            "outcome": "created",
            "submission_key": "g1q3-rca-s1-" + "a" * 64,
            "generation": 1,
        },
    )

    controller = AdmissionController(
        db_path=tmp_path / "queue.db",
        audit_dir=tmp_path / "audit",
    )
    adapter = object.__new__(FeishuAdapter)
    adapter.platform = Platform.FEISHU
    adapter._admission_enabled = True
    adapter._admission_controller = controller
    adapter._chat_locks = {}
    adapter._seen_message_ids = {}
    adapter._seen_message_order = []
    adapter._processing_message_ids = {}
    adapter._dedup_cache_size = 100
    adapter._dedup_lock = threading.Lock()
    adapter._dedup_state_path = tmp_path / "dedup.json"
    adapter._reactions_enabled = lambda: False
    sent = []

    async def gateway_handler(reconstructed):
        return await gateway_run.GatewayRunner._handle_message(runner, reconstructed)

    async def send(**kwargs):
        sent.append(kwargs)
        return SendResult(success=True, message_id="om_admission_ack")

    adapter._message_handler = gateway_handler
    adapter.send = send
    assert adapter._begin_message_processing(event.message_id) is True

    completed = await adapter._dispatch_inbound_event(event)
    assert completed is False
    [item] = controller.queue.list_pending()
    worker = QueueWorker(controller, adapter._process_queue_item)
    dequeued = controller.dequeue_next(item.lane, domain=item.domain)
    assert dequeued is item
    await worker._process_item(item)

    assert calls == [
        {
            "issue_url": issue_url,
            "mode": "run_or_join",
            "chat_id": G1Q3_GROUP_ID,
            "thread_id": "topic:om_test_message",
            "message_id": "om_test_message",
            "requester_id": "ou_test_user",
            "submit_enabled": True,
            "operator_authorized": True,
            "operator_rate_limit": 3,
            "operator_rate_window_seconds": 600,
            "allowed_chat_ids": (G1Q3_GROUP_ID,),
            "admission_runtime_config": (
                runner._rca_manual_admission_runtime_config
            ),
        }
    ]
    assert item.status == "completed"
    assert item.result["durable_admission"] is True
    assert item.result["durable_feishu_completion"] is True
    assert adapter._message_processing_completed(event.message_id) is True
    assert sent[-1]["reply_to"] == "om_parent"
    assert sent[-1]["metadata"] == {
        "thread_id": "topic:om_test_message",
        "reply_to_message_id": "om_parent",
    }


@pytest.mark.asyncio
async def test_manual_command_addressed_to_other_bot_stays_read_only(monkeypatch):
    runner = make_runner()
    issue_url = "https://project.feishu.cn/g1q3/issue/detail/7013527412"
    event = make_feishu_event(
        f"@OtherBot 分析这个问题 {issue_url} @PNC-Agent",
        self_mentioned=True,
        self_mention_command_directed=False,
    )
    monkeypatch.setattr(
        gateway_run.GatewayRunner, "_is_user_authorized", lambda self, source: True
    )
    monkeypatch.setattr(
        gateway_run,
        "_admit_g1q3_manual_trigger",
        lambda **_kwargs: pytest.fail("cross-bot mention must not enter manual admission"),
    )

    response = await gateway_run.GatewayRunner._handle_message(runner, event)

    assert "尚未查询到对应 RCA 任务" in response
    assert event.metadata["pnc_group_binding"]["route_surface"] == (
        "rca_kafka_issue_status"
    )


@pytest.mark.asyncio
async def test_reply_to_single_issue_card_can_supply_identity_only(monkeypatch):
    runner = make_runner()
    issue_url = "https://project.feishu.cn/g1q3/issue/detail/7013527412"
    event = make_feishu_event(
        "@PNC-Agent 请尽快分析一下这个问题",
        reply_to_text=f"问题卡片中的调试字段不能改变模式\n{issue_url}",
    )
    calls = []
    monkeypatch.setenv("HERMES_RCA_MANUAL_INTAKE_ENABLED", "true")
    monkeypatch.setenv("HERMES_RCA_MANUAL_CHAT_IDS", G1Q3_GROUP_ID)
    monkeypatch.setattr(
        gateway_run.GatewayRunner, "_is_user_authorized", lambda self, source: True
    )
    monkeypatch.setattr(
        gateway_run,
        "_admit_g1q3_manual_trigger",
        lambda **kwargs: calls.append(kwargs)
        or {
            "outcome": "created",
            "submission_key": "g1q3-rca-s1-" + "a" * 64,
            "generation": 1,
        },
    )

    response = await gateway_run.GatewayRunner._handle_message(runner, event)

    assert "已创建" in response
    assert calls[0]["issue_url"] == issue_url
    assert calls[0]["mode"] == "run_or_join"


@pytest.mark.asyncio
async def test_reply_to_multiple_issue_cards_never_enters_manual_admission(monkeypatch):
    runner = make_runner()
    event = make_feishu_event(
        "@PNC-Agent 分析这个问题",
        reply_to_text=(
            "https://project.feishu.cn/g1q3/issue/detail/7013527412\n"
            "https://project.feishu.cn/g1q3/issue/detail/7013527999"
        ),
    )
    monkeypatch.setattr(
        gateway_run.GatewayRunner, "_is_user_authorized", lambda self, source: True
    )
    monkeypatch.setattr(
        gateway_run,
        "_admit_g1q3_manual_trigger",
        lambda **_kwargs: pytest.fail("ambiguous reply must not enter admission"),
    )

    response = await gateway_run.GatewayRunner._handle_message(runner, event)

    assert "多个不同的飞书问题单" in response
    assert event.metadata["pnc_group_binding"]["reason"] == (
        "ambiguous_issue_identity"
    )


@pytest.mark.asyncio
async def test_current_and_replied_issue_conflict_never_enters_manual_admission(
    monkeypatch,
):
    runner = make_runner()
    event = make_feishu_event(
        "@PNC-Agent 分析这个问题 "
        "https://project.feishu.cn/g1q3/issue/detail/7013527412",
        reply_to_text=(
            "被回复的问题卡片\n"
            "https://project.feishu.cn/g1q3/issue/detail/7013527999"
        ),
    )
    monkeypatch.setattr(
        gateway_run.GatewayRunner, "_is_user_authorized", lambda self, source: True
    )
    monkeypatch.setattr(
        gateway_run,
        "_admit_g1q3_manual_trigger",
        lambda **_kwargs: pytest.fail("cross-source ambiguity must not enter admission"),
    )

    response = await gateway_run.GatewayRunner._handle_message(runner, event)

    assert "多个不同的飞书问题单" in response
    assert event.metadata["pnc_group_binding"]["reason"] == (
        "ambiguous_issue_identity"
    )


@pytest.mark.asyncio
async def test_manual_gateway_safe_off_never_calls_control_store(monkeypatch):
    runner = make_runner()
    issue_url = "https://project.feishu.cn/g1q3/issue/detail/7013527412"
    event = make_feishu_event(f"@PNC-Agent 重跑这个问题 {issue_url}")
    monkeypatch.delenv("HERMES_RCA_MANUAL_INTAKE_ENABLED", raising=False)
    monkeypatch.delenv("HERMES_RCA_MANUAL_CHAT_IDS", raising=False)
    monkeypatch.setattr(gateway_run, "load_config", lambda: {})
    monkeypatch.setattr(
        gateway_run.GatewayRunner, "_is_user_authorized", lambda self, source: True
    )
    monkeypatch.setattr(
        gateway_run,
        "_admit_g1q3_manual_trigger",
        lambda **_kwargs: pytest.fail("safe-off manual route must not open the store"),
    )

    response = await gateway_run.GatewayRunner._handle_message(runner, event)

    assert "安全关闭状态" in response
    assert "未创建任务" in response
    assert event.metadata["pnc_manual_authorization"]["authorized"] is False


@pytest.mark.asyncio
async def test_manual_requires_real_self_mention_even_when_group_mention_gate_is_off(
    monkeypatch, tmp_path
):
    runner = make_runner(receipt_dir=tmp_path / "receipts")
    issue_url = "https://project.feishu.cn/g1q3/issue/detail/7013527412"
    event = make_feishu_event(
        f"分析这个问题 {issue_url}", self_mentioned=False
    )
    monkeypatch.setenv("HERMES_RCA_MANUAL_INTAKE_ENABLED", "true")
    monkeypatch.setenv("HERMES_RCA_MANUAL_CHAT_IDS", G1Q3_GROUP_ID)
    monkeypatch.setattr(
        gateway_run.GatewayRunner, "_is_user_authorized", lambda self, source: True
    )
    monkeypatch.setattr(
        gateway_run,
        "_resolve_g1q3_issue_identity",
        lambda _handoff: ("project-key", "problem-type", "7013527412"),
    )
    monkeypatch.setattr(
        gateway_run,
        "_find_g1q3_rca_task_by_issue_identity",
        lambda *_identity: None,
    )
    monkeypatch.setattr(
        gateway_run,
        "_admit_g1q3_manual_trigger",
        lambda **_kwargs: pytest.fail("unmentioned manual request must not admit"),
    )

    response = await gateway_run.GatewayRunner._handle_message(runner, event)

    assert "尚未查询到对应 RCA 任务" in response
    assert event.metadata["pnc_group_binding"]["route_surface"] == (
        "rca_kafka_issue_status"
    )
    assert "pnc_manual_authorization" not in event.metadata
    assert "pnc_manual_rca_admission" not in event.metadata


def test_debug_authorization_snapshot_is_allowlist_bound_and_privacy_light(monkeypatch):
    monkeypatch.setenv("HERMES_RCA_MANUAL_INTAKE_ENABLED", "true")
    monkeypatch.setenv("HERMES_RCA_MANUAL_CHAT_IDS", G1Q3_GROUP_ID)
    monkeypatch.setenv("HERMES_RCA_MANUAL_DEBUG_ENABLED", "true")
    monkeypatch.setenv(
        "HERMES_RCA_MANUAL_DEBUG_USER_IDS", "ou_second,ou_test_user"
    )

    snapshot = gateway_run._g1q3_manual_authorization_snapshot(
        mode="debug",
        chat_id=G1Q3_GROUP_ID,
        requester_id="ou_test_user",
        mention_verified=True,
    )

    assert snapshot["debug_requested"] is True
    assert snapshot["debug_enabled"] is True
    assert snapshot["requester_allowed"] is True
    assert snapshot["manual_chat_allowlist_valid"] is True
    assert snapshot["chat_allowed"] is True
    assert snapshot["authorized"] is True
    serialized = json.dumps(snapshot, sort_keys=True)
    assert "ou_test_user" not in serialized
    assert "ou_second" not in serialized
    assert G1Q3_GROUP_ID not in serialized


def test_operator_authorization_snapshot_binds_exact_rate_config(monkeypatch):
    monkeypatch.setenv("HERMES_RCA_MANUAL_OPERATOR_RATE_LIMIT", "7")
    monkeypatch.setenv("HERMES_RCA_MANUAL_OPERATOR_RATE_WINDOW_SECONDS", "900")

    snapshot = gateway_run._g1q3_manual_authorization_snapshot(
        mode="debug",
        chat_id=G1Q3_GROUP_ID,
        requester_id="ou_test_user",
        mention_verified=True,
    )

    assert snapshot["schema_version"] == "pnc_rca_manual_authorization_v2"
    assert snapshot["manual_operator_rate_limit"] == 7
    assert snapshot["manual_operator_rate_window_seconds"] == 900


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("HERMES_RCA_MANUAL_OPERATOR_RATE_LIMIT", "0"),
        ("HERMES_RCA_MANUAL_OPERATOR_RATE_WINDOW_SECONDS", "not-an-integer"),
    ],
)
def test_operator_authorization_snapshot_rejects_invalid_rate_config(
    monkeypatch, name, value
):
    monkeypatch.setenv(name, value)

    with pytest.raises(ValueError, match="manual_operator_rate_config_invalid"):
        gateway_run._g1q3_manual_authorization_snapshot(
            mode="debug",
            chat_id=G1Q3_GROUP_ID,
            requester_id="ou_test_user",
            mention_verified=True,
        )


def test_rerun_uses_same_operator_allowlist_as_debug(monkeypatch):
    monkeypatch.setenv("HERMES_RCA_MANUAL_INTAKE_ENABLED", "true")
    monkeypatch.setenv("HERMES_RCA_MANUAL_CHAT_IDS", G1Q3_GROUP_ID)
    monkeypatch.setenv("HERMES_RCA_MANUAL_OPERATOR_ENABLED", "true")
    monkeypatch.setenv(
        "HERMES_RCA_MANUAL_OPERATOR_USER_IDS", "ou_operator,ou_second"
    )

    allowed = gateway_run._g1q3_manual_authorization_snapshot(
        mode="rerun",
        chat_id=G1Q3_GROUP_ID,
        requester_id="ou_operator",
        mention_verified=True,
    )
    denied = gateway_run._g1q3_manual_authorization_snapshot(
        mode="rerun",
        chat_id=G1Q3_GROUP_ID,
        requester_id="ou_regular",
        mention_verified=True,
    )
    ordinary = gateway_run._g1q3_manual_authorization_snapshot(
        mode="run_or_join",
        chat_id=G1Q3_GROUP_ID,
        requester_id="ou_regular",
        mention_verified=True,
    )

    assert allowed["debug_requested"] is False
    assert allowed["debug_enabled"] is True
    assert allowed["requester_allowed"] is True
    assert allowed["authorized"] is True
    assert denied["requester_allowed"] is False
    assert denied["authorized"] is False
    assert ordinary["requester_allowed"] is True
    assert ordinary["authorized"] is True


@pytest.mark.asyncio
async def test_rerun_denied_for_regular_user_is_audited_without_admission(
    monkeypatch, tmp_path
):
    runner = make_runner(receipt_dir=tmp_path / "receipts")
    issue_url = "https://project.feishu.cn/g1q3/issue/detail/7013527412"
    event = make_feishu_event(f"@PNC-Agent 重跑这个问题 {issue_url}")
    monkeypatch.setenv("HERMES_RCA_MANUAL_INTAKE_ENABLED", "true")
    monkeypatch.setenv("HERMES_RCA_MANUAL_CHAT_IDS", G1Q3_GROUP_ID)
    monkeypatch.setenv("HERMES_RCA_MANUAL_OPERATOR_ENABLED", "true")
    monkeypatch.setenv("HERMES_RCA_MANUAL_OPERATOR_USER_IDS", "ou_operator")
    monkeypatch.setattr(
        gateway_run.GatewayRunner, "_is_user_authorized", lambda self, source: True
    )
    monkeypatch.setattr(
        gateway_run,
        "_admit_g1q3_manual_trigger",
        lambda **_kwargs: pytest.fail("regular users must not reach rerun admission"),
    )

    response = await gateway_run.GatewayRunner._handle_message(runner, event)

    assert "没有 RCA debug 重跑权限" in response
    [receipt_path] = (tmp_path / "receipts").glob("*.jsonl")
    [receipt] = [json.loads(line) for line in receipt_path.read_text().splitlines()]
    assert receipt["decision_snapshot"]["handoff_contract"]["mode"] == "rerun"
    assert receipt["manual_authorization"]["requester_allowed"] is False
    assert receipt["manual_authorization"]["authorized"] is False
    assert receipt["gateway_runtime_identity"] == _gateway_runtime_identity()
    assert receipt_path.stat().st_mode & 0o777 == 0o600
    assert receipt_path.parent.stat().st_mode & 0o777 == 0o700


@pytest.mark.asyncio
async def test_rerun_operator_reaches_control_store_with_operator_proof(monkeypatch):
    runner = make_runner()
    issue_url = "https://project.feishu.cn/g1q3/issue/detail/7013527412"
    event = make_feishu_event(f"@PNC-Agent 重跑这个问题 {issue_url}")
    calls = []
    monkeypatch.setenv("HERMES_RCA_MANUAL_INTAKE_ENABLED", "true")
    monkeypatch.setenv("HERMES_RCA_MANUAL_CHAT_IDS", G1Q3_GROUP_ID)
    monkeypatch.setenv("HERMES_RCA_MANUAL_OPERATOR_ENABLED", "true")
    monkeypatch.setenv("HERMES_RCA_MANUAL_OPERATOR_USER_IDS", "ou_test_user")
    monkeypatch.setattr(
        gateway_run.GatewayRunner, "_is_user_authorized", lambda self, source: True
    )
    monkeypatch.setattr(
        gateway_run,
        "_admit_g1q3_manual_trigger",
        lambda **kwargs: calls.append(kwargs)
        or {
            "outcome": "created",
            "submission_key": "g1q3-rca-s2-" + "b" * 64,
            "generation": 2,
        },
    )

    response = await gateway_run.GatewayRunner._handle_message(runner, event)

    assert "已创建" in response
    assert calls[0]["mode"] == "rerun"
    assert calls[0]["operator_authorized"] is True


@pytest.mark.parametrize("configured", ["", "oc_unknown"])
def test_manual_chat_allowlist_empty_or_unknown_fails_closed(
    monkeypatch, configured
):
    monkeypatch.setenv("HERMES_RCA_MANUAL_INTAKE_ENABLED", "true")
    monkeypatch.setenv("HERMES_RCA_MANUAL_CHAT_IDS", configured)

    snapshot = gateway_run._g1q3_manual_authorization_snapshot(
        mode="run_or_join",
        chat_id=G1Q3_GROUP_ID,
        requester_id="ou_test_user",
        mention_verified=True,
    )

    assert snapshot["manual_chat_allowlist_valid"] is False
    assert snapshot["chat_allowed"] is False
    assert snapshot["authorized"] is False
    assert len(snapshot["manual_chat_allowlist_sha256"]) == 64


@pytest.mark.asyncio
async def test_manual_single_group_gray_does_not_open_other_fixed_group(
    monkeypatch, tmp_path
):
    runner = make_runner(receipt_dir=tmp_path / "receipts")
    issue_url = "https://project.feishu.cn/g1q3/issue/detail/7013527412"
    event = make_feishu_event(
        f"@PNC-Agent 分析这个问题 {issue_url}",
        chat_id=gateway_run.PNC_ALL_BUSINESS_TEST_GROUP_ID,
    )
    monkeypatch.setenv("HERMES_RCA_MANUAL_INTAKE_ENABLED", "true")
    monkeypatch.setenv("HERMES_RCA_MANUAL_CHAT_IDS", G1Q3_GROUP_ID)
    monkeypatch.setattr(
        gateway_run.GatewayRunner, "_is_user_authorized", lambda self, source: True
    )
    monkeypatch.setattr(
        gateway_run,
        "_admit_g1q3_manual_trigger",
        lambda **_kwargs: pytest.fail("inactive gray group must not admit"),
    )

    response = await gateway_run.GatewayRunner._handle_message(runner, event)

    assert "未进入 RCA 人工入口灰度" in response
    assert event.metadata["pnc_manual_authorization"]["chat_allowed"] is False
    assert "pnc_manual_rca_admission" not in event.metadata


def test_manual_env_false_overrides_enabled_config(monkeypatch):
    monkeypatch.setenv("HERMES_RCA_MANUAL_INTAKE_ENABLED", "false")
    monkeypatch.setenv("HERMES_RCA_MANUAL_CHAT_IDS", G1Q3_GROUP_ID)
    monkeypatch.setattr(
        gateway_run,
        "load_config",
        lambda: {"business_lines": {"rca": {"manual_intake_enabled": True}}},
    )

    assert gateway_run._g1q3_manual_intake_enabled() is False


def test_manual_gateway_passes_exact_kafka_policy_and_high_watermark(
    monkeypatch, tmp_path
):
    control_path = tmp_path / "control.sqlite3"
    policy_env = {
        "HERMES_RCA_KAFKA_CONTROL_DB_PATH": str(control_path),
        "HERMES_RCA_KAFKA_TOPIC": "feishu-project-workflow-event",
        "HERMES_RCA_KAFKA_CREATION_RULE_VERSION": "issue-created-v2",
        "HERMES_RCA_KAFKA_PROJECT_KEYS": "project-key",
        "HERMES_RCA_KAFKA_PROJECT_SIMPLE_NAMES": "g1q3",
        "HERMES_RCA_KAFKA_WORK_ITEM_TYPE_KEYS": "problem-type",
        "HERMES_RCA_KAFKA_STATUS_CHANGE_TYPES": "Reached",
        "HERMES_RCA_KAFKA_STATE_TRANSITIONS_JSON": json.dumps(
            [
                {
                    "state_key": "new-problem-state",
                    "pre_status": 1,
                    "cur_status": 2,
                }
            ]
        ),
        "HERMES_RCA_KAFKA_OUTBOX_HIGH_WATERMARK": "7",
    }
    for name, value in policy_env.items():
        monkeypatch.setenv(name, value)

    from gateway.pnc_rca_policy_config import (
        manual_rca_admission_runtime_config_from_env,
    )
    from gateway.pnc_rca_runtime_identity import (
        GATEWAY_RCA_RUNTIME_RELATIVE_FILES,
    )

    admission_runtime_config = manual_rca_admission_runtime_config_from_env()
    public_config = gateway_run._g1q3_manual_runtime_public_config(
        admission_runtime_config
    )
    expected_policy = admission_runtime_config.active_policy.to_dict()
    expected_policy_sha256 = hashlib.sha256(
        json.dumps(
            expected_policy,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    assert public_config["schema_version"] == (
        "pnc_rca_gateway_manual_runtime_config_v3"
    )
    assert public_config["activation_required"] is False
    assert public_config["workflow_event_policy"] == expected_policy
    assert public_config["workflow_event_policy_sha256"] == expected_policy_sha256
    assert public_config["creation_rule_version"] == "issue-created-v2"
    assert public_config["kafka_outbox_high_watermark"] == 7
    assert "gateway/pnc_rca_policy_config.py" in GATEWAY_RCA_RUNTIME_RELATIVE_FILES
    assert {
        "hermes_cli/__init__.py",
        "gateway/__init__.py",
        "gateway/platforms/__init__.py",
        "gateway/record_only/runtime.py",
        "gateway/record_only/transport.py",
    }.issubset(GATEWAY_RCA_RUNTIME_RELATIVE_FILES)

    monkeypatch.setenv(
        "HERMES_RCA_KAFKA_CREATION_RULE_VERSION",
        "drifted-after-gateway-start",
    )
    monkeypatch.setenv("HERMES_RCA_KAFKA_OUTBOX_HIGH_WATERMARK", "999")

    from gateway.pnc_rca_control_store import RcaControlStore

    RcaControlStore(control_path)

    result = gateway_run._admit_g1q3_manual_trigger(
        issue_url="https://project.feishu.cn/g1q3/issue/detail/7013527412",
        mode="run_or_join",
        chat_id=G1Q3_GROUP_ID,
        thread_id="topic:om_policy_root",
        message_id="om_policy_source",
        requester_id="ou_test_user",
        submit_enabled=True,
        operator_authorized=True,
        operator_rate_limit=3,
        operator_rate_window_seconds=600,
        allowed_chat_ids=(G1Q3_GROUP_ID,),
        admission_runtime_config=admission_runtime_config,
    )

    store = RcaControlStore(control_path)
    [outbox] = store.list_rows("rca_outbox")
    [policy] = store.list_rows("rca_policy_snapshots")
    assert result["outcome"] == "created"
    assert outbox["creation_rule_version"] == "issue-created-v2"
    assert policy["policy_version"] == "issue-created-v2"


def test_manual_gateway_activation_switch_is_strict_and_public(monkeypatch):
    config = _manual_admission_runtime_config()

    monkeypatch.setenv("HERMES_RCA_ACTIVATION_REQUIRED", "true")
    assert gateway_run._g1q3_manual_runtime_public_config(config)[
        "activation_required"
    ] is True

    monkeypatch.setenv("HERMES_RCA_ACTIVATION_REQUIRED", "invalid")
    with pytest.raises(ValueError, match="HERMES_RCA_ACTIVATION_REQUIRED"):
        gateway_run._g1q3_manual_runtime_public_config(config)


@pytest.mark.parametrize("value", ["1", "0", "yes", "on", "off", ""])
def test_manual_gateway_activation_switch_rejects_boolean_aliases(
    monkeypatch, value
):
    monkeypatch.setenv("HERMES_RCA_ACTIVATION_REQUIRED", value)
    with pytest.raises(ValueError, match="exactly true or false"):
        gateway_run._g1q3_rca_activation_required()


@pytest.mark.asyncio
async def test_manual_receipt_write_failure_blocks_admission(monkeypatch, tmp_path):
    from gateway import pnc_group_binding

    runner = make_runner(receipt_dir=tmp_path / "receipts")
    issue_url = "https://project.feishu.cn/g1q3/issue/detail/7013527412"
    event = make_feishu_event(f"@PNC-Agent 分析这个问题 {issue_url}")
    monkeypatch.setenv("HERMES_RCA_MANUAL_INTAKE_ENABLED", "true")
    monkeypatch.setenv("HERMES_RCA_MANUAL_CHAT_IDS", G1Q3_GROUP_ID)
    monkeypatch.setattr(
        gateway_run.GatewayRunner, "_is_user_authorized", lambda self, source: True
    )
    monkeypatch.setattr(
        pnc_group_binding,
        "write_pnc_group_binding_receipt",
        lambda **_kwargs: (_ for _ in ()).throw(OSError("disk full")),
    )
    monkeypatch.setattr(
        gateway_run,
        "_admit_g1q3_manual_trigger",
        lambda **_kwargs: pytest.fail("receipt failure must block control-store admission"),
    )

    response = await gateway_run.GatewayRunner._handle_message(runner, event)

    assert "授权回执写入失败" in response
    assert "安全中止" in response
    assert "pnc_manual_rca_admission" not in event.metadata
    assert not (tmp_path / "control.sqlite3").exists()


def test_group_binding_receipt_is_immutable_per_source_event(tmp_path):
    decision = PncGroupBindingDecision(
        decision="accepted",
        group_binding_id=G1Q3_RCA_GROUP_BINDING_ID,
        business_line_ref="rca",
        project_space_ref="g1q3_rca",
        template_id="rca_issue_intake",
        route_surface="rca_manual_intake",
        risk_gate="manual_intake_control_store",
    )
    arguments = {
        "receipt_dir": tmp_path / "receipts",
        "decision": decision,
        "platform": "feishu",
        "chat_id": G1Q3_GROUP_ID,
        "user_id": "ou_test_user",
        "message_id": "om_immutable_event",
    }

    path = write_pnc_group_binding_receipt(**arguments)
    original = path.read_bytes()
    [record] = [json.loads(line) for line in original.splitlines()]
    receipt_date = datetime.fromisoformat(record["timestamp"]).date()

    assert path.name == pnc_group_binding_receipt_filename(
        receipt_date=receipt_date,
        platform="feishu",
        chat_id=G1Q3_GROUP_ID,
        user_id="ou_test_user",
        message_id="om_immutable_event",
    )
    assert path.stat().st_mode & 0o777 == 0o600
    assert path.parent.stat().st_mode & 0o777 == 0o700
    with pytest.raises(FileExistsError):
        write_pnc_group_binding_receipt(**arguments)
    assert path.read_bytes() == original


@pytest.mark.asyncio
async def test_duplicate_manual_source_event_fails_closed_before_second_admission(
    monkeypatch, tmp_path
):
    runner = make_runner(receipt_dir=tmp_path / "receipts")
    issue_url = "https://project.feishu.cn/g1q3/issue/detail/7013527412"
    calls = []
    monkeypatch.setenv("HERMES_RCA_MANUAL_INTAKE_ENABLED", "true")
    monkeypatch.setenv("HERMES_RCA_MANUAL_CHAT_IDS", G1Q3_GROUP_ID)
    monkeypatch.setattr(
        gateway_run.GatewayRunner, "_is_user_authorized", lambda self, source: True
    )
    monkeypatch.setattr(
        gateway_run,
        "_admit_g1q3_manual_trigger",
        lambda **kwargs: calls.append(kwargs)
        or {
            "outcome": "created",
            "submission_key": "g1q3-rca-s1-" + "a" * 64,
            "generation": 1,
        },
    )

    first = await gateway_run.GatewayRunner._handle_message(
        runner, make_feishu_event(f"@PNC-Agent 分析这个问题 {issue_url}")
    )
    replay = await gateway_run.GatewayRunner._handle_message(
        runner, make_feishu_event(f"@PNC-Agent 分析这个问题 {issue_url}")
    )

    assert "已创建" in first
    assert "授权回执写入失败" in replay
    assert "安全中止" in replay
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_manual_gateway_runtime_identity_failure_blocks_admission(
    monkeypatch, tmp_path
):
    runner = make_runner(receipt_dir=tmp_path / "receipts")
    runner._rca_gateway_runtime_identity = None
    issue_url = "https://project.feishu.cn/g1q3/issue/detail/7013527412"
    event = make_feishu_event(f"@PNC-Agent 分析这个问题 {issue_url}")
    monkeypatch.setenv("HERMES_RCA_MANUAL_INTAKE_ENABLED", "true")
    monkeypatch.setenv("HERMES_RCA_MANUAL_CHAT_IDS", G1Q3_GROUP_ID)
    monkeypatch.setattr(
        gateway_run.GatewayRunner, "_is_user_authorized", lambda self, source: True
    )
    monkeypatch.setattr(
        gateway_run,
        "_admit_g1q3_manual_trigger",
        lambda **_kwargs: pytest.fail(
            "missing gateway identity must block control-store admission"
        ),
    )

    response = await gateway_run.GatewayRunner._handle_message(runner, event)

    assert "运行身份不可用" in response
    assert "安全中止" in response
    assert not (tmp_path / "receipts").exists()
    assert "pnc_manual_rca_admission" not in event.metadata


@pytest.mark.asyncio
async def test_manual_gateway_policy_snapshot_failure_blocks_admission(
    monkeypatch, tmp_path
):
    runner = make_runner(receipt_dir=tmp_path / "receipts")
    runner._rca_manual_admission_runtime_config = None
    issue_url = "https://project.feishu.cn/g1q3/issue/detail/7013527412"
    event = make_feishu_event(f"@PNC-Agent 分析这个问题 {issue_url}")
    monkeypatch.setenv("HERMES_RCA_MANUAL_INTAKE_ENABLED", "true")
    monkeypatch.setenv("HERMES_RCA_MANUAL_CHAT_IDS", G1Q3_GROUP_ID)
    monkeypatch.setattr(
        gateway_run.GatewayRunner, "_is_user_authorized", lambda self, source: True
    )
    monkeypatch.setattr(
        gateway_run,
        "_admit_g1q3_manual_trigger",
        lambda **_kwargs: pytest.fail(
            "missing frozen policy must block control-store admission"
        ),
    )

    response = await gateway_run.GatewayRunner._handle_message(runner, event)

    assert "策略快照不可用" in response
    assert "安全中止" in response
    assert not (tmp_path / "receipts").exists()
    assert "pnc_manual_rca_admission" not in event.metadata


@pytest.mark.asyncio
async def test_manual_bot_sender_is_explicitly_rejected_and_audited(
    monkeypatch, tmp_path
):
    runner = make_runner(receipt_dir=tmp_path / "receipts")
    issue_url = "https://project.feishu.cn/g1q3/issue/detail/7013527412"
    event = make_feishu_event(
        f"@PNC-Agent 分析这个问题 {issue_url}",
        self_mentioned=True,
        is_bot=True,
    )
    monkeypatch.setattr(
        gateway_run.GatewayRunner, "_is_user_authorized", lambda self, source: True
    )
    monkeypatch.setattr(
        gateway_run,
        "_admit_g1q3_manual_trigger",
        lambda **_kwargs: pytest.fail("bot sender must never reach manual admission"),
    )

    response = await gateway_run.GatewayRunner._handle_message(runner, event)

    assert "只接受真实用户" in response
    assert event.metadata["pnc_group_binding"]["decision"] == "reject"
    assert event.metadata["pnc_group_binding"]["reason"] == "manual_bot_sender_rejected"
    [receipt_path] = (tmp_path / "receipts").glob("*.jsonl")
    [receipt] = [json.loads(line) for line in receipt_path.read_text().splitlines()]
    assert receipt["decision"] == "reject"
    assert receipt["reason"] == "manual_bot_sender_rejected"
    assert receipt["manual_authorization"] is None


@pytest.mark.asyncio
async def test_g1q3_read_only_status_writes_binding_receipt_only(monkeypatch, tmp_path):
    token = set_hermes_home_override(tmp_path)
    runner = make_runner(receipt_dir=tmp_path)
    event = make_feishu_event("帮我看一下 case G1Q3-042 现在归因做到哪一步了")

    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    monkeypatch.setattr(gateway_run.GatewayRunner, "_is_user_authorized", lambda self, source: True)
    monkeypatch.setattr(
        gateway_run,
        "_submit_g1q3_rca_status_handoff",
        lambda **_kwargs: pytest.fail("read-only status must not submit"),
    )

    try:
        response = await gateway_run.GatewayRunner._handle_message(runner, event)
    finally:
        reset_hermes_home_override(token)

    assert "不能安全映射到唯一 Kafka 问题单" in response
    receipt_files = list(tmp_path.glob("*.jsonl"))
    assert len(receipt_files) == 1
    records = [json.loads(line) for line in receipt_files[0].read_text().splitlines()]
    assert len(records) == 1
    record = records[0]
    assert record["group_binding_id"] == "gb_g1q3_rca_feishu_group"
    assert record["group_id"] == G1Q3_GROUP_ID
    assert record["decision"] == "accepted"
    assert record["template_id"] == "rca_case_status_check"
    assert record["business_line_ref"] == "rca"
    assert record["project_space_ref"] == "g1q3_rca"
    assert record["requester"] == "ou_test_user"
    assert record["message_id"] == "om_test_message"
    assert record["gray_delivery_phase"] == "g1q3_rca_business_delivery_gray"
    assert record["route_surface"] == "rca_kafka_read_only_status"
    assert record["risk_gate"] == "kafka_only_read_only"


@pytest.mark.asyncio
async def test_g1q3_duplicate_status_query_does_not_mutate_existing_card(monkeypatch, tmp_path):
    token = set_hermes_home_override(tmp_path)
    runner = make_runner(receipt_dir=tmp_path)
    runner._g1q3_recent_handoffs = {"dedicated:rca_case_status_check:042": ("pnc_status_001", 0.0, 600.0)}
    event = make_feishu_event("帮我看一下 case G1Q3-042 现在归因做到哪一步了")
    sidecar_dir = tmp_path / "task-state"
    sidecar_dir.mkdir(parents=True)
    (sidecar_dir / "pnc_status_001.json").write_text(json.dumps({
        "task_card": {
            "task_id": "pnc_status_001",
            "chat_id": G1Q3_GROUP_ID,
            "thread_id": "topic:om_test_message",
            "user_state": "host-created",
            "milestones": [{"ts": "2026-06-12T00:00:00+00:00", "label": "任务建好"}],
            "delivery": {"boundaries": []},
        }
    }), encoding="utf-8")

    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    monkeypatch.setattr(gateway_run.time, "monotonic", lambda: 60.0)
    monkeypatch.setattr(gateway_run.GatewayRunner, "_is_user_authorized", lambda self, source: True)
    monkeypatch.setattr(
        gateway_run,
        "_submit_g1q3_rca_status_handoff",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("duplicate must not submit a new task")),
    )

    try:
        response = await gateway_run.GatewayRunner._handle_message(runner, event)
        body = json.loads((sidecar_dir / "pnc_status_001.json").read_text(encoding="utf-8"))
    finally:
        reset_hermes_home_override(token)

    assert "不能安全映射到唯一 Kafka 问题单" in response
    assert "one_card_policy" not in body["task_card"]
    assert body["task_card"]["milestones"] == [
        {"ts": "2026-06-12T00:00:00+00:00", "label": "任务建好"}
    ]


@pytest.mark.asyncio
async def test_g1q3_test_group_status_is_read_only_without_dedupe_state(monkeypatch, tmp_path):
    token = set_hermes_home_override(tmp_path)
    runner = make_runner(receipt_dir=tmp_path)
    runner._g1q3_recent_handoffs = {"dedicated:rca_case_status_check:042": ("pnc_status_dedicated", 0.0, 600.0)}
    event = make_feishu_event(
        "帮我看一下 case G1Q3-042 现在归因做到哪一步了",
        chat_id="oc_16614f4ba25b8c88b69c0b8e9ebc2fb5",
    )
    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    monkeypatch.setattr(gateway_run.time, "monotonic", lambda: 60.0)
    monkeypatch.setattr(gateway_run.GatewayRunner, "_is_user_authorized", lambda self, source: True)

    monkeypatch.setattr(
        gateway_run,
        "_submit_g1q3_rca_status_handoff",
        lambda **_kwargs: pytest.fail("read-only status must not submit"),
    )

    try:
        response = await gateway_run.GatewayRunner._handle_message(runner, event)
    finally:
        reset_hermes_home_override(token)

    assert "不能安全映射到唯一 Kafka 问题单" in response
    assert runner._g1q3_recent_handoffs["dedicated:rca_case_status_check:042"][0] == "pnc_status_dedicated"
    assert "test:rca_case_status_check:042" not in runner._g1q3_recent_handoffs


@pytest.mark.asyncio
async def test_all_business_test_group_unknown_prompt_does_not_create_integration_tools_task(monkeypatch):
    runner = make_runner()
    event = make_feishu_event(
        "今天下午谁有空看下这个问题",
        chat_id="oc_16614f4ba25b8c88b69c0b8e9ebc2fb5",
    )
    agent_calls = []

    monkeypatch.setattr(gateway_run.GatewayRunner, "_is_user_authorized", lambda self, source: True)
    monkeypatch.setattr(
        gateway_run,
        "_submit_integration_tools_intake_handoff",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("unknown test-group prompt must not create integration-tools task")),
    )

    async def _fake_agent(self, event, source=None, quick_key=None, run_generation=None):
        agent_calls.append(event.text)
        return "generic-response"

    monkeypatch.setattr(gateway_run.GatewayRunner, "_handle_message_with_agent", _fake_agent, raising=False)

    response = await gateway_run.GatewayRunner._handle_message(runner, event)

    assert response == "generic-response"
    assert agent_calls == ["今天下午谁有空看下这个问题"]
    assert "integration_tools_handoff" not in event.metadata


@pytest.mark.asyncio
async def test_all_business_test_group_g1q3_prompt_does_not_create_integration_tools_task(monkeypatch, tmp_path):
    token = set_hermes_home_override(tmp_path)
    runner = make_runner(receipt_dir=tmp_path)
    event = make_feishu_event(
        "帮我看一下 case G1Q3-042 现在归因做到哪一步了",
        chat_id="oc_16614f4ba25b8c88b69c0b8e9ebc2fb5",
    )

    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    monkeypatch.setattr(gateway_run.GatewayRunner, "_is_user_authorized", lambda self, source: True)
    monkeypatch.setattr(
        gateway_run,
        "_submit_integration_tools_intake_handoff",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("G1Q3 test prompt must not create integration-tools task")),
    )
    monkeypatch.setattr(
        gateway_run,
        "_submit_g1q3_rca_status_handoff",
        lambda **_kwargs: pytest.fail("read-only status must not submit"),
    )

    try:
        response = await gateway_run.GatewayRunner._handle_message(runner, event)
    finally:
        reset_hermes_home_override(token)

    assert "不能安全映射到唯一 Kafka 问题单" in response
    assert event.metadata["pnc_group_binding"]["decision"] == "accepted"
    assert "pnc_group_handoff" not in event.metadata
    assert "integration_tools_handoff" not in event.metadata
